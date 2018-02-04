#!/usr/bin/env python
"""
Dump a lot of data out of ElasticSearch using the Python API and native
scrolling support.  Output either as native json from ElasticSearch or as
serialized TOKIO TimeSeries (TTS) HDF5 files.
"""

import sys
import gzip
import json
import time
import datetime
import argparse
import warnings
import mimetypes

import dateutil.parser # because of how ElasticSearch returns time data
import dateutil.tz
import h5py

import tokio
import tokio.connectors.collectd_es

SCHEMA_VERSION = "1"

DATASET_NAMES = [
    'datatargets/readrates',
    'datatargets/writerates',
]

DATE_FMT = "%Y-%m-%dT%H:%M:%S"

QUERY_DISK_DATA = {
    "query": {
        "constant_score": {
            "filter": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {}
                            }
                        },
                        {
                            "prefix": {
                                "hostname": "bb"
                            }
                        },
                        {
                            "prefix": {
                                "plugin_instance": "nvme"
                            }
                        },
                        {
                            "term": {
                                "collectd_type": "disk_octets"
                            }
                        },
                        {
                            "term": {
                                "plugin": "disk"
                            }
                        }
                    ]
                }
            }
        }
    }
}

### Only return the following _source fields
SOURCE_FIELDS = [
    '@timestamp',
    'hostname',
    'plugin',
    'collectd_type',
    'type_instance',
    'plugin_instance',
    'value',
    'longterm',
    'midterm',
    'shortterm',
    'majflt',
    'minflt',
    'if_octets',
    'if_packets',
    'if_errors',
    'rx',
    'tx',
    'read',
    'write',
    'io_time',
]

def process_page(pages, datasets):
    """
    Go through a list of pages and insert their data into a numpy matrix.  In
    the future this should be a flush function attached to the CollectdEs
    connector class.
    """

    data_volume = [0, 0]
    index_errors = 0
    for doc in pages:
        # basic validity checking
        if '_source' not in doc:
            warnings.warn("No _source in doc")
            print json.dumps(doc, indent=4)
            continue
        source = doc['_source']

        # check to see if this is from plugin:disk
        if source['plugin'] != 'disk' or 'read' not in source:
            continue

        # We jump through these hoops because most of TOKIO uses tz-unaware
        # local datetimes and we don't want to inject a bunch of dateutil
        # dependencies elsewhere
        # timezone-aware string
        # `-> tz-aware utc datetime
        #     `-> local datetime
        #         `-> tz-unaware local datetime
        timestamp = dateutil.parser.parse(source['@timestamp'])\
                                          .astimezone(dateutil.tz.tzlocal())\
                                          .replace(tzinfo=None)
        col_name = "%s:%s" % (source['hostname'], source['plugin_instance'])

        read_ok = datasets['datatargets/readrates'].insert_element(timestamp, col_name, source['read'])
        write_ok = datasets['datatargets/writerates'].insert_element(timestamp, col_name, source['write'])

        data_volume[0] += source['read']
        data_volume[1] += source['write']

        if not read_ok:
            index_errors += 1
        if not write_ok:
            index_errors += 1

    # Metadata
    datasets['datatargets/readrates'].dataset_metadata.update({'units': 'bytes/sec'})
    datasets['datatargets/writerates'].dataset_metadata.update({'units': 'bytes/sec'})
    datasets['datatargets/readrates'].group_metadata.update({'source': 'collectd_disk'})
    datasets['datatargets/writerates'].group_metadata.update({'source': 'collectd_disk'})

    if index_errors > 0:
        warnings.warn("Out-of-bounds indices (%d total) were detected" % index_errors)

    print "Added %d bytes of read, %d bytes of write" % (data_volume[0], data_volume[1])

def run_disk_query(host, port, index, t_start, t_end, timeout=None):
    """
    Connect to ElasticSearch and execute a query for all disk data
    """
    query = tokio.connectors.collectd_es.build_timeseries_query(QUERY_DISK_DATA, t_start, t_end)

    ### Print query
    if tokio.DEBUG:
        print json.dumps(query, indent=4)

    ### Try to connect
    if timeout is None:
        es_obj = tokio.connectors.collectd_es.CollectdEs(
            host=host,
            port=port,
            index=index)
    else:
        es_obj = tokio.connectors.collectd_es.CollectdEs(
            host=host,
            port=port,
            index=index,
            timeout=timeout)

    ### Run query
    time0 = time.time()
    es_obj.query_and_scroll(
        query=query,
        source_filter=SOURCE_FIELDS,
        filter_function=lambda x: x['hits']['hits'],
        flush_every=50000,
        flush_function=lambda x: x,
    )
    if tokio.DEBUG:
        print "ElasticSearch query took %s seconds" % (time.time() - time0)

    return es_obj

def pages_to_hdf5(pages, output_file, init_start, init_end, timestep, num_devices):
    """
    Take pages from ElasticSearch query and store them in output_file
    """
    datasets = {}
    hdf5_file = h5py.File(output_file)

    schema_version = hdf5_file['/'].attrs.get('schema', SCHEMA_VERSION)
    schema = tokio.connectors.hdf5.SCHEMA.get(schema_version)
    if schema is None:
        raise KeyError("Schema version %d is not known by connectors.hdf5" % SCHEMA_VERSION)

    # Initialize datasets
    for dataset_name in DATASET_NAMES:
        hdf5_dataset_name = schema.get(dataset_name)
        if hdf5_dataset_name is None:
            warnings.warn("Skipping %s (not in schema)" % dataset_name)
        else:
            datasets[dataset_name] = tokio.timeseries.TimeSeries(dataset_name=hdf5_dataset_name,
                                                                 start=init_start,
                                                                 end=init_end,
                                                                 timestep=timestep,
                                                                 num_columns=num_devices,
                                                                 hdf5_file=hdf5_file)

    # Process all pages retrieved
    progress = 0
    processing_time = 0.0
    for page in pages:
        progress += 1
        time0 = time.time()
        process_page(page, datasets)
        timef = time.time()
        processing_time += timef - time0
        if tokio.DEBUG:
            print "Processed page %d of %d in %s seconds" % (progress, len(pages), timef - time0)

    if tokio.DEBUG:
        print "Processed %d pages in %s seconds" % (progress, processing_time)

    # Write datasets out to HDF5 file
    time0 = time.time()
    for dataset in datasets.itervalues():
        dataset.commit_dataset(hdf5_file)

    if tokio.DEBUG:
        print "Committed data to disk in %s seconds" % (time.time() - time0)

def cache_collectd_cli():
    """
    CLI interface for cache_collectdes
    """
    warnings.simplefilter('always', UserWarning) # One warning per invalid file

    # Parse CLI options
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("query_start", type=str, help="start time of query in %s format" % DATE_FMT)
    parser.add_argument("query_end", type=str, help="end time of query in %s format" % DATE_FMT)
    parser.add_argument('--init-start', type=str, default=None, help='min timestamp if creating new output file, in %s format (default: same as start)' % DATE_FMT)
    parser.add_argument('--init-end', type=str, default=None, help='max timestamp if creating new output file, in %s format (default: same as end)' % DATE_FMT)
    parser.add_argument('--debug', action='store_true', help="produce debug messages")
    parser.add_argument('--num-bbnodes', type=int, default=288, help='number of expected BB nodes (default: 288)')
    parser.add_argument('--timestep', type=int, default=10, help='collection frequency, in seconds (default: 10)')
    parser.add_argument('--timeout', type=int, default=30, help='ElasticSearch timeout time (default: 30)')
    parser.add_argument('--json', action='store_true', help="output to json instead of HDF5")
    parser.add_argument('--input-json', type=str, default=None, help="use cached output from previous ES query")
    parser.add_argument("-o", "--output", type=str, default='output.hdf5', help="output file (default: output.hdf5)")
    parser.add_argument('-h', '--host', type=str, default="localhost", help="hostname of ElasticSearch endpoint (default: localhost)")
    parser.add_argument('-p', '--port', type=int, default=9200, help="port of ElasticSearch endpoint (default: 9200)")
    parser.add_argument('-i', '--index', type=str, default='cori-collectd-*', help='ElasticSearch index to query (default:cori-collectd-*)')
    args = parser.parse_args()

    if args.debug:
        tokio.DEBUG = True

    # Convert CLI options into datetime
    try:
        query_start = datetime.datetime.strptime(args.query_start, DATE_FMT)
        query_end = datetime.datetime.strptime(args.query_end, DATE_FMT)
        init_start = query_start
        init_end = query_end
        if args.init_start:
            init_start = datetime.datetime.strptime(args.init_start, DATE_FMT)
        if args.init_end:
            init_end = datetime.datetime.strptime(args.init_end, DATE_FMT)
    except ValueError:
        sys.stderr.write("Start and end times must be in format %s\n" % DATE_FMT)
        raise

    # Basic input bounds checking
    if query_start >= query_end:
        raise Exception('query_start >= query_end')
    elif init_start >= init_end:
        raise Exception('query_start >= query_end')
    elif args.timestep < 1:
        raise Exception('--timestep must be > 0')

    # Read input from a cached json file (generated previously via the --json
    # option) or by querying ElasticSearch?
    if args.input_json is None:
        es_obj = run_disk_query(args.host, args.port, args.index, query_start, query_end, timeout=args.timeout)
        pages = es_obj.scroll_pages
        if args.debug:
            print "Loaded results from %s:%s" % (args.host, args.port)
    else:
        _, encoding = mimetypes.guess_type(args.input_json)
        if encoding == 'gzip':
            input_file = gzip.open(args.input_json, 'r')
        else:
            input_file = open(args.input_json, 'r')
        pages = json.load(input_file)
        input_file.close()
        if args.debug:
            print "Loaded results from %s" % args.input_json

    # Output as json or the default HDF5 format?
    if args.json:
        _, encoding = mimetypes.guess_type(args.output)
        output_file = gzip.open(args.output, 'w') if encoding == 'gzip' else open(args.output, 'w')
        json.dump(pages, output_file)
        output_file.close()
    else:
        pages_to_hdf5(pages, args.output, init_start, init_end, args.timestep, args.num_bbnodes)

    if args.debug:
        print "Wrote output to %s" % args.output

if __name__ == '__main__':
    cache_collectd_cli()
