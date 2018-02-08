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
import multiprocessing

import dateutil.parser # because of how ElasticSearch returns time data
import dateutil.tz
import h5py

import tokio
import tokio.connectors.collectd_es

SCHEMA_VERSION = "1"

DATASET_NAMES = [
    'datatargets/readrates',
    'datatargets/writerates',
    'datatargets/readoprates',
    'datatargets/writeoprates',
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
                            "regexp": {
                                "collectd_type": "disk_(octets|ops)"
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

def process_page(page):
    """
    Go through a list of docs and insert their data into a numpy matrix.  In
    the future this should be a flush function attached to the CollectdEs
    connector class.
    """

    _time0 = time.time()
    inserts = []
    for doc in page:
        # basic validity checking
        if '_source' not in doc:
            warnings.warn("No _source in doc")
            print json.dumps(doc, indent=4)
            continue
        source = doc['_source']

        # check to see if this is from plugin:disk
        if source['plugin'] == 'disk':
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

            if source['collectd_type'] == 'disk_octets':
                read_rate = source.get('read')
                write_rate = source.get('write')
                if read_rate is not None and write_rate is not None:
                    inserts.append(('datatargets/readrates', timestamp, col_name, read_rate))
                    inserts.append(('datatargets/writerates', timestamp, col_name, write_rate))
            elif source['collectd_type'] == 'disk_ops':
                read_rate = source.get('read')
                write_rate = source.get('write')
                if read_rate is not None and write_rate is not None:
                    inserts.append(('datatargets/readoprates', timestamp, col_name, read_rate))
                    inserts.append(('datatargets/writeoprates', timestamp, col_name, write_rate))

    _timef = time.time()
    if tokio.DEBUG:
        print "Extracted %d inserts in %.4f seconds" % (len(inserts), _timef - _time0)
    return inserts

def update_datasets(inserts, datasets):
    """
    Given a list of tuples to insert into a dataframe, insert those data serially
    """
    units = {
        'datatargets/readrates': 'bytes/sec',
        'datatargets/writerates': 'bytes/sec',
        'datatargets/readoprates': 'ops/sec',
        'datatargets/writeoprates': 'ops/sec',
    }

    data_volume = {}
    errors = {}
    for key in datasets.keys():
        data_volume[key] = 0.0
        errors[key] = 0

    for insert in inserts:
        try:
            (dataset_name, timestamp, col_name, rate) = insert
        except ValueError:
            print insert
            raise
        if datasets[dataset_name].insert_element(timestamp, col_name, rate):
            data_volume[dataset_name] += rate
        else:
            errors[dataset_name] += 1

    # Update dataset metadata
    for key in datasets.keys():
        unit = units.get(key, "unknown")
        datasets[key].dataset_metadata.update({'version': SCHEMA_VERSION, 'units': unit})
        datasets[key].group_metadata.update({'source': 'collectd_disk'})

    index_errors = sum(errors.itervalues())
    if index_errors > 0:
        warnings.warn("Out-of-bounds indices (%d total) were detected" % index_errors)

    for key in data_volume.keys():
        data_volume[key] *= datasets[key].timestep
    update_str = "Added %(datatargets/readrates)d bytes/%(datatargets/readoprates)d ops of read," \
        + " %(datatargets/writerates)d bytes/%(datatargets/writeoprates)d ops of write"
    print update_str % data_volume

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

def pages_to_hdf5(pages, output_file, init_start, init_end, timestep, num_devices, threads=1):
    """
    Take pages from ElasticSearch query and store them in output_file
    """
    datasets = {}
    hdf5_file = h5py.File(output_file)

    schema_version = hdf5_file.attrs.get('version', SCHEMA_VERSION)
    schema = tokio.connectors.hdf5.SCHEMA.get(schema_version)
    if schema is None:
        raise KeyError("Schema version %d is not known by connectors.hdf5" % SCHEMA_VERSION)

    # Immediately write back the version so that tokio.timeseries can detect it
    # when it commits data
    hdf5_file.attrs['version'] = SCHEMA_VERSION

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
    # Process all pages retrieved (this is computationally expensive)
    _time0 = time.time()
    updates = []
    num_elements = 0
    if threads > 1:
#       updates = multiprocessing.Pool(16).map(process_page, pages)
        for update in multiprocessing.Pool(threads).imap_unordered(process_page, pages):
            updates.append(update)
    else:
        for page in pages:
            updates.append(process_page(page))
    _timef = time.time()
    _extract_time = _timef - _time0
    if tokio.DEBUG:
        print "Extracted %d elements from %d pages in %.4f seconds" \
            % (sum([len(x) for x in updates]), len(pages), _extract_time)

    # Take the processed list of data to insert and actually insert them
    _time0 = time.time()
    for update in updates:
        update_datasets(update, datasets)
    _timef = time.time()
    _update_time = _timef - _time0
    if tokio.DEBUG:
        print "Inserted %d elements from %d pages in %.4f seconds" % (len(updates), len(pages), _update_time)
        print "Processed %d pages in %.4f seconds" % (len(pages), _extract_time + _update_time)

    # Write datasets out to HDF5 file
    _time0 = time.time()
    for dataset in datasets.itervalues():
        dataset.commit_dataset(hdf5_file)

    if tokio.DEBUG:
        print "Committed data to disk in %.4f seconds" % (time.time() - _time0)

def cache_collectd_cli():
    """
    CLI interface for cache_collectdes
    """
    warnings.simplefilter('always', UserWarning) # One warning per invalid file

    # Parse CLI options
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("query_start", type=str,
                        help="start time of query in %s format" % DATE_FMT)
    parser.add_argument("query_end", type=str,
                        help="end time of query in %s format" % DATE_FMT)
    parser.add_argument('--init-start', type=str, default=None,
                        help='min timestamp if creating new output file, in %s format' % DATE_FMT
                        + ' (default: same as start)')
    parser.add_argument('--init-end', type=str, default=None,
                        help='max timestamp if creating new output file, in %s format' % DATE_FMT
                        + ' (default: same as end)')
    parser.add_argument('--debug', action='store_true',
                        help="produce debug messages")
    parser.add_argument('--num-bbnodes', type=int, default=288,
                        help='number of expected BB nodes (default: 288)')
    parser.add_argument('--timestep', type=int, default=10,
                        help='collection frequency, in seconds (default: 10)')
    parser.add_argument('--timeout', type=int, default=30,
                        help='ElasticSearch timeout time (default: 30)')
    parser.add_argument('--threads', type=int, default=1,
                        help='parallel threads for document extraction (default: 1)')
    parser.add_argument('--json', action='store_true',
                        help="output to json instead of HDF5")
    parser.add_argument('--input-json', type=str, default=None,
                        help="use cached output from previous ES query")
    parser.add_argument("-o", "--output", type=str, default='output.hdf5',
                        help="output file (default: output.hdf5)")
    parser.add_argument('-h', '--host', type=str, default="localhost",
                        help="hostname of ElasticSearch endpoint (default: localhost)")
    parser.add_argument('-p', '--port', type=int, default=9200,
                        help="port of ElasticSearch endpoint (default: 9200)")
    parser.add_argument('-i', '--index', type=str, default='cori-collectd-*',
                        help='ElasticSearch index to query (default:cori-collectd-*)')
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
        es_obj = run_disk_query(args.host,
                                args.port,
                                args.index,
                                query_start,
                                query_end,
                                timeout=args.timeout)
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
        pages_to_hdf5(pages, args.output, init_start, init_end, args.timestep, args.num_bbnodes, args.threads)

    if args.debug:
        print "Wrote output to %s" % args.output

if __name__ == '__main__':
    cache_collectd_cli()
