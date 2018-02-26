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

DATASETS = {
    'datatargets/readrates': 'bytes/sec',
    'datatargets/writerates': 'bytes/sec',
    'datatargets/readoprates': 'ops/sec',
    'datatargets/writeoprates': 'ops/sec',
    'dataservers/memcached': 'bytes',
    'dataservers/membuffered': 'bytes',
    'dataservers/memfree': 'bytes',
    'dataservers/memused': 'bytes',
    'dataservers/memslab': 'bytes',
    'dataservers/memslab_unrecl': 'bytes',
    'dataservers/cpuload': '%',
    'dataservers/cpuuser': '%',
    'dataservers/cpusys': '%',
    'dataservers/_num_cpuload': 'cpu count',
    'dataservers/_num_cpuuser': 'cpu count',
    'dataservers/_num_cpusys': 'cpu count',
}

DATE_FMT = "%Y-%m-%dT%H:%M:%S"

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
                val1 = source.get('read')
                val2 = source.get('write')
                if val1 is not None and val2 is not None:
                    inserts.append(('datatargets/readrates', timestamp, col_name, val1))
                    inserts.append(('datatargets/writerates', timestamp, col_name, val2))
            elif source['collectd_type'] == 'disk_ops':
                val1 = source.get('read')
                val2 = source.get('write')
                if val1 is not None and val2 is not None:
                    inserts.append(('datatargets/readoprates', timestamp, col_name, val1))
                    inserts.append(('datatargets/writeoprates', timestamp, col_name, val2))
        elif source['plugin'] == 'cpu' and 'value' in source:
            timestamp = dateutil.parser.parse(source['@timestamp'])\
                                              .astimezone(dateutil.tz.tzlocal())\
                                              .replace(tzinfo=None)
            val1 = source.get('value')
            if source['type_instance'] == 'idle':
                # note that we store (100 - idle) as load
                inserts.append(('dataservers/cpuload', timestamp, source['hostname'],
                               100.0 - val1, lambda x, y: x + y))
                inserts.append(('dataservers/_num_cpuload', timestamp, source['hostname'],
                               1, lambda x, y: x + y))
            elif source['type_instance'] == 'user':
                inserts.append(('dataservers/cpuuser', timestamp, source['hostname'],
                               val1, lambda x, y: x + y))
                inserts.append(('dataservers/_num_cpuuser', timestamp, source['hostname'],
                               1, lambda x, y: x + y))
            elif source['type_instance'] == 'system':
                inserts.append(('dataservers/cpusys', timestamp, source['hostname'],
                               val1, lambda x, y: x + y))
                inserts.append(('dataservers/_num_cpusys', timestamp, source['hostname'],
                               1, lambda x, y: x + y))
        elif source['plugin'] == 'memory' and 'value' in source:
            timestamp = dateutil.parser.parse(source['@timestamp'])\
                                              .astimezone(dateutil.tz.tzlocal())\
                                              .replace(tzinfo=None)
            val1 = source.get('value')
            if source['type_instance'] == 'cached':
                inserts.append(('dataservers/memcached', timestamp, source['hostname'], val1))
            elif source['type_instance'] == 'buffered':
                inserts.append(('dataservers/membuffered', timestamp, source['hostname'], val1))
            elif source['type_instance'] == 'free':
                inserts.append(('dataservers/memfree', timestamp, source['hostname'], val1))
            elif source['type_instance'] == 'used':
                inserts.append(('dataservers/memused', timestamp, source['hostname'], val1))
            elif source['type_instance'] == 'slab_recl':
                inserts.append(('dataservers/memslab', timestamp, source['hostname'], val1))
            elif source['type_instance'] == 'slab_unrecl':
                inserts.append(('dataservers/memslab_unrecl', timestamp, source['hostname'], val1))

    _timef = time.time()
    if tokio.DEBUG:
        print "Extracted %d inserts in %.4f seconds" % (len(inserts), _timef - _time0)
        per_dataset = {}
        for insert in inserts:
            if insert[0] not in per_dataset:
                per_dataset[insert[0]] = 0
            per_dataset[insert[0]] += 1
        for dataset_name in sorted(per_dataset.keys()):
            print "  %6d entries for %s" % (per_dataset[dataset_name], dataset_name)
    return inserts

def update_datasets(inserts, datasets):
    """
    Given a list of tuples to insert into a dataframe, insert those data serially
    """
    data_volume = {}
    errors = {}
    for key in datasets.keys():
        data_volume[key] = 0.0
        errors[key] = 0

    for insert in inserts:
        try:
            if len(insert) == 4:
                (dataset_name, timestamp, col_name, rate) = insert
                reducer = None
            else:
                (dataset_name, timestamp, col_name, rate, reducer) = insert
        except ValueError:
            print insert
            raise
        if datasets[dataset_name].insert_element(timestamp, col_name, rate, reducer):
            data_volume[dataset_name] += rate
        else:
            errors[dataset_name] += 1

    # Update dataset metadata
    for key in datasets.keys():
        unit = DATASETS.get(key, "unknown")
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
    for dataset_name in DATASETS.keys():
        hdf5_dataset_name = schema.get(dataset_name)
        if hdf5_dataset_name is None:
            warnings.warn("Dataset %s is not in schema (passing through)" % dataset_name)
            hdf5_dataset_name = dataset_name
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

def main(argv=None):
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
    parser.add_argument('--input', type=str, default=None,
                        help="use cached ElasticSearch json as input")
    parser.add_argument("-o", "--output", type=str, default='output.hdf5',
                        help="output file (default: output.hdf5)")
    parser.add_argument('-h', '--host', type=str, default="localhost",
                        help="hostname of ElasticSearch endpoint (default: localhost)")
    parser.add_argument('-p', '--port', type=int, default=9200,
                        help="port of ElasticSearch endpoint (default: 9200)")
    parser.add_argument('-i', '--index', type=str, default='cori-collectd-*',
                        help='ElasticSearch index to query (default:cori-collectd-*)')
    args = parser.parse_args(argv)

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
        raise Exception('init_start >= init_end')
    elif args.timestep < 1:
        raise Exception('--timestep must be > 0')

    # Read input from a cached json file (generated previously via the --json
    # option) or by querying ElasticSearch?
    if args.input is None:
        ### Try to connect
        esdb = tokio.connectors.collectd_es.CollectdEs(
            host=args.host,
            port=args.port,
            index=args.index,
            timeout=args.timeout)

        for plugin_query in [tokio.connectors.collectd_es.QUERY_CPU_DATA,
                             tokio.connectors.collectd_es.QUERY_DISK_DATA,
                             tokio.connectors.collectd_es.QUERY_MEMORY_DATA]:
            esdb.query_timeseries(plugin_query,
                            query_start,
                            query_end,
                            timeout=args.timeout)
            pages = esdb.scroll_pages
            tokio.debug.debug_print("Loaded results from %s:%s" % (args.host, args.port))
            pages_to_hdf5(pages, args.output, init_start, init_end, args.timestep, args.num_bbnodes, args.threads)
    else:
        _, encoding = mimetypes.guess_type(args.input)
        if encoding == 'gzip':
            input_file = gzip.open(args.input, 'r')
        else:
            input_file = open(args.input, 'r')
        pages = json.load(input_file)
        input_file.close()
        tokio.debug.debug_print("Loaded results from %s" % args.input)
        pages_to_hdf5(pages, args.output, init_start, init_end, args.timestep, args.num_bbnodes, args.threads)

    print "Wrote output to %s" % args.output

if __name__ == '__main__':
    main()
