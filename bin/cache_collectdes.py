#!/usr/bin/env python
"""
Dump a lot of data out of ElasticSearch using the Python API and native
scrolling support
"""

import sys
import copy
import gzip
import json
import time
import datetime
import argparse
import warnings
import mimetypes

import dateutil.parser # because of how ElasticSearch returns time data
import h5py

import tokio
import tokio.connectors.collectd_es

### constants pertaining to HDF5 ###############################################

EPOCH = dateutil.parser.parse('1970-01-01T00:00:00.000Z')

### constants pertaining to collectd ElasticSearch #############################

DATE_FMT = "%Y-%m-%dT%H:%M:%S"

QUERY_DISK_DATA = {
    "query": {
        "bool": {
            "must": {
                "query_string": {
                    "query": "hostname:bb* AND plugin:disk AND plugin_instance:nvme* AND collectd_type:disk_octets",
                    ### doing a catch-all query over large swaths of time (e.g., one hour) seems to
                    ### lose a lot of information--not sure if there are degenerate document ids or
                    ### what.  TODO: debug this
                    "analyze_wildcard": True,
                },
            },
        },
    },
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

def build_timeseries_query(orig_query, start, end):
    """
    Given a query dict and a start/end datetime object, return a new query
    object with the correct time ranges bounded.
    """
    query = copy.deepcopy(orig_query)

    # Create the appropriate timeseries filter if it doesn't exist
    this_node = query
    for node_name in 'query', 'bool', 'filter', 'range', '@timestamp':
        if node_name not in this_node:
            this_node[node_name] = {}
        this_node = this_node[node_name]

    # Update the timeseries filter
    this_node['gte'] = long(time.mktime(start.timetuple()))
    this_node['lt'] = long(time.mktime(end.timetuple()))
    this_node['format'] = "epoch_second"

    return query

def update_mem_datasets(pages, read_dataset, write_dataset):
    """
    Go through a list of pages and insert their data into a numpy matrix.  In
    the future this should be a flush function attached to the CollectdEs
    connector class.
    """

    if read_dataset.timestep != write_dataset.timestep:
        raise Exception("read_dataset and write_dataset have different timesteps: %d != %d"
                        % (read_dataset.timestep, write_dataset.timestep))
    else:
        timestep = read_dataset.timestep

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

        # bounds checking
        timestamp = long((dateutil.parser.parse(source['@timestamp']) - EPOCH).total_seconds())
        t_index = (timestamp - read_dataset.time0) / timestep
        if t_index >= read_dataset.timestamps.shape[0] \
        or t_index >= write_dataset.timestamps.shape[0]:
            index_errors += 1
            continue

        # if this is a new hostname, create a new column for it
        col_name = "%s:%s" % (source['hostname'], source['plugin_instance'])
        s_index = read_dataset.column_map.get(col_name)
        if s_index is None:
            s_index = len(read_dataset.column_map)
            read_dataset.column_map[col_name] = s_index
            write_dataset.column_map[col_name] = s_index

        # actually copy the two data points into the datasets
        read_dataset.dataset[t_index, s_index] = source['read']
        write_dataset.dataset[t_index, s_index] = source['write']

        # accumulate the total number of bytes processed so far
        data_volume[0] += source['read'] * timestep
        data_volume[1] += source['write'] * timestep

    if index_errors > 0:
        warnings.warn("Out-of-bounds indices (%d total) were detected" % index_errors)

    print "Added %d bytes of read, %d bytes of write" % (data_volume[0], data_volume[1])

def run_disk_query(host, port, index, t_start, t_end):
    """
    Connect to ElasticSearch and execute a query for all disk data
    """
    query = build_timeseries_query(QUERY_DISK_DATA, t_start, t_end)

    ### Print query
    if tokio.DEBUG:
        print json.dumps(query, indent=4)

    ### Try to connect
    es_obj = tokio.connectors.collectd_es.CollectdEs(
        host=host,
        port=port,
        index=index)

    ### Run query
    es_obj.query_and_scroll(
        query=query,
        source_filter=SOURCE_FIELDS,
        filter_function=lambda x: x['hits']['hits'],
        flush_every=50000,
        flush_function=lambda x: x,
    )

    return es_obj

def pages_to_hdf5(t_start, t_end, pages, timestep, num_servers, output_file):
    """
    Take pages from ElasticSearch query and store them in output_file
    """

    output_hdf5 = h5py.File(output_file)

    # Determine the group of time series we will be populating
    group_name = '/bytes'
    if group_name in output_hdf5:
        target_group = output_hdf5[group_name]
    else:
        target_group = None

    # Determine the time series datasets in the group
    datasets = {}
    for dataset_name in 'readrates', 'writerates':
        target_dataset_name = group_name + '/' + dataset_name
        # Instantiate the TimeSeries object
        datasets[dataset_name] = tokio.timeseries.TimeSeries(start=t_start,
                                                             end=t_end,
                                                             timestep=timestep,
                                                             group=target_group)
        # Attach to or initialize the TimeSeries.dataset object
        if target_dataset_name in output_hdf5:
            datasets[dataset_name].attach_dataset(dataset=output_hdf5[target_dataset_name])
        else:
            datasets[dataset_name].init_dataset(dataset_name=target_dataset_name,
                                                columns=['']*num_servers)

    # Iterate over every cached page.
    progress = 0
    num_files = len(pages)
    for page in pages:
        progress += 1
        if tokio.DEBUG:
            print "Processing page %d of %d" % (progress, num_files)
        update_mem_datasets(page, datasets['readrates'], datasets['writerates'])

    # Build the list of column names from the retrieved data.  Note implicit assumption that
    # {read,write}_dataset have the same columns
    for column_name, index in datasets['readrates'].column_map.iteritems():
        # can't keep it in unicode; numpy doesn't support this
        for dataset_name in 'readrates', 'writerates':
            datasets[dataset_name].columns[index] = str(column_name)

    ### Write out the final HDF5 file after everything is loaded into memory.
    for dataset_name in 'readrates', 'writerates':
        datasets[dataset_name].commit_dataset(output_hdf5)

    output_hdf5.close()

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
        es_obj = run_disk_query(args.host, args.port, args.index, query_start, query_end)
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
        if encoding == 'gzip':
            output_file = gzip.open(args.output, 'w')
        else:
            output_file = open(args.output, 'w')
        json.dump(pages, output_file)
        output_file.close()
    else:
        pages_to_hdf5(init_start, init_end, pages, args.timestep, args.num_bbnodes, args.output)

    if args.debug:
        print "Wrote output to %s" % args.output

if __name__ == '__main__':
    cache_collectd_cli()
