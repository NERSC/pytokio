#!/usr/bin/env python
"""
Dump a lot of data out of ElasticSearch using the Python API and native
scrolling support
"""

import os
import sys
import copy
import json
import time
import datetime
import argparse
import warnings

import dateutil.parser # because of how ElasticSearch returns time data
import h5py
import numpy

import tokio.connectors.hdf5
import tokio.connectors.collectd_es

### constants pertaining to HDF5 ###############################################

COLLECTD_TIMESTEP = 10 ### anticipated ten second polling interval

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

def commit_mem_datasets(hdf5_filename, rows, columns, datasets):
    """
    Convert numpy arrays, a list of timestamps, and a list of column headers
    into groups and datasets within an HDF5 file.
    """
    # Create/open the HDF5 file
    hdf5_file = h5py.File(hdf5_filename)

    # Create the read rate dataset
    dataset_name = None
    for dataset_name, dataset in datasets.iteritems():
        check_create_hdf5_dataset(hdf5_file=hdf5_file,
                                  name=dataset_name,
                                  shape=dataset.shape,
                                  chunks=True,
                                  compression='gzip',
                                  dtype='f8')

        ### If we're updating an existing.  Otherwise sort the columns
        if 'columns' in hdf5_file[dataset_name].attrs['columns']:
            new_columns = hdf5_file[dataset_name].attrs['columns']
        else:
            new_columns = sorted(columns)

        ### Actually rearrange column data
        for new_index, column in enumerate(new_columns):
            moved_column = dataset[:, new_index]
            moved_index = columns.index(column)
            dataset[:, new_index] = dataset[:, moved_index]
            dataset[:, moved_index] = moved_column

        ### TODO: should only update the slice of the hdf5 file that needs to be
        ###       changed; for example, maybe calculate the min/max row and
        ###       column touched, then use these as slice indices

        if 'columns' not in hdf5_file[dataset_name].attrs and 'timestamps' not in hdf5_file:
            raise Exception("Existing HDF5 has dataset %s but no timestamps" % dataset_name)

        ### TODO: resume work here.  right now we are trying to implement
        ### updating an existing HDF5 file, but the mem_dataset is not
        ### initialized from an hdf5 file if present so the process of merging a
        ### mem_dataset into an existing hdf5 dataset that isn't a complete
        ### superset is complicated.  perhaps just initialize the dataset as a mem
        ### _or_ take whatever is in the target output file and start with that?
        raise Exception('this code path is currently incomplete')

        # Populate the read rate dataset and set basic metadata
        hdf5_file[dataset_name][:, :] = dataset
        hdf5_file[dataset_name].attrs['columns'] = new_columns
        print "Committed %s of shape %s" % (dataset_name, dataset.shape)

    # Create the dataset that contains the timestamps which correspond to each
    # row in the read/write datasets
    if dataset_name is not None:
        dataset_name = os.path.join(os.path.dirname(dataset_name), 'timestamps')
        check_create_hdf5_dataset(hdf5_file=hdf5_file,
                                  name=dataset_name,
                                  shape=rows.shape,
                                  dtype='i8')

        hdf5_file[dataset_name][:] = rows
        print "Committed %s of shape %s" % (dataset_name, dataset.shape)
    else:
        warnings.warn("No data to commit; HDF5 is untouched")

def update_mem_datasets(pages, rows, column_map, read_dataset, write_dataset):
    """
    Go through a list of pages and insert their data into a numpy matrix.  In
    the future this should be a flush function attached to the CollectdEs
    connector class.
    """
    if len(rows) < 2:
        raise IndexError("Got %d unique timestamps; need at least 2 to calculate timestep" % len(rows))
    time0 = rows[0] # in seconds since 1/1/1970
    timestep = rows[1] - rows[0] # in seconds

    data_volume = [0, 0]
    index_errors = 0
    for doc in pages:
        # basic validity checking
        if '_source' not in doc:
            warnings.warn("No _source in doc")
            print json.dumps(doc, indent=4)
            print json.dumps(page, indent=4)
            continue
        source = doc['_source']
        # check to see if this is from plugin:disk
        if source['plugin'] != 'disk' or 'read' not in source:
            continue
        timestamp = long((dateutil.parser.parse(source['@timestamp']) - EPOCH).total_seconds())
        t_index = (timestamp - time0) / timestep
        s_index = column_map.get(source['hostname'])
        # if this is a new hostname, create a new column for it
        if s_index is None:
            s_index = len(column_map)
            column_map[source['hostname']] = s_index
        # bounds checking
        if t_index >= read_dataset.shape[0] \
        or t_index >= write_dataset.shape[0]:
            index_errors += 1
            continue
        read_dataset[t_index, s_index] = source['read']
        write_dataset[t_index, s_index] = source['write']

        data_volume[0] += source['read'] * timestep
        data_volume[1] += source['write'] * timestep
    if index_errors > 0:
        warnings.warn("Out-of-bounds indices (%d total) were detected" % index_errors)
    print "Added %d bytes of read, %d bytes of write" % (data_volume[0], data_volume[1])

def init_mem_dataset(start_time, end_time, timestep=10, num_columns=10000):
    """
    Initialize an numpy array based on a start and end time and return a numpy
    array and a row of timestamps.  Independent of HDF5 to minimize overhead
    of randomly accessing a file-backed structure.
    """

    date_range = (end_time - start_time).total_seconds()
    num_bins = int(date_range / timestep)
    data_dataset = numpy.full((num_bins, num_columns), -0.0)
    time_list = []
    timestamp = start_time
    while timestamp < end_time:
        time_list.append(long(time.mktime(timestamp.timetuple())))
        timestamp += datetime.timedelta(seconds=timestep)
    rows = numpy.array(time_list)

    return rows, data_dataset


def check_create_hdf5_dataset(hdf5_file, name, shape, dtype, **kwargs):
    """
    Create a dataset if it does not exist.  If it does exist and has the correct
    shape, do nothing.
    """
    if name not in hdf5_file:
        hdf5_file.create_dataset(name=name,
                                 shape=shape,
                                 dtype=dtype,
                                 **kwargs)
### commenting below to allow update-in-place of hdf5 files
#   else:
#       if hdf5_file[name].shape != shape:
#           raise Exception('Dataset %s of shape %s already exists with shape %s' %
#                           (name,
#                           shape,
#                           hdf5_file[name].shape))

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
    rows, read_dataset = init_mem_dataset(t_start,
                                          t_end,
                                          timestep=timestep,
                                          num_columns=num_servers)
    rows, write_dataset = init_mem_dataset(t_start,
                                           t_end,
                                           timestep=timestep,
                                           num_columns=num_servers)

    ### Iterate over every cached page.
    column_map = {}
    progress = 0
    num_files = len(pages)
    for page in pages:
        progress += 1
        if tokio.DEBUG:
            print "Processing page %d of %d" % (progress, num_files)
        update_mem_datasets(page, rows, column_map, read_dataset, write_dataset)

    ### Build the list of column names from the retrieved data
    columns = [None] * len(column_map)
    for column_name, index in column_map.iteritems():
        columns[index] = str(column_name) # can't keep it in unicode; numpy doesn't support this

    ### Write out the final HDF5 file after everything is loaded into memory.
    commit_mem_datasets(output_file,
                        rows,
                        columns,
                        datasets={
                            '/bytes/readrates': read_dataset,
                            '/bytes/writerates': write_dataset
                        })

def cache_collectd_cli():
    warnings.simplefilter('always', UserWarning) # One warning per invalid file

    # Parse CLI options
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("query_start", type=str, help="start time of query in %s format" % DATE_FMT)
    parser.add_argument("query_end", type=str, help="end time of query in %s format" % DATE_FMT)
    parser.add_argument('--init-start', type=str, help='min timestamp if creating new output file, in %s format (default: same as start)' % DATE_FMT)
    parser.add_argument('--init-end', type=str, help='max timestamp if creating new output file, in %s format (default: same as end)' % DATE_FMT)
    parser.add_argument('--debug', action='store_true', help="produce debug messages")
    parser.add_argument('--num-bbnodes', type=int, default=288, help='number of expected BB nodes (default: 288)')
    parser.add_argument('--timestep', type=int, default=10, help='collection frequency, in seconds (default: 10)')
    parser.add_argument("-o", "--output", type=str, default='output.hdf5', help="output file (default: output.hdf5)")
    parser.add_argument('-h', '--host', type=str, default="localhost", help="hostname of ElasticSearch endpoint (default: localhost)")
    parser.add_argument('-p', '--port', type=int, default=9200, help="port of ElasticSearch endpoint (default: 9200)")
    parser.add_argument('-i', '--index', type=str, default='cori-collectd-*', help='ElasticSearch index to query (default:cori-collectd-*)')
    args = parser.parse_args()

    if args.debug:
        tokio.DEBUG = True

    ### Convert CLI options into datetime
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

    ### Basic input bounds checking
    if query_start >= query_end:
        raise Exception('query_start >= query_end')
    elif init_start >= init_end:
        raise Exception('query_start >= query_end')
    elif args.timestep < 1:
        raise Exception('--timestep must be > 0')

    es_obj = run_disk_query(args.host, args.port, args.index, query_start, query_end)

    pages_to_hdf5(init_start, init_end, es_obj.scroll_pages, args.timestep, args.num_bbnodes, args.output)

if __name__ == '__main__':
    cache_collectd_cli()
