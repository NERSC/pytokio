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
    for dataset_name, dataset in datasets.iteritems():
        check_create_hdf5_dataset(hdf5_file=hdf5_file,
                                  name=dataset_name,
                                  shape=dataset.shape,
                                  chunks=True,
                                  compression='gzip',
                                  dtype='f8')

        ### Sort the columns at this point
        new_columns = sorted(columns)
        for new_index, column in enumerate(new_columns):
            moved_column = dataset[:, new_index]
            moved_index = columns.index(column)
            dataset[:, new_index] = dataset[:, moved_index]
            dataset[:, moved_index] = moved_column

        ### TODO: should only update the slice of the hdf5 file that needs to be
        ###       changed; for example, maybe calculate the min/max row and
        ###       column touched, then use these as slice indices

        # Populate the read rate dataset and set basic metadata
        hdf5_file[dataset_name][:, :] = dataset
        hdf5_file[dataset_name].attrs['columns'] = new_columns
        print "Committed %s of shape %s" % (dataset_name, dataset.shape)

        # Create the dataset that contains the timestamps which correspond to each
        # row in the read/write datasets
        dataset_name = os.path.join(os.path.dirname(dataset_name), 'timestamps')
        check_create_hdf5_dataset(hdf5_file=hdf5_file,
                                  name=dataset_name,
                                  shape=rows.shape,
                                  dtype='i8')

        hdf5_file[dataset_name][:] = rows
        print "Committed %s of shape %s" % (dataset_name, dataset.shape)

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
    for page in pages:
        for doc in page:
            # basic validity checking
            if '_source' not in doc:
                print doc
                warnings.warn("No _source in doc %s" % str(doc['_id']))
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
    if name in hdf5_file:
        if hdf5_file[name].shape != shape:
            raise Exception('Dataset %s of shape %s already exists with shape %s' %
                            name,
                            shape,
                            hdf5_file[name].shape)
    else:
        hdf5_file.create_dataset(name=name,
                                 shape=shape,
                                 dtype=dtype,
                                 **kwargs)

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
        filter_function=lambda x: x['hits']['hits']
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
    parser.add_argument("start", type=str, help="start time in YYYY-MM-DDTHH:MM:SS format")
    parser.add_argument("end", type=str, help="end time in YYYY-MM-DDTHH:MM:SS format")
    parser.add_argument('--debug', action='store_true', help="produce debug messages")
    parser.add_argument('--num-bbnodes', type=int, default=288, help='number of expected BB nodes')
    parser.add_argument('--timestep', type=int, default=10, help='collection frequency, in seconds')
    parser.add_argument("-o", "--output", type=str, default='output.hdf5', help="output file")
    parser.add_argument('-h', '--host', type=str, default="localhost", help="hostname of ElasticSearch endpoint")
    parser.add_argument('-p', '--port', type=int, default=9200, help="port of ElasticSearch endpoint")
    parser.add_argument('-i', '--index', type=str, default='cori-collectd-*', help='ElasticSearch index to query')
    args = parser.parse_args()
    if not (args.start and args.end):
        parser.print_help()
        sys.exit(1)

    if args.debug:
        tokio.DEBUG = True

    ### Convert CLI options into datetime
    try:
        t_start = datetime.datetime.strptime(args.start, DATE_FMT)
        t_end = datetime.datetime.strptime(args.end, DATE_FMT)
    except ValueError:
        sys.stderr.write("Start and end times must be in format %s\n" % DATE_FMT)
        raise

    ### Basic input bounds checking
    if t_start >= t_end:
        raise Exception('t_start >= t_end')
    elif args.timestep < 1:
        raise Exception('--timestep must be > 0')

    es_obj = run_disk_query(args.host, args.port, args.index, t_start, t_end)

    pages_to_hdf5(t_start, t_end, es_obj.scroll_pages, args.timestep, args.num_bbnodes, args.output)

if __name__ == '__main__':
    cache_collectd_cli()
