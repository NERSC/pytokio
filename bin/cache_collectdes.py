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

class TokioTimeSeries(object):
    """
    In-memory representation of an HDF5 group in a TokioFile.  Can either
    initialize with no datasets, or initialize against an existing HDF5
    group.
    """
    def __init__(self, start=None, end=None, timestep=None, group=None):
        self.timestamps = None
        self.time0 = None
        self.timef = None
        self.timestep = None

        self.dataset = None
        self.dataset_name = None
        self.columns = None
        self.column_map = {}

        if group is None:
            self.init_group(start, end, timestep)
        else:
            if start is None or end is None or timestep is None:
                raise Exception("Must specify either ({start,end,timestep}|group)")
            else:
                self.attach_group(group)

    def attach_group(self, group):
        """
        Attach to an existing h5py Group object
        """
        if 'timestamps' not in group:
            raise Exception("Existing dataset contains no timestamps")

        self.timestamps = group['timestamps'][:]
        self.time0 = self.timestamps[0]
        self.timef = self.timestamps[-1]
        self.timestep = self.timestamps[1] - self.timestamps[0]

    def init_group(self, start, end, timestep):
        """
        Initialize the object from scratch
        """
        if start is None or end is None or timestep is None:
            raise Exception("Must specify either ({start,end,timestep}|group)")
        self.time0 = long(time.mktime(start.timetuple()))
        self.timef = long(time.mktime(end.timetuple()))
        self.timestep = timestep

        time_list = []
        timestamp = start
        while timestamp < end:
            time_list.append(long(time.mktime(timestamp.timetuple())))
            timestamp += datetime.timedelta(seconds=timestep)

        self.timestamps = numpy.array(time_list)
        print 'last timestamp is ', self.timestamps[-1]

    def init_dataset(self, *args, **kwargs):
        """
        Dimension-independent wrapper around init_dataset2d
        """
        self.init_dataset2d(*args, **kwargs)

    def attach_dataset(self, *args, **kwargs):
        """
        Dimension-independent wrapper around attach_dataset2d
        """
        self.attach_dataset2d(*args, **kwargs)

    def init_dataset2d(self, dataset_name, columns, default_value=-0.0):
        """
        Initialize the dataset from scratch
        """
        self.dataset_name = dataset_name
        self.columns = columns
        self.dataset = numpy.full((len(self.timestamps), len(columns)), default_value)

    def attach_dataset2d(self, dataset):
        """
        Initialize the dataset from an existing h5py Dataset objectg
        """
        self.dataset_name = dataset.name.split('/')[-1]
        self.dataset = dataset[:, :]
        if 'columns' in dataset.attrs:
            # convert to a list so we can call .index() on it
            self.columns = list(dataset.attrs['columns'])
        else:
            warnings.warn("attaching to a columnless dataset (%s)" % self.dataset_name)
            self.columns = [''] * dataset.shape[0]

    def commit_dataset(self, *args, **kwargs):
        """
        Dimension-independent wrapper around commit_dataset2d
        """
        self.commit_dataset2d(*args, **kwargs)

    def commit_dataset2d(self, hdf5_file):
        """
        Write contents of this object into an HDF5 file group
        """
        # Create the timestamps dataset
        #'/'.join(h.split('/')[0:-1])
        group_name = '/'.join(self.dataset_name.split('/')[0:-1])
        timestamps_dataset_name = group_name + '/' + 'timestamps'

        # If we are creating a new group, first insert the new timestamps
        if timestamps_dataset_name not in hdf5_file:
            hdf5_file.create_dataset(name=timestamps_dataset_name,
                                     shape=self.timestamps.shape,
                                     dtype='i8')
            # Copy the in-memory timestamp dataset into the HDF5 file
            hdf5_file[timestamps_dataset_name][:] = self.timestamps[:]
            time0_idx = 0
            timef_idx = time0_idx + len(self.timestamps[:])
            print 'time*_idx(1) = ', time0_idx, timef_idx
        # Otherwise, verify that our dataset will fit into the existing timestamps
        else:
            existing_time0 = hdf5_file[timestamps_dataset_name][0]
            timestep = hdf5_file[timestamps_dataset_name][1] - existing_time0
            # if the timestep in memory doesn't match the timestep of the existing
            # dataset, don't try to guess how to align them--just give up
            if timestep != self.timestep:
                raise Exception("Timestep in existing dataset %d does not match %d"
                                % (timestep, self.timestep))

            # Calculate where in the existing data set we need to start
            time0_idx = (self.time0 - existing_time0) / timestep
            timef_idx = time0_idx + (self.timef - self.time0) / timestep
            if time0_idx < 0:
                raise IndexError("Exiting dataset starts at %d, but we start earlier at %d"
                                 % (existing_time0, self.time0))
            if timef_idx > hdf5_file[timestamps_dataset_name].shape[0]:
                raise IndexError("Existing dataset ends at %d (%d), but we end later at %d (%d)"
                                 % (hdf5_file[timestamps_dataset_name].shape[0],
                                 hdf5_file[timestamps_dataset_name][-1],
                                 timef_idx,
                                 self.timef))
            print 'timef_idx(2) = ', time0_idx, timef_idx

        # Create the dataset in the HDF5 file
        if self.dataset_name not in hdf5_file:
            hdf5_file.create_dataset(name=self.dataset_name,
                                     shape=self.dataset.shape,
                                     dtype='f8',
                                     chunks=True,
                                     compression='gzip')

        # If we're updating an existing, use its column names.  Otherwise sort
        # the columns before committing them.
        if 'columns' in hdf5_file[self.dataset_name].attrs:
            new_columns = hdf5_file[self.dataset_name].attrs['columns']
        else:
            new_columns = sorted(self.columns)

        # Rearrange column data to match what's in the HDF5 file
        for new_index, column in enumerate(new_columns):
            moved_column = self.dataset[:, new_index]
            moved_index = self.columns.index(column)
            self.dataset[:, new_index] = self.dataset[:, moved_index]
            self.dataset[:, moved_index] = moved_column

        # Copy the in-memory dataset into the HDF5 file.  Note implicit
        # assumption that dataset is 2d
        try:
            hdf5_file[self.dataset_name][time0_idx:timef_idx, :] = self.dataset[:, :]
        except TypeError:
            print "Indices in target file: %d to %d (%s - %s)" % (
                time0_idx,
                timef_idx,
                hdf5_file[timestamps_dataset_name][time0_idx],
                hdf5_file[timestamps_dataset_name][timef_idx])
            print "Indices in dataset: %d to %d (%s - %s)" % (
                0,
                len(self.timestamps),
                self.time0,
                self.timef)
            raise
        hdf5_file[self.dataset_name].attrs['columns'] = new_columns
        print "Committed %s of shape %s" % (self.dataset_name, self.dataset.shape)

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
        s_index = read_dataset.column_map.get(source['hostname'])
        if s_index is None:
            s_index = len(read_dataset.column_map)
            read_dataset.column_map[source['hostname']] = s_index
            write_dataset.column_map[source['hostname']] = s_index

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

    group_name = '/bytes'
    if group_name in output_hdf5:
        target_group = output_hdf5[group_name]
    else:
        target_group = None

    datasets = {}
    for dataset_name in 'readrates', 'writerates':
        target_dataset_name = group_name + '/' + dataset_name
        datasets[dataset_name] = TokioTimeSeries(start=t_start,
                                                 end=t_end,
                                                 timestep=timestep,
                                                 group=target_group)
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

    if args.input_json is None:
        es_obj = run_disk_query(args.host, args.port, args.index, query_start, query_end)
        pages = es_obj.scroll_pages
        if args.debug:
            print "Loaded results from %s:%s" % (args.host, args.port)
    else:
        input_file = open(args.input_json, 'r')
        pages = json.load(input_file)
        input_file.close()
        if args.debug:
            print "Loaded results from %s" % args.input_json

    if args.json:
        output_file = open(args.output, 'w')
        json.dump(pages, output_file)
        output_file.close()
    else:
        pages_to_hdf5(init_start, init_end, pages, args.timestep, args.num_bbnodes, args.output)

    if args.debug:
        print "Wrote output to %s" % args.output

if __name__ == '__main__':
    cache_collectd_cli()
