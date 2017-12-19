#!/usr/bin/env python
"""
Dump a lot of data out of ElasticSearch using the Python API and native
scrolling support
"""

import os
import re
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
import numpy

import tokio.connectors.collectd_es

### constants pertaining to HDF5 ###############################################

EPOCH = dateutil.parser.parse('1970-01-01T00:00:00.000Z')
TSTAMP_KEY = 'timestamps'

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

        if group is not None:
            self.attach_group(group)
        else:
            if start is None or end is None or timestep is None:
                raise Exception("Must specify either ({start,end,timestep}|group)")
            else:
                self.init_group(start, end, timestep)

    def attach_group(self, group):
        """
        Attach to an existing h5py Group object
        """
        if TSTAMP_KEY not in group:
            raise Exception("Existing dataset contains no timestamps")

        self.timestamps = group[TSTAMP_KEY][:]
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

    def update_column_map(self):
        """
        Create the mapping of column names to column indices
        """
        self.column_map = {}
        for index, column_name in enumerate(self.columns):
            self.column_map[column_name] = index

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
        self.dataset_name = dataset.name
        self.dataset = dataset[:, :]
        if 'columns' in dataset.attrs:
            self.columns = list(dataset.attrs['columns'])
        else:
            warnings.warn("attaching to a columnless dataset (%s)" % self.dataset_name)
            self.columns = [''] * dataset.shape[0]
        self.update_column_map()

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
        group_name = '/'.join(self.dataset_name.split('/')[0:-1])
        timestamps_dataset_name = group_name + '/' + TSTAMP_KEY

        # If we are creating a new group, first insert the new timestamps
        if timestamps_dataset_name not in hdf5_file:
            hdf5_file.create_dataset(name=timestamps_dataset_name,
                                     shape=self.timestamps.shape,
                                     dtype='i8')
            # Copy the in-memory timestamp dataset into the HDF5 file
            hdf5_file[timestamps_dataset_name][:] = self.timestamps[:]
        # Otherwise, verify that our dataset will fit into the existing timestamps
        else:
            if not numpy.array_equal(self.timestamps, hdf5_file[timestamps_dataset_name][:]):
                raise Exception("Attempting to commit to a group with different timestamps")
  
        # Create the dataset in the HDF5 file
        if self.dataset_name not in hdf5_file:
            hdf5_file.create_dataset(name=self.dataset_name,
                                     shape=self.dataset.shape,
                                     dtype='f8',
                                     chunks=True,
                                     compression='gzip')

        # If we're updating an existing HDF5, use its column names and ordering.
        # Otherwise sort the columns before committing them.
        if 'columns' in hdf5_file[self.dataset_name].attrs:
            self.rearrange_columns(list(hdf5_file[self.dataset_name].attrs['columns']))
        else:
            self.sort_columns()
    
        # Copy the in-memory dataset into the HDF5 file.  Note implicit
        # assumption that dataset is 2d
        try:
            hdf5_file[self.dataset_name][:, :] = self.dataset[:, :]
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
        hdf5_file[self.dataset_name].attrs['columns'] = self.columns

    def sort_columns(self):
        """
        Rearrange the dataset's column data by sorting them by their headings
        """
        self.rearrange_columns(sorted_nodenames(self.columns))

    def rearrange_columns(self, new_order):
        """
        Rearrange the dataset's columnar data by an arbitrary column order given
        as an enumerable list
        """
        # validate the new order - new_order must contain at least all of
        # the elements in self.columns, but may contain more than that
        for new_key in new_order:
            if new_key not in self.columns:
                raise Exception("key %s in new_order not in columns" % new_key)

        # walk the new column order
        for new_index, new_column in enumerate(new_order):
            # new_order can contain elements that don't exist; this happens when
            # re-ordering a small dataset to be inserted into an existing,
            # larger dataset
            if new_column not in self.columns:
                warnings.warn("Column '%s' in new column order not present in TokioTimeSeries" % new_column)
                continue

            old_index = self.column_map[new_column]
            self.swap_columns(old_index, new_index)

    def swap_columns(self, index1, index2):
        """
        Swap two columns of the dataset in-place
        """
        # save the data from the column we're about to swap
        saved_column_data = self.dataset[:, index2].copy()
        saved_column_name = self.columns[index2]

        # swap column data
        self.dataset[:, index2] = self.dataset[:, index1]
        self.dataset[:, index1] = saved_column_data[:] 

        # swap column names too
        self.columns[index2] = self.columns[index1]
        self.columns[index1] = saved_column_name

        # update the column map
        self.column_map[self.columns[index2]] = index2
        self.column_map[self.columns[index1]] = index1

def sorted_nodenames(nodenames):
    """
    Gnarly routine to sort nodenames naturally.  Required for nodes named things
    like 'bb23' and 'bb231'.
    """
    def extract_int(string):
        """
        Convert input into an int if possible; otherwise return unmodified
        """
        try:
            return int(string)
        except ValueError:
            return string

    def natural_compare(string):
        """
        Tokenize string into alternating strings/ints if possible
        """
        return map(extract_int, re.findall(r'(\d+|\D+)', string))

    def natural_comp(a, b):
        """
        Cast the parts of a string that look like integers into integers, then
        sort based on strings and integers rather than only strings
        """
        return cmp(natural_compare(a), natural_compare(b))

    return sorted(nodenames, natural_comp)


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
        mime_type, encoding = mimetypes.guess_type(args.input_json)
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
        mime_type, encoding = mimetypes.guess_type(args.output)
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
