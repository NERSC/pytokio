#!/usr/bin/env python
"""
WORK IN PROGRESS

Currently a proof-of-concept to show how an HDF5 file can be populated with the
outputs of collectd's ElasticSearch integration.  Currently does NOT use the
collectd_es plugin, but rather ingests the raw ElasticSearch json output and
parses it.

Needs a lot of work to integrate connectors.hdf5 and connectors.collectdes.

Syntax:
    ./cache_collectdes.py 2017-08-27T00:00:00 2017-08-28T00:00:00 \
                          ../tests/inputs/sample_collectd_es.?.json.gz

Presently you must specify a whole day's worth of time range manually to get an
HDF5 file that encompasses a whole day.
"""

import gzip
import json
import time
import datetime
import argparse
import warnings

import h5py
import numpy

COLLECTD_TIMESTEP = 10 ### ten second polling interval
def cache_collectdes():
    """
    Parse cached json[.gz] files from ElasticSearch and convert them into HDF5
    files.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("start", type=str, help="start time in YYYY-MM-DDTHH:MM:SS format")
    parser.add_argument("end", type=str, help="end time in YYYY-MM-DDTHH:MM:SS format")
    parser.add_argument("files", nargs="+", default=None, type=str,
                        help="collectd elasticsearch outputs to process")
    #parser.add_argument("darshanlogs", nargs="*", default=None, help="darshan logs to process")
    parser.add_argument("-o", "--output", type=str, default='output.hdf5', help="output file")
    args = parser.parse_args()

    ### Decode the first and second ISDCT dumps
    t_start = datetime.datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%S")
    t_end = datetime.datetime.strptime(args.end, "%Y-%m-%dT%H:%M:%S")

    if t_start >= t_end:
        raise Exception('t_start >= t_end')

    ### Currently hard-coded knowledge of the number of Burst Buffer servers in
    ### Cori; in the future, we should leave this blank and choose the correct
    ### array size inside of commit_datasets after we know the total number of
    ### servers.  Alternatively, we can run a separate ElasticSearch query ahead
    ### of time to find out how many BB servers to expect before we start
    ### scrolling through the raw documents.
    rows, read_dataset = init_datasets(t_start, t_end, num_servers=288)
    rows, write_dataset = init_datasets(t_start, t_end, num_servers=288)

    ### Hard-coded names for Cori's burst buffer servers.  See above discussion
    ### about more generic solutions to this.
    columns = ["bb%d" % x for x in range(1, 289)]
    column_map = {}
    for index, bbname in enumerate(columns):
        column_map[bbname] = index

    ### Iterate over every cached page.  In practice this should loop over
    ### scrolled pages coming out of ElasticSearch.
    for input_filename in args.files:
        if input_filename.endswith('.gz'):
            input_file = gzip.open(input_filename, 'r')
        else:
            input_file = open(input_filename, 'r')
        page = json.load(input_file)
        update_datasets(page, rows, column_map, read_dataset, write_dataset)

    ### Write out the final HDF5 file after everything is loaded into memory.
    commit_datasets(args.output, rows, columns, read_dataset, write_dataset)

def commit_datasets(hdf5_filename, rows, columns, read_dataset, write_dataset):
    """
    Convert numpy arrays, a list of timestamps, and a list of column headers
    into groups and datasets within an HDF5 file.
    """
    # Create/open the HDF5 file
    hdf5_file = h5py.File(hdf5_filename)

    # Create the read rate dataset
    hdf5_file.create_dataset(name='/bytes/readrate',
                             shape=read_dataset.shape,
                             chunks=True,
                             compression='gzip',
                             dtype='f8')
    # Populate the read rate dataset and set basic metadata
    hdf5_file['/bytes/readrate'][:, :] = read_dataset
    hdf5_file['/bytes/readrate'].attrs['columns'] = columns

    # Create the write rate dataset
    hdf5_file.create_dataset(name='/bytes/writerate',
                             shape=write_dataset.shape,
                             chunks=True,
                             compression='gzip',
                             dtype='f8')
    # Populate the write rate dataset and set basic metadata
    hdf5_file['/bytes/writerate'][:, :] = write_dataset
    hdf5_file['/bytes/writerate'].attrs['columns'] = columns

    # Create the dataset that contains the timestamps which correspond to each
    # row in the read/write datasets
    hdf5_file.create_dataset(name='/bytes/timestamps',
                             shape=rows.shape,
                             dtype='i8')
    hdf5_file['/bytes/timestamps'][:] = rows

def init_datasets(start_time, end_time, num_servers=10000):
    """
    Initialize an HDF5 object.  Takes a start and end time and returns a numpy
    array and a row of timestamps that can be used to initialize an HDF5
    dataset.  Keeps everything in-memory as a numpy array to minimize overhead
    of interacting with h5py.
    """

    date_range = (end_time - start_time).total_seconds()
    num_bins = int(date_range / COLLECTD_TIMESTEP)
    data_dataset = numpy.full((num_bins, num_servers), -0.0)
    time_list = []
    timestamp = start_time
    while timestamp < end_time:
        time_list.append(long(time.mktime(timestamp.timetuple())))
        timestamp += datetime.timedelta(seconds=COLLECTD_TIMESTEP)
    rows = numpy.array(time_list)

    return rows, data_dataset

def update_datasets(page, rows, column_map, read_dataset, write_dataset):
    """
    Go through a list of pages and insert their data into a numpy matrix.  In
    the future this should be a flush function attached to the CollectdEs
    connector class.
    """
    time0 = rows[0] # in seconds since 1/1/1970
    timestep = rows[1] - rows[0] # in seconds

    # basic validity checking
    if 'hits' not in page or 'hits' not in page['hits']:
        warnings.warn("No hits in page")
        return

    for doc in page['hits']['hits']:
        # basic validity checking
        if '_source' not in doc:
            warnings.warn("No _source in doc %s" % doc['_id'])
            continue
        source = doc['_source']
        timestamp = datetime.datetime.strptime(source['@timestamp'].split('.')[0],
                                               '%Y-%m-%dT%H:%M:%S')
        t_index = (long(time.mktime(timestamp.timetuple())) - time0) / timestep
        s_index = column_map[source['hostname']]
        read_dataset[t_index, s_index] = source['read']
        write_dataset[t_index, s_index] = source['write']

if __name__ == "__main__":
    cache_collectdes()
