#!/usr/bin/env python
"""
Retrieve the contents of an LMT database and cache it locally.
"""

import os
import sys
import datetime
import argparse
import h5py
import tokio.timeseries
import tokio.connectors.lmtdb

DATE_FMT = "%Y-%m-%dT%H:%M:%S"
DATE_FMT_PRINT = "YYYY-MM-DDTHH:MM:SS"

SCHEMA_VERSION = "1"
OST_DATA_VERSION = "1"
OSS_DATA_VERSION = "1"
MDS_OPS_DATA_VERSION = "1"
MDS_DATA_VERSION = "1"

def archive_mds_data(lmtdb, datetime_start, datetime_end):
    """
    Extract and encode data from LMT's MDS_DATA table (CPU loads)
    """
    pass

def archive_mds_ops_data(lmtdb, datetime_start, datetime_end):
    """
    Extract and encode data from LMT's MDS_OPS_DATA table (metadata ops)
    """
    pass

def archive_oss_data(lmtdb, datetime_start, datetime_end):
    """
    Extract and encode data from LMT's OSS_DATA table (CPU loads)
    """
    pass

def archive_ost_data(lmtdb, query_start, query_end, timestep, output_file):
    """
    Extract and encode data from LMT's OST_DATA table (read/write bytes)
    """
    schema = tokio.connectors.hdf5.SCHEMA.get(SCHEMA_VERSION)
    if schema is None:
        raise KeyError("Schema version %d is not known by connectors.hdf5" % SCHEMA_VERSION)

    dataset_names = [
        'datatargets/readbytes',
        'datatargets/writebytes',
        'fullness/bytes',
        'fullness/bytestotal',
        'fullness/inodes',
        'fullness/inodestotal',
    ]

    # Initialize raw datasets - extend query by one extra timestep so we can calculate deltas
    query_end_plusplus = query_end + datetime.timedelta(seconds=timestep)
    datasets = {}
    for dataset_name in dataset_names:
        hdf5_dataset_name = schema.get(dataset_name)
        if hdf5_dataset_name is None:
            warnings.warn("Skipping %s (not in schema)" % dataset_name)
        else:
            datasets[dataset_name] = tokio.timeseries.TimeSeries(dataset_name=hdf5_dataset_name,
                                                                 start=query_start,
                                                                 end=query_end_plusplus,
                                                                 timestep=timestep,
                                                                 num_columns=len(lmtdb.ost_names),
                                                                 column_names=lmtdb.ost_names,
                                                                 hdf5_file=None,
                                                                 sort_hex=True)

    # Now query the OST_DATA table to get byte counts over the query time range
    results, columns = lmtdb.get_timeseries_data('OST_DATA', query_start, query_end_plusplus)

    # Index the columns before building the Timeseries objects
    try:
        idx_timestamp = columns.index('TIMESTAMP')
        idx_ostid = columns.index('OST_ID')
        idx_readbytes = columns.index('READ_BYTES')
        idx_writebytes = columns.index('WRITE_BYTES')
        idx_kbused = columns.index('KBYTES_USED')
        idx_kbfree = columns.index('KBYTES_FREE')
        idx_inodesused = columns.index('INODES_USED')
        idx_inodesfree = columns.index('INODES_FREE')
    except ValueError:
        raise ValueError("LMT database schema does not match expectation")

    # Loop through all the results of the timeseries query
    for row in results:
        if isinstance(row[idx_timestamp], basestring):
            # SQLite stores timestamps as a unicode string
            timestamp = datetime.datetime.strptime(row[idx_timestamp], "%Y-%m-%d %H:%M:%S")
        else:
            # MySQL timestamps are automatically converted to datetime.datetime
            timestamp = row[idx_timestamp]
        col_name = lmtdb.ost_id_map[row[idx_ostid]]
        datasets['datatargets/readbytes'].insert_element(timestamp, col_name, row[idx_readbytes])
        datasets['datatargets/writebytes'].insert_element(timestamp, col_name, row[idx_writebytes])
        datasets['fullness/bytes'].insert_element(timestamp, col_name, row[idx_kbused])
        datasets['fullness/inodes'].insert_element(timestamp, col_name, row[idx_inodesused])
        datasets['fullness/bytestotal'].insert_element(timestamp, col_name, row[idx_kbused] + row[idx_kbfree])
        datasets['fullness/inodestotal'].insert_element(timestamp, col_name, row[idx_inodesused] + row[idx_inodesfree])

    # Convert some datasets from absolute byte counts to deltas
    for dataset_name in 'datatargets/readbytes', 'datatargets/writebytes':
        datasets[dataset_name].convert_to_deltas()

    # Trim off the last row from the non-delta datasets to compensate for our
    # initial over-sizing of the time range by an extra timestamp
    for dataset_name in 'fullness/bytes', 'fullness/inodes', 'fullness/bytestotal', 'fullness/inodestotal':
        datasets[dataset_name].trim_rows(1)

    # Dataset metadata
    datasets['datatargets/readbytes'].dataset_metadata.update({'version': OST_DATA_VERSION, 'units': 'bytes'})
    datasets['datatargets/writebytes'].dataset_metadata.update({'version': OST_DATA_VERSION, 'units': 'bytes'})
    datasets['fullness/bytes'].dataset_metadata.update({'version': OST_DATA_VERSION, 'units': 'bytes'})
    datasets['fullness/inodes'].dataset_metadata.update({'version': OST_DATA_VERSION, 'units': 'inodes'})
    datasets['fullness/bytestotal'].dataset_metadata.update({'version': OST_DATA_VERSION, 'units': 'bytes'})
    datasets['fullness/inodestotal'].dataset_metadata.update({'version': OST_DATA_VERSION, 'units': 'inodes'})

    # Group metadata
    datasets['datatargets/readbytes'].group_metadata.update({'source': 'lmt'})
    datasets['datatargets/writebytes'].group_metadata.update({'source': 'lmt'})
    datasets['fullness/bytes'].group_metadata.update({'source': 'lmt'})
    datasets['fullness/inodes'].group_metadata.update({'source': 'lmt'})
    datasets['fullness/bytestotal'].group_metadata.update({'source': 'lmt'})
    datasets['fullness/inodestotal'].group_metadata.update({'source': 'lmt'})

    return datasets

def init_hdf5_file(datasets, init_start, init_end, hdf5_file):
    """
    Initialize the datasets at full dimensions in the HDF5 file if necessary
    """
    schema = tokio.connectors.hdf5.SCHEMA.get(SCHEMA_VERSION)
    for dataset_name, dataset in datasets.iteritems():
        hdf5_dataset_name = schema.get(dataset_name)
        if hdf5_dataset_name is None:
            warnings.warn("Dataset key %s is not in schema" % dataset_name)
            continue
        if hdf5_dataset_name not in hdf5_file:
            new_dataset = tokio.timeseries.TimeSeries(dataset_name=hdf5_dataset_name,
                                                      start=init_start,
                                                      end=init_end,
                                                      timestep=dataset.timestep,
                                                      num_columns=dataset.dataset.shape[1],
                                                      hdf5_file=hdf5_file)
            new_dataset.commit_dataset(hdf5_file)
            print "Initialized %s in %s with size %s" % (
                hdf5_dataset_name,
                hdf5_file.name,
                new_dataset.dataset.shape)

def archive_lmtdb(lmtdb, init_start, init_end, timestep, output_file, query_start, query_end):
    """
    Given a start and end time, retrieve all of the relevant contents of an LMT
    database.
    """
    datasets = archive_ost_data(lmtdb, query_start, query_end, timestep, output_file)

    with h5py.File(output_file) as hdf5_file:
        hdf5_file['/'].attrs['version'] = SCHEMA_VERSION

        init_hdf5_file(datasets, init_start, init_end, hdf5_file)

        for dataset in datasets.itervalues():
            print "Writing out %s" % dataset.dataset_name
            dataset.commit_dataset(hdf5_file)

    if tokio.DEBUG:
        print "Wrote output to %s" % output_file

def _archive_lmtdb():
    """
    Verify functionality when connecting to a remote database
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", type=str, default=None, help="input cache db file")
    parser.add_argument("-o", "--output", type=str, default='output.hdf5', help="output file (default: output.hdf5)")
    parser.add_argument('--init-start', type=str, default=None, help='min timestamp if creating new output file, in %s format (default: same as start)' % DATE_FMT_PRINT)
    parser.add_argument('--init-end', type=str, default=None, help='max timestamp if creating new output file, in %s format (default: same as end)' % DATE_FMT_PRINT)
    parser.add_argument('--debug', action='store_true', help="produce debug messages")
    parser.add_argument('--timestep', type=int, default=5, help='collection frequency, in seconds (default: 5)')
    parser.add_argument("--host", type=str, default=None, help="database hostname")
    parser.add_argument("--user", type=str, default=None, help="database user")
    parser.add_argument("--password", type=str, default=None, help="database password")
    parser.add_argument("--database", type=str, default=None, help="database name")
    parser.add_argument("query_start", type=str, help="start time in %s format" % DATE_FMT_PRINT)
    parser.add_argument("query_end", type=str, help="end time in %s format" % DATE_FMT_PRINT)
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

    if args.input is not None:
        lmtdb = tokio.connectors.lmtdb.LmtDb(cache_file=args.input)
    else:
        lmtdb = tokio.connectors.lmtdb.LmtDb(
            dbhost=args.host,
            dbuser=args.user,
            dbpassword=args.password,
            dbname=args.database)

    archive_lmtdb(lmtdb=lmtdb,
                  init_start=init_start,
                  init_end=init_end,
                  timestep=args.timestep,
                  output_file=args.output,
                  query_start=query_start,
                  query_end=query_end)

if __name__ == "__main__":
    _archive_lmtdb()
