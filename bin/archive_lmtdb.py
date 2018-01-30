#!/usr/bin/env python
"""
Retrieve the contents of an LMT database and cache it locally.
"""

import os
import sys
import datetime
import argparse
import h5py
import cache_collectdes
import tokio.connectors.lmtdb

DATE_FMT = "%Y-%m-%dT%H:%M:%S"
DATE_FMT_PRINT = "YYYY-MM-DDTHH:MM:SS"

VERSION = "1"

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

def archive_ost_data(lmtdb, init_start, init_end, timestep, output_file, query_start, query_end):
    """
    Extract and encode data from LMT's OST_DATA table (read/write bytes)
    """
    schema = tokio.connectors.hdf5.SCHEMA.get(VERSION)
    if schema is None:
        raise KeyError("Schema version %d is not known by connectors.hdf5" % VERSION)
    dataset_names = [
        schema['datatargets/readbytes'],
        schema['datatargets/writebytes'],
        schema['fullness/bytes'],
        schema['fullness/bytestotal'],
        schema['fullness/inodes'],
        schema['fullness/inodestotal'],
    ]

    # Pluck out group name from dataset names
    group_names = set([])
    for dataset_name in dataset_names:
        group_names.add(os.path.dirname(dataset_name))

    # Get number of columns
    results = lmtdb.query("SELECT OST_ID, OST_NAME FROM OST_INFO")
    columns = [ str(x[1]) for x in results ]

    # Load the entire OST_ID map into memory.  This won't scale to gargantuan
    # Lustre clusters, but it is faster than joining in SQL.
    ost_id_map = {}
    for result in results:
        ost_id_map[result[0]] = result[1]

    # Initialize raw datasets - extend query by one extra timestep so we can calculate deltas
    init_end_plusplus = init_end + datetime.timedelta(seconds=timestep)
    datasets = cache_collectdes.init_datasets(init_start=init_start,
                                              init_end=init_end_plusplus,
                                              timestep=timestep,
                                              num_columns=len(columns),
                                              output_file=None,
                                              dataset_names=dataset_names)

    # Set column names
    for dataset in datasets.itervalues():
        dataset.set_columns(columns)
        dataset.sort_hex = True # because Sonexion nodenames are hex-encoded

    # Now query the OST_DATA table to get byte counts over the query time range
    query_end_plusplus = query_end + datetime.timedelta(seconds=timestep)
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
        col_name = ost_id_map[row[idx_ostid]]
        datasets[schema['datatargets/readbytes']].insert_element(timestamp, col_name, row[idx_readbytes])
        datasets[schema['datatargets/writebytes']].insert_element(timestamp, col_name, row[idx_writebytes])
        datasets[schema['fullness/bytes']].insert_element(timestamp, col_name, row[idx_kbused])
        datasets[schema['fullness/inodes']].insert_element(timestamp, col_name, row[idx_inodesused])
        datasets[schema['fullness/bytestotal']].insert_element(timestamp, col_name, row[idx_kbused] + row[idx_kbfree])
        datasets[schema['fullness/inodestotal']].insert_element(timestamp, col_name, row[idx_inodesused] + row[idx_inodesfree])

    # Convert some datasets from absolute byte counts to deltas
    for dataset_name in 'datatargets/readbytes', 'datatargets/writebytes':
        datasets[schema[dataset_name]].convert_to_deltas()

    # Trim off the last row from the non-delta datasets to compensate for our
    # initial over-sizing of the time range by an extra timestamp
    for dataset_name in 'fullness/bytes', 'fullness/inodes', 'fullness/bytestotal', 'fullness/inodestotal':
        datasets[schema[dataset_name]].trim_rows(1)

    return datasets

def archive_lmtdb(lmtdb, init_start, init_end, timestep, output_file, query_start, query_end):
    """
    Given a start and end time, retrieve all of the relevant contents of an LMT
    database.
    """
    datasets = archive_ost_data(lmtdb, init_start, init_end, timestep, output_file, query_start, query_end)

    output_hdf5 = h5py.File(output_file)
    for dataset in datasets.itervalues():
        print "Writing out %s" % dataset.dataset_name
        dataset.commit_dataset(output_hdf5)
    output_hdf5.close()

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
    parser.add_argument('--timestep', type=int, default=10, help='collection frequency, in seconds (default: 10)')
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
