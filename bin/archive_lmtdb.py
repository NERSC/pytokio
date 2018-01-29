#!/usr/bin/env python
"""
Retrieve the contents of an LMT database and cache it locally.
"""

import os
import datetime
import argparse
import cache_collectdes
import tokio.connectors.lmtdb

DATE_FMT = "%Y-%m-%dT%H:%M:%S"

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
    query_end_plusplus = query_end + datetime.timedelta(seconds=timestep)
    raw_datasets = {}
    for group_name in group_names:
        raw_datasets.update(cache_collectdes.init_datasets(init_start=query_start,
                                                           init_end=query_end_plusplus,
                                                           timestep=timestep,
                                                           num_columns=len(columns),
                                                           output_file=None,
                                                           group_name=group_name,
                                                           dataset_names=dataset_names))

    # Set column names
    for dataset in raw_datasets.itervalues():
        dataset.set_columns(columns)

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
            # MySQL stores timestamps as a long (epoch)
            timestamp = datetime.fromtimestamp(row[idx_timestamp])
        col_name = ost_id_map[row[idx_ostid]]
        raw_datasets[schema['datatargets/readbytes']].insert_element(timestamp, col_name, row[idx_readbytes])
        raw_datasets[schema['datatargets/writebytes']].insert_element(timestamp, col_name, row[idx_writebytes])
        raw_datasets[schema['fullness/bytes']].insert_element(timestamp, col_name, row[idx_kbused])
        raw_datasets[schema['fullness/inodes']].insert_element(timestamp, col_name, row[idx_inodesused])
        raw_datasets[schema['fullness/bytestotal']].insert_element(timestamp, col_name, row[idx_kbused] + row[idx_kbfree])
        raw_datasets[schema['fullness/inodestotal']].insert_element(timestamp, col_name, row[idx_inodesused] + row[idx_inodesfree])

    return raw_datasets

def matrix_to_rates(matrix):
    """
    Subtract every row of a matrix from the row that precedes it to convert a
    matrix of monotonically increasing rows into deltas.  Returns an array with
    the same number of columns but one fewer row (taken off the bottom of the
    matrix)
    """
    base_matrix = matrix[0:-1, :]
    matrix_plusplus = matrix[1:, :]
    diff_matrix = matrix_plusplus - base_matrix

    # Correct missing data and replace with -0.0
    for irow in range(1, matrix.shape[0]):
        for icol in range(0, matrix.shape[1]):
            if matrix[irow-1, icol] > matrix[irow, icol]:
#               print "Replacing (%d, %d) = %d with -0.0" % (
#                   irow - 1, icol, diff_matrix[irow - 1, icol])
                diff_matrix[irow - 1, icol] = -0.0

    return diff_matrix

def archive_lmtdb(lmtdb, init_start, init_end, timestep, output_file, query_start, query_end):
    """
    Given a start and end time, retrieve all of the relevant contents of an LMT
    database.
    """
    pass

def _archive_lmtdb():
    """
    Verify functionality when connecting to a remote database
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("query_start", type=str, help="start time in %s format" % DATE_FMT)
    parser.add_argument("query_end", type=str, help="end time in %s format" % DATE_FMT)
    parser.add_argument('--init-start', type=str, default=None, help='min timestamp if creating new output file, in %s format (default: same as start)' % DATE_FMT)
    parser.add_argument('--init-end', type=str, default=None, help='max timestamp if creating new output file, in %s format (default: same as end)' % DATE_FMT)
    parser.add_argument('--debug', action='store_true', help="produce debug messages")
    parser.add_argument('--timestep', type=int, default=10, help='collection frequency, in seconds (default: 10)')

    parser.add_argument("-i", "--input", type=str, default=None, help="input cache db file")
    parser.add_argument("-o", "--output", type=str, default='output.hdf5', help="output file (default: output.hdf5)")
    parser.add_argument("--host", type=str, default=None, help="database hostname")
    parser.add_argument("--user", type=str, default=None, help="database user")
    parser.add_argument("--password", type=str, default=None, help="database password")
    parser.add_argument("--database", type=str, default=None, help="database name")
    args = parser.parse_args()

    if args.debug:
        tokio.DEBUG = True

    # Convert CLI options into datetime
    try:
        query_start = datetime.datetime.strptime(args.start, DATE_FMT)
        query_end = datetime.datetime.strptime(args.end, DATE_FMT)
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

    archive_lmtdb(lmtdb, init_start, init_end, timestep, output_file, query_start, query_end)

    if args.debug:
        print "Wrote output to %s" % args.output

if __name__ == "__main__":
    _archive_lmtdb()
