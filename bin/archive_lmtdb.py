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

    dataset_config = {
        'datatargets/readbytes': {
            "units": "bytes/sec",
            "delta": True,
            "column": "READ_BYTES",
        },
        'datatargets/writebytes': {
            "units": "bytes/sec",
            "delta": True,
            "column": "WRITE_BYTES",
        },
        'fullness/bytes': {
            "units": "KiB",
            "delta": False,
            "column": "KBYTES_USED",
        },
        'fullness/bytestotal': {
            "units": "KiB",
            "delta": False,
        },
        'fullness/inodes': {
            "units": "inodes",
            "delta": False,
            "column": "INODES_USED",
        },
        'fullness/inodestotal': {
            "units": "inodes",
            "delta": False,
        },
    }

    # Initialize raw datasets - extend query by one extra timestep so we can calculate deltas
    query_end_plusplus = query_end + datetime.timedelta(seconds=timestep)
    datasets = {}
    for dataset_name in dataset_config.keys():
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
        col_map = {
            'TIMESTAMP': columns.index('TIMESTAMP'),
            'OST_ID': columns.index('OST_ID'),
            'READ_BYTES': columns.index('READ_BYTES'),
            'WRITE_BYTES': columns.index('WRITE_BYTES'),
            'KBYTES_USED': columns.index('KBYTES_USED'),
            'KBYTES_FREE': columns.index('KBYTES_FREE'),
            'INODES_USED': columns.index('INODES_USED'),
            'INODES_FREE': columns.index('INODES_FREE'),
        }
    except ValueError:
        raise ValueError("LMT database schema does not match expectation")

    # Loop through all the results of the timeseries query
    for row in results:
        if isinstance(row[col_map['TIMESTAMP']], basestring):
            # SQLite stores timestamps as a unicode string
            timestamp = datetime.datetime.strptime(row[col_map['TIMESTAMP']], "%Y-%m-%d %H:%M:%S")
        else:
            # MySQL timestamps are automatically converted to datetime.datetime
            timestamp = row[col_map['TIMESTAMP']]
        col_name = lmtdb.ost_id_map[row[col_map['OST_ID']]]
        for dataset_name, config in dataset_config.iteritems():
            target_col = config.get('column')
            if target_col is not None:
                datasets[dataset_name].insert_element(timestamp, col_name, row[col_map[target_col]])
            elif dataset_name == 'fullness/bytestotal':
                datasets[dataset_name].insert_element(
                    timestamp, col_name, row[col_map['KBYTES_USED']] + row[col_map['KBYTES_FREE']])
            elif dataset_name == 'fullness/inodestotal':
                datasets[dataset_name].insert_element(
                    timestamp, col_name, row[col_map['INODES_USED']] + row[col_map['INODES_FREE']])
            else:
                errmsg = "%s in dataset_config but missing target_col" % dataset_name
                raise KeyError(errmsg)

    # Convert some datasets from absolute byte counts to deltas
    for dataset_name in [x for x in dataset_config if dataset_config[x]['delta']]:
        datasets[dataset_name].convert_to_deltas()

    # Trim off the last row from the non-delta datasets to compensate for our
    # initial over-sizing of the time range by an extra timestamp
    for dataset_name in [x for x in dataset_config if not dataset_config[x]['delta']]:
        datasets[dataset_name].trim_rows(1)

    # Dataset and group-wide metadata
    for dataset_name, config in dataset_config.iteritems():
        datasets[dataset_name].dataset_metadata.update({
            'version': SCHEMA_VERSION,
            'units': config['units']
        })
        datasets[dataset_name].group_metadata.update({'source': 'lmt'})

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
        hdf5_file.attrs['version'] = SCHEMA_VERSION

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
