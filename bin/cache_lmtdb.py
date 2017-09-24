#!/usr/bin/env python
"""
Retrieve the contents of an LMT database and cache it locally.
"""

import os
import datetime
import argparse
import tokio.connectors.cachingdb

_DB_HOST = os.environ.get('PYTOKIO_LMTDB_HOST', 'localhost')
_DB_USER = os.environ.get('PYTOKIO_LMTDB_USER', 'root')
_DB_PASSWORD = os.environ.get('PYTOKIO_LMTDB_PASSWORD', '')
_DB_DBNAME = os.environ.get('PYTOKIO_LMTDB_DBNAME', 'testdb')

LMTDB_TABLES = [
    (
        "FILESYSTEM_INFO",
        """FILESYSTEM_ID, FILESYSTEM_NAME, FILESYSTEM_MOUNT_NAME, SCHEMA_VERSION,
           PRIMARY KEY(FILESYSTEM_ID)""",
    ),
    (
        "MDS_DATA",
        """MDS_ID, TS_ID, PCT_CPU, KBYTES_FREE, KBYTES_USED, INODES_FREE, INODES_USED,
           PRIMARY KEY(MDS_ID, TS_ID)""",
    ),
    (
        "MDS_INFO",
        """MDS_ID, FILESYSTEM_ID, MDS_NAME, HOSTNAME, DEVICE_NAME,
           PRIMARY KEY(MDS_ID)""",
    ),
    (
        "MDS_OPS_DATA",
        """MDS_ID, TS_ID, OPERATION_ID, SAMPLES, SUM, SUMSQUARES,
           PRIMARY KEY(MDS_ID, TS_ID, OPERATION_ID)""",
    ),
    (
        "MDS_VARIABLE_INFO",
        """VARIABLE_ID, VARIABLE_NAME, VARIABLE_LABEL, THRESH_TYPE, THRESH_VAL1, THRESH_VAL2,
            PRIMARY KEY(VARIABLE_ID)""",
    ),
    (
        "OPERATION_INFO",
        """OPERATION_ID, OPERATION_NAME, UNITS,
           PRIMARY KEY(OPERATION_ID)""",
    ),
    (
        "OSS_DATA",
        """OSS_ID, TS_ID, PCT_CPU, PCT_MEMORY,
           PRIMARY KEY (OSS_ID, TS_ID)""",
    ),
    (
        "OSS_INFO",
        """OSS_ID, FILESYSTEM_ID, HOSTNAME, FAILOVERHOST,
           PRIMARY KEY(OSS_ID, HOSTNAME)""",
    ),
    (
        "OST_DATA",
        """OST_ID, TS_ID, READ_BYTES, WRITE_BYTES, PCT_CPU, KBYTES_FREE,
           KBYTES_USED, INODES_FREE, INODES_USED,
           PRIMARY KEY(OST_ID, TS_ID)""",
    ),
    (
        "OST_INFO",
        """OST_ID, OSS_ID, OST_NAME, HOSTNAME, OFFLINE, DEVICE_NAME,
           PRIMARY KEY (OST_ID)""",
    ),
    (
        "OST_VARIABLE_INFO",
        """VARIABLE_ID, VARIABLE_NAME, VARIABLE_LABEL, THRESH_TYPE, THRESH_VAL1, THRESH_VAL2,
           PRIMARY KEY (VARIABLE_ID)"""
    ),
    (
        "TIMESTAMP_INFO",
        """TS_ID, TIMESTAMP, PRIMARY KEY (TS_ID)""",
    ),
]

def retrieve_tables(lmtdb, datetime_start, datetime_end, limit=None):
    """
    Given a start and end time, retrieve and cache all of the relevant contents
    of an LMT database.
    """
    # First figure out the timestamp range
    query_str = "SELECT TS_ID FROM TIMESTAMP_INFO WHERE TIMESTAMP >= %(ps)s AND TIMESTAMP <= %(ps)s"
    query_variables = (
        datetime_start.strftime("%Y-%m-%d %H:%M:%S"),
        datetime_end.strftime("%Y-%m-%d %H:%M:%S"))

    result = lmtdb.query(query_str=query_str,
                         query_variables=query_variables)

    ts_ids = [x[0] for x in result]

    for lmtdb_table, table_schema in LMTDB_TABLES:
        schema_cmd = 'CREATE TABLE IF NOT EXISTS %s (%s)' % (
            lmtdb_table,
            table_schema)
        query_str = 'SELECT * from %s' % lmtdb_table

        ### if this table is indexed by time, restrict the time range.
        ### otherwise, dump the whole time-independent table
        if 'ts_id' in table_schema.lower():
            query_str += " WHERE TS_ID >= %d AND TS_ID <= %d" % (min(ts_ids), max(ts_ids))


        ### limits (mostly for testing)
        if limit is not None:
            query_str += " LIMIT %d" % limit

        result = lmtdb.query(
            query_str=query_str,
            table=lmtdb_table,
            table_schema=schema_cmd)

def cache_lmtdb():
    """
    Verify functionality when connecting to a remote database
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("start", type=str, help="start time in YYYY-MM-DDTHH:MM:SS format")
    parser.add_argument("end", type=str, help="end time in YYYY-MM-DDTHH:MM:SS format")
    parser.add_argument("-i", "--input", type=str, default=None, help="input cache db file")
    parser.add_argument("-o", "--output", type=str, default=None, help="output file")
    parser.add_argument("-l", "--limit", type=int, default=None,
                        help="restrict number of records returned per table")
    args = parser.parse_args()

    start = datetime.datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%S")
    end = datetime.datetime.strptime(args.end, "%Y-%m-%dT%H:%M:%S")

    cache_file = args.output

    ### Unlike nersc_jobsdb connector, CachingDb does not look for any
    ### environment variables intrinsically
    if args.input is not None:
        lmtdb = tokio.connectors.cachingdb.CachingDb(cache_file=args.input)
    else:
        lmtdb = tokio.connectors.cachingdb.CachingDb(
            dbhost=_DB_HOST,
            dbuser=_DB_USER,
            dbpassword=_DB_PASSWORD,
            dbname=_DB_DBNAME)

    retrieve_tables(lmtdb, start, end, args.limit)

    if cache_file is None:
        i = 0
        while True:
            cache_file = "lmtdb-%d.sqlite" % i
            if os.path.exists(cache_file):
                i += 1
            else:
                break
    print "Caching to %s" % cache_file
    lmtdb.save_cache(cache_file)

if __name__ == "__main__":
    cache_lmtdb()
