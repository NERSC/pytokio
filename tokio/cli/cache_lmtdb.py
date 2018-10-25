"""
Retrieve the contents of an LMT database and cache it locally.
"""

import os
import datetime
import argparse
import tokio.connectors.lmtdb

def retrieve_tables(lmtdb, datetime_start, datetime_end, limit=None):
    """
    Given a start and end time, retrieve and cache all of the relevant contents
    of an LMT database.
    """

    min_ts_id, max_ts_id = lmtdb.get_ts_ids(datetime_start, datetime_end)

    for lmtdb_table, table_schema in tokio.connectors.lmtdb.LMTDB_TABLES.items():
        query_str = 'SELECT * from %s' % lmtdb_table

        ### if this table is indexed by time, restrict the time range.
        ### otherwise, dump the whole time-independent table
        if 'ts_id' in [x.lower() for x in table_schema['columns']]:
            query_str += " WHERE TS_ID >= %d AND TS_ID <= %d" % (min_ts_id, max_ts_id)

        ### limits (mostly for testing)
        if limit is not None:
            query_str += " LIMIT %d" % limit

        result = lmtdb.query(
            query_str=query_str,
            table=lmtdb_table,
            table_schema=table_schema)

def main(argv=None):
    """Entry point for the CLI interface
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("start", type=str, help="start time in YYYY-MM-DDTHH:MM:SS format")
    parser.add_argument("end", type=str, help="end time in YYYY-MM-DDTHH:MM:SS format")
    parser.add_argument("-i", "--input", type=str, default=None, help="input cache db file")
    parser.add_argument("-o", "--output", type=str, default=None, help="output file")
    parser.add_argument("-l", "--limit", type=int, default=None,
                        help="restrict number of records returned per table")
    parser.add_argument("--host", type=str, default=None, help="database hostname")
    parser.add_argument("--user", type=str, default=None, help="database user")
    parser.add_argument("--password", type=str, default=None, help="database password")
    parser.add_argument("--database", type=str, default=None, help="database name")
    args = parser.parse_args(argv)

    start = datetime.datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%S")
    end = datetime.datetime.strptime(args.end, "%Y-%m-%dT%H:%M:%S")

    cache_file = args.output

    ### Unlike nersc_jobsdb connector, LmtDb does not look for any
    ### environment variables intrinsically
    if args.input is not None:
        lmtdb = tokio.connectors.lmtdb.LmtDb(cache_file=args.input)
    else:
        lmtdb = tokio.connectors.lmtdb.LmtDb(
            dbhost=args.host,
            dbuser=args.user,
            dbpassword=args.password,
            dbname=args.database)

    retrieve_tables(lmtdb, start, end, args.limit)

    if cache_file is None:
        i = 0
        while True:
            cache_file = "lmtdb-%d.sqlite" % i
            if os.path.exists(cache_file):
                i += 1
            else:
                break
    print("Caching to %s" % cache_file)
    lmtdb.save_cache(cache_file)
