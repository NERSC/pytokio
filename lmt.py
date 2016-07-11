#!/usr/bin/env python

import os
import sys
import time
import datetime
import argparse
import cPickle as pickle
import MySQLdb

LMT_HOST = os.environ.get('PYLMT_HOST')
LMT_USER = os.environ.get('PYLMT_USER')
LMT_PASSWORD = os.environ.get('PYLMT_PASSWORD')
LMT_DB = os.environ.get('PYLMT_DB')

_LMT_TIMESTEP = 5.0

_DATE_FMT = "%Y-%m-%d %H:%M:%S"

_MYSQL_FETCHMANY_LIMIT = 1000

_QUERY_OST_DATA = """
SELECT
    TIMESTAMP_INFO.`TIMESTAMP` as ts,
    OST_NAME as ostname,
    READ_BYTES as read_b,
    WRITE_BYTES as write_b
FROM
    OST_DATA
INNER JOIN TIMESTAMP_INFO ON TIMESTAMP_INFO.TS_ID = OST_DATA.TS_ID
INNER JOIN OST_INFO ON OST_INFO.OST_ID = OST_DATA.OST_ID
WHERE
    TIMESTAMP_INFO.`TIMESTAMP` >= ADDTIME('%s', '%.1f')
AND TIMESTAMP_INFO.`TIMESTAMP` < ADDTIME('%s', '%1.f')
ORDER BY ts, ostname;
""" % ( '%s', -1.5 * _LMT_TIMESTEP, '%s', _LMT_TIMESTEP / 2.0 )

### Find the most recent timestamp for each OST before a given time range.  This
### is to calculate the first row of diffs for a time range.  There is an
### implicit assumption that there will be at least one valid data point for
### each OST in the 24 hours preceding t_start.  If this is not the case, not
### every OST will be represented in the output of this query.
_QUERY_FIRST_OST_DATA = """
SELECT
    OST_INFO.OST_NAME,
    TIMESTAMP_INFO.`TIMESTAMP`,
    OST_DATA.READ_BYTES,
    OST_DATA.WRITE_BYTES
FROM
    (
        SELECT
            OST_DATA.OST_ID AS ostid,
            MAX(OST_DATA.TS_ID) AS newest_tsid
        FROM
            OST_DATA
        INNER JOIN TIMESTAMP_INFO ON TIMESTAMP_INFO.TS_ID = OST_DATA.TS_ID
        WHERE
            TIMESTAMP_INFO.`TIMESTAMP` < '%s'
        AND TIMESTAMP_INFO.`TIMESTAMP` > ADDTIME(
            '%s',
            '-24:00:00.0'
        )
        GROUP BY
            OST_DATA.OST_ID
    ) AS last_ostids
INNER JOIN OST_DATA ON last_ostids.newest_tsid = OST_DATA.TS_ID AND last_ostids.ostid = OST_DATA.OST_ID
INNER JOIN OST_INFO on OST_INFO.OST_ID = last_ostids.ostid
INNER JOIN TIMESTAMP_INFO ON TIMESTAMP_INFO.TS_ID = last_ostids.newest_tsid
"""

def get_ost_data( t_start, t_stop, mode='mysql' ):
    """
    Wrapper for backend-specific data dumpers that retrieve all counter data
    between two timestamps
    """
    if mode == 'pickle':
        return _ost_data_from_pickle( t_start, t_stop )
    else:
        return _ost_data_from_mysql( t_start, t_stop )

def _ost_data_from_mysql( t_start, t_stop ):
    """
    Generator function that connects to MySQL, runs a query, and buffers output
    """
    if LMT_PASSWORD:
        db = MySQLdb.connect( host=LMT_HOST, user=LMT_USER, passwd=LMT_PASSWORD, db=LMT_DB)
    else:
        db = MySQLdb.connect( host=LMT_HOST, user=LMT_USER, db=LMT_DB)

    query_str = _QUERY_OST_DATA % ( 
        t_start.strftime( _DATE_FMT ), 
        t_stop.strftime( _DATE_FMT ) 
    )

    try:
        cursor = db.cursor()
        cursor.execute( query_str )

        while True:
            rows = cursor.fetchmany(_MYSQL_FETCHMANY_LIMIT)
            if rows == ():
                break
            for row in rows:
                yield row
    except:
        db.close()
        raise

    db.close()


def _ost_data_from_pickle( t_start, t_stop ):
    """
    Generator function that reads the output of a previous MySQL query from a
    pickle file
    """
    pickle_file = "ost_data.%d-%d.pickle" % (
        int(time.mktime( t_start.timetuple() )),
        int(time.mktime( t_stop.timetuple() )) )
    with open( pickle_file, 'r' ) as fp:
        from_pickle = pickle.load( fp )

    for row in from_pickle:
        yield row

def _ost_data_to_pickle( t_start, t_stop, mode='mysql' ):
    """
    Retrieve and pickle a set of OST data
    """
    to_pickle = []
    for row in get_ost_data( t_start, t_stop, mode ):
        to_pickle.append( row )
    pickle_file = "ost_data.%d-%d.pickle" % (
        int(time.mktime( t_start.timetuple() )),
        int(time.mktime( t_stop.timetuple() )) )
    with open( pickle_file, 'w' ) as fp:
        pickle.dump( to_pickle, fp )

def _get_index_from_time( t ):
    return (t.hour * 3600 + t.minute * 60 + t.second) / int(_LMT_TIMESTEP)

def _fix_broken_first_ost( ostname, t_start, t_stop ):
    """
    If the record corresponding to t_start is preceded by a missing record, we must go
    back in time further to 
    """

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument( 'tstart', type=str, help="lower bound of time to scan, in YYY-mm-dd HH:MM:SS format" )
    parser.add_argument( 'tstop', type=str, help="upper bound of time to scan, in YYY-mm-dd HH:MM:SS format" )
    parser.add_argument( '--read-pickle',  action='store_true', help="attempt to read from pickle instead of MySQL" )
    parser.add_argument( '--write-pickle', action='store_true', help="write output to pickle" )
    args = parser.parse_args()
    if not ( args.tstart and args.tstop ):
        parser.print_help()
        sys.exit(1)
    if args.read_pickle and args.write_pickle:
        sys.stderr.write("--read-pickle and --write-pickle are mutually exclusive\n" )
        sys.exit(1)

    try:
        t_start = datetime.datetime.strptime( args.tstart, _DATE_FMT )
        t_stop = datetime.datetime.strptime( args.tstop, _DATE_FMT )
    except ValueError:
        sys.stderr.write("Start and end times must be in format %s\n" % _DATE_FMT );
        raise

    mode='mysql'
    if args.read_pickle:
        mode='pickle'

    if args.write_pickle:
        _ost_data_to_pickle( t_start, t_stop )
    else:
        for tup_out in get_ost_data( t_start, t_stop, mode=mode ):
            print _get_index_from_time( tup_out[0] ), '|'.join( [ str(x) for x in tup_out ] )
