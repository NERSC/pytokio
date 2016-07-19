#!/usr/bin/env python

import os
import sys
import time
import datetime
import cPickle as pickle
import MySQLdb
import tokio

_LMT_TIMESTEP = 5.0

_DATE_FMT = "%Y-%m-%d %H:%M:%S"

_MYSQL_FETCHMANY_LIMIT = 10000

_QUERY_OST_DATA = """
SELECT
    UNIX_TIMESTAMP(TIMESTAMP_INFO.`TIMESTAMP`) as ts,
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
    UNIX_TIMESTAMP(TIMESTAMP_INFO.`TIMESTAMP`),
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

_QUERY_TIMESTAMP_MAPPING = """
SELECT
    UNIX_TIMESTAMP(`TIMESTAMP`)
FROM
    TIMESTAMP_INFO
WHERE
    `TIMESTAMP` >= '%s'
AND `TIMESTAMP` < '%s'
ORDER BY
    TS_ID
"""

def connect(*args, **kwargs):
    return LMTDB( *args, **kwargs )

class LMTDB(object):
    def __init__(self, dbhost=None, dbuser=None, dbpassword=None, dbname=None, pickles=None):
        self.pickles = pickles
        if pickles is None:
            if dbhost is None:
                dbhost = os.environ.get('PYLMT_HOST')
            if dbuser is None:
                dbuser = os.environ.get('PYLMT_USER')
            if dbpassword is None:
                dbpassword = os.environ.get('PYLMT_PASSWORD')
            if dbname is None:
                dbname = os.environ.get('PYLMT_DB')
            self.db = MySQLdb.connect( 
                host=dbhost, 
                user=dbuser, 
                passwd=dbpassword, 
                db=dbname)
        else:
            self.db = None
    def __enter__(self):
        return self
    def __die__(self):
        if self.db:
            self.db.close()
            sys.stderr.write('closing DB connection\n')
    def __exit__(self, exc_type, exc_value, traceback):
        if self.db:
            self.db.close()
            sys.stderr.write('closing DB connection\n')

    def close( self ):
        if self.db:
            self.db.close()
            sys.stderr.write('closing DB connection\n')

    def get_ost_data( self, t_start, t_stop ):
        """
        Wrapper for backend-specific data dumpers that retrieve all counter data
        between two timestamps
        """
        if self.pickles is not None:
            return self._ost_data_from_pickle( t_start, t_stop )
        else:
            return self._ost_data_from_mysql( t_start, t_stop )

    def get_timestamp_map( self, t_start, t_stop ):
        """
        Get the timestamps associated with a t_start/t_stop from LMT
        """
        query_str = _QUERY_TIMESTAMP_MAPPING % (
            t_start.strftime( _DATE_FMT ), 
            t_stop.strftime( _DATE_FMT ) 
        )
        return self._query_mysql( query_str )


    def _ost_data_from_mysql( self, t_start, t_stop ):
        """
        Get read/write bytes data from LMT between [ t_start, t_stop ).  Split
        the time range into 1-hour chunks to avoid issuing massive JOINs to
        the LMT server and bogging down.
        """
        _TIME_CHUNK = datetime.timedelta(hours=1)

        t0 = t_start
        while t0 < t_stop:
            tf = t0 + _TIME_CHUNK
            if tf > t_stop:
                tf = t_stop
            query_str = _QUERY_OST_DATA % ( 
                t0.strftime( _DATE_FMT ), 
                tf.strftime( _DATE_FMT ) 
            )
            tokio._debug_print( "Retrieving %s >= t > %s" % (
                t0.strftime( _DATE_FMT ),
                tf.strftime( _DATE_FMT ) ) )
            t0 += _TIME_CHUNK
            for ret_tup in self._query_mysql( query_str ):
                yield ret_tup
        tokio._debug_print( "Finished because t0(=%s) !< t_stop(=%s)" % (
                t0.strftime( _DATE_FMT ), 
                tf.strftime( _DATE_FMT ) 
            ))


    def _query_mysql( self, query_str ):
        """
        Generator function that connects to MySQL, runs a query, and buffers output
        """
        cursor = self.db.cursor()
        t0 = time.time()
        cursor.execute( query_str )
        tokio._debug_print("Executed query in %f sec" % ( time.time() - t0 ))

        ### Iterate over chunks of output
        while True:
            tbench0 = time.time()
            rows = cursor.fetchmany(_MYSQL_FETCHMANY_LIMIT)
            if rows == ():
                break
            for row in rows:
                yield row
            tokio._debug_print("%d rows fetched in %f sec" % (_MYSQL_FETCHMANY_LIMIT, time.time() - tbench0))


    def _query_mysql_fetchmany( self, query_str ):
        """
        Generator function that connects to MySQL, runs a query, and buffers
        output.  Returns multiple rows at once.
        """
        t0 = time.time()
        cursor = self.db.cursor()
        cursor.execute( query_str ) ### this is what takes a long time
        tokio._debug_print("Executed query in %f sec" % ( time.time() - t0 ))

        while True:
            t0 = time.time()
            rows = cursor.fetchmany(_MYSQL_FETCHMANY_LIMIT)
            tokio._debug_print("fetchmany took %f sec" % ( time.time() - t0 ))
            if rows == ():
                break
            yield rows


    def _ost_data_from_pickle( self, t_start, t_stop, pickle_file=None ):
        """
        Generator function that reads the output of a previous MySQL query from a
        pickle file
        """
        if not pickle_file:
            pickle_file = "ost_data.%d-%d.pickle" % (
                int(time.mktime( t_start.timetuple() )),
                int(time.mktime( t_stop.timetuple() )) )

        with open( pickle_file, 'r' ) as fp:
            from_pickle = pickle.load( fp )

        for row in from_pickle:
            yield row

    def _pickle_ost_data( self, t_start, t_stop, pickle_file=None ):
        """
        Retrieve and pickle a set of OST data
        """
        to_pickle = []
        for row in self.get_ost_data( t_start, t_stop ):
            to_pickle.append( row )
        if not pickle_file:
            pickle_file = "ost_data.%d-%d.pickle" % (
                int(time.mktime( t_start.timetuple() )),
                int(time.mktime( t_stop.timetuple() )) )
        with open( pickle_file, 'w' ) as fp:
            pickle.dump( to_pickle, fp )

if __name__ == '__main__':
    pass
