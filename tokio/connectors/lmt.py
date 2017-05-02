#!/usr/bin/env python

import os
import sys
import time
import datetime
import MySQLdb
import numpy as np
import tokio

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
    TIMESTAMP_INFO.`TIMESTAMP` >= '%s'
AND TIMESTAMP_INFO.`TIMESTAMP` < '%s'
ORDER BY ts, ostname;
"""

### Find the most recent timestamp for each OST before a given time range.  This
### is to calculate the first row of diffs for a time range.  There is an
### implicit assumption that there will be at least one valid data point for
### each OST in the 24 hours preceding t_start.  If this is not the case, not
### every OST will be represented in the output of this query.
_QUERY_FIRST_OST_DATA = """
SELECT
    UNIX_TIMESTAMP(TIMESTAMP_INFO.`TIMESTAMP`),
    OST_INFO.OST_NAME,
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
            TIMESTAMP_INFO.`TIMESTAMP` < '{datetime}'
        AND TIMESTAMP_INFO.`TIMESTAMP` > SUBTIME(
            '{datetime}',
            '{lookbehind}'
        )
        GROUP BY
            OST_DATA.OST_ID
    ) AS last_ostids
INNER JOIN OST_DATA ON last_ostids.newest_tsid = OST_DATA.TS_ID AND last_ostids.ostid = OST_DATA.OST_ID
INNER JOIN OST_INFO on OST_INFO.OST_ID = last_ostids.ostid
INNER JOIN TIMESTAMP_INFO ON TIMESTAMP_INFO.TS_ID = last_ostids.newest_tsid
"""

def connect(*args, **kwargs):
    return LMTDB( *args, **kwargs )

class LMTDB(object):
    def __init__(self, dbhost=None, dbuser=None, dbpassword=None, dbname=None):
        if dbhost is None:
            dbhost = os.environ.get('PYLMT_HOST')
        if dbuser is None:
            dbuser = os.environ.get('PYLMT_USER')
        if dbpassword is None:
            dbpassword = os.environ.get('PYLMT_PASSWORD')
        if dbname is None:
            dbname = os.environ.get('PYLMT_DB')

        ### establish db connection
        self.db = MySQLdb.connect( 
            host=dbhost,
            user=dbuser,
            passwd=dbpassword,
            db=dbname)

        ### the list of OST names is an immutable property of a database, so
        ### fetch and cache it here
        self.ost_names = []
        for row in self._query_mysql('SELECT DISTINCT OST_NAME FROM OST_INFO ORDER BY OST_NAME;'):
            self.ost_names.append(row[0])
        self.ost_names = tuple(self.ost_names)

        ### do the same for OSSes
        self.oss_names = []
        for row in self._query_mysql('SELECT DISTINCT HOSTNAME FROM OSS_INFO ORDER BY HOSTNAME;'):
            self.oss_names.append(row[0])
        self.oss_names = tuple(self.oss_names)

        ### do the same for MDSes
        self.mds_names = []
        for row in self._query_mysql('SELECT DISTINCT MDS_NAME FROM MDS_INFO ORDER BY MDS_NAME;'):
            self.mds_names.append(row[0])
        self.mds_names = tuple(self.mds_names)

        ### do the same for MDS operations
        self.mds_op_names = []
        for row in self._query_mysql('SELECT DISTINCT OPERATION_NAME FROM OPERATION_INFO ORDER BY OPERATION_ID;'):
            self.mds_op_names.append(row[0])
        self.mds_op_names = tuple(self.mds_op_names)

    ### TODO: revisit the following methods and either implement them
    ### universally or drop them
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


    def get_rw_data( self, t_start, t_stop, timestep ):
        """
        Wrapper function for _get_rw_data that breaks a single large query into
        smaller queries over smaller time ranges.  This is an optimization to
        avoid the O(N*M) scaling of the JOINs in the underlying SQL query.
        """
        _TIME_CHUNK = datetime.timedelta(hours=1)
        t0 = t_start

        buf_r = None
        buf_w = None
        while t0 < t_stop:
            tf = t0 + _TIME_CHUNK
            if tf > t_stop:
                tf = t_stop
            ( tmp_r, tmp_w ) = self._get_rw_data( t0, tf, timestep )
            tokio._debug_print( "Retrieved %.2f GiB read, %.2f GiB written" % (
                 (tmp_r[-1,:].sum() - tmp_r[0,:].sum())/2**30,
                 (tmp_w[-1,:].sum() - tmp_w[0,:].sum())/2**30) )

            ### first chunk of output
            if buf_r is None:
                buf_r = tmp_r
                buf_w = tmp_w
            ### subsequent chunks get concatenated
            else:
                assert( tmp_r.shape[1] == buf_r.shape[1] )
                assert( tmp_w.shape[1] == buf_w.shape[1] )
                print buf_r.shape, tmp_r.shape
                buf_r = np.concatenate(( buf_r, tmp_r ), axis=0)
                buf_w = np.concatenate(( buf_w, tmp_w ), axis=0)
            t0 += _TIME_CHUNK

        tokio._debug_print( "Finished because t0(=%s) !< t_stop(=%s)" % (
                t0.strftime( _DATE_FMT ), 
                tf.strftime( _DATE_FMT ) ))
        return ( buf_r, buf_w )


    def _get_rw_data( self, t_start, t_stop, binning_timestep ):
        """
        Return a tuple of two objects:
            1. a M*N matrix of int64s that encode the total read bytes for N STs
               over M timesteps
            2. a M*N matrix of int64s that encode the total write bytes for N
               STs over M timesteps

        Time will be binned appropriately if binning_timestep > lmt_timestep.
        The number of OSTs (the N dimension) is derived from the database.
        """
        tokio._debug_print( "Retrieving %s >= t > %s" % (
            t_start.strftime( _DATE_FMT ),
            t_stop.strftime( _DATE_FMT ) ) )
        query_str = _QUERY_OST_DATA % ( 
            t_start.strftime( _DATE_FMT ), 
            t_stop.strftime( _DATE_FMT ) 
        )
        rows = self._query_mysql( query_str )

        ### Get the number of timesteps (# rows)
        ts_ct = int((t_stop - t_start).total_seconds() / binning_timestep)
        t0 = int(time.mktime(t_start.timetuple()))

        ### Get the number of OSTs and their names (# cols)
        ost_ct = len(self.ost_names)

        ### Initialize everything to -0.0; we use the signed zero to distinguish
        ### the absence of data from a measurement of zero
        buf_r = np.full( shape=(ts_ct, ost_ct), fill_value=-0.0, dtype='f8' )
        buf_w = np.full( shape=(ts_ct, ost_ct), fill_value=-0.0, dtype='f8' )

        if len(rows) > 0:
            for row in rows:
                icol = int((row[0] - t0) / binning_timestep)
                irow = self.ost_names.index( row[1] )
                buf_r[icol,irow] = row[2]
                buf_w[icol,irow] = row[3]

        return ( buf_r, buf_w )


    def get_last_rw_data_before( self, t, lookbehind=None ):
        """
        Get the last datum reported by each OST before the given timestamp t.
        Useful for calculating the change in bytes for the very first row
        returned by a query.

        Input:
            1. t is a datetime.datetime before which we want to find data
            2. lookbehind is a datetime.timedelta is how far back we're willing
               to look for valid data for each OST.  The larger this is, the
               slower the query
        Output is a tuple of:
            1. buf_r - a matrix of size (1, N) with the last read byte value for
               each of N OSTs
            2. buf_w - a matrix of size (1, N) with the last write byte value
               for each of N OSTs
            3. buf_t - a matrix of size (1, N) with the timestamp from which
               each buf_r and buf_w row datum was found
        """
        if lookbehind is None:
            lookbehind = datetime.timedelta(hours=1)

        lookbehind_str = "%d %02d:%02d:%02d" % (
            lookbehind.days,
            lookbehind.seconds / 3600, 
            lookbehind.seconds % 3600 / 60, 
            lookbehind.seconds % 60 )

        ost_ct = len(self.ost_names)
        buf_r = np.full( shape=(1, ost_ct), fill_value=-0.0, dtype='f8' )
        buf_w = np.full( shape=(1, ost_ct), fill_value=-0.0, dtype='f8' )
        buf_t = np.full( shape=(1, ost_ct), fill_value=-0.0, dtype='i8' )

        query_str = _QUERY_FIRST_OST_DATA.format( datetime=t.strftime( _DATE_FMT ), lookbehind=lookbehind_str )
        for tup in self._query_mysql( query_str ):
            try:
                tidx = self.ost_names.index( tup[1] )
            except ValueError:
                raise ValueError("unknown OST [%s] not present in %s" % (tup[1], self.ost_names))
            buf_r[0, tidx] = tup[2]
            buf_w[0, tidx] = tup[3]
            buf_t[0, tidx] = tup[0]

        return ( buf_r, buf_w, buf_t )


    def _query_mysql( self, query_str ):
        """
        Connects to MySQL, run a query, and yield the full output tuple.  No
        buffering or other witchcraft.
        """
        cursor = self.db.cursor()
        t0 = time.time()
        cursor.execute( query_str )
        tokio._debug_print("Executed query in %f sec" % ( time.time() - t0 ))

        t0 = time.time()
        rows = cursor.fetchall()
        tokio._debug_print("%d rows fetched in %f sec" % (_MYSQL_FETCHMANY_LIMIT, time.time() - t0))
        return rows


if __name__ == '__main__':
    pass
