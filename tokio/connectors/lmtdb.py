#!/usr/bin/env python
"""
Interface with an LMT database.  Provides wrappers for common queries using the
CachingDb class.
"""

import os
import time
import datetime
import numpy as np
from . import cachingdb

### Names and schemata of all LMT database tables worth caching
LMTDB_TABLES = {
    "FILESYSTEM_INFO": {
        'columns': [
            'FILESYSTEM_ID',
            'FILESYSTEM_NAME',
            'FILESYSTEM_MOUNT_NAME,'
            'SCHEMA_VERSION',
        ],
        'primary_key': ['FILESYSTEM_ID'],
    },
    "MDS_DATA": {
        'columns': [
            'MDS_ID',
            'TS_ID',
            'PCT_CPU',
            'KBYTES_FREE',
            'KBYTES_USED',
            'INODES_FREE',
            'INODES_USED',
        ],
        'primary_key': ['MDS_ID', 'TS_ID'],
    },
    "MDS_INFO": {
        'columns': [
            'MDS_ID',
            'FILESYSTEM_ID',
            'MDS_NAME',
            'HOSTNAME',
            'DEVICE_NAME'
        ],
        'primary_key': ['MDS_ID'],
    },
    "MDS_OPS_DATA": {
        'columns': [
            'MDS_ID',
            'TS_ID',
            'OPERATION_ID',
            'SAMPLES',
            'SUM',
            'SUMSQUARES',
        ],
        'primary_key': ['MDS_ID', 'TS_ID', 'OPERATION_ID'],
    },
    "MDS_VARIABLE_INFO": {
        'columns': [
            'VARIABLE_ID',
            'VARIABLE_NAME',
            'VARIABLE_LABEL',
            'THRESH_TYPE',
            'THRESH_VAL1',
            'THRESH_VAL2'
        ],
        'primary_key': ['VARIABLE_ID'],
    },
    "OPERATION_INFO": {
        'columns': [
            'OPERATION_ID',
            'OPERATION_NAME',
            'UNITS'
        ],
        'primary_key': ['OPERATION_ID'],
    },
    "OSS_DATA": {
        'columns': [
            'OSS_ID',
            'TS_ID',
            'PCT_CPU',
            'PCT_MEMORY'
        ],
        'primary_key': ['OSS_ID', 'TS_ID'],
    },
    "OSS_INFO": {
        'columns': [
            'OSS_ID',
            'FILESYSTEM_ID',
            'HOSTNAME',
            'FAILOVERHOST'
        ],
        'primary_key': [
            'OSS_ID',
            'HOSTNAME'
        ],
    },
    "OST_DATA": {
        'columns': [
            'OST_ID',
            'TS_ID',
            'READ_BYTES',
            'WRITE_BYTES',
            'PCT_CPU',
            'KBYTES_FREE',
            'KBYTES_USED',
            'INODES_FREE',
            'INODES_USED'
        ],
        'primary_key': ['OST_ID', 'TS_ID'],
    },
    "OST_INFO": {
        'columns': [
            'OST_ID',
            'OSS_ID',
            'OST_NAME',
            'HOSTNAME',
            'OFFLINE',
            'DEVICE_NAME'
        ],
        'primary_key': ['OST_ID'],
    },
    "OST_VARIABLE_INFO": {
        'columns': [
            'VARIABLE_ID',
            'VARIABLE_NAME',
            'VARIABLE_LABEL',
            'THRESH_TYPE',
            'THRESH_VAL1',
            'THRESH_VAL2'
        ],
        'primary_key': ['VARIABLE_ID'],
    },
    "TIMESTAMP_INFO": {
        'columns': ['TS_ID', 'TIMESTAMP'],
        'primary_key': ['TS_ID'],
    },
}

# Find the most recent timestamp for each OST before a given time range.  This
# is to calculate the first row of diffs for a time range.  There is an
# implicit assumption that there will be at least one valid data point for
# each OST in the 24 hours preceding t_start.  If this is not the case, not
# every OST will be represented in the output of this query.
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

class LmtDb(cachingdb.CachingDb):
    ### TODO: this needs a unit test
    def __init__(self, dbhost=None, dbuser=None, dbpassword=None, dbname=None, cache_file=None):
        # Get database parameters
        if dbhost is None:
            dbhost = os.environ.get('PYTOKIO_LMT_HOST')
        if dbuser is None:
            dbuser = os.environ.get('PYTOKIO_LMT_USER')
        if dbpassword is None:
            dbpassword = os.environ.get('PYTOKIO_LMT_PASSWORD')
        if dbname is None:
            dbname = os.environ.get('PYTOKIO_LMT_DB')

        super(LmtDb, self).__init__(
            dbhost=dbhost,
            dbuser=dbuser,
            dbpassword=dbpassword,
            dbname=dbname,
            cache_file=cache_file)

        # The list of OST names is an immutable property of a database, so
        # fetch and cache it here
        self.ost_names = []
        for row in self.query('SELECT DISTINCT OST_NAME FROM OST_INFO ORDER BY OST_NAME;'):
            self.ost_names.append(row[0])
        self.ost_names = tuple(self.ost_names)

        # Do the same for OSSes
        self.oss_names = []
        for row in self.query('SELECT DISTINCT HOSTNAME FROM OSS_INFO ORDER BY HOSTNAME;'):
            self.oss_names.append(row[0])
        self.oss_names = tuple(self.oss_names)

        # Do the same for MDSes
        self.mds_names = []
        for row in self.query('SELECT DISTINCT MDS_NAME FROM MDS_INFO ORDER BY MDS_NAME;'):
            self.mds_names.append(row[0])
        self.mds_names = tuple(self.mds_names)

        # Do the same for MDS operations
        self.mds_op_names = []
        for row in self.query('SELECT DISTINCT OPERATION_NAME FROM OPERATION_INFO ORDER BY OPERATION_ID;'):
            self.mds_op_names.append(row[0])
        self.mds_op_names = tuple(self.mds_op_names)

    ### TODO: this needs a unit test
    def get_ts_ids(self, datetime_start, datetime_end):
        """
        Given a starting and ending time, return the lowest and highest ts_id
        values that encompass those dates, inclusive.
        """
        # First figure out the timestamp range
        query_str = "SELECT TS_ID FROM TIMESTAMP_INFO WHERE TIMESTAMP >= %(ps)s AND TIMESTAMP <= %(ps)s"
        query_variables = (
            datetime_start.strftime("%Y-%m-%d %H:%M:%S"),
            datetime_end.strftime("%Y-%m-%d %H:%M:%S"))

        result = self.query(query_str=query_str,
                            query_variables=query_variables)
        ts_ids = [x[0] for x in result]
        ### TODO: make sure this works (it's templated down in test_bin_cache_lmtdb.py)
        return min(ts_ids), max(ts_ids)

    ### TODO: this needs a unit test
    def get_timeseries_data(self, table, datetime_start, datetime_end, timechunk=datetime.timedelta(hours=1)):
        """
        Wrapper for _get_timeseries_data that breaks a single large query into
        smaller queries over smaller time ranges.  This is an optimization to
        avoid the O(N*M) scaling of the JOINs in the underlying SQL query.
        """
        table_schema = LMTDB_TABLES.get(table.upper())
        if table_schema is None:
            raise KeyError("Table '%s' is not valid" % table)
        format_dict = {
            'schema': ', '.join(table_schema['columns']),
            'table': table,
        }

        index0 = len(self.saved_results.get(table, {'rows': []})['rows'])
        chunk_start = datetime_start
        ts_id_start, ts_id_end = self.get_ts_ids(datetime_start, datetime_end)
        while chunk_start < datetime_end:
            chunk_end = chunk_start + timechunk
            if chunk_end > datetime_end:
                chunk_end = datetime_end
            start_stamp = long(time.mktime(chunk_start.timetuple()))
            end_stamp = long(time.mktime(chunk_end.timetuple()))
            query_str = """SELECT
                               %(schema)s
                           FROM
                               %(table)s as t
                           INNER JOIN TIMESTAMP_INFO ON TIMESTAMP_INFO.TS_ID = %(table)s.TS_ID
                           WHERE
                               TIMESTAMP_INFO.TIMESTAMP >= %%(ps)s
                               AND TIMESTAMP_INFO.TIMESTAMP < %%(ps)s
                           """ % format_dict
            self.query(query_str, (start_stamp, end_stamp), table=table, table_schema=table_schema)
            chunk_start += timechunk

        return self.saved_results[table]['rows'][index0:]

    ### TODO: 9/28/2017 - this needs to be reviewed and kept/abandoned
    def get_last_rw_data_before(self, t, lookbehind=None):
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
        buf_r = np.full(shape=(1, ost_ct), fill_value=-0.0, dtype='f8')
        buf_w = np.full(shape=(1, ost_ct), fill_value=-0.0, dtype='f8')
        buf_t = np.full(shape=(1, ost_ct), fill_value=-0.0, dtype='i8')

        query_str = _QUERY_FIRST_OST_DATA.format(datetime=t.strftime(_DATE_FMT), lookbehind=lookbehind_str)
        for tup in self._query_mysql(query_str):
            try:
                tidx = self.ost_names.index(tup[1])
            except ValueError:
                raise ValueError("unknown OST [%s] not present in %s" % (tup[1], self.ost_names))
            buf_r[0, tidx] = tup[2]
            buf_w[0, tidx] = tup[3]
            buf_t[0, tidx] = tup[0]

        return (buf_r, buf_w, buf_t)
