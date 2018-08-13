#!/usr/bin/env python
"""
Interface with an LMT database.  Provides wrappers for common queries using the
CachingDb class.
"""

import os
import datetime
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

class LmtDb(cachingdb.CachingDb):
    """
    Class to wrap the connection to an LMT MySQL database or SQLite database
    """
    def __init__(self, dbhost=None, dbuser=None, dbpassword=None, dbname=None, cache_file=None):
        """
        Initialize LmtDb with either a MySQL or SQLite backend
        """
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
        # fetch and cache it here.  Also maintain a mapping of OST_ID to
        # OST_NAME so we don't have to do JOINs in SQL; this will not scale
        # to gargantuan Lustre clusters though.
        self.ost_names = []
        self.ost_id_map = {}
        for row in self.query('SELECT OST_ID, OST_NAME FROM OST_INFO'):
            self.ost_names.append(row[1])
            self.ost_id_map[row[0]] = row[1]
        self.ost_names = tuple(self.ost_names)

        # Do the same for OSSes
        self.oss_names = []
        self.oss_id_map = {}
        for row in self.query('SELECT OSS_ID, HOSTNAME FROM OSS_INFO'):
            self.oss_names.append(row[1])
            self.oss_id_map[row[0]] = row[1]
        self.oss_names = tuple(self.oss_names)

        # Do the same for MDSes
        self.mds_names = []
        self.mds_id_map = {}
        for row in self.query('SELECT MDS_ID, HOSTNAME FROM MDS_INFO'):
            self.mds_names.append(row[1])
            self.mds_id_map[row[0]] = row[1]
        self.mds_names = tuple(self.mds_names)

        # Do the same for MDS operations
        self.mds_op_names = []
        self.mds_op_id_map = {}
        for row in self.query('SELECT OPERATION_ID, OPERATION_NAME FROM OPERATION_INFO'):
            self.mds_op_names.append(row[1])
            self.mds_op_id_map[row[0]] = row[1]
        self.mds_op_names = tuple(self.mds_op_names)

    def get_ts_ids(self, datetime_start, datetime_end):
        """
        Given a starting and ending time, return the lowest and highest ts_id
        values that encompass those dates, inclusive.
        """
        # First figure out the timestamp range
        query_str = "SELECT TIMESTAMP_INFO.TS_ID FROM TIMESTAMP_INFO WHERE TIMESTAMP >= %(ps)s AND TIMESTAMP <= %(ps)s"
        query_variables = (
            datetime_start.strftime("%Y-%m-%d %H:%M:%S"),
            datetime_end.strftime("%Y-%m-%d %H:%M:%S"))

        result = self.query(query_str=query_str,
                            query_variables=query_variables)
        ts_ids = [x[0] for x in result]
        ### TODO: make sure this works (it's templated down in test_bin_cache_lmtdb.py)
        return min(ts_ids), max(ts_ids)

    def get_timeseries_data(self, table, datetime_start, datetime_end, timechunk=datetime.timedelta(hours=1)):
        """
        Break a timeseries query into smaller queries over smaller time ranges.
        This is an optimization to avoid the O(N*M) scaling of the JOINs in the
        underlying SQL query.
        """
        table_schema = LMTDB_TABLES.get(table.upper())
        if table_schema is None:
            raise KeyError("Table '%s' is not valid" % table)
        else:
            result_columns = ['TIMESTAMP'] + table_schema['columns']
        format_dict = {
            'schema': ', '.join(result_columns).replace("TS_ID,", "TIMESTAMP_INFO.TS_ID,"),
            'table': table,
        }

        index0 = len(self.saved_results.get(table, {'rows': []})['rows'])
        chunk_start = datetime_start
        while chunk_start < datetime_end:
            if timechunk is None:
                chunk_end = datetime_end
            else:
                chunk_end = chunk_start + timechunk
            if chunk_end > datetime_end:
                chunk_end = datetime_end
            start_stamp = chunk_start.strftime("%Y-%m-%d %H:%M:%S")
            end_stamp = chunk_end.strftime("%Y-%m-%d %H:%M:%S")

            query_str = """SELECT
                               %(schema)s
                           FROM
                               %(table)s
                           INNER JOIN TIMESTAMP_INFO ON TIMESTAMP_INFO.TS_ID = %(table)s.TS_ID
                           WHERE
                               TIMESTAMP_INFO.TIMESTAMP >= %%(ps)s
                               AND TIMESTAMP_INFO.TIMESTAMP < %%(ps)s
                           """ % format_dict
            self.query(query_str, (start_stamp, end_stamp), table=table, table_schema=table_schema)
            if timechunk is not None:
                chunk_start += timechunk

        return self.saved_results[table]['rows'][index0:], result_columns

    def get_mds_data(self, datetime_start, datetime_end, timechunk=datetime.timedelta(hours=1)):
        """Schema-agnostic method for retrieving MDS load data.

        Wraps get_timeseries_data() but fills in the exact table name used in
        the LMT database schema.

        Args:
            datetime_start (datetime.datetime): lower bound on time series data
                to retrieve, inclusive
            datetime_End (datetime.datetime): upper bound on time series data to
                retrieve, exclusive
            timechunk (datetime.timedelta): divide time range query into
                sub-ranges of this width to work around N*N scaling of JOINs

        Returns:
            Tuple of (results, column names)  where results are tuples of tuples
            as returned by the MySQL query and column names are the names of
            each column expressed in the individual rows of results.
        """
        return self.get_timeseries_data('MDS_DATA',
                                        datetime_start,
                                        datetime_end,
                                        timechunk=timechunk)

    def get_mds_ops_data(self, datetime_start, datetime_end, timechunk=datetime.timedelta(hours=1)):
        """Schema-agnostic method for retrieving metadata operations data.

        Wraps get_timeseries_data() but fills in the exact table name used in
        the LMT database schema.

        Args:
            datetime_start (datetime.datetime): lower bound on time series data
                to retrieve, inclusive
            datetime_End (datetime.datetime): upper bound on time series data to
                retrieve, exclusive
            timechunk (datetime.timedelta): divide time range query into
                sub-ranges of this width to work around N*N scaling of JOINs

        Returns:
            Tuple of (results, column names)  where results are tuples of tuples
            as returned by the MySQL query and column names are the names of
            each column expressed in the individual rows of results.
        """
        return self.get_timeseries_data('MDS_OPS_DATA',
                                        datetime_start,
                                        datetime_end,
                                        timechunk=timechunk)

    def get_oss_data(self, datetime_start, datetime_end, timechunk=datetime.timedelta(hours=1)):
        """Schema-agnostic method for retrieving OSS data.

        Wraps get_timeseries_data() but fills in the exact table name used in
        the LMT database schema.

        Args:
            datetime_start (datetime.datetime): lower bound on time series data
                to retrieve, inclusive
            datetime_End (datetime.datetime): upper bound on time series data to
                retrieve, exclusive
            timechunk (datetime.timedelta): divide time range query into
                sub-ranges of this width to work around N*N scaling of JOINs

        Returns:
            Tuple of (results, column names)  where results are tuples of tuples
            as returned by the MySQL query and column names are the names of
            each column expressed in the individual rows of results.
        """
        return self.get_timeseries_data('OSS_DATA',
                                        datetime_start,
                                        datetime_end,
                                        timechunk=timechunk)

    def get_ost_data(self, datetime_start, datetime_end, timechunk=datetime.timedelta(hours=1)):
        """Schema-agnostic method for retrieving OST data.

        Wraps get_timeseries_data() but fills in the exact table name used in
        the LMT database schema.

        Args:
            datetime_start (datetime.datetime): lower bound on time series data
                to retrieve, inclusive
            datetime_End (datetime.datetime): upper bound on time series data to
                retrieve, exclusive
            timechunk (datetime.timedelta): divide time range query into
                sub-ranges of this width to work around N*N scaling of JOINs

        Returns:
            Tuple of (results, column names)  where results are tuples of tuples
            as returned by the MySQL query and column names are the names of
            each column expressed in the individual rows of results.
        """
        return self.get_timeseries_data('OST_DATA',
                                        datetime_start,
                                        datetime_end,
                                        timechunk=timechunk)
