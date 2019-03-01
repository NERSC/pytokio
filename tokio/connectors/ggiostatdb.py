#!/usr/bin/env python
"""
Interface with a GGIOSTAT database.
"""

import os
import datetime
import ibm_db

GGIOSTATDB_CLUSTERS = [
    'CETUS',
    'COOLEY',
    'DCM',
    'DTN',
    'ESS',
    'MIRA',
    'NSD',
    'THETA',
    'VESTA',
]

GGIOSTATDB_SCHEMA_PREFIX='GPFS_PERF_'

GGIOSTATDB_TABLES = {
    "FILESYSTEMS": {
        'columns': [
            'NAME',
            'INSERTED_TIMESTAMP',
            'UPDATED_TIMESTAMP',
        ],
        'primary_key': ['NAME'],
    },
    "NODES" : {
        'columns': [
            'NAME',
            'IP',
            'CLUSTER_NAME',
            'INSERTED_TIMESTAMP',
            'UPDATED_TIMESTAMP',
        ],
        'primary_key': ['NAME'],
    },
    "NODE_FILESYSTEM_IO": {
        'columns': [
            'CLUSTER_NAME',
            'NODE_NAME',
            'FILESYSTEM_NAME',
            'SECONDS',
            'MICROSECONDS',
            'BYTES_READ',
            'BYTES_WRITTEN',
            'OPEN_COUNT',
            'CLOSE_COUNT',
            'READ_REQUESTS',
            'WRITE_REQUESTS',
            'READ_DIRECTORY',
            'INODE_UPDATES',
        ],
        'primary_key': ['FILESYSTEM_NAME', 'SECONDS'],
    },
}

class GgiostatDb(object):
    """
    Class to wrap the connection to a GGIOSTAT DB2 database
    """
    def __init__(self, file_system=None, dbname=None, dbuser=None, dbpassword=None):
        """
        Initialize GgiostatDb with a DB2 backend
        """
        if file_system is None:
            raise Exception("Must provide filesystem name as input to GGIOSTATDB class")
        self.file_system = file_system

        # Get database parameters
        if dbname is None:
            dbname = os.environ.get('PYTOKIO_GGIOSTAT_DB')
        if dbuser is None:
            dbuser = os.environ.get('PYTOKIO_GGIOSTAT_USER')
        if dbpassword is None:
            dbpassword = os.environ.get('PYTOKIO_GGIOSTAT_PASSWORD')

        self.conn = ibm_db.connect(dbname, dbuser, dbpassword)

        self.node_names = []
        self.node_name_map = {}
        for cluster in GGIOSTATDB_CLUSTERS:
            query_str='SELECT NAME FROM GPFS_PERF_'+cluster+'.NODES'
            for row in self.query(query_str):
                self.node_names.append(row[0])
                self.node_name_map[row[0]] = cluster

        self.saved_results = {}

    def query(self, query_str, query_variables=()):

        ### Collapse query string to remove extraneous whitespace
        query_str = ' '.join(query_str.split())

        results = []
        stmt = ibm_db.exec_immediate(self.conn, query_str)
        result = ibm_db.fetch_tuple(stmt)
        while result != False:
            results.append(result)
            result = ibm_db.fetch_tuple(stmt)

        return results

    def get_cluster_fs_data(self, format_dict, datetime_start, datetime_end, timechunk=datetime.timedelta(hours=1)):
        query_format_dict = format_dict.copy()

        results = []
        chunk_start = datetime_start
        while chunk_start < datetime_end:
            if timechunk is None:
                chunk_end = datetime_end
            else:
                chunk_end = chunk_start + timechunk
            if chunk_end > datetime_end:
                chunk_end = datetime_end
            start_stamp = int(chunk_start.timestamp())
            end_stamp = int(chunk_end.timestamp())
            query_format_dict.update({'start_stamp': start_stamp})
            query_format_dict.update({'end_stamp': end_stamp})

            query_str = """SELECT
                               %(schema)s
                           FROM
                               %(table)s
                           WHERE
                               SECONDS >= %(start_stamp)s
                               AND SECONDS < %(end_stamp)s
                               AND FILESYSTEM_NAME = '%(file_system)s'
                           """ % query_format_dict
            results += list(self.query(query_str))
            if timechunk is not None:
                chunk_start += timechunk

        return results

    def get_fs_data(self, datetime_start, datetime_end, timechunk=datetime.timedelta(hours=1)):
        """Schema-agnostic method for retrieving FS data.

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

        fs_data_table = 'NODE_FILESYSTEM_IO'
        table_schema = GGIOSTATDB_TABLES.get(fs_data_table)
        if table_schema is None:
            raise KeyError("Table '%s' is not valid" % fs_data_table)
        else:
            result_columns = table_schema['columns']
            result_columns.remove('CLUSTER_NAME')
            result_columns.remove('MICROSECONDS')
            result_columns.remove('INODE_UPDATES')
        format_dict = {
            'schema': ', '.join(result_columns),
            'file_system': self.file_system,
        }

        if fs_data_table not in self.saved_results:
            self.saved_results[fs_data_table] = {
                'rows': [],
                'schema': table_schema,
            }

        index0 = len(self.saved_results.get(fs_data_table, {'rows': []})['rows'])
        for cluster in GGIOSTATDB_CLUSTERS:
            format_dict.update({'table': GGIOSTATDB_SCHEMA_PREFIX+cluster+'.'+fs_data_table})
            results = self.get_cluster_fs_data(format_dict, datetime_start, datetime_end, timechunk=timechunk)
            self.saved_results[fs_data_table]['rows'] += results

        return self.saved_results[fs_data_table]['rows'][index0:], result_columns
