#!/usr/bin/env python
"""
This module provides generic infrastructure for retrieving data from a
relational database that contains immutable data.  It can use a local caching
database (sqlite3) to allow for reanalysis on platforms that cannot access the
original remote database or to reduce the load on remote databases.
"""

import warnings
try:
    import pymysql
    pymysql.install_as_MySQLdb()
except ImportError:
    pass
try:
    import MySQLdb
except ImportError:
    pass

import sqlite3

HIT_CACHE_DB = 1
HIT_REMOTE_DB = 2

class CachingDb(object):
    """Connect relational database with an optional caching layer interposed.
    """
    #pylint: disable=too-many-arguments
    def __init__(self, dbhost=None, dbuser=None, dbpassword=None, dbname=None, cache_file=None):
        """Connect to a relational database.

        If instantiated with a cache_file argument, all queries will go to that
        SQLite-based cache database.  If this class is not instantiated with a
        cache_file argument, all queries will go out to the remote database.

        If none of the connection arguments (``db*``) are specified, do not
        connect to a remote database and instead rely entirely on the caching
        database or a separate call to the ``connect()`` method.

        Arguments:
            dbhost (str, optional): hostname for the remote database
            dbuser (str, optional): username to use when connecting to database
            dbpassword (str, optional): password for authenticating to database
            dbname (str, optional): name of database to use when connecting
            cache_file (str, optional):  Path to an SQLite3 database to use as
                a caching layer.

        Attributes:
            saved_results (dict): in-memory data cache, keyed by table names
                and whose values are dictionaries with keys ``rows`` and
                ``schema``.  ``rows`` are a list of row tuples returned
                from earlier queries, and ``schema`` is the SQL statement
                required to create the table corresponding to ``rows``.
            last_hit (int): a flag to indicate whether the last query was
                found in the caching database or the remote database
            cache_file (str): path to the caching database's file
            cache_db (sqlite3.Connection): caching database connection handle
            cache_db_ps (str): paramstyle of the caching database as defined
                by `PEP-0249`_
            remote_db: remote database connection handle
            remote_db_ps (str): paramstyle of the remote database as defined
                by `PEP-0249`_

        .. _PEP-0249: https://www.python.org/dev/peps/pep-0249
        """

        # self.saved_results is the in-memory data cache.  It has a structure of
        # saved_results = {
        #    'table1': {
        #       'rows': [ (row1), (row2), (row3), ... ],
        #       'schema': 'create table if not exists table1(...)',
        #    }
        #    'table2': { ... }
        #    ...
        # }
        self.saved_results = {}
        self.last_hit = None

        # cache db
        self.cache_file = None
        self.cache_db = None
        self.cache_db_ps = None

        # actual db
        self.remote_db = None
        self.remote_db_ps = None

        # Connect to cache db if specified
        if cache_file is not None:
            self.connect_cache(cache_file)
            # TODO: check to ensure db isn't empty
#           result = self.query('select * from summary limit 1', (), nocache=True)
#           if len(result) == 0:
#               warnings.warn("Using an empty cache database; queries will return nothing")

        if dbhost is not None \
        and dbuser is not None \
        and dbpassword is not None \
        and dbname is not None:
            self.connect(dbhost=dbhost,
                         dbuser=dbuser,
                         dbpassword=dbpassword,
                         dbname=dbname)

    def connect(self, dbhost, dbuser, dbpassword, dbname):
        """Establish remote db connection.

        Connects to a remote MySQL database and defines the connection handler
        and paramstyle attributes.

        Args:
            dbhost (str): hostname for the remote database
            dbuser (str): username to use when connecting to database
            dbpassword (str): password for authenticating to database
            dbname (str): name of database to use when connecting
        """
        if self.cache_db is not None:
            # can't really do both local and remote dbs; all queries will be
            # run against the cache_db, which is probably not what someone
            # wants
            warnings.warn("attempting to use both remote and cache db; disabling cache db")
            self.close_cache()

        self.remote_db = MySQLdb.connect(host=dbhost,
                                         user=dbuser,
                                         passwd=dbpassword,
                                         db=dbname)
        self.remote_db_ps = get_paramstyle_symbol(MySQLdb.paramstyle)

    def close(self):
        """Destroy connection objects.

        Close the remote database connection handler and reset state of remote
        connection attributes.
        """
        self.remote_db = None
        self.remote_db_ps = None

    def connect_cache(self, cache_file):
        """Open the cache database file and set the handler attribute.

        Args:
            cache_file (str): Path to the SQLite3 caching database file to be
                used.
        """
        if cache_file is not None:
            self.cache_db = sqlite3.connect(cache_file)
            self.cache_file = cache_file
            self.cache_db_ps = get_paramstyle_symbol(sqlite3.paramstyle)

    def close_cache(self):
        """Close the cache database handler and reset caching db attributes.
        """
        if self.cache_db is not None:
            self.cache_db = self.cache_db.close()
            self.cache_file = None

    def drop_cache(self, tables=None):
        """Flush saved results from memory.

        If tables are specified, only drop those tables' results.  If no tables
        are provided, flush everything.

        Args:
            tables (list, optional): List of table names (str) to flush.  If
                omitted, flush all tables in cache.
        """
        drop_caches = set([])
        for table in self.saved_results:
            if tables is None or table in tables:
                drop_caches.add(table)

        for drop_cache in drop_caches:
            del self.saved_results[drop_cache]

    def save_cache(self, cache_file):
        """Commit the in-memory cache to a cache database.

        This method is currently very memory-inefficient and not good for
        caching giant pieces of a database without something wrapping it to feed
        it smaller pieces.

        Note:
            This manipulates the ``cache_db*`` attributes in a dirty way
            to prevent closing and re-opening the original cache db.  If the
            ``self.open_cache()`` is ever changed to include tracking more
            state, this function must also be updated to retain that state while
            the old cache db state is being temporarily shuffled out.

        Args:
            cache_file (str): Path to the cache file to be used to write out
                the cache contents.  This file will temporarily pre-empt the
                `cache_file` attribute and should be a different file.
        """
        ### Shuffle out the old cache db state (if it exists)
        old_state = {}
        if self.cache_file is not None and self.cache_file != cache_file:
            old_state = {
                'cache_file': self.cache_file,
                'cache_db': self.cache_db,
                'cache_db_ps': self.cache_db_ps,
            }

        ### Open a new cache db connection without closing the old cache db
        self.connect_cache(cache_file)

        ### Commit each table we've retained in memory
        drop_caches = set([])
        for table, table_info in self.saved_results.items():
            if len(self.saved_results[table]['rows']) < 1:
                warnings.warn("table %s has no rows" % table)
                continue

            num_fields = None
            ### Verify and preprocess each saved row
            for row in self.saved_results[table]['rows']:
                if num_fields is None:
                    num_fields = len(row)

                ### Verify that the rows we've saved are actually all of the
                ### same length so that they have a hope of being inserted
                ### into the schema
                if len(row) != num_fields:
                    warnings.warn(
                        "saved_results[%s] contains non-uniform rows (%d, %d); skipping table" %
                        table, len(row), num_fields)
                    continue
                else:
                    ### Prepend table name to row to facilitate the bulk insert
                    ### query below
#                   self.saved_results[table]['rows'][index] = (table,) + row
                    pass

            ### Create the table (if necessary).  This will throw all sorts of
            ### exceptions if
            ###   (1) the table doesn't already exist in the cache database, or
            ###   (2) 'schema' isn't set correctly by the downstream application
            if table_info['schema'] is not None:
                self.cache_db.execute(
                    "CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY(%s))" %
                    (table,
                     ', '.join(table_info['schema']['columns']),
                     ', '.join(table_info['schema']['primary_key'])))

            ### INSERT OR REPLACE so that the cache db never wins if a duplicate
            ### primary key is detected
            query_str = "insert or replace into %s values (%s)" % (table, ','.join(['?'] * num_fields))
            self.cache_db.executemany(
                query_str,
                table_info['rows'])
            self.cache_db.commit()

            ### Drop committed rows from memory
            drop_caches.add(table)

        for drop_cache in drop_caches:
            del self.saved_results[drop_cache]

        self.close_cache()

        ### Shuffle back in the state of the old cache db
        if len(old_state) > 0:
            self.cache_file = old_state['cache_file']
            self.cache_db = old_state['cache_db']
            self.cache_db_ps = old_state['cache_db_ps']

    def query(self, query_str, query_variables=(), table=None, table_schema=None):
        """Pass a query through all layers of cache and return on the first hit.

        If a table is specified, the results of this query can be saved to the
        cache db into a table of that name.

        Args:
            query_str (str): SQL query expressed as a string
            query_variables (tuple): parameters to be substituted into
                `query_str` if `query_str` is a parameterized query
            table (str, optional): name of table in the cache database to save
                the results of the query
            table_schema (str, optional): when `table` is specified, the SQL
                line to initialize the table in which the query results will
                be cached.

        Returns:
            tuple: Tuple of tuples corresponding to rows of fields as returned
            by the SQL query.
        """

        ### Collapse query string to remove extraneous whitespace
        query_str = ' '.join(query_str.split())

        ### Check the cache database (if available)
        if self.cache_db is not None:
            results = self._query_sqlite3(query_str, query_variables)
            self.last_hit = HIT_CACHE_DB
        ### Check the MySQL database (if available)
        elif self.remote_db is not None:
            results = self._query_mysql(query_str, query_variables)
            self.last_hit = HIT_REMOTE_DB
        else:
            raise RuntimeError('No databases available to query')

        if table is not None:
            ### Initialize the table if our intent is to save the result of this
            ### query.
            if table not in self.saved_results:
                self.saved_results[table] = {
                    'rows': [],
                    'schema': None,
                }
            ### Table schema can be defined or re-defined on any query.  It is
            ### up to the downstream application to manage this correctly.
            if table_schema is not None:
                self.saved_results[table]['schema'] = table_schema

            ### Append our results
            self.saved_results[table]['rows'] += list(results)

        return results

    def _query_sqlite3(self, query_str, query_variables):
        """Run a query against the cache database and return the full output.

        Args:
            query_str (str): SQL query expressed as a string
            query_variables (tuple): parameters to be substituted into
                `query_str` if `query_str` is a parameterized query
        """
        cursor = self.cache_db.cursor()
        if '%(ps)' in query_str:
            query_str = query_str % {'ps': self.cache_db_ps}
        cursor.execute(query_str, query_variables)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def _query_mysql(self, query_str, query_variables):
        """Run a query against the MySQL database and return the full output.

        Args:
            query_str (str): SQL query expressed as a string
            query_variables (tuple): parameters to be substituted into
                `query_str` if `query_str` is a parameterized query
        """
        cursor = self.remote_db.cursor()
        if '%(ps)' in query_str:
            query_str = query_str % {'ps': self.remote_db_ps}
        cursor.execute(query_str, query_variables)
        rows = cursor.fetchall()
        cursor.close()
        return rows

def get_paramstyle_symbol(paramstyle):
    """Infer the correct paramstyle for a database.paramstyle

    Provides a generic way to determine the paramstyle of a database connection
    handle.  See `PEP-0249`_ for more information.

    Args:
        paramstyle (str): Result of a generic database handler's `paramstyle`
            attribute

    Returns:
        str: The string corresponding to the paramstyle of the given database
        connection.

    .. _PEP-0249: https://www.python.org/dev/peps/pep-0249
    """
    if paramstyle == 'qmark':
        return "?"
    elif paramstyle == 'format' or paramstyle == 'pyformat':
        return "%s"
    else:
        raise Exception("Unsupported paramstyle %s" % paramstyle)
