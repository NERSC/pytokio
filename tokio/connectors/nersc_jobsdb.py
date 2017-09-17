#!/usr/bin/env python
"""
Extract job info from the NERSC jobs database.  Accessing the MySQL database is
not required (i.e., if you have everything stored in a cache, you never have to
touch MySQL).  However if you do have to connect to MySQL, you must set the
following environment variables:

    NERSC_JOBSDB_HOST
    NERSC_JOBSDB_USER
    NERSC_JOBSDB_PASSWORD
    NERSC_JOBSDB_DB

If you do not know what to use as these credentials, you will have to rely on
a cache database.
"""

import os
import warnings
try:
    import MySQLdb
except ImportError:
    pass
import sqlite3

CACHE_TABLE_SCHEMA = """
create table if not exists
    summary(stepid primary key, hostname, start integer, completion integer, numnodes integer)
"""

HIT_MEMORY   = 0
HIT_CACHE_DB = 1
HIT_REMOTE_DB = 2

class NerscJobsDb(object):
    """
    Connect to and interact with the NERSC jobs database.  Maintains a query
    cache where the results of queries are cached in memory.  If a query is
    repeated, its values are simply regurgitated from here rather than touching
    any databases.

    If this class is instantiated with a cache_file argument, all queries will
    go to that SQLite-based cache database if they are not found in the
    in-memory cache.

    If this class is not instantiated with a cache_file argument, all queries
    that do not exist in the in-memory cache will go out to the MySQL database.

    The in-memory query caching is possible because the job data in the NERSC
    jobs database is immutable and can be cached indefinitely once it appears
    there.  At any time the memory cache can be committed to a cache database to
    be used or transported later.
    """
    def __init__(self, dbhost=None, dbuser=None, dbpassword=None, dbname=None, cache_file=None):
        # in-memory query cache
        self.cached_queries = {}
        self.last_results = None
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
            # check to ensure db isn't empty
            result = self.query('select * from summary limit 1', (), nocache=True)
            if len(result) == 0:
                warnings.warn("Using an empty cache database; queries will return nothing")

        # Get database parameters only when not attempting to connect to a cache db
        if self.cache_db is None:
            if dbhost is None:
                dbhost = os.environ.get('NERSC_JOBSDB_HOST')
            if dbuser is None:
                dbuser = os.environ.get('NERSC_JOBSDB_USER')
            if dbpassword is None:
                dbpassword = os.environ.get('NERSC_JOBSDB_PASSWORD')
            if dbname is None:
                dbname = os.environ.get('NERSC_JOBSDB_DB')

        if dbhost is not None \
        and dbuser is not None \
        and dbpassword is not None \
        and dbname is not None:
            self.connect(dbhost=dbhost,
                         dbuser=dbuser,
                         dbpassword=dbpassword,
                         dbname=dbname)

    def connect(self, dbhost, dbuser, dbpassword, dbname):
        """
        Establish db connection
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

    def connect_cache(self, cache_file):
        """
        Open the cache db and note whether our intent is to only write to it, or
        read and write.
        """
        if cache_file is not None:
            self.cache_db = sqlite3.connect(cache_file)
            self.cache_file = cache_file
            self.cache_db_ps = get_paramstyle_symbol(sqlite3.paramstyle)
            ### Ensure that the correct table/schema exists
            self.cache_db.execute(CACHE_TABLE_SCHEMA)

    def clear_memory(self):
        """
        Clear the query cache
        """
        self.cached_queries = {}

    def close_cache(self):
        """
        Close a cache db and reset its intent
        """
        if self.cache_db is not None:
            self.cache_db = self.cache_db.close()
            self.cache_file = None

    def save_cache(self, cache_file):
        """
        Commit the in-memory cache to a cache database.  Currently very
        memory-inefficient and not good for caching giant pieces of a database
        without something wrapping it to feed it smaller pieces.
        """
        ### Note the state of old cache db
        old_cache_file = self.cache_file
        self.close_cache()

        ### Open a new cache db connection
        self.connect_cache(cache_file)

        ### Insert the contents of our query cache into the cache db
        bulk_insert = []
        for _, rows in self.cached_queries.iteritems():
            bulk_insert += rows
        # Note that we INSERT OR REPLACE here; the cache db never wins if a
        # duplicate jobid is detected
        self.cache_db.executemany("""
            insert or replace into summary(stepid, hostname, start, completion, numnodes)
            values (?,?,?,?,?)
            """, bulk_insert)
        self.cache_db.commit()

        ### Restore old state of cache db
        self.close_cache()
        self.connect_cache(old_cache_file)

    def get_concurrent_jobs(self, start_timestamp, end_timestamp, nersc_host):
        """
        Grab all of the jobs that were running, in part or in full, during the
        time window bounded by start_timestamp and end_timestamp.  Then
        calculate the fraction overlap for each job to calculate the number of
        core hours that were burned overall during the start/end time of
        interest.
        """
        query_str = """
        SELECT
            s.stepid,
            s.hostname,
            s.start,
            s.completion,
            s.numnodes
        FROM
            summary AS s
        WHERE
            s.hostname = %(ps)s
        AND s.completion > %(ps)s
        AND s.start < %(ps)s
        """

        totals = {
            'nodehrs': 0.0,
            'numnodes': 0,
            'numjobs': 0,
        }

        results = self.query(query_str, (nersc_host, start_timestamp, end_timestamp))

        for (_, _, this_start, this_end, nnodes) in results:
            real_start = max(this_start, start_timestamp)
            real_end = min(this_end, end_timestamp)
            delta_t = real_end - real_start
            totals['nodehrs'] += nnodes * delta_t
            totals['numnodes'] += nnodes
            totals['numjobs'] += 1

        totals['nodehrs'] /= 3600.0

        return totals

    def query(self, query_str, query_variables=(), nocache=False):
        """
        Pass a query through all layers of cache and return on the first hit.
        """

        ### Collapse query string to remove extraneous whitespace
        query_str = ' '.join(query_str.split())
        cache_key = query_str % {'ps': '%s'} % query_variables

        ### FIRST: check cache for the results of this query--we can get away
        ### with this because the NERSC Jobs DB is append-only
        if cache_key in self.cached_queries and not nocache:
            results = self.cached_queries[cache_key]
            self.last_hit = HIT_MEMORY
        ### SECOND: check the cache database (if available)
        elif self.cache_db is not None:
            results = self._query_sqlite3(query_str, query_variables)
            self.last_hit = HIT_CACHE_DB
        ### THIRD: check the MySQL database (if available)
        elif self.remote_db is not None:
            results = self._query_mysql(query_str, query_variables)
            self.last_hit = HIT_REMOTE_DB
        else:
            raise RuntimeError('No databases available to query')

        if not nocache:
            self.cached_queries[cache_key] = results
        self.last_results = results

        return results

    def _query_sqlite3(self, query_str, query_variables):
        """
        Run a query against the cache database and yield the full output.  No
        buffering, so be careful.
        """
        cursor = self.cache_db.cursor()
        cursor.execute(query_str % {'ps': self.cache_db_ps}, query_variables)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def _query_mysql(self, query_str, query_variables):
        """
        Run a query against the MySQL database and yield the full output tuple.
        No buffering, so be careful.

        """
        cursor = self.remote_db.cursor()
        cursor.execute(query_str % {'ps': self.remote_db_ps}, query_variables)
        rows = cursor.fetchall()
        cursor.close()
        return rows

def get_paramstyle_symbol(paramstyle):
    """
    Infer the correct paramstyle for a database.paramstyle (see PEP-0249)
    """
    if paramstyle == 'qmark':
        return "?"
    elif paramstyle == 'format' or paramstyle == 'pyformat':
        return "%s"
    else:
        raise Exception("Unsupported paramstyle %s" % paramstyle)
