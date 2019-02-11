#!/usr/bin/env python
"""
Extract job info from the NERSC jobs database.  Accessing the MySQL database is
not required (i.e., if you have everything stored in a cache, you never have to
touch MySQL).  However if you do have to connect to MySQL, you must set the
following environment variables::

    NERSC_JOBSDB_HOST
    NERSC_JOBSDB_USER
    NERSC_JOBSDB_PASSWORD
    NERSC_JOBSDB_DB

If you do not know what to use as these credentials, you will have to rely on
a cache database.
"""

import os
import warnings
import tokio.connectors.cachingdb as cachingdb

NERSC_JOBSDB_SCHEMA = {
    'columns': ['STEPID', 'HOSTNAME', 'START', 'COMPLETION', 'NUMNODES'],
    'primary_key': ['STEPID'],
}
NERSC_JOBSDB_QUERY = """
SELECT
    s.stepid,
    s.hostname,
    s.start,
    s.completion,
    s.numnodes
FROM
    summary AS s
"""

HIT_MEMORY = 0
HIT_CACHE_DB = 1
HIT_REMOTE_DB = 2

class NerscJobsDb(cachingdb.CachingDb):
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
        self.last_results = None # for debugging

        if dbhost is None:
            dbhost = os.environ.get('NERSC_JOBSDB_HOST')
        if dbuser is None:
            dbuser = os.environ.get('NERSC_JOBSDB_USER')
        if dbpassword is None:
            dbpassword = os.environ.get('NERSC_JOBSDB_PASSWORD')
        if dbname is None:
            dbname = os.environ.get('NERSC_JOBSDB_DB')

        super(NerscJobsDb, self).__init__(
            dbhost=dbhost,
            dbuser=dbuser,
            dbpassword=dbpassword,
            dbname=dbname,
            cache_file=cache_file)

    def drop_cache(self):
        """
        Clear the query cache
        """
        super(NerscJobsDb, self).drop_cache()
        self.cached_queries = {}

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
            index_start, index_end = self.cached_queries[cache_key]
            results = self.saved_results['summary']['rows'][index_start:index_end]
            self.last_hit = HIT_MEMORY
        else:
            if 'summary' not in self.saved_results:
                index_start = 0
            else:
                index_start = len(self.saved_results['summary']['rows'])
            results = super(NerscJobsDb, self).query(
                query_str,
                query_variables,
                table='summary',
                table_schema=NERSC_JOBSDB_SCHEMA)
            index_end = len(self.saved_results['summary']['rows'])
            if not nocache:
                self.cached_queries[cache_key] = (index_start, index_end)

        self.last_results = results # for debugging
        return results

    def get_concurrent_jobs(self, start_timestamp, end_timestamp, nersc_host):
        """
        Grab all of the jobs that were running, in part or in full, during the
        time window bounded by start_timestamp and end_timestamp.  Then
        calculate the fraction overlap for each job to calculate the number of
        core hours that were burned overall during the start/end time of
        interest.
        """
        query_str = NERSC_JOBSDB_QUERY + """
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

        results = self.query(
            query_str,
            (nersc_host, start_timestamp, end_timestamp))

        for (_, _, this_start, this_end, nnodes) in results:
            real_start = max(this_start, start_timestamp)
            real_end = min(this_end, end_timestamp)
            delta_t = real_end - real_start
            totals['nodehrs'] += nnodes * delta_t
            totals['numnodes'] += nnodes
            totals['numjobs'] += 1

        totals['nodehrs'] /= 3600.0

        return totals

#   def get_concurrent_jobs(self, start_timestamp, end_timestamp, nersc_host):
    def get_job_startend(self, jobid, nersc_host):
        """Return start and end time for a given job id

        Retrieves the time a job started and completed.

        Args:
            jobid (str): Job ID of interest
            nersc_host (str): NERSC host to which job ID of interest maps

        Returns:
            tuple of datetime.datetime: Two-item tuple of (start time,
            end time)
        """
        query_str = NERSC_JOBSDB_QUERY + """
        WHERE
            s.stepid LIKE %(ps)s
        AND s.hostname = %(ps)s
        ORDER BY s.completion
        """

        results = self.query(query_str, query_variables=("%s.%%" % jobid, nersc_host))

        start = None
        end = None
        for (_, _, this_start, this_end, _) in results:
            if start is not None:
                warnings.warn("Multiple start+end times found for job %s" % jobid)
            start = this_start
            end = this_end
        return (start, end)
