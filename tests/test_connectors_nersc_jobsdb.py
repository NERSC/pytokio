#!/usr/bin/env python
"""
Tests for the NerscJobsDb class and functionality.  Primarily relies on a local
cache db (mysql) and assumes that querying it is equivalent to querying a remote
MySQL database.
"""

import os
import tempfile
import nose
import tokio.connectors.nersc_jobsdb

# Express job start/end time as epoch.  Note that these specific start/stop
# times coincide with inputs/sample.darshan to facilitate integration testing
SAMPLE_QUERY = (1490000867L, 1490000983L, 'edison')

SAMPLE_CACHE_DB = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                               'inputs',
                               'sample.sqlite3')

# Expand sample_query into multiple queries
QUERY_RANGE = SAMPLE_QUERY[1] - SAMPLE_QUERY[0]
SAMPLE_QUERIES = [
    (
        SAMPLE_QUERY[0],
        SAMPLE_QUERY[0] + long(QUERY_RANGE / 3),
        SAMPLE_QUERY[2]
    ),
    (
        SAMPLE_QUERY[0] + long(QUERY_RANGE / 3) + 1,
        SAMPLE_QUERY[0] + long(QUERY_RANGE * 2 / 3),
        SAMPLE_QUERY[2]
    ),
    (
        SAMPLE_QUERY[0] + long(QUERY_RANGE * 2 / 3) + 1,
        SAMPLE_QUERY[1],
        SAMPLE_QUERY[2]
    ),
]

TEMP_FILE = None

def setup_tmpfile():
    """
    Create a temporary file
    """
    global TEMP_FILE
    TEMP_FILE = tempfile.NamedTemporaryFile(delete=False)

def teardown_tmpfile():
    """
    Destroy the temporary file regardless of if the wrapped function succeeded
    or not
    """
    global TEMP_FILE
    if not TEMP_FILE.closed:
        TEMP_FILE.close()
    os.unlink(TEMP_FILE.name)

def verify_concurrent_jobs(results, nerscjobsdb, expected_queries=None):
    """
    Ensure that results from database query are sensible
    """
    # make sure that we got something back
    assert len(results) > 0

    # make sure the query/queries got cached correctly
    if expected_queries is not None:
        assert len(nerscjobsdb.cached_queries) == expected_queries

    # ensure that reported results are consistent with query results
    assert len(nerscjobsdb.last_results) == results['numjobs']

def test_get_concurrent_jobs_mysql():
    """
    NerscJobsDb.get_concurrent_jobs functionality (MySQL)
    """
    try:
        nerscjobsdb = tokio.connectors.nersc_jobsdb.NerscJobsDb()
        results = nerscjobsdb.get_concurrent_jobs(*(SAMPLE_QUERY))
        verify_concurrent_jobs(results, nerscjobsdb, 1)
    except RuntimeError as error:
        raise nose.plugins.skip.SkipTest(error)

def test_get_concurrent_jobs_sqlite():
    """
    NerscJobsDb.get_concurrent_jobs() functionality (SQLite)
    """
    nerscjobsdb = tokio.connectors.nersc_jobsdb.NerscJobsDb(cache_file=SAMPLE_CACHE_DB)
    results = nerscjobsdb.get_concurrent_jobs(*(SAMPLE_QUERY))
    verify_concurrent_jobs(results, nerscjobsdb, 1)

def test_memory_cache():
    """
    NerscJobsDb memory cache functionality
    """
    nerscjobsdb = tokio.connectors.nersc_jobsdb.NerscJobsDb(cache_file=SAMPLE_CACHE_DB)

    ### First query will hit the cache database, then be cached in memory
    results = nerscjobsdb.get_concurrent_jobs(*(SAMPLE_QUERY))
    assert nerscjobsdb.last_hit == tokio.connectors.nersc_jobsdb.HIT_CACHE_DB
    verify_concurrent_jobs(results, nerscjobsdb, 1)

    ### Second time query is run, it should be served out of cache
    results = nerscjobsdb.get_concurrent_jobs(*(SAMPLE_QUERY))
    assert nerscjobsdb.last_hit == tokio.connectors.nersc_jobsdb.HIT_MEMORY
    verify_concurrent_jobs(results, nerscjobsdb, 1)

    ### Drop the query cache from memory
    nerscjobsdb.clear_memory()

    ### Third time should go back to hitting the memory cache
    results = nerscjobsdb.get_concurrent_jobs(*(SAMPLE_QUERY))
    assert nerscjobsdb.last_hit == tokio.connectors.nersc_jobsdb.HIT_CACHE_DB
    verify_concurrent_jobs(results, nerscjobsdb, 1)

@nose.tools.with_setup(setup_tmpfile, teardown_tmpfile)
def test_save_cache():
    """
    NerscJobsDb.save_cache() functionality
    """
    ### First run a query against the ground truth source
    nerscjobsdb = tokio.connectors.nersc_jobsdb.NerscJobsDb(cache_file=SAMPLE_CACHE_DB)
    truth_result = nerscjobsdb.get_concurrent_jobs(*(SAMPLE_QUERY))
    truth_rows = nerscjobsdb.last_results
    assert len(truth_result) > 0

    ### Cache the results in pieces to force append behavior
    piecewise_results = []
    piecewise_rows = []
    for sample_query in SAMPLE_QUERIES:
        ### Perform part of the original query
        print "Querying %s to %s on %s" % sample_query
        piecewise_result = nerscjobsdb.get_concurrent_jobs(*(sample_query))
        print "Got %d hits from source %d" % (len(nerscjobsdb.last_results), nerscjobsdb.last_hit)
        assert len(piecewise_result) > 0

        ### Keep a running list of all rows these piecewise queries return
        piecewise_results.append(piecewise_result)
        piecewise_rows += nerscjobsdb.last_results

        ### Save (append) results to a new cache db
        nerscjobsdb.save_cache(TEMP_FILE.name)
        ### clear_memory() would necessary if NerscJobsDb didn't INSERT OR
        ### REPLACE on save_cache; each successive query and save writes
        ### the full contents retrieved from all past queries without it
#       nerscjobsdb.clear_memory()

    ### Open the newly minted cache db
    nerscjobsdb = tokio.connectors.nersc_jobsdb.NerscJobsDb(cache_file=TEMP_FILE.name)
    test_result = nerscjobsdb.get_concurrent_jobs(*(SAMPLE_QUERY))
    test_rows = nerscjobsdb.last_results

    ### Verify the queries should return the same number of rows
    assert len(test_rows) == len(truth_rows)

    ### Verify that the original query against the ground-truth database and the
    ### same query against this newly created database return the same reduced
    ### result (total nodehrs, total jobs, and total nodes)
    assert len(test_result) > 0
    for key, value in test_result.iteritems():
        print "Comparing truth(%s=%d) to piecewise(%s=%d)" % (
            key, truth_result[key],
            key, value)
        assert value != 0
        assert value == truth_result[key]

    ### Verify that the original query against the ground-truth database and the
    ### same query against this newly created database return the same set of
    ### jobids
    piecewise_jobs = set([])
    truth_jobs = set([])
    for row in piecewise_rows:
        piecewise_jobs.add(row[0])
    for row in truth_rows:
        truth_jobs.add(row[0])

    print "Piecewise gave %d rows; ground truth gave %d" % (len(piecewise_jobs), len(truth_jobs))
    assert len(piecewise_jobs - truth_jobs) == 0
