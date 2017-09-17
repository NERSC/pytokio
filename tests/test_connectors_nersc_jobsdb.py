#!/usr/bin/env python
"""
Tests for the NerscJobsDb class and functionality.  Primarily relies on a local
cache db (mysql) and assumes that querying it is equivalent to querying a remote
MySQL database.
"""

import os
import tempfile
import nose # nose.plugins.skip.SkipTest(e)
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
#   TEMP_FILE = tempfile.NamedTemporaryFile(delete=False)
    TEMP_FILE = open('temp_file.sqlite', 'wb')

def teardown_tmpfile():
    """
    Destroy the temporary file regardless of if the wrapped function succeeded
    or not
    """
    global TEMP_FILE
    if not TEMP_FILE.closed:
        TEMP_FILE.close()
#   os.unlink(TEMP_FILE.name)

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
    NerscJobsDb.get_concurrent_jobs functionality (SQLite)
    """
    nerscjobsdb = tokio.connectors.nersc_jobsdb.NerscJobsDb(cache_file=SAMPLE_CACHE_DB)
    results = nerscjobsdb.get_concurrent_jobs(*(SAMPLE_QUERY))
    verify_concurrent_jobs(results, nerscjobsdb, 1)

@nose.tools.with_setup(setup_tmpfile, teardown_tmpfile)
def test_save_cache():
    """
    NerscJobsDb.save_cache functionality
    """
    nerscjobsdb = tokio.connectors.nersc_jobsdb.NerscJobsDb(cache_file=SAMPLE_CACHE_DB)
    truth_result = nerscjobsdb.get_concurrent_jobs(*(SAMPLE_QUERY))
    truth_rows = nerscjobsdb.last_results
    assert len(truth_result) > 0

    # Save the results in pieces
    piecewise_results = []
    piecewise_rows = []
    for sample_query in SAMPLE_QUERIES:
        # Perform part of the original query
        print "Querying %s to %s on %s" % sample_query
        piecewise_result = nerscjobsdb.get_concurrent_jobs(*(sample_query))
        print "Got %d hits from source %d" % (len(nerscjobsdb.last_results), nerscjobsdb.last_hit)
        assert len(piecewise_result) > 0

        piecewise_results.append(piecewise_result)
        piecewise_rows += nerscjobsdb.last_results

        # Save (append) results to a new cache db
        nerscjobsdb.save_cache(TEMP_FILE.name)
#       nerscjobsdb.clear_memory()

    # Open the newly minted cache db
    nerscjobsdb = tokio.connectors.nersc_jobsdb.NerscJobsDb(cache_file=TEMP_FILE.name)
    test_result = nerscjobsdb.get_concurrent_jobs(*(SAMPLE_QUERY))

    # Verify that the two queries' results are equivalent
    assert len(test_result) > 0
    for key, value in test_result.iteritems():
        print "Comparing truth(%s=%d) to piecewise(%s=%d)" % (
            key, truth_result[key],
            key, value)
        assert value != 0
        assert value == truth_result[key]

    # Verify that the raw query outputs result in the same set of jobs
    piecewise_jobs = set([])
    truth_jobs = set([])
    for row in piecewise_rows:
        piecewise_jobs.add(row[0])
    for row in truth_rows:
        truth_jobs.add(row[0])

    print "Piecewise gave %d rows; ground truth gave %d" % (len(piecewise_jobs), len(truth_jobs))
    assert len(piecewise_jobs - truth_jobs) == 0
