#!/usr/bin/env python
"""
Tests for the NerscJobsDb class and functionality.  Primarily relies on a local
cache db (mysql) and assumes that querying it is equivalent to querying a remote
MySQL database.
"""

import nose
import tokiotest
import tokio.connectors.nersc_jobsdb

# Express job start/end time as epoch.  Note that these specific start/stop
# times coincide with inputs/sample.darshan to facilitate integration testing
SAMPLE_QUERY = (
    tokiotest.SAMPLE_NERSCJOBSDB_START,
    tokiotest.SAMPLE_NERSCJOBSDB_END,
    tokiotest.SAMPLE_NERSCJOBSDB_HOST)

SAMPLE_CACHE_DB = tokiotest.SAMPLE_NERSCJOBSDB_FILE

# Expand sample_query into multiple queries
QUERY_RANGE = SAMPLE_QUERY[1] - SAMPLE_QUERY[0]
SAMPLE_QUERIES = [
    (
        SAMPLE_QUERY[0],
        SAMPLE_QUERY[0] + int(QUERY_RANGE / 3),
        SAMPLE_QUERY[2]
    ),
    (
        SAMPLE_QUERY[0] + int(QUERY_RANGE / 3) + 1,
        SAMPLE_QUERY[0] + int(QUERY_RANGE * 2 / 3),
        SAMPLE_QUERY[2]
    ),
    (
        SAMPLE_QUERY[0] + int(QUERY_RANGE * 2 / 3) + 1,
        SAMPLE_QUERY[1],
        SAMPLE_QUERY[2]
    ),
]

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
    results1 = nerscjobsdb.get_concurrent_jobs(*(SAMPLE_QUERY))
    print("Got %d hits from source %d" % (len(results1), nerscjobsdb.last_hit))
    assert nerscjobsdb.last_hit == tokio.connectors.nersc_jobsdb.HIT_CACHE_DB
    verify_concurrent_jobs(results1, nerscjobsdb, 1)

    ### Second time query is run, it should be served out of cache
    results2 = nerscjobsdb.get_concurrent_jobs(*(SAMPLE_QUERY))
    print("Got %d hits from source %d" % (len(results2), nerscjobsdb.last_hit))
    assert nerscjobsdb.last_hit == tokio.connectors.nersc_jobsdb.HIT_MEMORY
    verify_concurrent_jobs(results2, nerscjobsdb, 1)

    ### Results should be the same (and nonzero, per verify_concurrent_jobs)
    assert results1 == results2

    ### Drop the query cache from memory
    nerscjobsdb.drop_cache()

    ### Third time should go back to hitting the memory cache
    results = nerscjobsdb.get_concurrent_jobs(*(SAMPLE_QUERY))
    print("Got %d hits from source %d" % (len(results), nerscjobsdb.last_hit))
    assert nerscjobsdb.last_hit == tokio.connectors.nersc_jobsdb.HIT_CACHE_DB
    verify_concurrent_jobs(results, nerscjobsdb, 1)

    ### Results should be the same (and nonzero, per verify_concurrent_jobs)
    assert results == results1 == results2

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
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
        print("Querying %s to %s on %s" % sample_query)
        piecewise_result = nerscjobsdb.get_concurrent_jobs(*(sample_query))
        print("Got %d hits from source %d" % (len(nerscjobsdb.last_results), nerscjobsdb.last_hit))
        assert len(piecewise_result) > 0

        ### Keep a running list of all rows these piecewise queries return
        piecewise_results.append(piecewise_result)
        piecewise_rows += nerscjobsdb.last_results

        ### Save (append) results to a new cache db
        nerscjobsdb.save_cache(tokiotest.TEMP_FILE.name)
        ### clear_memory() would necessary if NerscJobsDb didn't INSERT OR
        ### REPLACE on save_cache; each successive query and save writes
        ### the full contents retrieved from all past queries without it
#       nerscjobsdb.drop_cache()

    ### Open the newly minted cache db
    nerscjobsdb = tokio.connectors.nersc_jobsdb.NerscJobsDb(cache_file=tokiotest.TEMP_FILE.name)
    test_result = nerscjobsdb.get_concurrent_jobs(*(SAMPLE_QUERY))
    test_rows = nerscjobsdb.last_results

    ### Verify the queries should return the same number of rows
    assert len(test_rows) == len(truth_rows)

    ### Verify that the original query against the ground-truth database and the
    ### same query against this newly created database return the same reduced
    ### result (total nodehrs, total jobs, and total nodes)
    assert len(test_result) > 0
    for key, value in test_result.items():
        print("Comparing truth(%s=%d) to piecewise(%s=%d)" % (
            key, truth_result[key],
            key, value))
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

    print("Piecewise gave %d rows; ground truth gave %d" % (len(piecewise_jobs), len(truth_jobs)))
    assert len(piecewise_jobs - truth_jobs) == 0
