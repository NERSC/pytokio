#!/usr/bin/env python
"""
Tests for the LmtDb class and functionality.  Primarily relies on a local
cache db (mysql) and assumes that querying it is equivalent to querying a remote
MySQL database.
"""

import datetime
import nose
import tokiotest
import tokio.connectors.lmtdb

# Express job start/end time as epoch.  Note that these specific start/stop
# times coincide with inputs/sample.darshan to facilitate integration testing
SAMPLE_QUERY = (tokiotest.SAMPLE_LMTDB_START, tokiotest.SAMPLE_LMTDB_END)

SAMPLE_CACHE_DB = tokiotest.SAMPLE_LMTDB_FILE

# Expand sample_query into multiple queries
QUERY_RANGE = SAMPLE_QUERY[1] - SAMPLE_QUERY[0]
SAMPLE_QUERIES = [
    (
        SAMPLE_QUERY[0],
        SAMPLE_QUERY[0] + int(QUERY_RANGE / 3)
    ),
    (
        SAMPLE_QUERY[0] + int(QUERY_RANGE / 3) + 1,
        SAMPLE_QUERY[0] + int(QUERY_RANGE * 2 / 3)
    ),
    (
        SAMPLE_QUERY[0] + int(QUERY_RANGE * 2 / 3) + 1,
        SAMPLE_QUERY[1]
    ),
]

def verify_lmtdb_obj(lmtdb):
    """
    Ensure that LmtDb object is initialized correctly
    """
    assert len(lmtdb.oss_names) > 0
    assert len(lmtdb.ost_names) > 0
    assert len(lmtdb.mds_names) > 0
    assert len(lmtdb.mds_op_names) > 0

# def verify_lmtdb_results(results, lmtdb, expected_queries):
#     """
#     Ensure that results from database query are sensible
#     """
#     # make sure that we got something back
#     assert len(results) > 0
#
#     # make sure the query/queries got cached correctly
#     if expected_queries is not None:
#         assert len(lmtdb.cached_queries) == expected_queries
#
#     # ensure that reported results are consistent with query results
#     assert len(lmtdb.last_results) == results['numjobs']

def test_init_mysql():
    """
    LmtDb functionality (MySQL)
    """
    try:
        lmtdb = tokio.connectors.lmtdb.LmtDb()
        verify_lmtdb_obj(lmtdb)
    except RuntimeError as error:
        raise nose.plugins.skip.SkipTest(error)

def test_init_sqlite():
    """
    LmtDb functionality (SQLite)
    """
    lmtdb = tokio.connectors.lmtdb.LmtDb(cache_file=SAMPLE_CACHE_DB)
    verify_lmtdb_obj(lmtdb)

def test_ts_id():
    """
    LmtDb.get_ts_ids()
    """
    lmtdb = tokio.connectors.lmtdb.LmtDb(cache_file=SAMPLE_CACHE_DB)
    dt_start = datetime.datetime.fromtimestamp(tokiotest.SAMPLE_LMTDB_START)
    dt_end = datetime.datetime.fromtimestamp(tokiotest.SAMPLE_LMTDB_END)

    start, end = lmtdb.get_ts_ids(dt_start, dt_end)
    print("Intended start: %s" % dt_start)
    print("Actual start:   %s" % start)
    print("Intended end: %s" % dt_end)
    print("Actual end:   %s" % end)

def test_get_timeseries_data():
    """
    LmtDb.get_timeseries_data()
    """
    lmtdb = tokio.connectors.lmtdb.LmtDb(cache_file=SAMPLE_CACHE_DB)
    dt_start = datetime.datetime.fromtimestamp(tokiotest.SAMPLE_LMTDB_START)
    dt_end = datetime.datetime.fromtimestamp(tokiotest.SAMPLE_LMTDB_END)
    result0 = lmtdb.get_timeseries_data('OST_DATA', dt_start, dt_end)
    assert len(result0) > 0
    result1 = lmtdb.get_timeseries_data('OST_DATA',
                                        dt_start,
                                        dt_end,
                                        datetime.timedelta(seconds=10))
    result2 = lmtdb.get_timeseries_data('OST_DATA',
                                        dt_start,
                                        dt_end,
                                        datetime.timedelta(seconds=30))
    result3 = lmtdb.get_timeseries_data('OST_DATA',
                                        dt_start,
                                        dt_end,
                                        datetime.timedelta(seconds=60))
    assert result0 == result1 == result2 == result3
    print(result0)
