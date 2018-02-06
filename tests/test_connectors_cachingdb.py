#!/usr/bin/env python
"""
Tests for the CachingDb class and functionality.  Relies on a sample LMT
database to exercise its baseline functionality; in most environments, this
database is not available, so only the sqlite3 functionality will be exercised.
"""

import os
import nose
import tokiotest
import tokio.connectors.cachingdb

_DB_HOST = os.environ.get('TEST_DB_HOST', 'localhost')
_DB_USER = os.environ.get('TEST_DB_USER', 'root')
_DB_PASSWORD = os.environ.get('TEST_DB_PASSWORD', '')
_DB_DBNAME = os.environ.get('TEST_DB_DBNAME', 'testdb')

### You must ensure that each TEST_TABLES table has at least
### LIMIT_CYCLES[-1] records for this test.  This is why we stop
### at 24 below (the sample input from LMT has a table with only 24 OST
### records)
LIMIT_CYCLES = [8, 16, 24]

TEST_TABLES = [
    (
        'OSS_DATA',
        'OSS_ID, TS_ID, PCT_CPU, PCT_MEMORY, PRIMARY KEY (OSS_ID, TS_ID)',
    ),
    (
        'OSS_INFO',
        'OSS_ID, FILESYSTEM_ID, HOSTNAME, FAILOVERHOST, PRIMARY KEY(OSS_ID, HOSTNAME)',
    ),
    (
        'OST_DATA',
        """OST_ID, TS_ID, READ_BYTES, WRITE_BYTES, PCT_CPU, KBYTES_FREE,
           KBYTES_USED, INODES_FREE, INODES_USED, PRIMARY KEY(OST_ID, TS_ID)""",
    ),
    (
        'OST_INFO',
        'OST_ID, OSS_ID, OST_NAME, HOSTNAME, OFFLINE, DEVICE_NAME, PRIMARY KEY (OST_ID)',
    ),
]


def query_without_saving(test_db):
    """
    cachingdb does not save results when table=None
    """
    for test_table, _ in TEST_TABLES:
        limit = 10
        result = test_db.query(
            query_str='SELECT * from %s LIMIT %d' % (test_table, limit))
        assert len(result) > 0
        assert len(test_db.saved_results) == 0 # no saved results

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def verify_cache_functionality(test_db):
    """
    Performs the following integration test:

    1. get a limited number of records from each table in a database
    2. ensure that all results are saved in memory
    3. cache the entire db object to sqlite
    4. ensure that all tables saved in memory were correctly written out
    5. ensure that the degenerate results stored in memory are written out to
       the cache db only once

    Also verifies the following desired behavior:

    1. save_cache works when table_schema is only provided once, and not on the
       final query
    """
    for test_table, test_table_schema in TEST_TABLES:
        sum_limits = 0
        for iteration, limit in enumerate(LIMIT_CYCLES):
            sum_limits += limit
            if iteration == 0:
                schema_param = 'CREATE TABLE IF NOT EXISTS %s (%s)' % (
                    test_table,
                    test_table_schema)
            else:
                schema_param = None
            result = test_db.query(
                query_str='SELECT * from %s LIMIT %d' % (test_table, limit),
                table=test_table,
                table_schema=schema_param)
            print "Got %d records from a limit of %d in %s" % (
                len(result),
                limit,
                test_table)
            assert len(result) == limit
            assert len(test_db.saved_results[test_table]['rows']) == sum_limits

    ### Cache all TEST_TABLES to a single file
    test_db.save_cache(tokiotest.TEMP_FILE.name)
    print "Created", tokiotest.TEMP_FILE.name

    ### Destroy any residual state, just in case
    test_db.close()
    del test_db

    ### Open the cache database we just created
    test_db = tokio.connectors.cachingdb.CachingDb(cache_file=tokiotest.TEMP_FILE.name)

    ### Confirm that all tables were written
    result = test_db.query("SELECT COUNT(name) from sqlite_master WHERE type='table'")
    print "Found %d tables in %s" % (result[0][0], test_db.cache_file)
    assert result[0][0] == len(TEST_TABLES)

    ### Confirm that the size of each table is correct
    for test_table, test_table_schema in TEST_TABLES:
        result = test_db.query(
            query_str='SELECT * from %s' % (test_table),
            table=test_table,
            table_schema=schema_param)
        print "Found table %s with %d records" % (test_table, len(result))
        assert len(result) == LIMIT_CYCLES[-1]

TEST_FUNCTIONS = [
    (
        "cachingdb does not save results when table=None",
        query_without_saving,
    ),
    (
        "cachingdb.save_cache functionality",
        verify_cache_functionality,
    ),
]

def test_remote_db():
    """
    Verify functionality when connecting to a remote database
    """
    try: 
        tokio.connectors.cachingdb.MySQLdb
    except AttributeError as error:
        raise nose.SkipTest(error)

    try:
        test_db = tokio.connectors.cachingdb.CachingDb(
            dbhost=_DB_HOST,
            dbuser=_DB_USER,
            dbpassword=_DB_PASSWORD,
            dbname=_DB_DBNAME)

    except tokio.connectors.cachingdb.MySQLdb.OperationalError as error:
        raise nose.SkipTest(error)

    for description, test_function in TEST_FUNCTIONS:
        func = test_function
        func.description = description
        yield func, test_db

def test_cache_db():
    """
    Verify functionality when connecting to a remote database
    """
    test_db = tokio.connectors.cachingdb.CachingDb(cache_file=tokiotest.SAMPLE_LMTDB_FILE)

    for description, test_function in TEST_FUNCTIONS:
        func = test_function
        func.description = description
        yield func, test_db
