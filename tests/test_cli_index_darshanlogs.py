#!/usr/bin/env python
"""Test cli.index_darshanlogs utility
"""

import os
import glob
import shutil
import sqlite3
import warnings
import nose
import tokiotest
import tokio#.connectors.darshan - TODO: fix the import problems
import tokio.cli.index_darshanlogs

SAMPLE_DARSHAN_LOGS = glob.glob(os.path.join(os.getcwd(), 'inputs', '*.darshan'))
TABLES = [
    tokio.cli.index_darshanlogs.MOUNTS_TABLE,
    tokio.cli.index_darshanlogs.HEADERS_TABLE,
    tokio.cli.index_darshanlogs.SUMMARIES_TABLE
]

def verify_index_db(output_file):
    """Verifies schemata and correctness of an index database
    """
    conn = sqlite3.connect(output_file)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    rows = cursor.fetchall()
    tables = [row[0] for row in rows]

    print("%s contains %d tables" % (output_file, len(tables)))
    assert len(tables) == 3
    for table in TABLES:
        print("Verifying existence of table %s" % table)
        assert table in tables

    table_len = {}
    for table in tables:
        print("Checking length of table %s" % table)
        table_len[table] = get_table_len(table, conn=conn, cursor=cursor)
        assert table_len[table] > 0

    print("Checking full consistency of %s" % tokio.cli.index_darshanlogs.SUMMARIES_TABLE)
    cursor.execute("""
        SELECT 
            *
        FROM
            %s AS s
        INNER JOIN
            %s AS h ON h.log_id = s.log_id,
            %s AS m ON m.fs_id = s.fs_id""" % (
                tokio.cli.index_darshanlogs.SUMMARIES_TABLE,
                tokio.cli.index_darshanlogs.HEADERS_TABLE,
                tokio.cli.index_darshanlogs.MOUNTS_TABLE))
    rows = cursor.fetchall()
    print("  fully joined summaries has %d rows" % len(rows))
    assert len(rows) == table_len[tokio.cli.index_darshanlogs.SUMMARIES_TABLE]

    conn.close()

def get_table_len(table, output_file=None, conn=None, cursor=None):
    """Retrieve the row count for a table in the index db
    """
    if not conn:
        conn = sqlite3.connect(output_file)
    if not cursor:
        cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM %s" % table)
    num_rows = cursor.fetchall()[0][0]
    print("  table %s has %d rows" % (table, num_rows))
    return num_rows

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_input_dir():
    """cli.index_darshanlogs with input dir
    """
    tokiotest.check_darshan()
    argv = ['--quiet',
            '--output',
            tokiotest.TEMP_FILE.name] + [os.path.dirname(SAMPLE_DARSHAN_LOGS[0])]
    print("Executing: %s" % " ".join(argv))
    tokiotest.run_bin(tokio.cli.index_darshanlogs, argv)
    verify_index_db(tokiotest.TEMP_FILE.name)
    assert get_table_len(tokio.cli.index_darshanlogs.HEADERS_TABLE,
                         output_file=tokiotest.TEMP_FILE.name) > 1

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_input_file():
    """cli.index_darshanlogs with one input log
    """
    tokiotest.check_darshan()
    argv = ['--output', tokiotest.TEMP_FILE.name] + [SAMPLE_DARSHAN_LOGS[0]]
    print("Executing: %s" % " ".join(argv))
    tokiotest.run_bin(tokio.cli.index_darshanlogs, argv)
    verify_index_db(tokiotest.TEMP_FILE.name)
    assert get_table_len(tokio.cli.index_darshanlogs.HEADERS_TABLE,
                         output_file=tokiotest.TEMP_FILE.name) == 1

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_input_files():
    """cli.index_darshanlogs with multiple input logs
    """
    tokiotest.check_darshan()
    argv = ['--output', tokiotest.TEMP_FILE.name] + SAMPLE_DARSHAN_LOGS
    print("Executing: %s" % " ".join(argv))
    tokiotest.run_bin(tokio.cli.index_darshanlogs, argv)
    verify_index_db(tokiotest.TEMP_FILE.name)
    assert get_table_len(tokio.cli.index_darshanlogs.HEADERS_TABLE,
                         output_file=tokiotest.TEMP_FILE.name) > 1

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_multithreaded():
    """cli.index_darshanlogs --threads
    """
    tokiotest.check_darshan()
    argv = ['--threads', '4', '--output', tokiotest.TEMP_FILE.name] + SAMPLE_DARSHAN_LOGS
    print("Executing: %s" % " ".join(argv))
    tokiotest.run_bin(tokio.cli.index_darshanlogs, argv)
    verify_index_db(tokiotest.TEMP_FILE.name)

@tokiotest.needs_darshan
def test_get_file_mount():
    """cli.index_darshanlogs.get_file_mount
    """
    tokiotest.check_darshan()
    # key = test case, val = true value
    test_cases = {
        '/var/spool/whatever': '/',
        '/hello/something.txt': '/hello',
        '/hello/something/somethingelse': '/hello',
        '/hello/world/something.txt': '/hello/world',
        '/skippy/dippy': '/skippy',
        '/skip/lip/dip': '/skip',
    }
    mount_lists = {
        'ordered': [
            '/',
            '/hello',
            '/hello/world',
            '/skip',
            '/skippy',
        ],
        'disordered': [
            '/hello/world',
            '/hello',
            '/',
            '/skippy',
            '/skip',
        ],
    }

    for label, mount_list in mount_lists.items():
        print("Testing %s mount list" % label)
        for test_case, truth in test_cases.items():
            mnt, logical = tokio.cli.index_darshanlogs.get_file_mount(test_case, mount_list)
            print("  File %s is under mount %s" % (test_case, mnt))
            assert mnt == truth
            print("  File %s exists in fs %s" % (test_case, logical))
            assert not logical.startswith('/') or logical == '/'

@tokiotest.needs_darshan
def test_summarize_by_fs():
    """cli.index_darshanlogs.summarize_by_fs
    """
    tokiotest.check_darshan()
    result = tokio.cli.index_darshanlogs.summarize_by_fs(tokiotest.SAMPLE_DARSHAN_LOG)
    assert result
    assert 'summaries' in result
    assert 'headers' in result
    assert 'mounts' in result
    assert result['summaries']
    assert result['headers']
    assert result['mounts']

    truth = tokio.connectors.darshan.Darshan(tokiotest.SAMPLE_DARSHAN_LOG)
    truth.darshan_parser_base()

    print("Verify that result contains only the subset of mounts actually used")
    assert len(result['mounts']) < len(truth['mounts'])

    for mount in result['mounts']:
        if mount != "UNKNOWN":
            print("Ensure that result mount %s is in actual Darshan log" % mount)
            assert mount in truth['mounts']

    print("Verify that there aren't more mounts than files opened")
    assert len(result['summaries']) >= len(result['mounts'])

    assert 'filename' in result['headers']
    assert 'exe' in result['headers']
    assert 'username' in result['headers']
    assert 'exename' in result['headers']

    print("Verify max_mb functionality")
    warnings.filterwarnings('ignore')
    result = tokio.cli.index_darshanlogs.summarize_by_fs(tokiotest.SAMPLE_DARSHAN_LOG,
                                                         max_mb=1.0/1024.0)
    assert not result

def make_index_db(dest_db, mod_queries=None, src_db=tokiotest.SAMPLE_DARSHAN_INDEX_DB):
    """Creates a copy of the sample db with missing data

    Args:
        new_db(str): Path to file where database should be copied.  Usually
            tokiotest.TEST_FILE.name
        delete_query(str or list of str): Any SQL query/queries to apply to the
            copied database.

    Returns:
        conn (sqlite3.Connection): Handle to the newly created database
    """
    shutil.copyfile(src_db, dest_db)
    conn = sqlite3.connect(dest_db)
    cursor = conn.cursor()

    if mod_queries:
        if isinstance(mod_queries, str):
            cursor.execute(mod_queries)
        else:
            for mod_query in mod_queries:
                cursor.execute(mod_query)
        conn.commit()

    return conn

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_get_existing_logs():
    """cli.index_darshanlogs.get_existing_logs
    """
    tokiotest.TEMP_FILE.close()

    test_file = tokiotest.SAMPLE_DARSHAN_INDEX_DB
    conn = sqlite3.connect(test_file)

    existing_logs = tokio.cli.index_darshanlogs.get_existing_logs(conn)

    assert existing_logs
    conn.close()

    print('Test database with empty summaries table (everything else valid)')
    conn = make_index_db(dest_db=tokiotest.TEMP_FILE.name,
                         mod_queries="DELETE FROM summaries")
    existing_logs = tokio.cli.index_darshanlogs.get_existing_logs(conn)
    conn.close()
    assert not existing_logs

    print('Test database with some summaries table data missing')
    conn = make_index_db(dest_db=tokiotest.TEMP_FILE.name,
                         mod_queries="DELETE FROM summaries WHERE rowid % 2 = 0")
    existing_logs = tokio.cli.index_darshanlogs.get_existing_logs(conn)
    conn.close()
    assert existing_logs

    print('Test database with some headers table data missing')
    conn = make_index_db(dest_db=tokiotest.TEMP_FILE.name,
                         mod_queries="DELETE FROM headers WHERE log_id % 2 = 0")
    existing_logs = tokio.cli.index_darshanlogs.get_existing_logs(conn)
    conn.close()
    assert existing_logs

    print('Test database with all headers table data missing')
    conn = make_index_db(dest_db=tokiotest.TEMP_FILE.name,
                         mod_queries="DELETE FROM headers")
    existing_logs = tokio.cli.index_darshanlogs.get_existing_logs(conn)
    conn.close()
    assert not existing_logs

    print("Test malformed database (all summaries, missing headers)")
    conn = make_index_db(dest_db=tokiotest.TEMP_FILE.name,
                         mod_queries="DELETE FROM headers WHERE log_id % 2 = 0")
    existing_logs = tokio.cli.index_darshanlogs.get_existing_logs(conn)
    assert existing_logs

    conn.close()

def test_process_log_list():
    """cli.index_darshanlogs.process_log_list
    """
    print('Test database with some summaries table data missing')
    conn = make_index_db(dest_db=tokiotest.TEMP_FILE.name)

    # Get full list of logs in database.  We assume sample database is fully
    # consistent so we don't unnecessarily couple this test to get_existing_logs()
    cursor = conn.cursor()
    cursor.execute("SELECT filename from HEADERS")
    rows = cursor.fetchall()
    full_log_list = [row[0] for row in rows]

    print("Test fully populated database")
    assert not tokio.cli.index_darshanlogs.process_log_list(conn, full_log_list)

    print("Test database with all headers, only half summaries")
    cursor.execute("DELETE FROM summaries WHERE log_id % 2 = 0")
    conn.commit()
    new_log_list = tokio.cli.index_darshanlogs.process_log_list(conn, full_log_list)
    print("Of %d total logs, %d are new to the database" % (len(full_log_list), len(new_log_list)))
    assert new_log_list
    assert len(new_log_list) < len(full_log_list)

    print("Test consistent and half-populated database")
    cursor.execute("DELETE FROM headers WHERE log_id % 2 = 0")
    conn.commit()
    new_log_list = tokio.cli.index_darshanlogs.process_log_list(conn, full_log_list)
    print("Of %d total logs, %d are new to the database" % (len(full_log_list), len(new_log_list)))
    assert new_log_list
    assert len(new_log_list) < len(full_log_list)

    print("Test unpopulated database")
    cursor.execute("DELETE FROM headers")
    cursor.execute("DELETE FROM summaries")
    conn.commit()
    new_log_list = tokio.cli.index_darshanlogs.process_log_list(conn, full_log_list)
    print("Of %d total logs, %d are new to the database" % (len(full_log_list), len(new_log_list)))
    assert new_log_list
    assert len(new_log_list) == len(full_log_list)

    conn.close()

    print("Test malformed database (all summaries, missing headers)")
    conn = make_index_db(dest_db=tokiotest.TEMP_FILE.name)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM headers WHERE log_id % 2 = 0")
    conn.commit()
    new_log_list = tokio.cli.index_darshanlogs.process_log_list(conn, full_log_list)
    print("Of %d total logs, %d are new to the database" % (len(full_log_list), len(new_log_list)))
    assert new_log_list
    assert len(new_log_list) < len(full_log_list)

    conn.close()

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_update():
    """cli.index_darshanlogs with an existing database
    """
    tokiotest.check_darshan()

    # create a database with a couple of entries
    argv = ['--quiet',
            '--output',
            tokiotest.TEMP_FILE.name] + [os.path.dirname(SAMPLE_DARSHAN_LOGS[0])]
    print("Executing: %s" % " ".join(argv))
    tokiotest.run_bin(tokio.cli.index_darshanlogs, argv)

    # hack on database
    conn = sqlite3.connect(tokiotest.TEMP_FILE.name)
    cursor = conn.cursor()

    print("Initial database:")
    orig_num_rows = {}
    for table in TABLES:
        orig_num_rows[table] = get_table_len(table=table, conn=conn, cursor=cursor)

    print("Test database with all headers, all summaries")
    tokiotest.run_bin(tokio.cli.index_darshanlogs, argv)
    for table in TABLES:
        num_rows = get_table_len(table=table, conn=conn, cursor=cursor)
        assert num_rows == orig_num_rows[table]

    @nose.tools.raises(sqlite3.IntegrityError)
    def all_headers_half_summaries():
        """Test database with half of the summaries rows missing
        """
        print("Test database with all headers, only half summaries")
        cursor.execute("DELETE FROM summaries WHERE log_id % 2 = 0")
        conn.commit()
        # sqlite3.IntegrityError
        tokiotest.run_bin(tokio.cli.index_darshanlogs, argv)
        for table in TABLES:
            num_rows = get_table_len(table=table, conn=conn, cursor=cursor)
            assert num_rows == orig_num_rows[table]
    all_headers_half_summaries()

    print("Test consistent and half-populated database")
    cursor.execute("DELETE FROM headers WHERE log_id % 2 = 0")
    conn.commit()
    tokiotest.run_bin(tokio.cli.index_darshanlogs, argv)
    for table in TABLES:
        num_rows = get_table_len(table=table, conn=conn, cursor=cursor)
        assert num_rows == orig_num_rows[table]

    print("Test unpopulated database")
    cursor.execute("DELETE FROM headers")
    cursor.execute("DELETE FROM summaries")
    conn.commit()
    tokiotest.run_bin(tokio.cli.index_darshanlogs, argv)
    for table in TABLES:
        num_rows = get_table_len(table=table, conn=conn, cursor=cursor)
        assert num_rows == orig_num_rows[table]
