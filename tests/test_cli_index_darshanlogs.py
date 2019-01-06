#!/usr/bin/env python
"""Test cli.index_darshanlogs utility
"""

import os
import glob
import sqlite3
import warnings
import nose
import tokiotest
import tokio.cli.index_darshanlogs

### For tests that base all tests off of the sample Darshan log
SAMPLE_DARSHAN_LOGS = glob.glob(os.path.join(os.getcwd(), 'inputs', '*.darshan'))
LOGS_FROM_DIR = glob.glob(os.path.join(tokiotest.SAMPLE_DARSHAN_LOG_DIR, '*', '*', '*', '*.darshan'))
FILTER_FOR_EXE = ['vpicio_uni', 'dbscan_read']

def verify_index_db(output_file):
    conn = sqlite3.connect(output_file)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    rows = cursor.fetchall()
    tables = [row[0] for row in rows]

    print("%s contains %d tables" % (output_file, len(tables)))
    assert len(tables) == 3
    assert tokio.cli.index_darshanlogs.MOUNTS_TABLE in tables
    assert tokio.cli.index_darshanlogs.HEADERS_TABLE in tables
    assert tokio.cli.index_darshanlogs.SUMMARIES_TABLE in tables

    for table in tables:
        print("Checking length of table %s" % table)
        cursor.execute("SELECT COUNT(*) from %s" % table)
        rows = cursor.fetchall()
        print("  table %s has %d rows" % (table, len(rows)))
        assert len(rows) > 0

    conn.close()

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_input_dir():
    """cli.index_darshanlogs with input dir
    """
    # Need lots of error/warning suppression since our input dir contains a ton of non-Darshan logs
    warnings.filterwarnings('ignore')
    tokiotest.check_darshan()
    argv = ['--output', tokiotest.TEMP_FILE.name] + [os.path.dirname(SAMPLE_DARSHAN_LOGS[0])]
    print("Executing: %s" % " ".join(argv))
    output_str = tokiotest.run_bin(tokio.cli.index_darshanlogs, argv)
    verify_index_db(tokiotest.TEMP_FILE.name)

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_input_file():
    """cli.index_darshanlogs with one input log
    """
    tokiotest.check_darshan()
    argv = ['--output', tokiotest.TEMP_FILE.name] + [SAMPLE_DARSHAN_LOGS[0]]
    print("Executing: %s" % " ".join(argv))
    output_str = tokiotest.run_bin(tokio.cli.index_darshanlogs, argv)
    verify_index_db(tokiotest.TEMP_FILE.name)

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_input_files():
    """cli.index_darshanlogs with multiple input logs
    """
    tokiotest.check_darshan()
    argv = ['--output', tokiotest.TEMP_FILE.name] + SAMPLE_DARSHAN_LOGS
    print("Executing: %s" % " ".join(argv))
    output_str = tokiotest.run_bin(tokio.cli.index_darshanlogs, argv)
    verify_index_db(tokiotest.TEMP_FILE.name)

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_multithreaded():
    """cli.index_darshanlogs --threads
    """
    tokiotest.check_darshan()
    argv = ['--threads', '4', '--output', tokiotest.TEMP_FILE.name] + SAMPLE_DARSHAN_LOGS
    print("Executing: %s" % " ".join(argv))
    output_str = tokiotest.run_bin(tokio.cli.index_darshanlogs, argv)
    verify_index_db(tokiotest.TEMP_FILE.name)

def test_get_file_mount():
    """cli.index_darshanlogs.get_file_mount
    """
    raise nose.SkipTest("test not implemented")

def test_summarize_by_fs():
    """cli.index_darshanlogs.summarize_by_fs
    """
    raise nose.SkipTest("test not implemented")

def test_get_existing_logs():
    """cli.index_darshanlogs.get_existing_logs
    """
    raise nose.SkipTest("test not implemented")

def test_process_log_list():
    """cli.index_darshanlogs.process_log_list
    """
    raise nose.SkipTest("test not implemented")
