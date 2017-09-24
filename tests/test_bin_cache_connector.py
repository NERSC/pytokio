#!/usr/bin/env python
"""
Test each connector's standalone CLI cache tool
"""

import os
import json
import sqlite3
import StringIO
import datetime
import subprocess
import pandas
import nose
import tokiotest

# For cache_lfsstatus.py
os.environ['PYTOKIO_H5LMT_BASE_DIR'] = os.path.join(tokiotest.INPUT_DIR, '%Y-%m-%d')
os.environ['PYTOKIO_LFSSTATUS_BASE_DIR'] = os.path.join(tokiotest.INPUT_DIR, '%Y-%m-%d')

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def verify_sqlite(output_str):
    """
    Ensure that the database contains at least one table, and that table
    contains at least one row.
    """
    output_file = output_str.split(None, 3)[-1]
    print "Using output_file %s" % output_file
    tmpdb = sqlite3.connect(output_file)
    cursor = tmpdb.cursor()
    ## Count number of tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    tables = cursor.fetchall()
    print tables
    print "Found %d tables in %s" % (len(tables), output_file)
    assert len(tables) > 0
    for table in [x[0] for x in tables]:
        cursor.execute('SELECT count(*) FROM %s' % table)
        rows = cursor.fetchall()
        num_rows = rows[0][0]
        print "Found %d rows in %s" % (num_rows, table)
        assert len(rows) > 0

def verify_json(json_str):
    """
    Ensure that json is loadable
    """
    data = json.loads(json_str)
    assert len(data) > 0

def verify_csv(csv_str):
    """
    Ensure that csv is loadable by Pandas
    """
    data = pandas.read_csv(StringIO.StringIO(csv_str))
    assert len(data) > 0

def verify_sacct(csv_str):
    """
    Ensure that native format is vaguely valid (treat it as a |-separated csv)
    """
    data = pandas.read_csv(StringIO.StringIO(csv_str), sep="|")
    assert len(data) > 0

CACHE_CONNECTOR_CONFIGS = [
    {
        'binary':     os.path.join(tokiotest.BIN_DIR, 'cache_isdct.py'),
        'args':       ['--json', tokiotest.SAMPLE_NERSCISDCT_FILE],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(tokiotest.BIN_DIR, 'cache_isdct.py'),
        'args':       ['--csv', tokiotest.SAMPLE_NERSCISDCT_FILE],
        'validators': [verify_csv,],
    },
    {
        'binary':     os.path.join(tokiotest.BIN_DIR, 'cache_darshan.py'),
        'args':       ['--base', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(tokiotest.BIN_DIR, 'cache_darshan.py'),
        'args':       ['--perf', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(tokiotest.BIN_DIR, 'cache_darshan.py'),
        'args':       ['--total', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(tokiotest.BIN_DIR, 'cache_darshan.py'),
        'args':       ['--base', '--perf', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(tokiotest.BIN_DIR, 'cache_darshan.py'),
        'args':       ['--base', '--total', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(tokiotest.BIN_DIR, 'cache_darshan.py'),
        'args':       ['--perf', '--total', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(tokiotest.BIN_DIR, 'cache_darshan.py'),
        'args':       ['--base', '--perf', '--total', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(tokiotest.BIN_DIR, 'cache_slurm.py'),
        'args':       ['--json', tokiotest.SAMPLE_SLURM_CACHE_FILE],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(tokiotest.BIN_DIR, 'cache_slurm.py'),
        'args':       ['--csv', tokiotest.SAMPLE_SLURM_CACHE_FILE],
        'validators': [verify_csv,],
    },
    {
        'binary':     os.path.join(tokiotest.BIN_DIR, 'cache_slurm.py'),
        'args':       ['--native', tokiotest.SAMPLE_SLURM_CACHE_FILE],
        'validators': [verify_sacct,],
    },
    {
        'binary':     os.path.join(tokiotest.BIN_DIR, 'cache_topology.py'),
        'args':       [
            '--craysdb-cache', tokiotest.SAMPLE_XTDB2PROC_FILE,
            '--slurm-cache', tokiotest.SAMPLE_SLURM_CACHE_FILE,
        ],
        'validators': [verify_json,],
    },
    {
        'description': 'bin/cache_lfsstatus.py --fullness, no cache',
        'binary':      os.path.join(tokiotest.BIN_DIR, 'cache_lfsstatus.py'),
        'args':        [
            '--fullness',
            '--',
            tokiotest.SAMPLE_DARSHAN_SONEXION_ID,
            tokiotest.SAMPLE_DARSHAN_START_TIME.replace(' ', 'T'),
        ],
        'validators':  [verify_json,],
    },
    {
        'description': 'bin/cache_lfsstatus.py --fullness, explicit cache',
        'binary':      os.path.join(tokiotest.BIN_DIR, 'cache_lfsstatus.py'),
        'args':        [
            '--fullness',
            tokiotest.SAMPLE_OSTFULLNESS_FILE,
            tokiotest.SAMPLE_DARSHAN_SONEXION_ID,
            tokiotest.SAMPLE_DARSHAN_START_TIME.replace(' ', 'T'),
        ],
        'validators':  [verify_json,],
    },
    {
        'description': 'bin/cache_lfsstatus.py --failure, no cache',
        'binary':      os.path.join(tokiotest.BIN_DIR, 'cache_lfsstatus.py'),
        'args':        [
            '--failure',
            '--',
            tokiotest.SAMPLE_DARSHAN_SONEXION_ID,
            tokiotest.SAMPLE_DARSHAN_START_TIME.replace(' ', 'T'),
        ],
        'validators':  [verify_json,],
    },
    {
        'description': 'bin/cache_lfsstatus.py --failure, explicit cache',
        'binary':      os.path.join(tokiotest.BIN_DIR, 'cache_lfsstatus.py'),
        'args':        [
            '--failure',
            tokiotest.SAMPLE_OSTMAP_FILE,
            tokiotest.SAMPLE_DARSHAN_SONEXION_ID,
            tokiotest.SAMPLE_DARSHAN_START_TIME.replace(' ', 'T'),
        ],
        'validators':  [verify_json,],
    },
    {
        'description': 'bin/cache_nersc_jobsdb.py',
        'binary':      os.path.join(tokiotest.BIN_DIR, 'cache_nersc_jobsdb.py'),
        'args':        [
            '-i', tokiotest.SAMPLE_NERSCJOBSDB_FILE,
            datetime.datetime.fromtimestamp(
                tokiotest.SAMPLE_NERSCJOBSDB_START).strftime("%Y-%m-%dT%H:%M:%S"),
            datetime.datetime.fromtimestamp(
                tokiotest.SAMPLE_NERSCJOBSDB_END).strftime("%Y-%m-%dT%H:%M:%S"),
            'edison',
        ],
        'validators': [verify_sqlite,],
        'to_file':    [True],
        'validate_contents': False,
    },
    {
        'description': 'bin/cache_lmtdb.py',
        'binary':      os.path.join(tokiotest.BIN_DIR, 'cache_lmtdb.py'),
        'args':        [
            '-i', tokiotest.SAMPLE_LMTDB_FILE,
            datetime.datetime.fromtimestamp(
                tokiotest.SAMPLE_LMTDB_START).strftime("%Y-%m-%dT%H:%M:%S"),
            datetime.datetime.fromtimestamp(
                tokiotest.SAMPLE_LMTDB_END).strftime("%Y-%m-%dT%H:%M:%S"),
        ],
        'validators': [verify_sqlite,],
        'to_file':    [True],
        'validate_contents': False,
    },
    {
        'description': 'bin/cache_lmtdb.py --limit',
        'binary':      os.path.join(tokiotest.BIN_DIR, 'cache_lmtdb.py'),
        'args':        [
            '--limit', '2',
            '-i', tokiotest.SAMPLE_LMTDB_FILE,
            datetime.datetime.fromtimestamp(
                tokiotest.SAMPLE_LMTDB_START).strftime("%Y-%m-%dT%H:%M:%S"),
            datetime.datetime.fromtimestamp(
                tokiotest.SAMPLE_LMTDB_END).strftime("%Y-%m-%dT%H:%M:%S"),
        ],
        'validators': [verify_sqlite,],
        'to_file':    [True],
        'validate_contents': False,
    },
]

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def run_cache_connector(config, to_file=False):
    """
    Test a connector cache (cache_*.py) CLI interface
    """
    if config['binary'].endswith('cache_darshan.py'):
        tokiotest.check_darshan()

    if to_file:
        cmd = [config['binary']] \
            + ['-o', tokiotest.TEMP_FILE.name] \
            + config['args']
        print "Caching to", tokiotest.TEMP_FILE.name
        print "Executing:", ' '.join(cmd)
        output_str = subprocess.check_output(cmd)
        ### validate_contents means return the contents of the tempfile rather
        ### than the stdout of the subprocess.  Some cache tools cannot print to
        ### stdout (e.g., anything that caches to a binary format like sqlite)
        ### so we need to pass the file name to the validator instead.
        if config.get('validate_contents', True):
            output_str = tokiotest.TEMP_FILE.read()
    else:
        cmd = [config['binary']] + config['args']
        print "Caching to stdout"
        print "Executing:", ' '.join(cmd)
        output_str = subprocess.check_output(cmd)

    for validator in config['validators']:
        validator(output_str)

def craft_description(config, suffix):
    """
    Take a cache_*.py command invocation and craft a clever test description
    """
    if 'description' in config:
        result = "%s %s" % (config['description'], suffix)
    elif 'cache_topology' in config['binary']:
        result = "%s %s" % (
            os.sep.join(config['binary'].split(os.sep)[-2:]),
            suffix)
    else:
        result = "%s %s %s" % (
            os.sep.join(config['binary'].split(os.sep)[-2:]),
            ' '.join(config['args'][0:-1]),
            suffix)
    return result

@tokiotest.needs_darshan
def test():
    """
    Test all connector cache scripts
    """
    for config in CACHE_CONNECTOR_CONFIGS:
        func = run_cache_connector

        for to_file in config.get('to_file', [True, False]):
            if to_file:
                func.description = craft_description(config, '(to file)')
            else:
                func.description = craft_description(config, '(to stdout)')
            yield func, config, to_file
