#!/usr/bin/env python
"""
Test each connector's standalone CLI cache tool
"""

import os
import json
import sqlite3
import StringIO
import datetime
import pandas
import nose
import tokiotest
import tokiobin.cache_isdct
import tokiobin.cache_darshan
import tokiobin.cache_slurm
import tokiobin.cache_topology
import tokiobin.cache_lfsstatus
import tokiobin.cache_nersc_jobsdb
import tokiobin.cache_lmtdb

# For cache_lfsstatus.py
os.environ['PYTOKIO_H5LMT_BASE_DIR'] = os.path.join(tokiotest.INPUT_DIR, '%Y-%m-%d')
os.environ['PYTOKIO_LFSSTATUS_BASE_DIR'] = os.path.join(tokiotest.INPUT_DIR, '%Y-%m-%d')

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def verify_sqlite(output_str):
    """
    Ensure that the database contains at least one table, and that table
    contains at least one row.
    """
    ### Try to find the caching file name from the application's stdout
    output_file = None
    for line in output_str.splitlines():
        if line.startswith('Caching to'):
            output_file = line.strip().split(None, 3)[-1]
            break
    if output_file is None:
        print "Could not find cache file name in output:"
        print output_str
        assert output_file is not None
    print "Using output_file [%s]" % output_file
    assert os.path.isfile(output_file)
    tmpdb = sqlite3.connect(output_file)
    cursor = tmpdb.cursor()
    ## Count number of tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    tables = cursor.fetchall()
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
        'name':       'bin/cache_isdct.py',
        'binary':     tokiobin.cache_isdct,
        'args':       ['--json', tokiotest.SAMPLE_NERSCISDCT_FILE],
        'validators': [verify_json,],
    },
    {
        'name':       'bin/cache_isdct.py',
        'binary':     tokiobin.cache_isdct,
        'args':       ['--csv', tokiotest.SAMPLE_NERSCISDCT_FILE],
        'validators': [verify_csv,],
    },
    {
        'name':       'bin/cache_darshan.py',
        'binary':     tokiobin.cache_darshan,
        'args':       ['--base', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'name':       'bin/cache_darshan.py',
        'binary':     tokiobin.cache_darshan,
        'args':       ['--perf', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'name':       'bin/cache_darshan.py',
        'binary':     tokiobin.cache_darshan,
        'args':       ['--total', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'name':       'bin/cache_darshan.py',
        'binary':     tokiobin.cache_darshan,
        'args':       ['--base', '--perf', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'name':       'bin/cache_darshan.py',
        'binary':     tokiobin.cache_darshan,
        'args':       ['--base', '--total', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'name':       'bin/cache_darshan.py',
        'binary':     tokiobin.cache_darshan,
        'args':       ['--perf', '--total', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'name':       'bin/cache_darshan.py',
        'binary':     tokiobin.cache_darshan,
        'args':       ['--base', '--perf', '--total', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'name':       'bin/cache_slurm.py',
        'binary':     tokiobin.cache_slurm,
        'args':       ['--json', tokiotest.SAMPLE_SLURM_CACHE_FILE],
        'validators': [verify_json,],
    },
    {
        'name':       'bin/cache_slurm.py',
        'binary':     tokiobin.cache_slurm,
        'args':       ['--csv', tokiotest.SAMPLE_SLURM_CACHE_FILE],
        'validators': [verify_csv,],
    },
    {
        'name':       'bin/cache_slurm.py',
        'binary':     tokiobin.cache_slurm,
        'args':       ['--native', tokiotest.SAMPLE_SLURM_CACHE_FILE],
        'validators': [verify_sacct,],
    },
    {
        'name':       'bin/cache_topology.py',
        'binary':     tokiobin.cache_topology,
        'args':       [
            '--craysdb-cache', tokiotest.SAMPLE_XTDB2PROC_FILE,
            '--slurm-cache', tokiotest.SAMPLE_SLURM_CACHE_FILE,
        ],
        'validators': [verify_json,],
    },
    {
        'description': 'bin/cache_lfsstatus.py --fullness, no cache',
        'binary':     tokiobin.cache_lfsstatus,
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
        'binary':     tokiobin.cache_lfsstatus,
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
        'binary':     tokiobin.cache_lfsstatus,
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
        'binary':     tokiobin.cache_lfsstatus,
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
        'binary':     tokiobin.cache_nersc_jobsdb,
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
        'binary':     tokiobin.cache_lmtdb,
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
        'binary':     tokiobin.cache_lmtdb,
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
    if config['binary'] == tokiobin.cache_darshan:
        tokiotest.check_darshan()

    if to_file:
        argv = ['-o', tokiotest.TEMP_FILE.name] + config['args']
        print "Caching to", tokiotest.TEMP_FILE.name
        print "Executing:", ' '.join(argv)
        output_str = tokiotest.run_bin(config['binary'], argv)

        # (validate_contents == True) means the associated validator function
        # expects the contents of the output file rather than the name of the
        # output file
        if config.get('validate_contents', True):
            output_str = tokiotest.TEMP_FILE.read()
    else:
        argv = config['args']
        print "Caching to stdout"
        print "Executing:", ' '.join(argv)
        output_str = tokiotest.run_bin(config['binary'], argv)

    for validator in config['validators']:
        validator(output_str)

def craft_description(config, suffix):
    """
    Take a cache_*.py command invocation and craft a clever test description
    """
    if 'description' in config:
        result = "%s %s" % (config['description'], suffix)
    elif 'name' in config:
        result = "%s %s %s" % (
            config['name'],
            ' '.join(config['args'][0:-1]),
            suffix)
    elif config['binary'] == tokiobin.cache_topology:
        result = "%s %s" % (
            os.sep.join(config['binary'].split(os.sep)[-2:]),
            suffix)
    else:
        result = "%s %s %s" % (
            config['binary'],
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
