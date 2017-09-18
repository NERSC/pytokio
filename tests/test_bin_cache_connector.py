#!/usr/bin/env python
"""
Test each connector's standalone CLI cache tool
"""

import os
import json
import StringIO
import subprocess
import pandas
import nose
import tokiotest

# For cache_lfsstatus.py
os.environ['PYTOKIO_H5LMT_BASE'] = tokiotest.INPUT_DIR

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

BIN_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'bin')

CACHE_CONNECTOR_CONFIGS = [
    {
        'binary':     os.path.join(BIN_DIR, 'cache_isdct.py'),
        'args':       ['--json', tokiotest.SAMPLE_NERSCISDCT_FILE],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_isdct.py'),
        'args':       ['--csv', tokiotest.SAMPLE_NERSCISDCT_FILE],
        'validators': [verify_csv,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       ['--base', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       ['--perf', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       ['--total', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       ['--base', '--perf', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       ['--base', '--total', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       ['--perf', '--total', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       ['--base', '--perf', '--total', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_slurm.py'),
        'args':       ['--json', tokiotest.SAMPLE_SLURM_CACHE_FILE],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_slurm.py'),
        'args':       ['--csv', tokiotest.SAMPLE_SLURM_CACHE_FILE],
        'validators': [verify_csv,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_slurm.py'),
        'args':       ['--native', tokiotest.SAMPLE_SLURM_CACHE_FILE],
        'validators': [verify_sacct,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_topology.py'),
        'args':       [
            '--craysdb-cache', tokiotest.SAMPLE_XTDB2PROC_FILE,
            '--slurm-cache', tokiotest.SAMPLE_SLURM_CACHE_FILE,
        ],
        'validators': [verify_json,],
    },
    {
        'description': 'bin/cache_lfsstatus.py --fullness, no cache',
        'binary':      os.path.join(BIN_DIR, 'cache_lfsstatus.py'),
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
        'binary':      os.path.join(BIN_DIR, 'cache_lfsstatus.py'),
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
        'binary':      os.path.join(BIN_DIR, 'cache_lfsstatus.py'),
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
        'binary':      os.path.join(BIN_DIR, 'cache_lfsstatus.py'),
        'args':        [
            '--failure',
            tokiotest.SAMPLE_OSTMAP_FILE,
            tokiotest.SAMPLE_DARSHAN_SONEXION_ID,
            tokiotest.SAMPLE_DARSHAN_START_TIME.replace(' ', 'T'),
        ],
        'validators':  [verify_json,],
    },
]

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def run_cache_connector(binary, args, validators, to_file=False):
    """
    Test a connector cache (cache_*.py) CLI interface
    """
    if binary.endswith('cache_darshan.py'):
        tokiotest.check_darshan()

    if to_file:
        cmd = [binary] + ['-o', tokiotest.TEMP_FILE.name] + args
        print "Caching to", tokiotest.TEMP_FILE.name
        print "Executing:", cmd
        subprocess.check_output(cmd)
        output_str = tokiotest.TEMP_FILE.read()
    else:
        cmd = [binary] + args
        print "Caching to stdout"
        print "Executing:", cmd
        output_str = subprocess.check_output(cmd)

    for validator in validators:
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

        func.description = craft_description(config, '(to stdout)')
        yield func, config['binary'], config['args'], config['validators'], False

        func.description = craft_description(config, '(to file)')
        yield func, config['binary'], config['args'], config['validators'], True
