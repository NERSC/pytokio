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

INPUT_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'inputs')
BIN_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'bin')

CACHE_CONNECTOR_CONFIGS = [
    {
        'binary':     os.path.join(BIN_DIR, 'cache_isdct.py'),
        'args':       ['--json', os.path.join(INPUT_DIR, 'sample_nersc_isdct.tgz')],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_isdct.py'),
        'args':       ['--csv', os.path.join(INPUT_DIR, 'sample_nersc_isdct.tgz')],
        'validators': [verify_csv,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       ['--base', os.path.join(INPUT_DIR, 'sample.darshan')],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       ['--perf', os.path.join(INPUT_DIR, 'sample.darshan')],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       ['--total', os.path.join(INPUT_DIR, 'sample.darshan')],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       ['--base', '--perf', os.path.join(INPUT_DIR, 'sample.darshan')],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       ['--base', '--total', os.path.join(INPUT_DIR, 'sample.darshan')],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       ['--perf', '--total', os.path.join(INPUT_DIR, 'sample.darshan')],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       ['--base', '--perf', '--total', os.path.join(INPUT_DIR, 'sample.darshan')],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_slurm.py'),
        'args':       ['--json', os.path.join(INPUT_DIR, 'sample.slurm')],
        'validators': [verify_json,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_slurm.py'),
        'args':       ['--csv', os.path.join(INPUT_DIR, 'sample.slurm')],
        'validators': [verify_csv,],
    },
    {
        'binary':     os.path.join(BIN_DIR, 'cache_slurm.py'),
        'args':       ['--native', os.path.join(INPUT_DIR, 'sample.slurm')],
        'validators': [verify_sacct,],
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
        cmd = [binary] + args + ['-o', tokiotest.TEMP_FILE.name]
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

@tokiotest.needs_darshan
def test():
    """
    Test all connector cache scripts
    """
    for config in CACHE_CONNECTOR_CONFIGS:
        func = run_cache_connector

        func.description = "%s %s (to stdout)" % (
            os.sep.join(config['binary'].split(os.sep)[-2:]),
            ' '.join(config['args'][0:-1]))
        yield func, config['binary'], config['args'], config['validators'], False

        func.description = "%s %s (to file)" % (
            os.sep.join(config['binary'].split(os.sep)[-2:]),
            ' '.join(config['args'][0:-1]))
        yield func, config['binary'], config['args'], config['validators'], True
