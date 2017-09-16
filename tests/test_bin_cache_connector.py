#!/usr/bin/env python
"""
This script tests the basic functionality of each connector's standalone cache
script that just wraps a CLI interface around the connector's caching
methods.

"""

import os
import nose
import json
import pandas
import tempfile
import StringIO
import subprocess

def verify_json(json_str):
    data = json.loads(json_str)
    assert len(data) > 0

def verify_csv(csv_str):
    data = pandas.read_csv(StringIO.StringIO(csv_str))
    assert len(data) > 0

INPUT_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'inputs')
BIN_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'bin')

CACHE_CONNECTOR_CONFIGS = [
    {
        'binary':    os.path.join(BIN_DIR, 'cache_isdct.py'),
        'args':       [ '--json', os.path.join(INPUT_DIR, 'sample_nersc_isdct.tgz') ],
        'validators': [ verify_json, ],
    },
    {
        'binary':    os.path.join(BIN_DIR, 'cache_isdct.py'),
        'args':       [ '--csv', os.path.join(INPUT_DIR, 'sample_nersc_isdct.tgz') ],
        'validators': [ verify_csv, ],
    },
    {
        'binary':    os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       [ '--base', os.path.join(INPUT_DIR, 'sample.darshan') ],
        'validators': [ verify_json, ],
    },
    {
        'binary':    os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       [ '--perf', os.path.join(INPUT_DIR, 'sample.darshan') ],
        'validators': [ verify_json, ],
    },
    {
        'binary':    os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       [ '--total', os.path.join(INPUT_DIR, 'sample.darshan') ],
        'validators': [ verify_json, ],
    },
    {
        'binary':    os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       [ '--base', '--perf', os.path.join(INPUT_DIR, 'sample.darshan') ],
        'validators': [ verify_json, ],
    },
    {
        'binary':    os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       [ '--base', '--total', os.path.join(INPUT_DIR, 'sample.darshan') ],
        'validators': [ verify_json, ],
    },
    {
        'binary':    os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       [ '--perf', '--total', os.path.join(INPUT_DIR, 'sample.darshan') ],
        'validators': [ verify_json, ],
    },
    {
        'binary':    os.path.join(BIN_DIR, 'cache_darshan.py'),
        'args':       [ '--base', '--perf', '--total', os.path.join(INPUT_DIR, 'sample.darshan') ],
        'validators': [ verify_json, ],
    },
]

TEMP_FILE = None
FNULL = None

def setup_tmpfile():
    global TEMP_FILE
    global FNULL
    TEMP_FILE = tempfile.NamedTemporaryFile(delete=False)
    FNULL = open(os.devnull, 'w')

def teardown_tmpfile():
    global TEMP_FILE
    global FNULL
    TEMP_FILE.close()
    os.unlink(TEMP_FILE.name)
    FNULL.close()

def run_cache_connector(binary, args, validators):
    global TEMP_FILE

    ### first test caching to stdout
    cmd = [ binary ] + args
    print "Executing:", cmd
    print "Caching to stdout..."
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    output_str = p.communicate()[0]
    assert p.returncode == 0
    for validator in validators:
        validator(output_str)

    ### then test caching to a file
    cmd = [ binary ] + args + [ '-o', TEMP_FILE.name ]
    print "Executing:", cmd
    print "Caching to file..."
    returncode = subprocess.call(cmd, stdout=FNULL)
    assert returncode == 0
    for validator in validators:
        validator(output_str)

@nose.tools.with_setup(setup_tmpfile, teardown_tmpfile)
def test():
    """
    Test all connector cache scripts
    """
    for config in CACHE_CONNECTOR_CONFIGS:
        func = run_cache_connector
        func.description = "Testing cache connector script: %s" % os.path.basename(config['binary'])
        yield func, config['binary'], config['args'], config['validators']
