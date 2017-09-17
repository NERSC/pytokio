#!/usr/bin/env python
"""
This script tests the basic functionality of each connector's standalone cache
script that just wraps a CLI interface around the connector's caching
methods.

"""

import os
import errno
import json
import pandas
import tempfile
import StringIO
import subprocess
import nose
import tokio.connectors.darshan

def verify_json(json_str):
    data = json.loads(json_str)
    assert len(data) > 0

def verify_csv(csv_str):
    data = pandas.read_csv(StringIO.StringIO(csv_str))
    assert len(data) > 0

def verify_sacct(csv_str):
    data = pandas.read_csv(StringIO.StringIO(csv_str), sep="|")
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
    {
        'binary':    os.path.join(BIN_DIR, 'cache_slurm.py'),
        'args':       [ '--json', os.path.join(INPUT_DIR, 'sample.slurm') ],
        'validators': [ verify_json, ],
    },
    {
        'binary':    os.path.join(BIN_DIR, 'cache_slurm.py'),
        'args':       [ '--csv', os.path.join(INPUT_DIR, 'sample.slurm') ],
        'validators': [ verify_csv, ],
    },
    {
        'binary':    os.path.join(BIN_DIR, 'cache_slurm.py'),
        'args':       [ '--native', os.path.join(INPUT_DIR, 'sample.slurm') ],
        'validators': [ verify_sacct, ],
    },
]

TEMP_FILE = None
SKIP_DARSHAN = False # if darshan-parser isn't present
FNULL = None

def setup():
    global TEMP_FILE
    global SKIP_DARSHAN
    global FNULL
    TEMP_FILE = tempfile.NamedTemporaryFile(delete=False)
    FNULL = open(os.devnull, 'w')
    try:
        subprocess.check_output(tokio.connectors.darshan.DARSHAN_PARSER_BIN, stderr=subprocess.STDOUT)
    except OSError as error:
        if error[0] == errno.ENOENT:
            SKIP_DARSHAN = True
    except subprocess.CalledProcessError:
        # this is ok--there's no way to make darshan-parser return zero without
        # giving it a real darshan log
        pass

def teardown():
    global TEMP_FILE
    global FNULL
    if not TEMP_FILE.closed:
        TEMP_FILE.close()
    if os.path.exists(TEMP_FILE.name):
        os.unlink(TEMP_FILE.name)
    FNULL.close()

def run_cache_connector(binary, args, validators):
    global TEMP_FILE

    if SKIP_DARSHAN and binary.endswith('cache_darshan.py'):
        raise nose.SkipTest("%s not available for %s" % (
                            tokio.connectors.darshan.DARSHAN_PARSER_BIN,
                            binary))

    ### first test caching to stdout
    cmd = [ binary ] + args
    print "Executing:", cmd
    print "Caching to stdout..."
    output_str = subprocess.check_output(cmd)
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

@nose.tools.with_setup(setup, teardown)
def test():
    """
    Test all connector cache scripts
    """
    for config in CACHE_CONNECTOR_CONFIGS:
        func = run_cache_connector
        func.description = "Testing cache connector script: %s" % os.path.basename(config['binary'])
        yield func, config['binary'], config['args'], config['validators']
