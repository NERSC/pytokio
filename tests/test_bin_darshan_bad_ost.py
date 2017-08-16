#!/usr/bin/env python

import os
import json
import subprocess

### For tests that base all tests off of the sample Darshan log
SAMPLE_BAD_DARSHAN_LOG = os.path.join(os.getcwd(), 'inputs', 'sample-badost.darshan')
SAMPLE_GOOD_DARSHAN_LOG = os.path.join(os.getcwd(), 'inputs', 'sample-goodost.darshan')
SAMPLE_1FILE_DARSHAN_LOG = os.path.join(os.getcwd(), 'inputs', 'sample.darshan')

STRONG_CORRELATION_CUTOFF = "0.50"
MODEST_CORRELATION_CUTOFF = "0.10"
STRONG_PVALUE_CUTOFF = "0.00001"
MODEST_PVALUE_CUTOFF = "0.01"

BINARY = os.path.join('..', 'bin', 'darshan_bad_ost.py')

def test_good_log():
    """
    Detect no false positives in a good Darshan log
    """
    p = subprocess.Popen([ BINARY, '--json', "-p", MODEST_PVALUE_CUTOFF, SAMPLE_GOOD_DARSHAN_LOG ], stdout=subprocess.PIPE)
    output_str = p.communicate()[0]
    assert p.returncode == 0
    decoded_result = json.loads(output_str)
    assert len(decoded_result) == 0

def test_bad_log():
    """
    Detect a very bad OST
    """
    p = subprocess.Popen([ BINARY, '--json', "-p", STRONG_PVALUE_CUTOFF, "-c", STRONG_CORRELATION_CUTOFF, SAMPLE_BAD_DARSHAN_LOG ], stdout=subprocess.PIPE)
    output_str = p.communicate()[0]
    assert p.returncode == 0
    decoded_result = json.loads(output_str)
    print "Received %d very bad OSTs:" % len(decoded_result)
    print json.dumps(decoded_result,indent=4)
    assert len(decoded_result) == 1

def test_single_file_log():
    """
    Handle a log with insufficient data for correlation
    """
    p = subprocess.Popen([ BINARY, '--json', SAMPLE_1FILE_DARSHAN_LOG ], stdout=subprocess.PIPE)
    output_str = p.communicate()[0]
    assert p.returncode == 0
    decoded_result = json.loads(output_str)
    assert len(decoded_result) == 0

def test_multi_file_log():
    """
    Correctly handle multiple input logs
    """
    p = subprocess.Popen([ BINARY, '--json', '-c', MODEST_CORRELATION_CUTOFF, SAMPLE_BAD_DARSHAN_LOG, SAMPLE_GOOD_DARSHAN_LOG ], stdout=subprocess.PIPE)
    output_str = p.communicate()[0]
    assert p.returncode == 0
    decoded_result = json.loads(output_str)
    assert len(decoded_result) == 0
