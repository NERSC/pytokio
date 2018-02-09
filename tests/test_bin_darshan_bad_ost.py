#!/usr/bin/env python
"""
Test the bin/darshan_bad_ost.py tool
"""

import os
import json
import tokiotest
import tokiobin.darshan_bad_ost

### For tests that base all tests off of the sample Darshan log
SAMPLE_BAD_DARSHAN_LOG = os.path.join(os.getcwd(), 'inputs', 'sample-badost.darshan')
SAMPLE_GOOD_DARSHAN_LOG = os.path.join(os.getcwd(), 'inputs', 'sample-goodost.darshan')
SAMPLE_1FILE_DARSHAN_LOG = os.path.join(os.getcwd(), 'inputs', 'sample.darshan')

STRONG_CORRELATION_CUTOFF = "0.50"
MODEST_CORRELATION_CUTOFF = "0.10"
STRONG_PVALUE_CUTOFF = "0.00001"
MODEST_PVALUE_CUTOFF = "0.01"

@tokiotest.needs_darshan
def test_good_log():
    """
    Detect no false positives in a good Darshan log
    """
    tokiotest.check_darshan()
    argv = ['--json',
            "-p", MODEST_PVALUE_CUTOFF,
            SAMPLE_GOOD_DARSHAN_LOG]
    output_str = tokiotest.run_bin(tokiobin.darshan_bad_ost, argv)
    decoded_result = json.loads(output_str)
    assert len(decoded_result) == 0

@tokiotest.needs_darshan
def test_bad_log():
    """
    Detect a very bad OST
    """
    tokiotest.check_darshan()
    argv = ['--json',
            "-p", STRONG_PVALUE_CUTOFF,
            "-c", STRONG_CORRELATION_CUTOFF,
            SAMPLE_BAD_DARSHAN_LOG]
    output_str = tokiotest.run_bin(tokiobin.darshan_bad_ost, argv)
    decoded_result = json.loads(output_str)
    print "Received %d very bad OSTs:" % len(decoded_result)
    print json.dumps(decoded_result, indent=4)
    assert len(decoded_result) == 1

@tokiotest.needs_darshan
def test_single_file_log():
    """
    Handle a log with insufficient data for correlation
    """
    tokiotest.check_darshan()
    argv = ['--json', SAMPLE_1FILE_DARSHAN_LOG]
    output_str = tokiotest.run_bin(tokiobin.darshan_bad_ost, argv)
    decoded_result = json.loads(output_str)
    assert len(decoded_result) == 0

@tokiotest.needs_darshan
def test_multi_file_log():
    """
    Correctly handle multiple input logs
    """
    tokiotest.check_darshan()
    argv = ['--json',
            '-c', MODEST_CORRELATION_CUTOFF,
            SAMPLE_BAD_DARSHAN_LOG,
            SAMPLE_GOOD_DARSHAN_LOG]
    output_str = tokiotest.run_bin(tokiobin.darshan_bad_ost, argv)
    decoded_result = json.loads(output_str)
    assert len(decoded_result) == 0
