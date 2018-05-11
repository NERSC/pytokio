#!/usr/bin/env python
"""
Test the bin/summarize_darshanlogs.py and bin/darshan_scoreboard.py tools
"""

import os
import glob
import json
import warnings
import nose
import tokiotest
import tokiobin.summarize_darshanlogs
import tokiobin.darshan_scoreboard

### For tests that base all tests off of the sample Darshan log
SAMPLE_DARSHAN_LOGS = glob.glob(os.path.join(os.getcwd(), 'inputs', '*.darshan'))

@tokiotest.needs_darshan
def test_input_dir():
    """
    bin/summarize_darshanlogs.py with input dir
    """
    # Need lots of error/warning suppression since our input dir contains a ton of non-Darshan logs
    warnings.filterwarnings('ignore')
    tokiotest.check_darshan()
    argv = [os.path.dirname(SAMPLE_DARSHAN_LOGS[0])]
    print "Executing:", " ".join(argv)
    output_str = tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)
    decoded_result = json.loads(output_str)
    assert len(decoded_result) > 0

@tokiotest.needs_darshan
def test_input_file():
    """
    bin/summarize_darshanlogs.py with one input log
    """
    tokiotest.check_darshan()
    argv = [SAMPLE_DARSHAN_LOGS[0]]
    print "Executing:", " ".join(argv)
    output_str = tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)
    decoded_result = json.loads(output_str)
    assert len(decoded_result) > 0

@tokiotest.needs_darshan
def test_input_files():
    """
    bin/summarize_darshanlogs.py with multiple input logs
    """
    tokiotest.check_darshan()
    argv = SAMPLE_DARSHAN_LOGS
    print "Executing:", " ".join(argv)
    output_str = tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)
    decoded_result = json.loads(output_str)
    assert len(decoded_result) > 0

@tokiotest.needs_darshan
def test_multithreaded():
    """
    bin/summarize_darshanlogs.py --threads
    """
    tokiotest.check_darshan()
    argv = ['--threads', '4'] + SAMPLE_DARSHAN_LOGS
    print "Executing:", " ".join(argv)
    output_str = tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)
    decoded_result = json.loads(output_str)
    assert len(decoded_result) > 0

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_scoreboard():
    """
    bin/darshan_scoreboard.py ascii output
    """
    argv = ['--output', tokiotest.TEMP_FILE.name] + SAMPLE_DARSHAN_LOGS
    print "Executing:", " ".join(argv)
    tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)

    argv = [tokiotest.TEMP_FILE.name]
    print "Executing:", " ".join(argv)
    output_str = tokiotest.run_bin(tokiobin.darshan_scoreboard, argv)
    assert len(output_str.splitlines()) > 5

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_scoreboard_json():
    """
    bin/darshan_scoreboard.py --json
    """
    argv = ['--output', tokiotest.TEMP_FILE.name] + SAMPLE_DARSHAN_LOGS
    print "Executing:", " ".join(argv)
    tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)

    argv = ['--json', tokiotest.TEMP_FILE.name]
    print "Executing:", " ".join(argv)
    output_str = tokiotest.run_bin(tokiobin.darshan_scoreboard, argv)
    decoded_result = json.loads(output_str)
    assert len(decoded_result) > 0
