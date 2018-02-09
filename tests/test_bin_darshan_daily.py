#!/usr/bin/env python
"""
Test the bin/darshan_daily_summary.py and bin/darshan_daily_scoreboard.py tools
"""

import os
import glob
import json
import subprocess
import warnings
import nose
import tokiotest

### For tests that base all tests off of the sample Darshan log
SAMPLE_DARSHAN_LOGS = glob.glob(os.path.join(os.getcwd(), 'inputs', '*.darshan'))

DARSHAN_DAILY_SUMMARY = os.path.join(tokiotest.BIN_DIR, 'darshan_daily_summary.py')
DARSHAN_DAILY_SCOREBOARD = os.path.join(tokiotest.BIN_DIR, 'darshan_daily_scoreboard.py')

@tokiotest.needs_darshan
def test_input_dir():
    """
    bin/darshan_daily_summary.py with input dir
    """
    # Need lots of error/warning suppression since our input dir contains a ton of non-Darshan logs
    warnings.filterwarnings('ignore')
    tokiotest.check_darshan()
    cmd = [DARSHAN_DAILY_SUMMARY, os.path.dirname(SAMPLE_DARSHAN_LOGS[0])]
    print "Executing:", " ".join(cmd)
    with open(os.devnull, 'w') as devnull:
        output_str = subprocess.check_output(cmd, stderr=devnull)
    decoded_result = json.loads(output_str)
    assert len(decoded_result) > 0

@tokiotest.needs_darshan
def test_input_file():
    """
    bin/darshan_daily_summary.py with one input log
    """
    tokiotest.check_darshan()
    cmd = [DARSHAN_DAILY_SUMMARY, SAMPLE_DARSHAN_LOGS[0]]
    print "Executing:", " ".join(cmd)
    output_str = subprocess.check_output(cmd)
    decoded_result = json.loads(output_str)
    assert len(decoded_result) > 0

@tokiotest.needs_darshan
def test_input_files():
    """
    bin/darshan_daily_summary.py with multiple input logs
    """
    tokiotest.check_darshan()
    cmd = [DARSHAN_DAILY_SUMMARY] + SAMPLE_DARSHAN_LOGS
    print "Executing:", " ".join(cmd)
    output_str = subprocess.check_output(cmd)
    decoded_result = json.loads(output_str)
    assert len(decoded_result) > 0

@tokiotest.needs_darshan
def test_multithreaded():
    """
    bin/darshan_daily_summary.py --threads
    """
    tokiotest.check_darshan()
    cmd = [DARSHAN_DAILY_SUMMARY, '--threads', '4'] + SAMPLE_DARSHAN_LOGS
    print "Executing:", " ".join(cmd)
    output_str = subprocess.check_output(cmd)
    decoded_result = json.loads(output_str)
    assert len(decoded_result) > 0

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_scoreboard():
    """
    bin/darshan_daily_scoreboard.py ascii output
    """
    cmd = [DARSHAN_DAILY_SUMMARY, '--output', tokiotest.TEMP_FILE.name] + SAMPLE_DARSHAN_LOGS
    print "Executing:", " ".join(cmd)
    subprocess.check_output(cmd)

    cmd = [DARSHAN_DAILY_SCOREBOARD, tokiotest.TEMP_FILE.name]
    print "Executing:", " ".join(cmd)
    output_str = subprocess.check_output(cmd)
    assert len(output_str.splitlines()) > 5

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_scoreboard_json():
    """
    bin/darshan_daily_scoreboard.py --json
    """
    cmd = [DARSHAN_DAILY_SUMMARY, '--output', tokiotest.TEMP_FILE.name] + SAMPLE_DARSHAN_LOGS
    print "Executing:", " ".join(cmd)
    subprocess.check_output(cmd)

    cmd = [DARSHAN_DAILY_SCOREBOARD, '--json', tokiotest.TEMP_FILE.name]
    print "Executing:", " ".join(cmd)
    output_str = subprocess.check_output(cmd)
    decoded_result = json.loads(output_str)
    assert len(decoded_result) > 0
