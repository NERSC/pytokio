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
LOGS_FROM_DIR = glob.glob(os.path.join(tokiotest.SAMPLE_DARSHAN_LOG_DIR, '*', '*', '*', '*.darshan'))
FILTER_FOR_EXE = ['vpicio_uni', 'dbscan_read']

@tokiotest.needs_darshan
def test_input_dir():
    """bin/summarize_darshanlogs.py with input dir
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
    """bin/summarize_darshanlogs.py with one input log
    """
    tokiotest.check_darshan()
    argv = [SAMPLE_DARSHAN_LOGS[0]]
    print "Executing:", " ".join(argv)
    output_str = tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)
    decoded_result = json.loads(output_str)
    assert len(decoded_result) > 0

@tokiotest.needs_darshan
def test_input_files():
    """bin/summarize_darshanlogs.py with multiple input logs
    """
    tokiotest.check_darshan()
    argv = SAMPLE_DARSHAN_LOGS
    print "Executing:", " ".join(argv)
    output_str = tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)
    decoded_result = json.loads(output_str)
    assert len(decoded_result) > 0

@tokiotest.needs_darshan
def test_multithreaded():
    """bin/summarize_darshanlogs.py --threads
    """
    tokiotest.check_darshan()
    argv = ['--threads', '4'] + SAMPLE_DARSHAN_LOGS
    print "Executing:", " ".join(argv)
    output_str = tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)
    decoded_result = json.loads(output_str)
    assert len(decoded_result) > 0

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_scoreboard():
    """bin/darshan_scoreboard.py ascii output
    """
    tokiotest.check_darshan()
    argv = ['--output', tokiotest.TEMP_FILE.name] + LOGS_FROM_DIR
    print "Executing:", " ".join(argv)
    tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)

    argv = [tokiotest.TEMP_FILE.name]
    print "Executing:", " ".join(argv)
    output_str = tokiotest.run_bin(tokiobin.darshan_scoreboard, argv)
    assert len(output_str.splitlines()) > 5

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_scoreboard_json():
    """bin/darshan_scoreboard.py --json
    """
    tokiotest.check_darshan()
    argv = ['--output', tokiotest.TEMP_FILE.name] + LOGS_FROM_DIR
    print "Executing:", " ".join(argv)
    tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)

    argv = ['--json', tokiotest.TEMP_FILE.name]
    print "Executing:", " ".join(argv)
    output_str = tokiotest.run_bin(tokiobin.darshan_scoreboard, argv)
    decoded_result = json.loads(output_str)
    print "Result:", decoded_result
    assert len(decoded_result) > 0

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_scoreboard_limit_user():
    """bin/darshan_scoreboard.py --limit-user
    """
    tokiotest.check_darshan()
    argv = ['--output', tokiotest.TEMP_FILE.name] + LOGS_FROM_DIR
    print "Executing:", " ".join(argv)
    tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)

    argv = ['--json', '--limit-user', tokiotest.SAMPLE_DARSHAN_LOG_USER, tokiotest.TEMP_FILE.name]
    print "Executing:", " ".join(argv)
    output_str = tokiotest.run_bin(tokiobin.darshan_scoreboard, argv)
    decoded_result = json.loads(output_str)
    print "Result:", decoded_result
    assert decoded_result['per_user']

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_scoreboard_exclude_user():
    """bin/darshan_scoreboard.py --exclude-user
    """
    tokiotest.check_darshan()
    argv = ['--output', tokiotest.TEMP_FILE.name] + LOGS_FROM_DIR
    print "Executing:", " ".join(argv)
    tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)

    argv = ['--json', '--exclude-user', "%s" % tokiotest.SAMPLE_DARSHAN_LOG_USER, tokiotest.TEMP_FILE.name]
    print "Executing:", " ".join(argv)
    output_str = tokiotest.run_bin(tokiobin.darshan_scoreboard, argv)
    decoded_result = json.loads(output_str)
    print "Result:", decoded_result
    assert not decoded_result['per_user']
    assert not decoded_result['per_exe']
    assert not decoded_result['per_fs']

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_scoreboard_limit_fs():
    """bin/darshan_scoreboard.py --limit-fs
    """
    tokiotest.check_darshan()
    argv = ['--output', tokiotest.TEMP_FILE.name] + LOGS_FROM_DIR
    print "Executing:", " ".join(argv)
    tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)

    argv = ['--json', '--limit-fs', tokiotest.SAMPLE_DARSHAN_ALL_MOUNTS, tokiotest.TEMP_FILE.name]
    print "Executing:", " ".join(argv)
    output_str = tokiotest.run_bin(tokiobin.darshan_scoreboard, argv)
    decoded_result = json.loads(output_str)
    print "Result:", decoded_result
    assert decoded_result['per_user']
    assert decoded_result['per_fs']
    assert decoded_result['per_exe']

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_scoreboard_exclude_fs():
    """bin/darshan_scoreboard.py --exclude-fs
    """
    tokiotest.check_darshan()
    argv = ['--output', tokiotest.TEMP_FILE.name] + LOGS_FROM_DIR
    print "Executing:", " ".join(argv)
    tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)

    argv = ['--json', '--exclude-fs', "%s" % tokiotest.SAMPLE_DARSHAN_ALL_MOUNTS, tokiotest.TEMP_FILE.name]
    print "Executing:", " ".join(argv)
    output_str = tokiotest.run_bin(tokiobin.darshan_scoreboard, argv)
    decoded_result = json.loads(output_str)
    print decoded_result
    assert not decoded_result['per_user']
    assert not decoded_result['per_exe']
    assert not decoded_result['per_fs']

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_scoreboard_limit_fs_logical():
    """bin/darshan_scoreboard.py --limit-fs, logical fs names
    """
    tokiotest.check_darshan()
    argv = ['--output', tokiotest.TEMP_FILE.name] + LOGS_FROM_DIR
    print "Executing:", " ".join(argv)
    tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)

    argv = ['--json', '--limit-fs', tokiotest.SAMPLE_DARSHAN_ALL_MOUNTS_LOGICAL, tokiotest.TEMP_FILE.name]
    print "Executing:", " ".join(argv)
    output_str = tokiotest.run_bin(tokiobin.darshan_scoreboard, argv)
    decoded_result = json.loads(output_str)
    print "Result:", decoded_result
    assert decoded_result['per_user']
    assert decoded_result['per_fs']
    assert decoded_result['per_exe']

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_scoreboard_exclude_fs_logical():
    """bin/darshan_scoreboard.py --exclude-fs, logical fs names
    """
    tokiotest.check_darshan()
    argv = ['--output', tokiotest.TEMP_FILE.name] + LOGS_FROM_DIR
    print "Executing:", " ".join(argv)
    tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)

    argv = ['--json', '--exclude-fs', "%s" % tokiotest.SAMPLE_DARSHAN_ALL_MOUNTS_LOGICAL, tokiotest.TEMP_FILE.name]
    print "Executing:", " ".join(argv)
    output_str = tokiotest.run_bin(tokiobin.darshan_scoreboard, argv)
    decoded_result = json.loads(output_str)
    print decoded_result
    assert not decoded_result['per_user']
    assert not decoded_result['per_exe']
    assert not decoded_result['per_fs']

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_scoreboard_limit_exe():
    """bin/darshan_scoreboard.py --limit-exe
    """
    tokiotest.check_darshan()
    argv = ['--output', tokiotest.TEMP_FILE.name] + LOGS_FROM_DIR
    print "Executing:", " ".join(argv)
    tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)

    # try to get back a few specific exes that we know exist (FILTER_FOR_EXE)
    for appname in FILTER_FOR_EXE + [','.join(FILTER_FOR_EXE)]:
        argv = ['--json', '--limit-exe', appname, tokiotest.TEMP_FILE.name]
        print "Executing:", " ".join(argv)
        output_str = tokiotest.run_bin(tokiobin.darshan_scoreboard, argv)
        decoded_result = json.loads(output_str)
        print "Result:", decoded_result

        wanted_apps = appname.split(',')
        # make sure that we got back the same number of apps as we queried for
        assert len(decoded_result['per_exe']) == len(wanted_apps)

        # make sure that each app we wanted is defined in the results
        for wanted_app in wanted_apps:
            assert decoded_result['per_exe'][wanted_app]

@tokiotest.needs_darshan
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_scoreboard_exclude_exe():
    """bin/darshan_scoreboard.py --exclude-exe
    """
    tokiotest.check_darshan()
    argv = ['--output', tokiotest.TEMP_FILE.name] + LOGS_FROM_DIR
    print "Executing:", " ".join(argv)
    tokiotest.run_bin(tokiobin.summarize_darshanlogs, argv)

    # get a reference dataset without anything removed
    argv = ['--json', tokiotest.TEMP_FILE.name]
    print "Executing:", " ".join(argv)
    output_str = tokiotest.run_bin(tokiobin.darshan_scoreboard, argv)
    reference_result = json.loads(output_str)
    print "Result:", reference_result 

    for appname in FILTER_FOR_EXE + [','.join(FILTER_FOR_EXE)]:
        argv = ['--json', '--exclude-exe', appname, tokiotest.TEMP_FILE.name]
        print "Executing:", " ".join(argv)
        output_str = tokiotest.run_bin(tokiobin.darshan_scoreboard, argv)
        decoded_result = json.loads(output_str)
        print "Result:", decoded_result

        # make sure the app is not included in the results
        assert appname not in decoded_result['per_exe']

        # make sure that we successfully removed something that was present in
        # the unfiltered reference
        assert len(decoded_result['per_exe']) < len(reference_result['per_exe'])
