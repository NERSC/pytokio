#!/usr/bin/env python

import os
import json
import subprocess

### For tests that base all tests off of the sample Darshan log
SAMPLE_DARSHAN_LOG = os.path.join(os.getcwd(), 'inputs', 'sample.darshan')

### For tests that function without the Darshan log--these values must reflect
### the contents of SAMPLE_DARSHAN_LOG for the tests to actually pass
SAMPLE_DARSHAN_JOBID = '69443066'
SAMPLE_DARSHAN_START_TIME = '2017-03-20 02:07:47' # start_time_asci: Mon Mar 20 02:07:47 2017
SAMPLE_DARSHAN_END_TIME = '2017-03-20 02:09:43' # end_time_asci: Mon Mar 20 02:09:43 2017
SAMPLE_DARSHAN_FILE_SYSTEM = '_test'

### Other cached files corresponding to SAMPLE_DARSHAN_LOG
SAMPLE_XTDB2PROC_FILE = os.path.join(os.getcwd(), 'inputs', 'sample.xtdb2proc')
SAMPLE_OSTMAP_FILE = os.path.join(os.getcwd(), 'inputs', 'sample_ost-map.txt')
SAMPLE_OSTFULLNESS_FILE = os.path.join(os.getcwd(), 'inputs', 'sample_ost-fullness.txt')

BINARY = os.path.join('..', 'bin', 'summarize_job.py')

def verify_output(output_str, key=None, value=None):
    """
    Given the stdout of summarize_job.py --json, ensure that it is valid json,
    and ensure that a given key is present
    """
    for parsed_data in json.loads(output_str):
        if key is not None:
            assert key in parsed_data.keys()
            if value is not None:
                assert parsed_data[key] == value

    return True

def test_darshan_summary():
    """
    Baseline integration of darshan and LMT data

    """
    p = subprocess.Popen([ BINARY, '--json', SAMPLE_DARSHAN_LOG ], stdout=subprocess.PIPE)
    output_str = p.communicate()[0]
    assert p.returncode == 0
    assert verify_output(output_str, key='darshan_agg_perf_by_slowest_posix')
    assert verify_output(output_str, key='lmt_tot_gibs_written')

def test_darshan_summary_with_craysdb():
    """
    Integration of CraySdb

    requires either an SDB cache file or access to xtdb2proc
    requires either access to Slurm or a Slurm job cache file (to map jobid to node list)

    """
    p = subprocess.Popen([ BINARY, '--craysdb', SAMPLE_XTDB2PROC_FILE, '--json', SAMPLE_DARSHAN_LOG ], stdout=subprocess.PIPE)
    output_str = p.communicate()[0]
    assert p.returncode == 0
    assert verify_output(output_str, key='darshan_agg_perf_by_slowest_posix')
    assert verify_output(output_str, key='lmt_tot_gibs_written')
    assert verify_output(output_str, key='craysdb_job_max_radius')

def test_darshan_summary_with_lfsstatus():
    """
    Integration of lfsstatus

    """
    p = subprocess.Popen([ BINARY,
                           '--json',
                           '--ost',
                           '--ost-fullness', SAMPLE_OSTFULLNESS_FILE,
                           '--ost-map', SAMPLE_OSTMAP_FILE,
                           SAMPLE_DARSHAN_LOG ], stdout=subprocess.PIPE)
    output_str = p.communicate()[0]
    print output_str
    assert p.returncode == 0
    assert verify_output(output_str, key='darshan_agg_perf_by_slowest_posix')
    assert verify_output(output_str, key='lmt_tot_gibs_written')

def test_summary_without_darshan():
    """
    LMT-only functionality when no darshan log is present

    """
    os.environ['PYTOKIO_H5LMT_BASE'] = os.path.join(os.getcwd(), 'inputs' )
    p = subprocess.Popen([ BINARY,
                           '--json',
                           '--jobid', SAMPLE_DARSHAN_JOBID,
                           '--start-time', SAMPLE_DARSHAN_START_TIME,
                           '--end-time', SAMPLE_DARSHAN_END_TIME,
                           '--file-system', SAMPLE_DARSHAN_FILE_SYSTEM,
                           ], stdout=subprocess.PIPE)
    output_str = p.communicate()[0]
    assert p.returncode == 0
    assert verify_output(output_str, key='lmt_tot_gibs_written')
