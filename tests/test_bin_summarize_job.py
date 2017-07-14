#!/usr/bin/env python

import os
import subprocess

### For tests that base all tests off of the sample Darshan log
SAMPLE_DARSHAN_LOG = os.path.join(os.getcwd(), 'inputs', 'sample.darshan')

### For tests that function without the Darshan log--these values must reflect
### the contents of SAMPLE_DARSHAN_LOG for the tests to actually pass
SAMPLE_DARSHAN_JOBID = '69443066'
SAMPLE_DARSHAN_START_TIME = '2017-03-20 02:07:47' # start_time_asci: Mon Mar 20 02:07:47 2017
SAMPLE_DARSHAN_END_TIME = '2017-03-20 02:09:43' # end_time_asci: Mon Mar 20 02:09:43 2017
SAMPLE_DARSHAN_FILE_SYSTEM = 'scratch2'

### Other cached files corresponding to SAMPLE_DARSHAN_LOG
SAMPLE_XTDB2PROC_FILE = os.path.join(os.getcwd(), 'inputs', 'sample.xtdb2proc')
SAMPLE_OSTMAP_FILE = os.path.join(os.getcwd(), 'inputs', 'sample_ost-map.txt')
SAMPLE_OSTFULLNESS_FILE = os.path.join(os.getcwd(), 'inputs', 'sample_ost-fullness.txt')

BINARY = os.path.join('..', 'bin', 'summarize_job.py')

def test_darshan_summary():
    """
    Basic summarize_job.py functionality (darshan and LMT integration)

    """
    assert subprocess.check_call([ BINARY, '--json', SAMPLE_DARSHAN_LOG ], stdout=open(os.devnull, 'w')) == 0

#def test_darshan_summary_with_craysdb():
#    """
#    requires either an SDB cache file or access to xtdb2proc
#    requires access to NERSC jobs db (to map jobid to node list)
#    """
#    assert subprocess.check_call([ BINARY, '--craysdb', SAMPLE_XTDB2PROC_FILE, '--json', SAMPLE_DARSHAN_LOG ]) == 0

def test_darshan_summary_with_lfsstatus():
    """
    Integration between lfsstatus and summarize_job.py

    """
    assert subprocess.check_call([ BINARY,
                                   '--json',
                                   '--ost',
                                   '--ost-fullness', SAMPLE_OSTFULLNESS_FILE,
                                   '--ost-map', SAMPLE_OSTMAP_FILE,
                                   SAMPLE_DARSHAN_LOG ], stdout=open(os.devnull, 'w')) == 0

def test_summary_without_darshan():
    """
    Functionality when no darshan log is present

    """
    assert subprocess.check_call([ BINARY,
                                   '--json',
                                   '--jobid', SAMPLE_DARSHAN_JOBID,
                                   '--start-time', SAMPLE_DARSHAN_START_TIME,
                                   '--end-time', SAMPLE_DARSHAN_END_TIME,
                                   '--file-system', SAMPLE_DARSHAN_FILE_SYSTEM,
                                   ], stdout=open(os.devnull, 'w')) == 0
