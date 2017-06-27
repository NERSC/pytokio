#!/usr/bin/env python

import os
import subprocess

INPUTS = os.path.join(os.getcwd(), 'inputs')

# For tests that base all tests off of the sample Darshan log
SAMPLE_DARSHAN_LOG = os.path.join(INPUTS, 'sample.darshan')

# Other cached files corresponding to SAMPLE_DARSHAN_LOG
SAMPLE_XTDB2PROC_FILE = os.path.join(INPUTS, 'sample.xtdb2proc')
SAMPLE_OSTMAP_FILE = os.path.join(INPUTS, 'sample_ost-map.txt')
SAMPLE_OSTFULLNESS_FILE = os.path.join(INPUTS, 'sample_ost-fullness.txt')

# For tests that function without the Darshan log
#
# These values must reflect the contents of SAMPLE_DARSHAN_LOG 
# for the tests to actually pass
SAMPLE_DARSHAN_JOBID = '69443066'
SAMPLE_DARSHAN_START_TIME = '2017-03-20 02:07:47'
SAMPLE_DARSHAN_END_TIME = '2017-03-20 02:09:43' 
SAMPLE_DARSHAN_FILE_SYSTEM = 'scratch2'

BINARY = os.path.join('..', 'abcutil', 'summarize_job.py')

def test_darshan_summary():
    assert not subprocess.check_call([BINARY, '--json', SAMPLE_DARSHAN_LOG],
                                     stdout=open(os.devnull, 'w'))

def test_darshan_summary_with_lfsstatus():
    assert not subprocess.check_call([BINARY, '--json','--ost', 
                                      '--ost-fullness', SAMPLE_OSTFULLNESS_FILE, 
                                      '--ost-map', SAMPLE_OSTMAP_FILE, 
                                      SAMPLE_DARSHAN_LOG
                                  ], stdout=open(os.devnull, 'w'))

def test_darshan_summary_with_lmt():
    assert not subprocess.check_call([ BINARY, '--json',
                                       '--jobid', SAMPLE_DARSHAN_JOBID,
                                       '--start-time', SAMPLE_DARSHAN_START_TIME,
                                       '--end-time', SAMPLE_DARSHAN_END_TIME,
                                       '--file-system', SAMPLE_DARSHAN_FILE_SYSTEM,
                                   ], stdout=open(os.devnull, 'w'))

#def test_darshan_summary_with_craysdb():

#    """
#    requires either an SDB cache file or access to xtdb2proc
#    requires access to NERSC jobs db (to map jobid to node list)
#    """
#    assert subprocess.check_call([ BINARY, '--craysdb', SAMPLE_XTDB2PROC_FILE, '--json', SAMPLE_DARSHAN_LOG ]) == 0
