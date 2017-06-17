#!/usr/bin/env python

import os
import subprocess

SAMPLE_INPUT = os.path.join(os.getcwd(), 'inputs', 'sample.darshan')
SAMPLE_XTDB2PROC_FILE = os.path.join(os.getcwd(), 'inputs', 'sample.xtdb2proc')

BINARY = os.path.join('..', 'abcutil', 'summarize_job.py')

def test_darshan_summary():
    assert subprocess.check_call([ BINARY, '--json', SAMPLE_INPUT ], stdout=open(os.devnull, 'w')) == 0

#def test_darshan_summary_with_craysdb():
#    """
#    requires either an SDB cache file or access to xtdb2proc
#    requires access to NERSC jobs db (to map jobid to node list)
#    """
#    assert subprocess.check_call([ BINARY, '--craysdb', SAMPLE_XTDB2PROC_FILE, '--json', SAMPLE_INPUT ]) == 0

### cannot test --ost because it does not yet accept a path to a cache file

### cannot test darshan-less summary because summarize_job.py will currently fail if it cannot find valid LMT data
