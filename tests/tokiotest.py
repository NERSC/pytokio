#!/usr/bin/env python
"""
Useful helpers that are used throughout the TOKIO test suite
"""

import os
import errno
import tempfile
import subprocess
import nose
import tokio.connectors.darshan

### Sample input files and their expected contents
INPUT_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'inputs')
BIN_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'bin')

### For tests that function without the Darshan log--these values must reflect
### the contents of SAMPLE_DARSHAN_LOG for the tests to actually pass
SAMPLE_DARSHAN_LOG = os.path.join(INPUT_DIR, 'sample.darshan')
SAMPLE_DARSHAN_JOBID = '4478544'
SAMPLE_DARSHAN_JOBHOST = 'edison'
SAMPLE_DARSHAN_START_TIME = '2017-03-20 02:07:47'
SAMPLE_DARSHAN_END_TIME = '2017-03-20 02:09:43'
SAMPLE_DARSHAN_FILE_SYSTEM = 'scratch2'
SAMPLE_DARSHAN_SONEXION_ID = 'snx11035'

### Other cached files corresponding to SAMPLE_DARSHAN_LOG
SAMPLE_XTDB2PROC_FILE = os.path.join(INPUT_DIR, 'sample.xtdb2proc')
SAMPLE_OSTMAP_FILE = os.path.join(INPUT_DIR, 'sample_ost-map.txt')
SAMPLE_OSTFULLNESS_FILE = os.path.join(INPUT_DIR, 'sample_ost-fullness.txt')
SAMPLE_NERSCJOBSDB_FILE = os.path.join(INPUT_DIR, 'sample.sqlite3')
SAMPLE_H5LMT_FILE = os.path.join(INPUT_DIR, 'sample.h5lmt')

### The following SLURM_CACHE_* all correspond to SLURM_CACHE_FILE; if you
### change one, you must change them all.
SAMPLE_SLURM_CACHE_FILE = os.path.join(INPUT_DIR, 'sample.slurm')
SAMPLE_SLURM_CACHE_KEYS = ['start', 'end', 'jobidraw']
SAMPLE_SLURM_CACHE_JOBCT = 1
SAMPLE_SLURM_CACHE_NODECT = 128
SAMPLE_SLURM_CACHE_MAX_WALLSECS = 3600


SAMPLE_NERSCISDCT_FILE = os.path.join(INPUT_DIR, 'sample_nersc_isdct.tgz')

### Global state
SKIP_DARSHAN = None
TEMP_FILE = None

def needs_darshan(func):
    """
    Need to check if darshan-parser is available; if not, just skip all
    Darshan-related tests
    """
    global SKIP_DARSHAN
    if SKIP_DARSHAN is not None:
        return func
    try:
        subprocess.check_output(tokio.connectors.darshan.DARSHAN_PARSER_BIN,
                                stderr=subprocess.STDOUT)
    except OSError as error:
        if error[0] == errno.ENOENT:
            SKIP_DARSHAN = True
    except subprocess.CalledProcessError:
        # this is ok--there's no way to make darshan-parser return zero without
        # giving it a real darshan log
        pass
    return func

def check_darshan():
    """
    If we don't have darshan, skip the test
    """
    global SKIP_DARSHAN
    if SKIP_DARSHAN:
        raise nose.SkipTest("%s not available" % tokio.connectors.darshan.DARSHAN_PARSER_BIN)

def create_tempfile(delete=True):
    """
    Create a temporary file
    """
    global TEMP_FILE
    TEMP_FILE = tempfile.NamedTemporaryFile(delete=delete)

def delete_tempfile():
    """
    Destroy the temporary file regardless of if the wrapped function succeeded
    or not
    """
    global TEMP_FILE
    if not TEMP_FILE.closed:
        TEMP_FILE.close()
    if os.path.isfile(TEMP_FILE.name):
        os.unlink(TEMP_FILE.name)
