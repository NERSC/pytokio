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

### For lfsstatus connector/tool.  These values must reflect the contents of
### SAMPLE_OSTMAP_FILE and SAMPLE_OSTFULLNESS_FILE for the tests to actually
### pass.
SAMPLE_OSTMAP_FILE = os.path.join(INPUT_DIR, 'sample_ost-map.txt')
SAMPLE_OSTMAP_FILE_GZ = SAMPLE_OSTMAP_FILE + ".gz"
SAMPLE_OSTMAP_START = 1489998203
SAMPLE_OSTMAP_END = 1489998203
SAMPLE_OSTMAP_OVERLOAD_OSS = 1
SAMPLE_OSTMAP_OST_PER_OSS = 1
SAMPLE_OSTFULLNESS_FILE = os.path.join(INPUT_DIR, 'sample_ost-fullness.txt')
SAMPLE_OSTFULLNESS_FILE_GZ = SAMPLE_OSTFULLNESS_FILE + ".gz"
SAMPLE_OSTFULLNESS_START = 1489998203
SAMPLE_OSTFULLNESS_END = 1490081107


### Other cached files corresponding to SAMPLE_DARSHAN_LOG
SAMPLE_NERSCJOBSDB_FILE = os.path.join(INPUT_DIR, 'sample_nersc_jobsdb.sqlite3')
SAMPLE_NERSCJOBSDB_START = 1489872299
SAMPLE_NERSCJOBSDB_END = 1490167256
SAMPLE_NERSCJOBSDB_HOST = 'edison'
SAMPLE_LMTDB_FILE = os.path.join(INPUT_DIR, 'sample_lmtdb.sqlite3')
SAMPLE_LMTDB_START = 1506182400
SAMPLE_LMTDB_END = 1506182430
SAMPLE_XTDB2PROC_FILE = os.path.join(INPUT_DIR, 'sample.xtdb2proc')
SAMPLE_H5LMT_FILE = os.path.join(INPUT_DIR, 'sample.h5lmt')
SAMPLE_TOKIOTS_FILE = os.path.join(INPUT_DIR, 'sample_tokiots.hdf5')

### The following SLURM_CACHE_* all correspond to SLURM_CACHE_FILE; if you
### change one, you must change them all.
SAMPLE_SLURM_CACHE_FILE = os.path.join(INPUT_DIR, 'sample.slurm')
SAMPLE_SLURM_CACHE_KEYS = ['start', 'end', 'jobidraw']
SAMPLE_SLURM_CACHE_JOBCT = 1
SAMPLE_SLURM_CACHE_NODECT = 128
SAMPLE_SLURM_CACHE_MAX_WALLSECS = 3600

SAMPLE_NERSCISDCT_FILE = os.path.join(INPUT_DIR, 'sample_nersc_isdct.tgz')
# SAMPLE_NERSCISDCT_PREV_FILE is used to verify the .diff() method.  It should
# be sufficiently different from SAMPLE_NERSCISDCT_FILE to exercise .diff()'s
# ability to detect new and missing SSDs and calculate the difference between
# certain monotonic counters correctly.
SAMPLE_NERSCISDCT_DIFF_FILE = os.path.join(INPUT_DIR, 'sample_nersc_isdct.json.gz')
SAMPLE_NERSCISDCT_PREV_FILE = os.path.join(INPUT_DIR, 'sample_nersc_isdct-1.json.gz')
SAMPLE_NERSCISDCT_DIFF_RM = 6  # how many devices were removed between _PREV_FILE and _FILE
SAMPLE_NERSCISDCT_DIFF_ADD = 2 # how many devices were added between _PREV_FILE and _FILE
SAMPLE_NERSCISDCT_DIFF_ERRS = 1 # how many devices incremented error counters
SAMPLE_NERSCISDCT_DIFF_MONOTONICS = [ # counters whose values should be bigger today than yesterday
    'timestamp',
    'data_units_written_bytes',
    'data_units_read_bytes'
]
SAMPLE_NERSCISDCT_DIFF_ZEROS = ['physical_size'] # diff should always be numeric zero
SAMPLE_NERSCISDCT_DIFF_EMPTYSTR = ['model_number'] # diff should always be an empty string

SAMPLE_COLLECTDES_FILE = os.path.join(INPUT_DIR, 'sample_collectdes-full.json.gz')
SAMPLE_COLLECTDES_NUMNODES = 64
SAMPLE_COLLECTDES_TIMESTEP = 10
SAMPLE_COLLECTDES_START = '2017-12-13T00:00:00' 
SAMPLE_COLLECTDES_END = '2017-12-13T01:00:00'
# SAMPLE_COLLECTDES_FILE2 should be a complete subset of SAMPLE_COLLECTDES_FILE
SAMPLE_COLLECTDES_FILE2 = os.path.join(INPUT_DIR, 'sample_collectdes-part.json.gz')
SAMPLE_COLLECTDES_START2 = '2017-12-13T00:30:00' 
SAMPLE_COLLECTDES_END2 = '2017-12-13T01:00:00'

SAMPLE_COLLECTDES_HDF5 = os.path.join(INPUT_DIR, 'sample_tokiots.hdf5')
SAMPLE_COLLECTDES_DSET = '/bytes/readrates'

SAMPLE_COLLECTDES_INDEX = 'cori-collectd-*' # this test will ONLY work at NERSC
SAMPLE_COLLECTDES_QUERY = {
    "query": {
        "bool": {
            "must": {
                "query_string": {
                    "query": "hostname:bb* AND plugin:disk AND collectd_type:disk_octets AND plugin_instance:nvme*",
                    "analyze_wildcard": True,
                },
            },
            "filter": {
                "range": {
                    "@timestamp": {},
                },
            },
        },
    },
}


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
