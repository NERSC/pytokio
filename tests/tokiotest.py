#!/usr/bin/env python
"""
Useful helpers that are used throughout the TOKIO test suite
"""

import os
import sys
import gzip
import errno
import tempfile
import subprocess
import datetime

try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

import nose

### Sample input files and their expected contents
PYTOKIO_HOME = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..')
INPUT_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'inputs')
BIN_DIR = os.path.join(PYTOKIO_HOME, 'bin')

sys.path.insert(0, os.path.abspath(PYTOKIO_HOME))

import tokio.connectors.darshan

SAMPLE_TIMESTAMP_DATE_FMT = "%Y-%m-%dT%H:%M:%S"
SAMPLE_TIMESTAMP_END_NOW = datetime.datetime.now().strftime(SAMPLE_TIMESTAMP_DATE_FMT)
SAMPLE_TIMESTAMP_START_NOW = (datetime.datetime.now() - datetime.timedelta(minutes=1))\
                            .strftime(SAMPLE_TIMESTAMP_DATE_FMT)

### For tests that function without the Darshan log--these values must reflect
### the contents of SAMPLE_DARSHAN_LOG for the tests to actually pass
SAMPLE_DARSHAN_LOG = os.path.join(INPUT_DIR, 'sample.darshan')
SAMPLE_DARSHAN_JOBID = '4478544'
SAMPLE_DARSHAN_JOBHOST = 'edison'
SAMPLE_DARSHAN_START_TIME = '2017-03-20 02:07:47'
SAMPLE_DARSHAN_END_TIME = '2017-03-20 02:09:43'
SAMPLE_DARSHAN_FILE_SYSTEM = 'scratch2'
SAMPLE_DARSHAN_ALL_MOUNTS = '/scratch1,/scratch2'
SAMPLE_DARSHAN_SONEXION_ID = 'snx11035'
SAMPLE_DARSHAN_LOG_DIR = os.path.join(INPUT_DIR, 'darshanlogs')
SAMPLE_DARSHAN_LOG_USER = 'glock'
SAMPLE_DARSHAN_JOBID_2 = 4487503
SAMPLE_DARSHAN_LOGS_PER_DIR = 2 # minimum number of darshan logs in each day of DARSHAN_LOG_DIR

### For lfsstatus connector/tool.  These values must reflect the contents of
### SAMPLE_OSTMAP_FILE and SAMPLE_OSTFULLNESS_FILE for the tests to actually
### pass.
SAMPLE_LCTL_DL_T_FILE = os.path.join(INPUT_DIR, 'lctl-dl-t.txt')
SAMPLE_LFS_DF_FILE = os.path.join(INPUT_DIR, 'lfs-df.txt')
SAMPLE_LCTL_DL_T_GZ = os.path.join(INPUT_DIR, 'lctl-dl-t.txt.gz')
SAMPLE_LFS_DF_GZ = os.path.join(INPUT_DIR, 'lfs-df.txt.gz')
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
SAMPLE_LMTDB_FILE = os.path.join(INPUT_DIR, 'snx11025_2018-01-28.sqlite3')
SAMPLE_LMTDB_H5LMT = os.path.join(INPUT_DIR, 'snx11025_2018-01-28.h5lmt')
SAMPLE_LMTDB_TTS_HDF5 = os.path.join(INPUT_DIR, 'snx11025_2018-01-28.hdf5')
SAMPLE_LMTDB_START = 1517126400
SAMPLE_LMTDB_END = 1517126700
SAMPLE_LMTDB_START_STAMP = datetime.datetime.fromtimestamp(SAMPLE_LMTDB_START).strftime(SAMPLE_TIMESTAMP_DATE_FMT)
SAMPLE_LMTDB_END_STAMP = datetime.datetime.fromtimestamp(SAMPLE_LMTDB_END).strftime(SAMPLE_TIMESTAMP_DATE_FMT)
SAMPLE_LMTDB_TIMESTEP = 5
SAMPLE_LMTDB_MAX_INDEX = 60
SAMPLE_LMTDB_NONMONO = os.path.join(INPUT_DIR, 'lmtdb-reset.sqlite3')
SAMPLE_LMTDB_NONMONO_START_STAMP = "2018-04-18T00:00:00"
SAMPLE_LMTDB_NONMONO_END_STAMP = "2018-04-19T00:00:00"
SAMPLE_XTDB2PROC_FILE = os.path.join(INPUT_DIR, 'sample.xtdb2proc.gz')
SAMPLE_H5LMT_FILE = os.path.join(INPUT_DIR, 'sample.h5lmt')
SAMPLE_H5LMT_DATES = ['2017-03-20', '2017-03-21']
SAMPLE_TOKIOTS_FILE = os.path.join(INPUT_DIR, 'sample_tokiots.hdf5')
SAMPLE_TIMESERIES_FILES = {
    "TOKIO HDF5": SAMPLE_TOKIOTS_FILE,
    "pylmt HDF5": SAMPLE_H5LMT_FILE,
}
SAMPLE_TIMESERIES_DATASETS = [
    "datatargets/readbytes",
    "datatargets/writerates"
]

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
SAMPLE_COLLECTDES_NUMNODES = 288
SAMPLE_COLLECTDES_SSDS_PER = 4
SAMPLE_COLLECTDES_TIMESTEP = 10
SAMPLE_COLLECTDES_START = '2017-12-13T00:00:00'
SAMPLE_COLLECTDES_END = '2017-12-13T01:00:00'
# SAMPLE_COLLECTDES_FILE2 should be a complete subset of SAMPLE_COLLECTDES_FILE
SAMPLE_COLLECTDES_FILE2 = os.path.join(INPUT_DIR, 'sample_collectdes-part.json.gz')
SAMPLE_COLLECTDES_START2 = '2017-12-13T00:30:00'
SAMPLE_COLLECTDES_END2 = '2017-12-13T01:00:00'

# SAMPLE_COLLECTDES_CPULOAD contains cpuload info to exercise the timeseries reducer
SAMPLE_COLLECTDES_CPULOAD = os.path.join(INPUT_DIR, 'collectdes_cpuloads.json.gz')

SAMPLE_COLLECTDES_HDF5 = os.path.join(INPUT_DIR, 'sample_tokiots.hdf5')
SAMPLE_COLLECTDES_DSET = '/datatargets/readrates'
SAMPLE_COLLECTDES_DSET2 = '/datatargets/writerates'

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

class CaptureOutputs(object):
    """Context manager to capture stdout/stderr
    """
    def __enter__(self):
        self.actual_stdout = sys.stdout
        self.actual_stderr = sys.stderr
        self.stdout = StringIO.StringIO()
        self.stderr = StringIO.StringIO()
        sys.stdout = self.stdout
        sys.stderr = self.stderr
        return self

    def __exit__(self, *args):
        sys.stdout = self.actual_stdout
        sys.stderr = self.actual_stderr

def run_bin(module, argv, also_error=False):
    """Run a standalone pytokio script directly and return its stdout
    
    Args:
        module: a module containing a main(argv) function
        argv (list of str): command-line parameters to pass to module.main()
        also_error (bool): return a tuple of (stdout, stderr) instead of only
            stdout

    Returns:
        str containing stdout of the called main or tuple of strings containing
        stdout and stderr
    """
    with CaptureOutputs() as output:
        module.main(argv)
        stdout = output.stdout.getvalue()
        stderr = output.stderr.getvalue()
    if also_error:
        return stdout, stderr
    return stdout

### Skipping external dependencies when unavailable ############################
SKIP_DARSHAN = None
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

SKIP_LFSHEALTH = None
def needs_lustre_cli(func):
    """
    Check if Lustre CLI is available; if not, just skip tests
    """
    global SKIP_LFSHEALTH
    if SKIP_LFSHEALTH is not None:
        return func
    try:
        subprocess.check_output(tokio.connectors.lfshealth.LCTL_DL_T, stderr=subprocess.STDOUT)
        subprocess.check_output(tokio.connectors.lfshealth.LFS_DF, stderr=subprocess.STDOUT)
    except OSError as error:
        if error[0] == errno.ENOENT:
            SKIP_LFSHEALTH = True
    except subprocess.CalledProcessError:
        # this is ok--there's no way to make darshan-parser return zero without
        # giving it a real darshan log
        pass

    return func

def check_lustre_cli():
    """
    If lctl or lfs isn't available, skip the test
    """
    global SKIP_LFSHEALTH
    if SKIP_LFSHEALTH:
        raise nose.SkipTest("%s or %s not available" % (tokio.connectors.lfshealth.LCTL,
                                                        tokio.connectors.lfshealth.LFS))

SKIP_SLURM = None
def needs_slurm(func):
    """
    Check if Slurm CLI is available; if not, just skip tests
    """
    global SKIP_SLURM
    if SKIP_SLURM is not None:
        return func
    try:
        subprocess.check_output(tokio.connectors.slurm.SACCT, stderr=subprocess.STDOUT)
    except OSError as error:
        if error[0] == errno.ENOENT:
            SKIP_SLURM = True
    except subprocess.CalledProcessError:
        pass

    return func

def check_slurm():
    """
    If sacct isn't available, skip the test
    """
    global SKIP_SLURM
    if SKIP_SLURM:
        raise nose.SkipTest("%s not available" % (tokio.connectors.slurm.SACCT))


### Managing temporary files ###################################################

TEMP_FILE = None
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

def gunzip(input_filename, output_filename):
    """
    To check support for both compressed and uncompressed data streams, create
    an uncompressed version of an input file on the fly
    """
    try_unlink(output_filename)
    with gzip.open(input_filename, 'rb') as input_file:
        file_content = input_file.read()
    with open(output_filename, 'w+b') as output_file:
        print "Creating %s" % output_filename
        output_file.write(file_content)

def try_unlink(output_filename):
    """
    Destroy a temporarily decompressed input file
    """
    if os.path.exists(output_filename):
        print "Destroying %s" % output_filename
        os.unlink(output_filename)
