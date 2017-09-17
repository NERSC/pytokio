#!/usr/bin/env python

import os
import json
import errno
import pandas
import StringIO
import subprocess
import nose
import tokio.connectors.darshan

INPUT_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'inputs')

### For tests that base all tests off of the sample Darshan log
SAMPLE_DARSHAN_LOG = os.path.join(INPUT_DIR, 'sample.darshan')
SAMPLE_DARSHAN_LOG_2 = os.path.join(INPUT_DIR, 'sample-2.darshan')

### For tokio.tools.hdf5, which is used by summarize_job.py
os.environ['H5LMT_BASE'] = INPUT_DIR

### For tests that function without the Darshan log--these values must reflect
### the contents of SAMPLE_DARSHAN_LOG for the tests to actually pass
SAMPLE_DARSHAN_JOBID = '4478544'
SAMPLE_DARSHAN_JOBHOST = 'edison'
SAMPLE_DARSHAN_START_TIME = '2017-03-20 02:07:47' # start_time_asci: Mon Mar 20 02:07:47 2017
SAMPLE_DARSHAN_END_TIME = '2017-03-20 02:09:43' # end_time_asci: Mon Mar 20 02:09:43 2017
SAMPLE_DARSHAN_FILE_SYSTEM = 'scratch2'

### Other cached files corresponding to SAMPLE_DARSHAN_LOG
SAMPLE_XTDB2PROC_FILE = os.path.join(INPUT_DIR, 'sample.xtdb2proc')
SAMPLE_OSTMAP_FILE = os.path.join(INPUT_DIR, 'sample_ost-map.txt')
SAMPLE_OSTFULLNESS_FILE = os.path.join(INPUT_DIR, 'sample_ost-fullness.txt')
SAMPLE_NERSCJOBSDB_FILE = os.path.join(INPUT_DIR, 'sample.sqlite3')
SAMPLE_SLURM_CACHE_FILE = os.path.join(INPUT_DIR, 'sample.slurm')

BINARY = os.path.join('..', 'bin', 'summarize_job.py')

SKIP_DARSHAN = False # if darshan-parser isn't present
def setup():
    """
    Need to check if darshan-parser is available; if not, just skip all
    Darshan-related tests
    """
    global SKIP_DARSHAN
    try:
        output_str = subprocess.check_output(tokio.connectors.darshan.DARSHAN_PARSER_BIN)
    except OSError as error:
        if error[0] == errno.ENOENT:
            SKIP_DARSHAN = True
    except subprocess.CalledProcessError:
        # this is ok--there's no way to make darshan-parser return zero without
        # giving it a real darshan log
        pass

def verify_output_json(output_str, key=None, value=None):
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

def verify_output_csv(output_str, key=None, value=None, expected_rows=None):
    """
    Given the stdout of summarize_job.py, ensure that it is re-readable by
    pandas.read_csv and ensure that a given column (key) is present
    """
    df = pandas.read_csv(StringIO.StringIO(output_str))
    if key is not None:
        assert key in df.columns

    if value is not None:
        assert df[key][0] == value

    if expected_rows is not None:
        assert len(df) == expected_rows

    return True

def test_darshan_summary_json():
    """
    Baseline integration of darshan and LMT data (json)

    """
    global SKIP_DARSHAN
    if SKIP_DARSHAN:
        raise nose.SkipTest("%s not available" % tokio.connectors.darshan.DARSHAN_PARSER_BIN)

    output_str = subprocess.check_output([
        BINARY,
        '--json',
        SAMPLE_DARSHAN_LOG])
    assert verify_output_json(output_str, key='darshan_agg_perf_by_slowest_posix')
    assert verify_output_json(output_str, key='lmt_tot_gibs_written')

def test_darshan_summary_csv():
    """
    Baseline integration of darshan and LMT data (csv)

    """
    global SKIP_DARSHAN
    if SKIP_DARSHAN:
        raise nose.SkipTest("%s not available" % tokio.connectors.darshan.DARSHAN_PARSER_BIN)

    output_str = subprocess.check_output([
        BINARY,
        SAMPLE_DARSHAN_LOG])
    assert verify_output_csv(output_str, key='darshan_agg_perf_by_slowest_posix')
    assert verify_output_csv(output_str, key='lmt_tot_gibs_written')

def test_darshan_summaries():
    """
    Correctly handle multiple Darshan logs (csv)

    """
    global SKIP_DARSHAN
    if SKIP_DARSHAN:
        raise nose.SkipTest("%s not available" % tokio.connectors.darshan.DARSHAN_PARSER_BIN)

    output_str = subprocess.check_output([
        BINARY,
        SAMPLE_DARSHAN_LOG,
        SAMPLE_DARSHAN_LOG_2])
    assert verify_output_csv(output_str, key='darshan_agg_perf_by_slowest_posix', expected_rows=2)
    assert verify_output_csv(output_str, key='lmt_tot_gibs_written', expected_rows=2)

def test_bogus_darshans():
    """
    Correctly handle mix of valid and invalid Darshan logs

    """
    global SKIP_DARSHAN
    if SKIP_DARSHAN:
        raise nose.SkipTest("%s not available" % tokio.connectors.darshan.DARSHAN_PARSER_BIN)

    with open(os.devnull, 'w') as FNULL:
        output_str = subprocess.check_output([
            BINARY,
            SAMPLE_DARSHAN_LOG,      # valid log
            SAMPLE_XTDB2PROC_FILE,   # not valid log
            SAMPLE_DARSHAN_LOG_2,    # valid log
            'garbagefile'            # file doesn't exist
            ], stderr=FNULL)
    # subprocess.check_output will throw exception if returncode != 0
    assert verify_output_csv(output_str, key='darshan_agg_perf_by_slowest_posix', expected_rows=2)
    assert verify_output_csv(output_str, key='lmt_tot_gibs_written', expected_rows=2)

def test_darshan_summary_with_topology():
    """
    Integration of CraySdb

    requires either an SDB cache file or access to xtdb2proc
    requires either access to Slurm or a Slurm job cache file (to map jobid to node list)

    """
    global SKIP_DARSHAN
    if SKIP_DARSHAN:
        raise nose.SkipTest("%s not available" % tokio.connectors.darshan.DARSHAN_PARSER_BIN)

    output_str = subprocess.check_output([
        BINARY,
        '--topology', SAMPLE_XTDB2PROC_FILE,
        '--json',
        SAMPLE_DARSHAN_LOG ])
    assert verify_output_json(output_str, key='darshan_agg_perf_by_slowest_posix')
    assert verify_output_json(output_str, key='lmt_tot_gibs_written')
    assert verify_output_json(output_str, key='topology_job_max_radius')

def test_darshan_summary_with_lfsstatus():
    """
    Integration of tools.lfsstatus

    """
    global SKIP_DARSHAN
    if SKIP_DARSHAN:
        raise nose.SkipTest("%s not available" % tokio.connectors.darshan.DARSHAN_PARSER_BIN)

    output_str = subprocess.check_output([
        BINARY,
        '--json',
        '--ost',
        '--ost-fullness', SAMPLE_OSTFULLNESS_FILE,
        '--ost-map', SAMPLE_OSTMAP_FILE,
        SAMPLE_DARSHAN_LOG ])
    print output_str
    assert verify_output_json(output_str, key='darshan_agg_perf_by_slowest_posix')
    assert verify_output_json(output_str, key='lmt_tot_gibs_written')
    assert verify_output_json(output_str, key='fshealth_ost_overloaded_pct')

def test_darshan_summary_with_nersc_jobsdb():
    """
    Integration of NerscJobsDb
    """
    global SKIP_DARSHAN
    if SKIP_DARSHAN:
        raise nose.SkipTest("%s not available" % tokio.connectors.darshan.DARSHAN_PARSER_BIN)

    output_str = subprocess.check_output([
        BINARY,
        '--json',
        '--concurrentjobs', SAMPLE_NERSCJOBSDB_FILE,
        '--jobhost', SAMPLE_DARSHAN_JOBHOST,
        SAMPLE_DARSHAN_LOG ])
    print output_str
    assert verify_output_json(output_str, key='darshan_agg_perf_by_slowest_posix')
    assert verify_output_json(output_str, key='lmt_tot_gibs_written')
    assert verify_output_json(output_str, key='jobsdb_concurrent_nodehrs')

def test_summary_without_darshan():
    """
    LMT-only functionality when no darshan log is present
    """
    os.environ['PYTOKIO_H5LMT_BASE'] = os.path.join(os.getcwd(), 'inputs' )
    output_str = subprocess.check_output([
        BINARY,
        '--json',
        '--slurm-jobid', SAMPLE_DARSHAN_JOBID,
        '--start-time', SAMPLE_DARSHAN_START_TIME,
        '--end-time', SAMPLE_DARSHAN_END_TIME,
        '--file-system', SAMPLE_DARSHAN_FILE_SYSTEM,
        ])
    assert verify_output_json(output_str, key='lmt_tot_gibs_written')

def test_most_summary_without_darshan():
    """
    Most functionality when no darshan log is present
    """
    os.environ['PYTOKIO_H5LMT_BASE'] = os.path.join(os.getcwd(), 'inputs' )
    output_str = subprocess.check_output([
        BINARY,
        '--json',
        '--slurm-jobid', SAMPLE_SLURM_CACHE_FILE,
        '--start-time', SAMPLE_DARSHAN_START_TIME,
        '--end-time', SAMPLE_DARSHAN_END_TIME,
        '--file-system', SAMPLE_DARSHAN_FILE_SYSTEM,

        '--concurrentjobs', SAMPLE_NERSCJOBSDB_FILE,
        '--jobhost', SAMPLE_DARSHAN_JOBHOST,

        '--ost',
        '--ost-fullness', SAMPLE_OSTFULLNESS_FILE,
        '--ost-map', SAMPLE_OSTMAP_FILE,

        '--topology', SAMPLE_XTDB2PROC_FILE,
        ])
    assert verify_output_json(output_str, key='lmt_tot_gibs_written')
    assert verify_output_json(output_str, key='jobsdb_concurrent_nodehrs')
    assert verify_output_json(output_str, key='fshealth_ost_overloaded_pct')
    assert verify_output_json(output_str, key='topology_job_max_radius')
