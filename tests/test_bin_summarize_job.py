#!/usr/bin/env python
"""
Test the bin/summarize_job.py tool
"""

import os
import json
import StringIO
import subprocess
import pandas
import tokiotest

INPUT_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'inputs')

### For tests that base all tests off of the sample Darshan log
SAMPLE_DARSHAN_LOG = os.path.join(INPUT_DIR, 'sample.darshan')
SAMPLE_DARSHAN_LOG_2 = os.path.join(INPUT_DIR, 'sample-2.darshan')

### For tokio.tools.hdf5, which is used by summarize_job.py
os.environ['PYTOKIO_H5LMT_BASE'] = INPUT_DIR

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
    dataframe = pandas.read_csv(StringIO.StringIO(output_str))
    if key is not None:
        assert key in dataframe.columns

    if value is not None:
        assert dataframe[key][0] == value

    if expected_rows is not None:
        assert len(dataframe) == expected_rows

    return True

@tokiotest.needs_darshan
def test_json():
    """
    Baseline integration of darshan and LMT data (json)

    """
    tokiotest.check_darshan()
    output_str = subprocess.check_output([
        BINARY,
        '--json',
        SAMPLE_DARSHAN_LOG])
    assert verify_output_json(output_str, key='darshan_agg_perf_by_slowest_posix')
    assert verify_output_json(output_str, key='lmt_tot_gibs_written')

@tokiotest.needs_darshan
def test_csv():
    """
    Baseline integration of darshan and LMT data (csv)

    """
    tokiotest.check_darshan()
    output_str = subprocess.check_output([
        BINARY,
        SAMPLE_DARSHAN_LOG])
    assert verify_output_csv(output_str, key='darshan_agg_perf_by_slowest_posix')
    assert verify_output_csv(output_str, key='lmt_tot_gibs_written')

@tokiotest.needs_darshan
def test_darshan_summaries():
    """
    Correctly handle multiple Darshan logs (csv)

    """
    tokiotest.check_darshan()
    output_str = subprocess.check_output([
        BINARY,
        SAMPLE_DARSHAN_LOG,
        SAMPLE_DARSHAN_LOG_2])
    assert verify_output_csv(output_str, key='darshan_agg_perf_by_slowest_posix', expected_rows=2)
    assert verify_output_csv(output_str, key='lmt_tot_gibs_written', expected_rows=2)

@tokiotest.needs_darshan
def test_bogus_darshans():
    """
    Correctly handle mix of valid and invalid Darshan logs

    """
    tokiotest.check_darshan()
    with open(os.devnull, 'w') as devnull:
        output_str = subprocess.check_output([
            BINARY,
            SAMPLE_DARSHAN_LOG,      # valid log
            SAMPLE_XTDB2PROC_FILE,   # not valid log
            SAMPLE_DARSHAN_LOG_2,    # valid log
            'garbagefile'            # file doesn't exist
            ], stderr=devnull)
    # subprocess.check_output will throw exception if returncode != 0
    assert verify_output_csv(output_str, key='darshan_agg_perf_by_slowest_posix', expected_rows=2)
    assert verify_output_csv(output_str, key='lmt_tot_gibs_written', expected_rows=2)

@tokiotest.needs_darshan
def test_with_topology():
    """
    Integration of topology (CraySDB + Slurm)

    requires either an SDB cache file or access to xtdb2proc
    requires either access to Slurm or a Slurm job cache file (to map jobid to node list)

    """
    tokiotest.check_darshan()
    output_str = subprocess.check_output([
        BINARY,
        '--topology', SAMPLE_XTDB2PROC_FILE,
        '--slurm-jobid', SAMPLE_SLURM_CACHE_FILE,
        '--json',
        SAMPLE_DARSHAN_LOG])
    assert verify_output_json(output_str, key='darshan_agg_perf_by_slowest_posix')
    assert verify_output_json(output_str, key='lmt_tot_gibs_written')
    assert verify_output_json(output_str, key='topology_job_max_radius')

@tokiotest.needs_darshan
def test_with_lfsstatus():
    """
    Integration of tools.lfsstatus

    """
    tokiotest.check_darshan()
    output_str = subprocess.check_output([
        BINARY,
        '--json',
        '--ost',
        '--ost-fullness', SAMPLE_OSTFULLNESS_FILE,
        '--ost-map', SAMPLE_OSTMAP_FILE,
        SAMPLE_DARSHAN_LOG])
    print output_str
    assert verify_output_json(output_str, key='darshan_agg_perf_by_slowest_posix')
    assert verify_output_json(output_str, key='lmt_tot_gibs_written')
    assert verify_output_json(output_str, key='fshealth_ost_overloaded_pct')

@tokiotest.needs_darshan
def test_with_nersc_jobsdb():
    """
    Integration of NerscJobsDb
    """
    tokiotest.check_darshan()
    output_str = subprocess.check_output([
        BINARY,
        '--json',
        '--concurrentjobs', SAMPLE_NERSCJOBSDB_FILE,
        '--jobhost', SAMPLE_DARSHAN_JOBHOST,
        SAMPLE_DARSHAN_LOG])
    print output_str
    assert verify_output_json(output_str, key='darshan_agg_perf_by_slowest_posix')
    assert verify_output_json(output_str, key='lmt_tot_gibs_written')
    assert verify_output_json(output_str, key='jobsdb_concurrent_nodehrs')

def test_without_darshan():
    """
    LMT-only functionality when no darshan log is present
    """
    output_str = subprocess.check_output([
        BINARY,
        '--json',
        '--slurm-jobid', SAMPLE_DARSHAN_JOBID,
        '--start-time', SAMPLE_DARSHAN_START_TIME,
        '--end-time', SAMPLE_DARSHAN_END_TIME,
        '--file-system', SAMPLE_DARSHAN_FILE_SYSTEM,])
    assert verify_output_json(output_str, key='lmt_tot_gibs_written')

def test_most_without_darshan():
    """
    Most functionality when no darshan log is present
    """
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

        '--topology', SAMPLE_XTDB2PROC_FILE,])
    assert verify_output_json(output_str, key='lmt_tot_gibs_written')
    assert verify_output_json(output_str, key='jobsdb_concurrent_nodehrs')
    assert verify_output_json(output_str, key='fshealth_ost_overloaded_pct')
    assert verify_output_json(output_str, key='topology_job_max_radius')
