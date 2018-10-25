#!/usr/bin/env python
"""
Test the cli.summarize_job tool
"""

import os
import json
try:
    import StringIO as io
except ImportError:
    import io
import pandas
import tokiotest
import tokio.cli.summarize_job

### For tests that base all tests off of the sample Darshan log
SAMPLE_DARSHAN_LOG_2 = os.path.join(tokiotest.INPUT_DIR, 'sample-2.darshan')

def verify_output_json(output_str, key=None, value=None):
    """
    Given the stdout of summarize_job.py --json, ensure that it is valid json,
    and ensure that a given key is present
    """
    for parsed_data in json.loads(output_str):
        if key is not None:
            print("Checking if %s is present" % key)
            assert key in list(parsed_data.keys())
            if value is not None:
                assert parsed_data[key] == value

    return True

def verify_output_csv(output_str, key=None, value=None, expected_rows=None):
    """
    Given the stdout of summarize_job.py, ensure that it is re-readable by
    pandas.read_csv and ensure that a given column (key) is present
    """
    dataframe = pandas.read_csv(io.StringIO(output_str))
    if key is not None:
        assert key in dataframe.columns

    if value is not None:
        assert dataframe[key][0] == value

    if expected_rows is not None:
        assert len(dataframe) == expected_rows

    return True

@tokiotest.needs_darshan
def test_get_biggest_fs():
    """
    cli.summarize_job.get_biggest_fs() functionality
    """
    tokiotest.check_darshan()
    argv = ['--json', tokiotest.SAMPLE_DARSHAN_LOG]
    print("Executing: %s" % ' '.join(argv))
    output_str = tokiotest.run_bin(tokio.cli.summarize_job, argv)
    assert verify_output_json(output_str, key='darshan_biggest_read_fs')

@tokiotest.needs_darshan
def test_get_biggest_api():
    """
    cli.summarize_job.get_biggest_api() functionality
    """
    tokiotest.check_darshan()
    argv = ['--json', tokiotest.SAMPLE_DARSHAN_LOG]
    print("Executing: %s" % ' '.join(argv))
    output_str = tokiotest.run_bin(tokio.cli.summarize_job, argv)
    assert verify_output_json(output_str, key='darshan_biggest_read_api')

@tokiotest.needs_darshan
def test_json():
    """
    cli.summarize_job: darshan and LMT data (json)
    """
    tokiotest.check_darshan()
    argv = ['--json', tokiotest.SAMPLE_DARSHAN_LOG]
    print("Executing: %s" % ' '.join(argv))
    output_str = tokiotest.run_bin(tokio.cli.summarize_job, argv)
    assert verify_output_json(output_str, key='darshan_agg_perf_by_slowest_posix')
    assert verify_output_json(output_str, key='darshan_biggest_read_api')
    assert verify_output_json(output_str, key='darshan_biggest_read_fs')
    assert verify_output_json(output_str, key='fs_tot_gibs_written')

@tokiotest.needs_darshan
def test_csv():
    """
    cli.summarize_job: darshan and LMT data (csv)
    """
    tokiotest.check_darshan()
    argv = [tokiotest.SAMPLE_DARSHAN_LOG]
    print("Executing: %s" % ' '.join(argv))
    output_str = tokiotest.run_bin(tokio.cli.summarize_job, argv)
    assert verify_output_csv(output_str, key='darshan_agg_perf_by_slowest_posix')
    assert verify_output_csv(output_str, key='darshan_biggest_read_api')
    assert verify_output_csv(output_str, key='darshan_biggest_read_fs')
    assert verify_output_csv(output_str, key='fs_tot_gibs_written')

@tokiotest.needs_darshan
def test_darshan_summaries():
    """
    cli.summarize_job: multiple Darshan logs (csv)
    """
    tokiotest.check_darshan()
    argv = [tokiotest.SAMPLE_DARSHAN_LOG, SAMPLE_DARSHAN_LOG_2]
    print("Executing: %s" % ' '.join(argv))
    output_str = tokiotest.run_bin(tokio.cli.summarize_job, argv)
    assert verify_output_csv(output_str, key='darshan_agg_perf_by_slowest_posix', expected_rows=2)
    assert verify_output_csv(output_str, key='darshan_biggest_read_api')
    assert verify_output_csv(output_str, key='darshan_biggest_read_fs')
    assert verify_output_csv(output_str, key='fs_tot_gibs_written', expected_rows=2)

@tokiotest.needs_darshan
def test_bogus_darshans():
    """
    cli.summarize_job: mix of valid and invalid Darshan logs
    """
    tokiotest.check_darshan()
    argv = ['--silent-errors',
            tokiotest.SAMPLE_DARSHAN_LOG,       # valid log
            tokiotest.SAMPLE_XTDB2PROC_FILE,    # not valid log
            SAMPLE_DARSHAN_LOG_2,               # valid log
            'garbagefile']                      # file doesn't exist
    print("Executing: %s" % ' '.join(argv))
    output_str = tokiotest.run_bin(tokio.cli.summarize_job, argv)
    assert verify_output_csv(output_str, key='darshan_agg_perf_by_slowest_posix', expected_rows=2)
    assert verify_output_csv(output_str, key='darshan_biggest_read_api')
    assert verify_output_csv(output_str, key='darshan_biggest_read_fs')
    assert verify_output_csv(output_str, key='fs_tot_gibs_written', expected_rows=2)

@tokiotest.needs_darshan
def test_with_topology():
    """
    cli.summarize_job --topology --slurm-jobid

    requires either an SDB cache file or access to xtdb2proc
    requires either access to Slurm or a Slurm job cache file (to map jobid to node list)
    """
    tokiotest.check_darshan()
    argv = ['--topology', tokiotest.SAMPLE_XTDB2PROC_FILE,
            '--slurm-jobid', tokiotest.SAMPLE_SLURM_CACHE_FILE,
            '--json',
            tokiotest.SAMPLE_DARSHAN_LOG]
    print("Executing: %s" % ' '.join(argv))
    output_str = tokiotest.run_bin(tokio.cli.summarize_job, argv)
    assert verify_output_json(output_str, key='darshan_agg_perf_by_slowest_posix')
    assert verify_output_json(output_str, key='darshan_biggest_read_api')
    assert verify_output_json(output_str, key='darshan_biggest_read_fs')
    assert verify_output_json(output_str, key='fs_tot_gibs_written')
    assert verify_output_json(output_str, key='topology_job_max_radius')

@tokiotest.needs_darshan
def test_with_lfsstatus():
    """
    cli.summarize_job --ost --ost-fullness --ost-map
    """
    tokiotest.check_darshan()
    argv = ['--json',
            '--ost',
            '--ost-fullness', tokiotest.SAMPLE_OSTFULLNESS_FILE,
            '--ost-map', tokiotest.SAMPLE_OSTMAP_FILE,
            tokiotest.SAMPLE_DARSHAN_LOG]
    print("Executing: %s" % ' '.join(argv))
    output_str = tokiotest.run_bin(tokio.cli.summarize_job, argv)
    assert verify_output_json(output_str, key='darshan_agg_perf_by_slowest_posix')
    assert verify_output_json(output_str, key='darshan_biggest_read_api')
    assert verify_output_json(output_str, key='darshan_biggest_read_fs')
    assert verify_output_json(output_str, key='fs_tot_gibs_written')
    assert verify_output_json(output_str, key='fshealth_ost_overloaded_pct')

@tokiotest.needs_darshan
def test_with_nersc_jobsdb():
    """
    cli.summarize_job --concurrentjobs --jobhost
    """
    tokiotest.check_darshan()
    argv = ['--json',
            '--concurrentjobs', tokiotest.SAMPLE_NERSCJOBSDB_FILE,
            '--jobhost', tokiotest.SAMPLE_DARSHAN_JOBHOST,
            tokiotest.SAMPLE_DARSHAN_LOG]
    print("Executing: %s" % ' '.join(argv))
    output_str = tokiotest.run_bin(tokio.cli.summarize_job, argv)
    assert verify_output_json(output_str, key='darshan_agg_perf_by_slowest_posix')
    assert verify_output_json(output_str, key='darshan_biggest_read_api')
    assert verify_output_json(output_str, key='darshan_biggest_read_fs')
    assert verify_output_json(output_str, key='fs_tot_gibs_written')
    assert verify_output_json(output_str, key='jobsdb_concurrent_nodehrs')

def test_without_darshan():
    """
    cli.summarize_job sans Darshan log (h5lmt-only)
    """
    argv = ['--json',
            '--slurm-jobid', tokiotest.SAMPLE_DARSHAN_JOBID,
            '--start-time', tokiotest.SAMPLE_DARSHAN_START_TIME,
            '--end-time', tokiotest.SAMPLE_DARSHAN_END_TIME,
            '--file-system', tokiotest.SAMPLE_DARSHAN_FILE_SYSTEM]
    print("Executing: %s" % ' '.join(argv))
    output_str = tokiotest.run_bin(tokio.cli.summarize_job, argv)
    assert verify_output_json(output_str, key='fs_tot_gibs_written')

def test_most_without_darshan():
    """
    cli.summarize_job sans Darshan log (most functionality)
    """
    argv = ['--json',
            '--slurm-jobid', tokiotest.SAMPLE_SLURM_CACHE_FILE,
            '--start-time', tokiotest.SAMPLE_DARSHAN_START_TIME,
            '--end-time', tokiotest.SAMPLE_DARSHAN_END_TIME,
            '--file-system', tokiotest.SAMPLE_DARSHAN_FILE_SYSTEM,

            '--concurrentjobs', tokiotest.SAMPLE_NERSCJOBSDB_FILE,
            '--jobhost', tokiotest.SAMPLE_DARSHAN_JOBHOST,

            '--ost',
            '--ost-fullness', tokiotest.SAMPLE_OSTFULLNESS_FILE,
            '--ost-map', tokiotest.SAMPLE_OSTMAP_FILE,

            '--topology', tokiotest.SAMPLE_XTDB2PROC_FILE]
    print("Executing: %s" % ' '.join(argv))
    output_str = tokiotest.run_bin(tokio.cli.summarize_job, argv)
    assert verify_output_json(output_str, key='fs_tot_gibs_written')
    assert verify_output_json(output_str, key='jobsdb_concurrent_nodehrs')
    assert verify_output_json(output_str, key='fshealth_ost_overloaded_pct')
    assert verify_output_json(output_str, key='topology_job_max_radius')
