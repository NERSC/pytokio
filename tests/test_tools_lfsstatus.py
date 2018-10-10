#!/usr/bin/env python
"""
Test the lfsstatus tool API
"""

import json
import datetime

import nose
import tokiotest
from tokiotest import SAMPLE_OSTMAP_FILE, SAMPLE_OSTFULLNESS_FILE, SAMPLE_DARSHAN_SONEXION_ID
import tokio
import tokio.tools.lfsstatus as lfsstatus

# These should correspond to the first and last BEGIN in the sample ost-map.txt
# and ost-fullness.txt files.  If you change the contents of those files, you
# MUST update these as well.
SAMPLE_OSTFULLNESS_START = datetime.datetime.fromtimestamp(tokiotest.SAMPLE_OSTFULLNESS_START)
SAMPLE_OSTFULLNESS_END = datetime.datetime.fromtimestamp(tokiotest.SAMPLE_OSTFULLNESS_END)
SAMPLE_OSTFULLNESS_DELTA = (SAMPLE_OSTFULLNESS_END - SAMPLE_OSTFULLNESS_START).total_seconds() / 2.0
SAMPLE_OSTFULLNESS_DELTA = datetime.timedelta(seconds=SAMPLE_OSTFULLNESS_DELTA)
SAMPLE_OSTFULLNESS_HALFWAY = SAMPLE_OSTFULLNESS_START + SAMPLE_OSTFULLNESS_DELTA
SAMPLE_OSTFULLNESS_BEFORE = SAMPLE_OSTFULLNESS_START - datetime.timedelta(seconds=1)
SAMPLE_OSTFULLNESS_AFTER = SAMPLE_OSTFULLNESS_END + datetime.timedelta(seconds=1)

SAMPLE_OSTMAP_START = datetime.datetime.fromtimestamp(tokiotest.SAMPLE_OSTMAP_START)
SAMPLE_OSTMAP_END = datetime.datetime.fromtimestamp(tokiotest.SAMPLE_OSTMAP_END)
SAMPLE_OSTMAP_DELTA = (SAMPLE_OSTMAP_END - SAMPLE_OSTMAP_START).total_seconds() / 2.0
SAMPLE_OSTMAP_DELTA = datetime.timedelta(seconds=SAMPLE_OSTMAP_DELTA)
SAMPLE_OSTMAP_HALFWAY = SAMPLE_OSTMAP_START + SAMPLE_OSTMAP_DELTA
SAMPLE_OSTMAP_BEFORE = SAMPLE_OSTMAP_START - datetime.timedelta(seconds=1)
SAMPLE_OSTMAP_AFTER = SAMPLE_OSTMAP_END + datetime.timedelta(seconds=1)

def wrap_get_fullness(datetime_target, cache_file):
    """
    Encapsulate test and validation of lfsstatus.get_fullness into a
    single function
    """
    result = lfsstatus.get_fullness(
        SAMPLE_DARSHAN_SONEXION_ID,
        datetime_target,
        cache_file=cache_file)
    verify_fullness(result)

def wrap_get_failures(datetime_target, cache_file):
    """
    Encapsulate test and validation of lfsstatus.get_failures into a
    single function
    """
    result = lfsstatus.get_failures(
        SAMPLE_DARSHAN_SONEXION_ID,
        datetime_target,
        cache_file=cache_file)
    verify_failures(result)

CACHE_FILES = {
    wrap_get_fullness: SAMPLE_OSTFULLNESS_FILE,
    wrap_get_failures: SAMPLE_OSTMAP_FILE,
}

TEST_CONDITIONS = {
    wrap_get_fullness: [
        {
            'description': "lfsstatus.get_fullness() baseline functionality",
            'datetime_target': SAMPLE_OSTFULLNESS_HALFWAY,
        },
        {
            'description': "lfsstatus.get_fullness() first timestamp",
            'datetime_target': SAMPLE_OSTFULLNESS_START,
        },
        {
            'description': "lfsstatus.get_fullness() last timestamp",
            'datetime_target': SAMPLE_OSTFULLNESS_END,
        },
        {
            'description': "lfsstatus.get_fullness() before first timestamp",
            'datetime_target': SAMPLE_OSTFULLNESS_BEFORE,
        },
        {
            'description': "lfsstatus.get_fullness() after file",
            'datetime_target': SAMPLE_OSTFULLNESS_AFTER,
        },
    ],
    wrap_get_failures: [
        {
            'description': "lfsstatus.get_failures() baseline functionality",
            'datetime_target': SAMPLE_OSTMAP_HALFWAY,
        },
        {
            'description': "lfsstatus.get_failures() first timestamp",
            'datetime_target': SAMPLE_OSTMAP_START,
        },
        {
            'description': "lfsstatus.get_failures() last timestamp",
            'datetime_target': SAMPLE_OSTMAP_END,
        },
        {
            'description': "lfsstatus.get_failures() before file",
            'datetime_target': SAMPLE_OSTMAP_BEFORE,
        },
        {
            'description': "lfsstatus.get_failures() after file",
            'datetime_target': SAMPLE_OSTMAP_AFTER,
        },
    ],
}

def verify_fullness(result):
    """
    Verify correctness of get_fullness()
    """
    print(json.dumps(result, indent=4, sort_keys=True))
    assert result['ost_avg_full_kib'] > 0
    assert 0.0 < result['ost_avg_full_pct'] < 100.0
    assert result['ost_count'] > 1
    assert result['ost_least_full_id'] != result['ost_most_full_id']
    assert result['ost_next_timestamp'] > result['ost_actual_timestamp']
    assert 'ost_requested_timestamp' in result

def verify_failures(result):
    """
    Verify correctness of get_failures()
    """
    print(json.dumps(result, indent=4, sort_keys=True))
    assert result['ost_next_timestamp'] > result['ost_actual_timestamp']
    assert result['ost_overloaded_oss_count'] == tokiotest.SAMPLE_OSTMAP_OVERLOAD_OSS
    ### ensure that ost_avg_overloaded_ost_per_oss is calculated correctly
    assert int(result['ost_avg_overloaded_ost_per_oss']) == \
        int(float(result['ost_overloaded_ost_count']) / result['ost_overloaded_oss_count'])
    assert 'ost_requested_timestamp' in result

@nose.tools.nottest
def run_test_matrix(func):
    """Iterate over all test cases
    """
    for config in TEST_CONDITIONS[func]:
        test_func = func
        test_func.description = config['description'] + ", no cache"
        yield test_func, config['datetime_target'], None

        test_func = func
        test_func.description = config['description'] + ", cache"
        yield test_func, config['datetime_target'], CACHE_FILES[func]

def test_get_fullness_hdf5():
    """tools.lfsstatus.get_fullness, HDF5
    """
    raise nose.SkipTest("No valid sample input")
    tokio.config.CONFIG["lfsstatus_fullness_providers"] = ["hdf5"]
    func = wrap_get_fullness
    for test_func in run_test_matrix(func):
        test_func[0].description += " via HDF5"
        yield test_func 

def test_get_fullness_lfsstate():
    """tools.lfsstatus.get_fullness, nersc_lfsstate
    """
    tokio.config.CONFIG["lfsstatus_fullness_providers"] = ["nersc_lfsstate"]
    func = wrap_get_fullness
    for test_func in run_test_matrix(func):
        test_func[0].description += " via nersc_lfsstate"
        yield test_func 

def test_get_failures_lfsstate():
    """tools.lfsstatus.get_failures, nersc_lfsstate
    """
    func = wrap_get_failures
    for test_func in run_test_matrix(func):
        test_func[0].description += " via nersc_lfsstate"
        yield test_func 

def test_get_fullness_hdf5_fallthru():
    """tools.lfsstatus.get_fullness, invalid hdf5 -> nersc_lfsstate
    """
    tokio.config.CONFIG["lfsstatus_fullness_providers"] = [
        "hdf5",
        "nersc_lfsstate"
    ]
    tmp = tokio.config.CONFIG["hdf5_files"]
    tokio.config.CONFIG["hdf5_files"] = "xxx"

    func = wrap_get_fullness
    for test_func in run_test_matrix(func):
        test_func[0].description += " via invalid hdf5 -> nersc_lfsstate"
        yield test_func 

    tokio.config.CONFIG["hdf5_files"] = tmp

def test_get_fullness_lfsstate_fallthru():
    """tools.lfsstatus.get_fullness, invalid nersc_lfsstate -> hdf5
    """
    raise nose.SkipTest("No valid sample input")
    tokio.config.CONFIG["lfsstatus_fullness_providers"] = [
        "nersc_lfsstate",
        "hdf5",
    ]
    tmp = tokio.config.CONFIG["lfsstatus_fullness_files"]
    tokio.config.CONFIG["lfsstatus_fullness_files"] = "xxx"

    func = wrap_get_fullness
    for test_func in run_test_matrix(func):
        test_func[0].description += " via invalid nersc_lfsstate -> hdf5"
        yield test_func 

    tokio.config.CONFIG["lfsstatus_fullness_files"] = tmp

def swap_config_param(key, value):
    """Decorator to swap config values
    """
    def wrap_function(func):
        def wrapped_function(*args, **kwargs):
            old_value = tokio.config.CONFIG.get(key)
            tokio.config.CONFIG[key] = value

            func(*args, **kwargs)

            if old_value is not None:
                tokio.config.CONFIG[key] = old_value
            else:
                del tokio.config.CONFIG[key]
        return wrapped_function
    return wrap_function
