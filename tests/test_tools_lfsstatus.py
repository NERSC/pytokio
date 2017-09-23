#!/usr/bin/env python
"""
Test the lfsstatus tool API
"""

import os
import datetime
import tokiotest
from tokiotest import SAMPLE_OSTMAP_FILE, SAMPLE_OSTFULLNESS_FILE, SAMPLE_DARSHAN_SONEXION_ID
import tokio
import tokio.tools.lfsstatus as lfsstatus

# Point this script at our inputs directory instead of the site-specific default
tokio.config.LFSSTATUS_BASE_DIR = os.path.join(tokiotest.INPUT_DIR, "%Y-%m-%d")

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

def wrap_get_fullness_at_datetime(datetime_target, cache_file):
    """
    Encapsulate test and validation of lfsstatus.get_fullness_at_datetime into a
    single function
    """
    result = lfsstatus.get_fullness_at_datetime(SAMPLE_DARSHAN_SONEXION_ID, datetime_target, cache_file)
    verify_fullness(result)

def wrap_get_failures_at_datetime(datetime_target, cache_file):
    """
    Encapsulate test and validation of lfsstatus.get_failures_at_datetime into a
    single function
    """
    result = lfsstatus.get_failures_at_datetime(SAMPLE_DARSHAN_SONEXION_ID, datetime_target, cache_file)
    verify_failures(result)

CACHE_FILES = {
    wrap_get_fullness_at_datetime: SAMPLE_OSTFULLNESS_FILE,
    wrap_get_failures_at_datetime: SAMPLE_OSTMAP_FILE,
}

TEST_CONDITIONS = {
    wrap_get_fullness_at_datetime: [
        {
            'description': "lfsstatus.get_fullness_at_datetime() baseline functionality",
            'datetime_target': SAMPLE_OSTFULLNESS_HALFWAY,
        },
        {
            'description': "lfsstatus.get_fullness_at_datetime() first timestamp",
            'datetime_target': SAMPLE_OSTFULLNESS_START,
        },
        {
            'description': "lfsstatus.get_fullness_at_datetime() last timestamp",
            'datetime_target': SAMPLE_OSTFULLNESS_END,
        },
        {
            'description': "lfsstatus.get_fullness_at_datetime() before first timestamp",
            'datetime_target': SAMPLE_OSTFULLNESS_BEFORE,
        },
        {
            'description': "lfsstatus.get_fullness_at_datetime() after file",
            'datetime_target': SAMPLE_OSTFULLNESS_AFTER,
        },
    ],
    wrap_get_failures_at_datetime: [
        {
            'description': "lfsstatus.get_failures_at_datetime() baseline functionality",
            'datetime_target': SAMPLE_OSTMAP_HALFWAY,
        },
        {
            'description': "lfsstatus.get_failures_at_datetime() first timestamp",
            'datetime_target': SAMPLE_OSTMAP_START,
        },
        {
            'description': "lfsstatus.get_failures_at_datetime() last timestamp",
            'datetime_target': SAMPLE_OSTMAP_END,
        },
        {
            'description': "lfsstatus.get_failures_at_datetime() before file",
            'datetime_target': SAMPLE_OSTMAP_BEFORE,
        },
        {
            'description': "lfsstatus.get_failures_at_datetime() after file",
            'datetime_target': SAMPLE_OSTMAP_AFTER,
        },
    ],
}

def verify_fullness(result):
    """
    Verify correctness of get_fullness_at_datetime()
    """
    # TODO
    assert result

def verify_failures(result):
    """
    Verify correctness of get_failures_at_datetime()
    """
    # TODO
    assert result


def test_get_functions():
    """
    Iterate over all test cases
    """
    for func, tests in TEST_CONDITIONS.iteritems():
        for config in tests:
            test_func = func
            test_func.description = config['description'] + ", no cache"
            yield test_func, config['datetime_target'], None

            test_func = func
            test_func.description = config['description'] + ", cache"
            yield test_func, config['datetime_target'], CACHE_FILES[func]

# TODO: factor these summaries out into the objects themselves
# def lfsstatus.summarize_maps_data(fs_data): # fs_data is either nersc_lfsstate.NerscLfsOstFullness
#                                                                 or nersc_lfsstate.NerscLfsOstMap
# def lfsstatus.summarize_df_data(fs_data):
