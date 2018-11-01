"""Test tools.darshan interfaces
"""
import sys
import nose.tools
import datetime
import tokio.tools.darshan
import tokiotest

DATETIME_START = datetime.datetime.strptime(tokiotest.SAMPLE_DARSHAN_START_TIME, "%Y-%m-%d %H:%M:%S")

def wrap_find_darshanlogs(test_input):
    """Allow named args to pass through nosetests
    """
    print("Running: %s" % test_input['descr'])
    print("Test args: %s" % test_input['params'])
    results = tokio.tools.darshan.find_darshanlogs(**(test_input['params']))
    assert (test_input['pass_criteria'])(results)

def wrap_load_darshanlogs(test_input):
    """Allow named args to pass through nosetests
    """
    tokiotest.check_darshan()
    print("Running: %s" % test_input['descr'])
    print("Test args: %s" % test_input['params'])
    expected_exception = test_input.get('expect_exception')
    if expected_exception:
        nose.tools.assert_raises(expected_exception,
                                 tokio.tools.darshan.load_darshanlogs,
                                 **(test_input['params']))
    else:
        results = tokio.tools.darshan.load_darshanlogs(**(test_input['params']))
        assert (test_input['pass_criteria'])(results)

def wrap_load_darshanlogs_assert_raises(test_input, exception):
    """Allow named args to pass through nosetests; expect an exception
    """
    tokiotest.check_darshan()
    print("Running: %s" % test_input['descr'])
    print("Test args: %s" % test_input['params'])
    nose.tools.assert_raises(exception, tokio.tools.darshan.load_darshanlogs, **(test_input['params']))

TEST_MATRIX = [
    {
        'descr': "valid jobid",
        'test_function': wrap_find_darshanlogs,
        'params': {
            'datetime_start': DATETIME_START,
            'datetime_end': None,
            'username': None,
            'jobid': tokiotest.SAMPLE_DARSHAN_JOBID,
        },
        'pass_criteria': lambda x: len(x) > 0,
    },
    {
        'descr': "valid jobid w/ valid user",
        'test_function': wrap_find_darshanlogs,
        'params': {
            'datetime_start': DATETIME_START,
            'datetime_end': None,
            'username': tokiotest.SAMPLE_DARSHAN_LOG_USER,
            'jobid': tokiotest.SAMPLE_DARSHAN_JOBID,
        },
        'pass_criteria': lambda x: len(x) > 0,
    },
    {
        'descr': "valid jobid w/ invalid user",
        'test_function': wrap_find_darshanlogs,
        'params': {
            'datetime_start': DATETIME_START,
            'datetime_end': None,
            'username': 'invaliduser',
            'jobid': tokiotest.SAMPLE_DARSHAN_JOBID,
        },
        'pass_criteria': lambda x: len(x) == 0,
    },
    {
        'descr': "valid jobid w/ multiple dates",
        'test_function': wrap_find_darshanlogs,
        'params': {
            'datetime_start': DATETIME_START - datetime.timedelta(days=1),
            'datetime_end': DATETIME_START + datetime.timedelta(days=1),
            'username': None,
            'jobid': tokiotest.SAMPLE_DARSHAN_JOBID,
        },
        'pass_criteria': lambda x: len(x) > 0,
    },
    {
        'descr': "valid jobid, wrong date",
        'test_function': wrap_find_darshanlogs,
        'params': {
            'datetime_start': DATETIME_START,
            'datetime_end': None,
            'username': None,
            'jobid': tokiotest.SAMPLE_DARSHAN_JOBID_2,
        },
        'pass_criteria': lambda x: len(x) == 0,
    },
    {
        'descr': "valid jobid, second day",
        'test_function': wrap_find_darshanlogs,
        'params': {
            'datetime_start': DATETIME_START - datetime.timedelta(days=1),
            'datetime_end': DATETIME_START + datetime.timedelta(days=1),
            'username': None,
            'jobid': tokiotest.SAMPLE_DARSHAN_JOBID_2,
        },
        'pass_criteria': lambda x: len(x) > 0,
    },
    {
        'descr': "invalid jobid",
        'test_function': wrap_find_darshanlogs,
        'params': {
            'datetime_start': DATETIME_START,
            'datetime_end': None,
            'username': None,
            'jobid': 5,
        },
        'pass_criteria': lambda x: len(x) == 0,
    },
    {
        'descr': "no jobid, one day",
        'test_function': wrap_find_darshanlogs,
        'params': {
            'datetime_start': DATETIME_START,
            'datetime_end': None,
            'username': None,
            'jobid': None,
        },
        'pass_criteria': lambda x: len(x) >= tokiotest.SAMPLE_DARSHAN_LOGS_PER_DIR,
    },
    {
        'descr': "no jobid, many days",
        'test_function': wrap_find_darshanlogs,
        'params': {
            'datetime_start': DATETIME_START - datetime.timedelta(days=1),
            'datetime_end': DATETIME_START + datetime.timedelta(days=1),
            'username': None,
            'jobid': None,
        },
        'pass_criteria': lambda x: len(x) >= (2 * tokiotest.SAMPLE_DARSHAN_LOGS_PER_DIR),
    },
    {
        'descr': "load base",
        'test_function': wrap_load_darshanlogs,
        'params': {
            'datetime_start': DATETIME_START,
            'datetime_end': None,
            'username': None,
            'jobid': tokiotest.SAMPLE_DARSHAN_JOBID,
            'which': 'base'
        },
        'pass_criteria': lambda x: len(x) > 0,
    },
    {
        'descr': "load base,perf,total",
        'test_function': wrap_load_darshanlogs,
        'params': {
            'datetime_start': DATETIME_START,
            'datetime_end': None,
            'username': None,
            'jobid': tokiotest.SAMPLE_DARSHAN_JOBID,
            'which': 'base,perf,total'
        },
        'pass_criteria': lambda x: len(x) > 0,
    },
    {
        'descr': "load invalid",
        'test_function': wrap_load_darshanlogs,#lambda x: wrap_load_darshanlogs_assert_raises(x, TypeError),
        'params': {
            'datetime_start': DATETIME_START,
            'datetime_end': None,
            'username': None,
            'jobid': tokiotest.SAMPLE_DARSHAN_JOBID,
            'which': 'asdflkj'
        },
        'pass_criteria': lambda x: len(x) > 0,
        'expect_exception': TypeError,
    },
    {
        'descr': "valid jobid w/ valid user, valid log dir key",
        'test_function': wrap_find_darshanlogs,
        'params': {
            'datetime_start': DATETIME_START,
            'datetime_end': None,
            'system': tokiotest.SAMPLE_DARSHAN_LOG_DIR_KEY,
            'username': tokiotest.SAMPLE_DARSHAN_LOG_USER,
            'jobid': tokiotest.SAMPLE_DARSHAN_JOBID,
        },
        'pass_criteria': lambda x: len(x) > 0,
    },
    {
        'descr': "valid jobid w/ valid user, invalid log dir key",
        'test_function': wrap_find_darshanlogs,
        'params': {
            'datetime_start': DATETIME_START,
            'datetime_end': None,
            'username': tokiotest.SAMPLE_DARSHAN_LOG_USER,
            'jobid': tokiotest.SAMPLE_DARSHAN_JOBID,
            'system': 'invalid key',
        },
        'pass_criteria': lambda x: len(x) == 0,
    },
]

def test_find_darshanlogs():
    """tools.darshan.find_darshanlogs()
    """
    for test in TEST_MATRIX:
        test_func = tokiotest.needs_darshan(test['test_function'])
        test_func.description = "tools.darshan.find_darshanlogs(): " + test['descr']
        yield test_func, test
