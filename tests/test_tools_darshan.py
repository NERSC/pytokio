"""Test tools.darshan interfaces
"""
import datetime
import tokio.tools.darshan
import tokiotest

DATETIME_START = datetime.datetime.strptime(tokiotest.SAMPLE_DARSHAN_START_TIME, "%Y-%m-%d %H:%M:%S")

TEST_MATRIX = [
    {
        'descr': "valid jobid",
        'params': {
            'datetime_start': DATETIME_START,
            'datetime_end': None,
            'username': None,
            'jobid': tokiotest.SAMPLE_DARSHAN_JOBID,
            'darshan_log_dir': tokiotest.SAMPLE_DARSHAN_LOG_DIR,
        },
        'pass_criteria': lambda x: len(x) > 0,
    },
    {
        'descr': "valid jobid w/ valid user",
        'params': {
            'datetime_start': DATETIME_START,
            'datetime_end': None,
            'username': tokiotest.SAMPLE_DARSHAN_LOG_USER,
            'jobid': tokiotest.SAMPLE_DARSHAN_JOBID,
            'darshan_log_dir': tokiotest.SAMPLE_DARSHAN_LOG_DIR,
        },
        'pass_criteria': lambda x: len(x) > 0,
    },
    {
        'descr': "valid jobid w/ invalid user",
        'params': {
            'datetime_start': DATETIME_START,
            'datetime_end': None,
            'username': 'invaliduser',
            'jobid': tokiotest.SAMPLE_DARSHAN_JOBID,
            'darshan_log_dir': tokiotest.SAMPLE_DARSHAN_LOG_DIR,
        },
        'pass_criteria': lambda x: len(x) == 0,
    },
    {
        'descr': "valid jobid w/ multiple dates",
        'params': {
            'datetime_start': DATETIME_START - datetime.timedelta(days=1),
            'datetime_end': DATETIME_START + datetime.timedelta(days=1),
            'username': None,
            'jobid': tokiotest.SAMPLE_DARSHAN_JOBID,
            'darshan_log_dir': tokiotest.SAMPLE_DARSHAN_LOG_DIR,
        },
        'pass_criteria': lambda x: len(x) > 0,
    },
    {
        'descr': "valid jobid, wrong date",
        'params': {
            'datetime_start': DATETIME_START,
            'datetime_end': None,
            'username': None,
            'jobid': tokiotest.SAMPLE_DARSHAN_JOBID_2,
            'darshan_log_dir': tokiotest.SAMPLE_DARSHAN_LOG_DIR,
        },
        'pass_criteria': lambda x: len(x) == 0,
    },
    {
        'descr': "valid jobid, second day",

        'params': {
            'datetime_start': DATETIME_START - datetime.timedelta(days=1),
            'datetime_end': DATETIME_START + datetime.timedelta(days=1),
            'username': None,
            'jobid': tokiotest.SAMPLE_DARSHAN_JOBID_2,
            'darshan_log_dir': tokiotest.SAMPLE_DARSHAN_LOG_DIR,
        },
        'pass_criteria': lambda x: len(x) > 0,
    },
    {
        'descr': "invalid jobid",
        'params': {
            'datetime_start': DATETIME_START,
            'datetime_end': None,
            'username': None,
            'jobid': 5,
            'darshan_log_dir': tokiotest.SAMPLE_DARSHAN_LOG_DIR,
        },
        'pass_criteria': lambda x: len(x) == 0,
    },
    {
        'descr': "no jobid, one day",
        'params': {
            'datetime_start': DATETIME_START,
            'datetime_end': None,
            'username': None,
            'jobid': None,
            'darshan_log_dir': tokiotest.SAMPLE_DARSHAN_LOG_DIR,
        },
        'pass_criteria': lambda x: len(x) >= tokiotest.SAMPLE_DARSHAN_LOGS_PER_DIR,
    },
    {
        'descr': "no jobid, many days",
        'params': {
            'datetime_start': DATETIME_START - datetime.timedelta(days=1),
            'datetime_end': DATETIME_START + datetime.timedelta(days=1),
            'username': None,
            'jobid': None,
            'darshan_log_dir': tokiotest.SAMPLE_DARSHAN_LOG_DIR,
        },
        'pass_criteria': lambda x: len(x) >= (2 * tokiotest.SAMPLE_DARSHAN_LOGS_PER_DIR),
    },
]

def wrap_function(test_input):
    """Allow named args to pass through nosetests
    """
    print "Running:", test_input['descr']
    print "Test args:", test_input['params']
    results = tokio.tools.darshan.find_darshanlogs(**(test_input['params']))
    assert (test_input['pass_criteria'])(results)

def test_find_darshanlogs():
    """tools.darshan.find_darshanlogs()
    """

    for test in TEST_MATRIX:
        test_func = wrap_function
        test_func.description = "tools.darshan.find_darshanlogs(): " + test['descr']
        yield test_func, test
