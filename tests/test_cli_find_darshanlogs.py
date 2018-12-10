#!/usr/bin/env python
"""
Test the cli.darshan_bad_ost tool
"""

import nose.tools
import test_tools_darshan
import tokiotest
import tokio.cli.find_darshanlogs

def wrap_function(test_input):
    """Allow named args to pass through nosetests
    """
    print("Running: %s" % test_input['descr'])

    argv = []
    if test_input['params']['datetime_start'] is not None:
        argv += ['--start', test_input['params']['datetime_start'].strftime("%Y-%m-%d")]
    if test_input['params']['datetime_end'] is not None:
        argv += ['--end', test_input['params']['datetime_end'].strftime("%Y-%m-%d")]
    if test_input['params']['username'] is not None:
        argv += ['--username', test_input['params']['username']]
    if test_input['params']['jobid'] is not None:
        argv += ['--jobid', str(test_input['params']['jobid'])]
    if 'which' in test_input['params']:
        tokiotest.check_darshan()
        argv += ['--load', test_input['params']['which']]
    if 'system' in test_input['params']:
        argv += ['--host', test_input['params']['system']]

    print("Test args: %s" % argv)

    expected_exception = test_input.get('expect_exception')
    if expected_exception:
        # assert_raises doesn't seem to work correctly here
#       nose.tools.assert_raises(expected_exception,
#                                tokiotest.run_bin(tokio.cli.find_darshanlogs, argv))
        caught = False
        try:
            output_str = tokiotest.run_bin(tokio.cli.find_darshanlogs, argv)
        except expected_exception:
            caught = True
        assert caught

    else:
        output_str = tokiotest.run_bin(tokio.cli.find_darshanlogs, argv)
        results = output_str.splitlines()
        assert (test_input['pass_criteria'])(results)

def test_bin_find_darshanlogs():
    """
    Run the tools.darshan test cases through the CLI interface
    """

    for test in test_tools_darshan.TEST_MATRIX:
        test_func = tokiotest.needs_darshan(wrap_function)
        test_func.description = 'cli.find_darshanlogs: ' + test['descr']
        yield test_func, test
