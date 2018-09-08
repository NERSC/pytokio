#!/usr/bin/env python
"""
Test the functionality of the pytokio REST API.
"""
# Test by hand by first launching the flask app:
#
#   PYTOKIO_H5LMT_BASE_DIR=$(greadlink -f ../tests/inputs)/%Y-%m-%d ./pytokio_rest.py
#
# Then accessing the API using the test data:
#
#   http://localhost:18880/hdf5/scratch2/mdscpu?start=1490012544&end=1490016603

import os
import time
import datetime
import json
import nose
import tokio
import tokio.config
import tokiotest
import pytokio_rest
tokio.config.H5LMT_BASE_DIR = os.path.join(tokiotest.INPUT_DIR, "%Y-%m-%d")

APP = None

def setup():
    """
    Wire up the Flask test infrastructure
    """
    global APP
    APP = pytokio_rest.APP.test_client()
    APP.testing = True

def verify_json_result(result, expected_status=200):
    """
    Ensure that result contains valid json and a good status code
    """
    print("Expected status %s, got %s" % (expected_status, result.status_code))
    assert result.status_code == expected_status
    print("Retrieved data: %s" % result.data)
    try:
        return json.loads(result.data)
    except ValueError:
        raise ValueError("Did not receive valid JSON output")

@nose.tools.with_setup(setup)
def test_index():
    """
    Top level API endpoint
    """
    global APP
    result = APP.get('/')
    verify_json_result(result)

@nose.tools.with_setup(setup)
def test_hdf5_index():
    """
    HDF5 route index
    """
    global APP
    result = APP.get('/hdf5')
    result_obj = verify_json_result(result)
    valid_fsnames = tokio.config.CONFIG['hdf5_files'].keys()
    for fsname in result_obj:
        assert fsname in valid_fsnames

@nose.tools.with_setup(setup)
def test_file_system_route():
    """
    File system index
    """
    global APP
    for file_system_name in tokio.config.CONFIG['hdf5_files'].keys():
        result = APP.get('/hdf5/' + file_system_name + '/')
        result_obj = verify_json_result(result)

        ### verify that all valid resources are being returned
        valid_resources = tokio.connectors.hdf5.V1_GROUPNAME.keys()
        for resource in result_obj:
            assert resource in valid_resources

    result = APP.get('/hdf5/' + 'invalid_fs/')
    verify_json_result(result, 400)

@nose.tools.with_setup(setup)
def test_hdf5_resource_route():
    """
    HDF5 resource tests
    """
    global APP

    start_time = datetime.datetime.strptime(tokiotest.SAMPLE_DARSHAN_START_TIME,
                                            "%Y-%m-%d %H:%M:%S")
    end_time = datetime.datetime.strptime(tokiotest.SAMPLE_DARSHAN_END_TIME,
                                          "%Y-%m-%d %H:%M:%S")
    start_time = int(time.mktime(start_time.timetuple()))
    end_time = int(time.mktime(end_time.timetuple()))
    rest_options = "?start=%d&end=%d" % (start_time, end_time)
    file_system_name = tokiotest.SAMPLE_DARSHAN_FILE_SYSTEM

    for group_name in tokio.connectors.hdf5.V1_GROUPNAME:
        route = '/'.join(['', 'hdf5', file_system_name, group_name])
        route += rest_options
        print("Testing %s" % route)
        result = APP.get(route)
        verify_json_result(result)

@nose.tools.with_setup(setup)
def test_hdf5_invalid_opts():
    """
    HDF5 invalid inputs
    """
    global APP

    start_time = datetime.datetime.strptime(tokiotest.SAMPLE_DARSHAN_START_TIME,
                                            "%Y-%m-%d %H:%M:%S")
    end_time = datetime.datetime.strptime(tokiotest.SAMPLE_DARSHAN_END_TIME,
                                          "%Y-%m-%d %H:%M:%S")
    start_time = int(time.mktime(start_time.timetuple()))
    end_time = int(time.mktime(end_time.timetuple()))
    ### note: intentionally reversing start/end time
    fail_tests = [
        "?start=%d&end=%d" % (end_time, start_time),
        "?start=%d&end=%d" % (start_time, end_time + 86400),
    ]
    file_system_name = tokiotest.SAMPLE_DARSHAN_FILE_SYSTEM

    for group_name in tokio.connectors.hdf5.V1_GROUPNAME:
        for rest_options in fail_tests:
            route = '/'.join(['', 'hdf5', file_system_name, group_name])
            route += rest_options
            print("Testing %s" % route)
            result = APP.get(route)
            verify_json_result(result, 400)
