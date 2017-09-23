#!/usr/bin/env python
"""
Test the bin/compare_isdct.py tool
"""

import os
import json
import subprocess
import tokiotest

BINARY = os.path.join(tokiotest.BIN_DIR, 'compare_isdct.py')

def test_all_json():
    """
    compare_isdct --all json output
    """
    output_str = subprocess.check_output([
        BINARY,
        '--all',
        tokiotest.SAMPLE_NERSCISDCT_PREV_FILE,
        tokiotest.SAMPLE_NERSCISDCT_DIFF_FILE])
    result = json.loads(output_str)
    ### this is borrowed from test_connectors_nersc_isdct
    assert len(result['added_devices']) == tokiotest.SAMPLE_NERSCISDCT_DIFF_ADD
    assert len(result['removed_devices']) == tokiotest.SAMPLE_NERSCISDCT_DIFF_RM
    assert len(result['devices']) > 0
    for counters in result['devices'].itervalues():
        for counter in tokiotest.SAMPLE_NERSCISDCT_DIFF_MONOTONICS:
            assert counters[counter] > 0
        for counter in tokiotest.SAMPLE_NERSCISDCT_DIFF_ZEROS:
            assert counter not in counters
        for counter in tokiotest.SAMPLE_NERSCISDCT_DIFF_EMPTYSTR:
            assert counter not in counters

def test_reduced_json():
    """
    compare_isdct reduced json output
    """
    output_str = subprocess.check_output([
        BINARY,
        tokiotest.SAMPLE_NERSCISDCT_PREV_FILE,
        tokiotest.SAMPLE_NERSCISDCT_DIFF_FILE])
    result = json.loads(output_str)
    for reduction in ('ave', 'count', 'min', 'max', 'sum'):
        for counter in tokiotest.SAMPLE_NERSCISDCT_DIFF_MONOTONICS:
            assert result["%s_%s" % (reduction, counter)] > 0
        for counter in tokiotest.SAMPLE_NERSCISDCT_DIFF_ZEROS:
            assert "%s_%s" % (reduction, counter) not in result
    for counter in tokiotest.SAMPLE_NERSCISDCT_DIFF_EMPTYSTR:
        assert "count_%s" % counter not in result

def test_all_json_w_zeros():
    """
    compare_isdct --all --report-zeros json output
    """
    output_str = subprocess.check_output([
        BINARY,
        '--all',
        "--report-zeros",
        tokiotest.SAMPLE_NERSCISDCT_PREV_FILE,
        tokiotest.SAMPLE_NERSCISDCT_DIFF_FILE])
    result = json.loads(output_str)
    ### this is borrowed from test_connectors_nersc_isdct
    assert len(result['added_devices']) == tokiotest.SAMPLE_NERSCISDCT_DIFF_ADD
    assert len(result['removed_devices']) == tokiotest.SAMPLE_NERSCISDCT_DIFF_RM
    assert len(result['devices']) > 0
    for counters in result['devices'].itervalues():
        for counter in tokiotest.SAMPLE_NERSCISDCT_DIFF_MONOTONICS:
            assert counters[counter] > 0
        for counter in tokiotest.SAMPLE_NERSCISDCT_DIFF_ZEROS:
            assert counters[counter] == 0
        for counter in tokiotest.SAMPLE_NERSCISDCT_DIFF_EMPTYSTR:
            assert counters[counter] == ""

def test_reduced_json_w_zeros():
    """
    compare_isdct reduced --report-zeros json output
    """
    output_str = subprocess.check_output([
        BINARY,
        "--report-zeros",
        tokiotest.SAMPLE_NERSCISDCT_PREV_FILE,
        tokiotest.SAMPLE_NERSCISDCT_DIFF_FILE])
    result = json.loads(output_str)
    for reduction in ('ave', 'min', 'max', 'sum'):
        for counter in tokiotest.SAMPLE_NERSCISDCT_DIFF_MONOTONICS:
            assert result["%s_%s" % (reduction, counter)] > 0
        for counter in tokiotest.SAMPLE_NERSCISDCT_DIFF_ZEROS:
            print "result[%s_%s] == %d" % (
                reduction, counter, result["%s_%s" % (reduction, counter)])
            assert result["%s_%s" % (reduction, counter)] == 0
    for counter in tokiotest.SAMPLE_NERSCISDCT_DIFF_EMPTYSTR:
        assert result["count_%s" % counter] > 0

def test_reduced_json_gibs():
    """
    compare_isdct reduced --gibs json output
    """
    output_str = subprocess.check_output([
        BINARY,
        "--gibs",
        tokiotest.SAMPLE_NERSCISDCT_PREV_FILE,
        tokiotest.SAMPLE_NERSCISDCT_DIFF_FILE])
    result = json.loads(output_str)
    success = 0
    for counter in result.keys():
        assert not counter.endswith('_bytes')
        if counter.endswith('_gibs'):
            success += 1
    assert success > 0

def test_all_json_w_gibs():
    """
    compare_isdct --all --gibs json output
    """
    output_str = subprocess.check_output([
        BINARY,
        '--all',
        "--gibs",
        tokiotest.SAMPLE_NERSCISDCT_PREV_FILE,
        tokiotest.SAMPLE_NERSCISDCT_DIFF_FILE])
    result = json.loads(output_str)
    success = 0
    for counters in result['devices'].itervalues():
        for counter in counters.keys():
            assert not counter.endswith('_bytes')
            if counter.endswith('_gibs'):
                success += 1
    assert success > 0

def test_summary():
    """
    compare_isdct --summary human-readable output
    """
    output_str = subprocess.check_output([
        BINARY,
        '--summary',
        tokiotest.SAMPLE_NERSCISDCT_PREV_FILE,
        tokiotest.SAMPLE_NERSCISDCT_DIFF_FILE])

    ### look for a section on devices removed
    if tokiotest.SAMPLE_NERSCISDCT_DIFF_RM > 0:
        func = validate_summary_section
        func.description = "compare_isdct --summary device removal"
        yield func, output_str, 'devices removed', verify_nid_line

    ### look for a section on devices added
    if tokiotest.SAMPLE_NERSCISDCT_DIFF_ADD > 0:
        func = validate_summary_section
        func.description = "compare_isdct --summary device installation"
        yield func, output_str, 'devices installed', verify_nid_line

    ### look for a section on errors detected
    if tokiotest.SAMPLE_NERSCISDCT_DIFF_ERRS > 0:
        func = validate_summary_section
        func.description = "compare_isdct --summary error detection"
        yield func, output_str, 'errors detected', verify_errors_line

    ### look for a section with the workload statistics
    func = validate_summary_section
    func.description = "compare_isdct --summary workload statistics"
    yield func, output_str, 'workload statistics', verify_workload_line

def validate_summary_section(output_str, section_header, line_verify_func):
    """
    Scan the output text of --summary, look for a section, then parse it to the
    extent necessary to validate the structure of its contents
    """
    print section_header, line_verify_func
    ### look for a section on errors detected
    found_section = False
    found_contents = False
    for line in output_str.splitlines():
        if found_section:
            if line.strip() == "":
                break
            else:
                line_verify_func(line)
                found_contents = True
        elif section_header in line.lower():
            found_section = True
    assert found_section
    assert found_contents

def verify_errors_line(line):
    """
    Verify the struture of sections that report a nidname, serial number,
    counter, and delta value
    """
    print "errors line [%s]" % line
    assert line.startswith('nid')
    assert len(line.split()) == 4

def verify_workload_line(line):
    """
    Verify the structure and contents of the workload statisics summary section
    """
    print "workload line [%s]" % line
    if line.lower().startswith('read') or line.lower().startswith('written'):
        assert line.split()[1] > 0.0

def verify_nid_line(line):
    """
    Verify the struture of sections that report a nidname and a serial number
    """
    print "nid line [%s]" % line
    assert line.startswith('nid')
    assert len(line.split()) == 2
