#!/usr/bin/env python
"""
Test the bin/compare_isdct.py tool
"""

import json
import tokiotest
import test_connectors_nersc_isdct
import tokiobin.compare_isdct

def test_all_json():
    """
    bin/compare_isdct.py --all json output
    """
    argv = ['--all',
            tokiotest.SAMPLE_NERSCISDCT_PREV_FILE,
            tokiotest.SAMPLE_NERSCISDCT_DIFF_FILE]
    output_str = tokiotest.run_bin(tokiobin.compare_isdct, argv)
    result = json.loads(output_str)
    test_connectors_nersc_isdct.validate_diff(result, report_zeros=False)

def test_reduced_json():
    """
    bin/compare_isdct.py reduced json output
    """
    argv = [tokiotest.SAMPLE_NERSCISDCT_PREV_FILE,
            tokiotest.SAMPLE_NERSCISDCT_DIFF_FILE]
    output_str = tokiotest.run_bin(tokiobin.compare_isdct, argv)
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
    bin/compare_isdct.py --all --report-zeros json output
    """
    argv = ["--all",
            "--report-zeros",
            tokiotest.SAMPLE_NERSCISDCT_PREV_FILE,
            tokiotest.SAMPLE_NERSCISDCT_DIFF_FILE]
    output_str = tokiotest.run_bin(tokiobin.compare_isdct, argv)
    result = json.loads(output_str)
    test_connectors_nersc_isdct.validate_diff(result, report_zeros=True)

def test_reduced_json_w_zeros():
    """
    bin/compare_isdct.py reduced --report-zeros json output
    """
    argv = ["--report-zeros",
            tokiotest.SAMPLE_NERSCISDCT_PREV_FILE,
            tokiotest.SAMPLE_NERSCISDCT_DIFF_FILE]
    output_str = tokiotest.run_bin(tokiobin.compare_isdct, argv)
    result = json.loads(output_str)
    for reduction in ('ave', 'min', 'max', 'sum'):
        for counter in tokiotest.SAMPLE_NERSCISDCT_DIFF_MONOTONICS:
            assert result["%s_%s" % (reduction, counter)] > 0
        for counter in tokiotest.SAMPLE_NERSCISDCT_DIFF_ZEROS:
            print("result[%s_%s] == %d" % (
                reduction, counter, result["%s_%s" % (reduction, counter)]))
            assert result["%s_%s" % (reduction, counter)] == 0
    for counter in tokiotest.SAMPLE_NERSCISDCT_DIFF_EMPTYSTR:
        assert result["count_%s" % counter] > 0

def test_reduced_json_gibs():
    """
    bin/compare_isdct.py reduced --gibs json output
    """
    argv = ["--gibs",
            tokiotest.SAMPLE_NERSCISDCT_PREV_FILE,
            tokiotest.SAMPLE_NERSCISDCT_DIFF_FILE]
    output_str = tokiotest.run_bin(tokiobin.compare_isdct, argv)
    result = json.loads(output_str)
    success = 0
    for counter in list(result.keys()):
        assert not counter.endswith('_bytes')
        if counter.endswith('_gibs'):
            success += 1
    assert success > 0

def test_all_json_w_gibs():
    """
    bin/compare_isdct.py --all --gibs json output
    """
    argv = ["--all",
            "--gibs",
            tokiotest.SAMPLE_NERSCISDCT_PREV_FILE,
            tokiotest.SAMPLE_NERSCISDCT_DIFF_FILE]
    output_str = tokiotest.run_bin(tokiobin.compare_isdct, argv)
    result = json.loads(output_str)
    success = 0
    for counters in result['devices'].values():
        for counter in counters:
            assert not counter.endswith('_bytes')
            if counter.endswith('_gibs'):
                success += 1
    assert success > 0

def test_summary():
    """
    bin/compare_isdct.py --summary human-readable output
    """
    argv = ["--summary",
            tokiotest.SAMPLE_NERSCISDCT_PREV_FILE,
            tokiotest.SAMPLE_NERSCISDCT_DIFF_FILE]
    output_str = tokiotest.run_bin(tokiobin.compare_isdct, argv)

    ### look for a section on devices removed
    if tokiotest.SAMPLE_NERSCISDCT_DIFF_RM > 0:
        func = validate_summary_section
        func.description = "bin/compare_isdct.py --summary device removal"
        yield func, output_str, 'devices removed', verify_nid_line

    ### look for a section on devices added
    if tokiotest.SAMPLE_NERSCISDCT_DIFF_ADD > 0:
        func = validate_summary_section
        func.description = "bin/compare_isdct.py --summary device installation"
        yield func, output_str, 'devices installed', verify_nid_line

    ### look for a section on errors detected
    if tokiotest.SAMPLE_NERSCISDCT_DIFF_ERRS > 0:
        func = validate_summary_section
        func.description = "bin/compare_isdct.py --summary error detection"
        yield func, output_str, 'errors detected', verify_errors_line

    ### look for a section with the workload statistics
    func = validate_summary_section
    func.description = "bin/compare_isdct.py --summary workload statistics"
    yield func, output_str, 'workload statistics', verify_workload_line

def validate_summary_section(output_str, section_header, line_verify_func):
    """
    Scan the output text of --summary, look for a section, then parse it to the
    extent necessary to validate the structure of its contents
    """
    print(section_header, line_verify_func)
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
    print("errors line [%s]" % line)
    assert line.startswith('nid')
    assert len(line.split()) == 4

def verify_workload_line(line):
    """
    Verify the structure and contents of the workload statisics summary section
    """
    print("workload line [%s]" % line)
    if line.lower().startswith('read') or line.lower().startswith('written'):
        assert float(line.split()[1]) > 0.0

def verify_nid_line(line):
    """
    Verify the struture of sections that report a nidname and a serial number
    """
    print("nid line [%s]" % line)
    assert line.startswith('nid')
    assert len(line.split()) == 2
