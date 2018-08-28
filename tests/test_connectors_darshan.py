#!/usr/bin/env python
"""
Test the Darshan connector
"""

import tokiotest
import tokio.connectors.darshan

def verify_darshan(darshan_data):
    """
    Verify that all components of a Darshan object are defined
    """
    assert darshan_data is not None
    # Make sure mount table parsing works
    assert 'mounts' in darshan_data
    assert darshan_data['mounts']
    # Make sure header parsing works
    assert 'header' in darshan_data
    assert darshan_data['header']
    # Ensure that counters were found
    assert 'counters' in darshan_data
    assert darshan_data['counters']
    # Ensure the POSIX module and files were found (it should always be present)
    assert 'posix' in darshan_data['counters']
    assert darshan_data['counters']['posix']

def verify_base_counters(darshan_data):
    """
    Verify that the base counters are correctly populated
    """
    # Examine the first POSIX file record containing base counters
    first_base_key = None
    for key in darshan_data['counters']['posix']:
        if key not in ('_perf', '_total'):
            print("Found first base key", key)
            first_base_key = key
            break
    assert first_base_key is not None
    posix_record = darshan_data['counters']['posix'][first_base_key]
    assert posix_record
    # Ensure that it contains an OPENS counter
    assert 'OPENS' in posix_record.values()[0]
    # Ensure that multiple modules were found (STDIO should always exist too)
    assert 'stdio' in darshan_data['counters']

def verify_total_counters(darshan_data):
    """
    Verify that the total counters are correctly populated
    """
    # Ensure that the total counters were extracted
    assert '_total' in darshan_data['counters']['posix']
    # Ensure that it contains an OPENS counter
    assert 'OPENS' in darshan_data['counters']['posix']['_total']
    # Ensure that multiple modules were found (STDIO should always exist too)
    assert 'stdio' in darshan_data['counters']
    # Ensure that it contains an OPENS counter
    assert 'OPENS' in darshan_data['counters']['stdio']['_total']

def verify_perf_counters(darshan_data):
    """
    Verify that the perf counters are correctly populated
    """
    # Ensure that the perf counters were extracted
    assert '_perf' in darshan_data['counters']['posix']
    # Look for a few important counters
    assert 'total_bytes' in darshan_data['counters']['posix']['_perf']
    assert 'agg_perf_by_slowest' in darshan_data['counters']['posix']['_perf']
    # Make sure all counters appear in all modules
    for module in darshan_data['counters']:
        for counter in darshan_data['counters']['posix']['_perf']:
            # the lustre module does not provide any perf information
            assert module == 'lustre' or counter in darshan_data['counters'][module]['_perf']

@tokiotest.needs_darshan
def test_base():
    """
    darshan_parser_base() method
    """
    tokiotest.check_darshan()
    darshan = tokio.connectors.darshan.Darshan(tokiotest.SAMPLE_DARSHAN_LOG)
    darshan.darshan_parser_base()
    verify_darshan(darshan)
    verify_base_counters(darshan)

@tokiotest.needs_darshan
def test_total():
    """
    darshan_parser_total() method
    """
    tokiotest.check_darshan()
    darshan = tokio.connectors.darshan.Darshan(tokiotest.SAMPLE_DARSHAN_LOG)
    darshan.darshan_parser_total()
    verify_darshan(darshan)
    verify_total_counters(darshan)

@tokiotest.needs_darshan
def test_perf():
    """
    darshan_parser_perf() method
    """
    tokiotest.check_darshan()
    darshan = tokio.connectors.darshan.Darshan(tokiotest.SAMPLE_DARSHAN_LOG)
    darshan.darshan_parser_perf()
    verify_darshan(darshan)
    verify_perf_counters(darshan)

@tokiotest.needs_darshan
def test_all():
    """
    ensure that all parsers produce non-conflicting keys
    """
    tokiotest.check_darshan()
    # try parsing in different orders just to make sure that no method is nuking the others
    darshan = tokio.connectors.darshan.Darshan(tokiotest.SAMPLE_DARSHAN_LOG)
    darshan.darshan_parser_perf()
    darshan.darshan_parser_base()
    darshan.darshan_parser_total()
    verify_darshan(darshan)
    verify_perf_counters(darshan)
    verify_base_counters(darshan)
    verify_total_counters(darshan)

    darshan = tokio.connectors.darshan.Darshan(tokiotest.SAMPLE_DARSHAN_LOG)
    darshan.darshan_parser_base()
    darshan.darshan_parser_perf()
    darshan.darshan_parser_total()
    verify_darshan(darshan)
    verify_perf_counters(darshan)
    verify_base_counters(darshan)
    verify_total_counters(darshan)

    darshan = tokio.connectors.darshan.Darshan(tokiotest.SAMPLE_DARSHAN_LOG)
    darshan.darshan_parser_base()
    darshan.darshan_parser_total()
    darshan.darshan_parser_perf()
    verify_darshan(darshan)
    verify_perf_counters(darshan)
    verify_base_counters(darshan)
    verify_total_counters(darshan)

    darshan = tokio.connectors.darshan.Darshan(tokiotest.SAMPLE_DARSHAN_LOG)
    darshan.darshan_parser_perf()
    darshan.darshan_parser_total()
    darshan.darshan_parser_base()
    verify_darshan(darshan)
    verify_perf_counters(darshan)
    verify_base_counters(darshan)
    verify_total_counters(darshan)
