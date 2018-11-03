"""Test functions used throughout pytokio
"""

import nose
import tokio.common

def test_common_humanize_units():
    """tokio.common.humanize_units
    """
    inputs = [
        {'byte_count': 0, 'divisor': 1024, 'expected_unit': 'bytes'},
        {'byte_count': 0, 'divisor': 1000, 'expected_unit': 'bytes'},
        {'byte_count': 1, 'divisor': 1024, 'expected_unit': 'bytes'},
        {'byte_count': 1, 'divisor': 1000, 'expected_unit': 'bytes'},
        {'byte_count': 1023, 'divisor': 1024, 'expected_unit': 'bytes'},
        {'byte_count': 1023, 'divisor': 1000, 'expected_unit': 'KB'},
        {'byte_count': 1024, 'divisor': 1024, 'expected_unit': 'KiB'},
        {'byte_count': 1024, 'divisor': 1000, 'expected_unit': 'KB'},
        {'byte_count': 1024*1023, 'divisor': 1024, 'expected_unit': 'KiB'},
        {'byte_count': 1024*1023, 'divisor': 1000, 'expected_unit': 'MB'},
        {'byte_count': 1024*1024, 'divisor': 1024, 'expected_unit': 'MiB'},
        {'byte_count': 1024*1024, 'divisor': 1000, 'expected_unit': 'MB'},
        {'byte_count': 1024*1024*1023, 'divisor': 1024, 'expected_unit': 'MiB'},
        {'byte_count': 1024*1024*1023, 'divisor': 1000, 'expected_unit': 'GB'},
        {'byte_count': 1024*1024*1024, 'divisor': 1024, 'expected_unit': 'GiB'},
        {'byte_count': 1024*1024*1024, 'divisor': 1000, 'expected_unit': 'GB'},
        {'byte_count': 1024*1024*1024*1023, 'divisor': 1024, 'expected_unit': 'GiB'},
        {'byte_count': 1024*1024*1024*1023, 'divisor': 1000, 'expected_unit': 'TB'},
        {'byte_count': 1024*1024*1024*1024, 'divisor': 1024, 'expected_unit': 'TiB'},
        {'byte_count': 1024*1024*1024*1024, 'divisor': 1000, 'expected_unit': 'TB'},
    ]

    for args in inputs:
        results = tokio.common.humanize_units(byte_count=args['byte_count'], divisor=args['divisor'])
        print("%d bytes in units of %d -> %f %s (expecting units %s)" % (
            args['byte_count'],
            args['divisor'],
            results[0],
            results[1],
            args['expected_unit']))
        assert (results[0] >= 1.0 or args['byte_count'] == 0) and results[0] < args['divisor']
        assert results[1] == args['expected_unit']

def test_common_humanize_units_invalid_divisor():
    """tokio.common.humanize_units, invalid divisor
    """
    nose.tools.assert_raises(RuntimeError,
                             tokio.common.humanize_units,
                             1023,
                             512)
