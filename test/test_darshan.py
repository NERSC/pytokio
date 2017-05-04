#!/usr/bin/env python

import os
import tokio.grabbers.darshan

SAMPLE_INPUT = os.path.join(os.getcwd(), 'inputs', 'sample.darshan')

def test_base():
    darshan_data = tokio.grabbers.darshan.darshan_parser_base(SAMPLE_INPUT)

    assert darshan_data is not None

    ### make sure mount table parsing works
    assert 'mounts' in darshan_data
    assert len(darshan_data['mounts'].keys()) > 0

    ### make sure header parsing works
    assert 'header' in darshan_data
    assert len(darshan_data['header'].keys()) > 0

    ### ensure that counters were found
    assert 'counters' in darshan_data
    assert len(darshan_data['counters'].keys()) > 0

    ### ensure the POSIX module was found (it should always be present)
    assert 'posix' in darshan_data['counters']

    ### ensure some POSIX files were found
    assert len(darshan_data['counters']['posix'].keys()) > 0

    ### examine the first POSIX file record
    posix_record = darshan_data['counters']['posix'].itervalues().next()

    ### ensure that it contains counters
    assert len(posix_record) > 0

    ### ensure that it contains an OPENS counter
    assert 'OPENS' in posix_record.itervalues().next()

    ### ensure that multiple modules were found (STDIO should always exist too)
    assert 'stdio' in darshan_data['counters']

def test_total():
    darshan_data = tokio.grabbers.darshan.darshan_parser_total(SAMPLE_INPUT)

    assert darshan_data is not None

    ### make sure mount table parsing works
    assert 'mounts' in darshan_data
    assert len(darshan_data['mounts'].keys()) > 0

    ### make sure header parsing works
    assert 'header' in darshan_data
    assert len(darshan_data['header'].keys()) > 0

    ### ensure that counters were found
    assert 'counters' in darshan_data
    assert len(darshan_data['counters'].keys()) > 0

    ### ensure the POSIX module was found (it should always be present)
    assert 'posix' in darshan_data['counters']

    ### ensure some POSIX files were found
    assert len(darshan_data['counters']['posix'].keys()) > 0

    ### ensure that the total counters were extracted
    assert '_total' in darshan_data['counters']['posix']

    ### ensure that it contains an OPENS counter
    assert 'OPENS' in darshan_data['counters']['posix']['_total']

    ### ensure that multiple modules were found (STDIO should always exist too)
    assert 'stdio' in darshan_data['counters']

    ### ensure that it contains an OPENS counter
    assert 'OPENS' in darshan_data['counters']['stdio']['_total']


def test_perf():
    darshan_data = tokio.grabbers.darshan.darshan_parser_perf(SAMPLE_INPUT)

    assert darshan_data is not None

    ### make sure mount table parsing works
    assert 'mounts' in darshan_data
    assert len(darshan_data['mounts'].keys()) > 0

    ### make sure header parsing works
    assert 'header' in darshan_data
    assert len(darshan_data['header'].keys()) > 0

    ### ensure that counters were found
    assert 'counters' in darshan_data
    assert len(darshan_data['counters'].keys()) > 0

    ### ensure the POSIX module was found (it should always be present)
    assert 'posix' in darshan_data['counters']

    ### ensure some POSIX files were found
    assert len(darshan_data['counters']['posix'].keys()) > 0

    ### ensure that the perf counters were extracted
    assert '_perf' in darshan_data['counters']['posix']

    ### look for a few important counters
    assert 'total_bytes' in darshan_data['counters']['posix']['_perf']
    assert 'agg_perf_by_slowest' in darshan_data['counters']['posix']['_perf']

    ### make sure all counters appear in all modules
    for module in darshan_data['counters'].keys():
        for counter in darshan_data['counters']['posix']['_perf'].keys():
            assert counter in darshan_data['counters'][module]['_perf']
