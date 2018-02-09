#!/usr/bin/env python
"""
Test the CraySDB connector
"""

import errno
import nose
import tokiotest
import tokio.connectors.craysdb

def verify_craysdbproc(craysdbproc):
    """
    Correctness tests of an CraySDB object
    """
    assert craysdbproc
    print "Found %d entries" % len(craysdbproc)
    for nidnum, record in craysdbproc.iteritems():
        assert record
        assert nidnum == record['processor_id']
        assert record['process_slots_free'] >= record['process_slots']
        tmp = ['cab_position', 'cab_row', 'cage', 'cpu', 'process_slots', \
               'slot', 'x_coord', 'y_coord', 'z_coord']
        for key in tmp:
            assert record[key] >= 0

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_craysdbproc_from_cache():
    """
    Initialize CraySdb from decompressed cache
    """
    # Create an uncompressed cache file
    tokiotest.TEMP_FILE.close()
    tokiotest.gunzip(tokiotest.SAMPLE_XTDB2PROC_FILE, tokiotest.TEMP_FILE.name)
    print "Decompressed %s to %s" % (tokiotest.SAMPLE_XTDB2PROC_FILE, tokiotest.TEMP_FILE.name)

    # Read from a cache file
    craysdbproc = tokio.connectors.craysdb.CraySdbProc(tokiotest.TEMP_FILE.name)
    verify_craysdbproc(craysdbproc)

def test_craysdbproc_from_gz_cache():
    """
    Initialize CraySdb from compressed cache
    """
    # Read from a cache file
    craysdbproc = tokio.connectors.craysdb.CraySdbProc(tokiotest.SAMPLE_XTDB2PROC_FILE)
    verify_craysdbproc(craysdbproc)

def test_craysdbproc_from_sdb():
    """
    Initialize CraySdb from sdb CLI
    """
    # Read from the Cray Service Database
    try:
        craysdbproc = tokio.connectors.craysdb.CraySdbProc()
    except OSError as exception:
        # Sdb isn't available
        if exception.errno == errno.ENOENT:
            raise nose.SkipTest("craysdb CLI not available")
    else:
        verify_craysdbproc(craysdbproc)

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_craysdbproc_serializer():
    """
    serialized CraySdb can be used to initialize
    """
    # Read from a cache file
    craysdbproc = tokio.connectors.craysdb.CraySdbProc(tokiotest.SAMPLE_XTDB2PROC_FILE)
    # Serialize the object, then re-read it and verify it
    print "Caching to %s" % tokiotest.TEMP_FILE.name
    craysdbproc.save_cache(tokiotest.TEMP_FILE.name)
    # Open a second file handle to this cached file to load it
    craysdbproc = tokio.connectors.craysdb.CraySdbProc(tokiotest.TEMP_FILE.name)
    verify_craysdbproc(craysdbproc)
