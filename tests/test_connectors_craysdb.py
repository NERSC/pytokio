#!/usr/bin/env python

import os
import tempfile
import tokio.connectors.craysdb

SAMPLE_XTDB2PROC_FILE = os.path.join(os.getcwd(), 'inputs', 'sample.xtdb2proc')

def verify_craysdbproc(craysdbproc):
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

def test_craysdbproc_from_cache():
    # Read from a cache file
    craysdbproc = tokio.connectors.craysdb.CraySDBProc(SAMPLE_XTDB2PROC_FILE)
    verify_craysdbproc(craysdbproc)

def test_craysdbproc_from_sdb():
    # Read from the Cray Service Database
    try:
        craysdbproc = tokio.connectors.craysdb.CraySDBProc()
    except OSError as exception:
        # Sdb isn't available
        # if exception.errno == 2:
        pass
    else:
        verify_craysdbproc(craysdbproc)

def test_craysdbproc_serializer():
    # Read from a cache file
    craysdbproc = tokio.connectors.craysdb.CraySDBProc(SAMPLE_XTDB2PROC_FILE)
    # Serialize the object, then re-read it and verify it
    cache_file = tempfile.NamedTemporaryFile(delete=False)
    print "Caching to %s" % cache_file.name
    craysdbproc.save_cache(cache_file.name)
    # Open a second file handle to this cached file to load it
    craysdbproc = tokio.connectors.craysdb.CraySDBProc(cache_file.name)
    cache_file.close()
    verify_craysdbproc(craysdbproc)

