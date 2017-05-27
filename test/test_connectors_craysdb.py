#!/usr/bin/env python

import os
import json
import tempfile
import tokio.connectors.craysdb

SAMPLE_XTDB2PROC_FILE = os.path.join(os.getcwd(), 'inputs', 'sample.xtdb2proc')

def verify_craysdbproc(craysdbproc):
    assert len(craysdbproc) > 0
    for nidnum, record in craysdbproc.iteritems():
        assert len(record) > 0
        assert nidnum == record['processor_id']
        for key in 'cab_position', 'cab_row', 'cage', 'cpu', 'process_slots', \
                    'slot', 'x_coord', 'y_coord', 'z_coord':
            assert record[key] >= 0
        assert record['process_slots_free'] >= record['process_slots']

def test_craysdbproc_from_cache():
    ### read from a cache file
    craysdbproc = tokio.connectors.craysdb.CraySDBProc( SAMPLE_XTDB2PROC_FILE )

    ### verify that the results are sensible
    verify_craysdbproc(craysdbproc)

def test_craysdbproc_from_sdb():
    ### read+verify from the Cray Service Database (if available)
    try:
        craysdbproc = tokio.connectors.craysdb.CraySDBProc()
    except OSError as exception:
        if exception.errno == 2:
            ### sdb isn't available
            pass
    else:
        verify_craysdbproc(craysdbproc)

def test_craysdbproc_serializer():
    ### read from a cache file
    craysdbproc = tokio.connectors.craysdb.CraySDBProc( SAMPLE_XTDB2PROC_FILE )

    ### serialize the object, then re-read it and verify it
    cache_file = tempfile.NamedTemporaryFile(delete=False)
    print "Caching to %s" % cache_file.name
    craysdbproc.save_cache(cache_file.name)

    ### open a second file handle to this cached file to load it
    craysdbproc = tokio.connectors.craysdb.CraySDBProc( cache_file.name )

    ### destroy the cache file
    cache_file.close()

    ### verify the integrity of what we serialized
    verify_craysdbproc(craysdbproc)
