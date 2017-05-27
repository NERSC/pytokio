#!/usr/bin/env python

import os
import json
import tokio.connectors.craysdb

SAMPLE_XTDB2PROC_FILE = os.path.join(os.getcwd(), 'inputs', 'sample.xtdb2proc')

def verify_craysdbproc(craysdbproc):
    for nidnum, record in craysdbproc.iteritems():
        assert nidnum == record['processor_id']
        for key in 'cab_position', 'cab_row', 'cage', 'cpu', 'process_slots', \
                    'slot', 'x_coord', 'y_coord', 'z_coord':
            assert record[key] >= 0
        assert record['process_slots_free'] >= record['process_slots']

def test_craysdbproc_from_cache():
    craysdbproc = tokio.connectors.craysdb.CraySDBProc( SAMPLE_XTDB2PROC_FILE )
    verify_craysdbproc(craysdbproc)

    try:
        craysdbproc = tokio.connectors.craysdb.CraySDBProc()
    except OSError as exception:
        if exception.errno == 2:
            ### sdb isn't available
            pass
    else:
        verify_craysdbproc(craysdbproc)

if __name__ == "__main__":
    test_craysdbproc_from_cache()
