#!/usr/bin/env python

import os
import json
import tokio.connectors.craysdb

SAMPLE_XTDB2PROC_FILE = os.path.join(os.getcwd(), 'inputs', 'sample.xtdb2proc')
SAMPLE_XTPROCADMIN_FILE = os.path.join(os.getcwd(), 'inputs', 'sample.xtprocadmin')

def test_craysdbproc_from_cache():
    craysdbproc = tokio.connectors.craysdb.CraySDBProc( SAMPLE_XTDB2PROC_FILE )
    print type(craysdbproc)
    print craysdbproc.keys()
    print json.dumps(craysdbproc, indent=4)

# {
#     "1": {
#         "alloc_mode": "'interactive'", 
#         "cab_position": "0", 
#         "cab_row": "0", 
#         "cabinet": "null", 
#         "cage": "0", 
#         "cpu": "1", 
#         "next_red_black_switch": "null", 
#         "od_allocator_id": "0", 
#         "process_slots": "4", 
#         "process_slots_free": "4", 
#         "processor_id": "1", 
#         "processor_spec": "null\n", 
#         "processor_status": "'up'", 
#         "processor_type": "'service'", 
#         "slot": "0", 
#         "x_coord": "0", 
#         "y_coord": "0", 
#         "z_coord": "0"
#     }
# }

if __name__ == "__main__":
    test_craysdbproc_from_cache()
