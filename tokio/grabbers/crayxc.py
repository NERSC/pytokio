#!/usr/bin/env python
"""
Common utilities to extract the topology information from a Cray XC system.

TODO: improve the cache mechanism to not be such a hack
"""

import os
import sys

### TODO: this is a terrible thing to do
XC_PROC_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'edison.proc')
XC_PROCADMIN_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'edison.xtprocadmin')

xc_procadmin_data = {}
xc_proc_file_data = {}

def load_xc_proc_file(xc_proc_file):
    """
    Load a cached xtdb2proc output file for a system
    """
    global xc_proc_file_data
    xc_proc_file_data = {}
    with open(xc_proc_file, 'r') as fp:
        for line in fp:
            if line.startswith('#'):
                continue
            fields = line.split(',')
            record = {}
            for field in fields:
                key, val = field.split('=', 1)
                record[key] = val
            assert 'processor_id' in record
            assert record['processor_id'] not in xc_proc_file_data
            xc_proc_file_data[record['processor_id']] = record

def load_xc_procadmin_file(xc_procadmin_file):
    """
    Load a cached xtprocadmin output file for a system
    """
    global xc_procadmin_data
    xc_procadmin_data = {}
    with open(xc_procadmin_file, 'r') as fp:
        for line in fp:
            args = line.strip().split()
            if args[0] == "NID":
                continue
            xc_procadmin_data[args[0]] = {
                'nodename': args[2],
                'type': args[3],
                'status': args[4],
                'mode': args[5]
            }

def get_position(nodename, xc_proc_file=None):
    """
    Get an absolute X, Y, Z position of the Aries router corresponding to the
    given nodename
    """
    if len(xc_proc_file_data.keys()) == 0:
        if xc_proc_file is None:
            load_xc_proc_file(XC_PROC_FILE)
        else:
            load_xc_proc_file(xc_proc_file)

    if nodename.startswith('nid'):
        node_num = int(nodename[3:])
    else:
        raise Exception("Nodename %s does not appear to take form nidXXXXX" % nodename)

    return int(xc_proc_file_data[str(node_num)]['x_coord']), \
           int(xc_proc_file_data[str(node_num)]['y_coord']), \
           int(xc_proc_file_data[str(node_num)]['z_coord'])

if __name__ == "__main__":
    print get_position(sys.argv[1])
