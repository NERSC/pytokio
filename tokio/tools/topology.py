#!/usr/bin/env python

import math
import warnings
from ..connectors import slurm, craysdb

def get_job_diameter(jobid, cache_file=None):
    """
    An extremely crude way to reduce a job's node allocation into a scalar
    metric

    """
    job_info = slurm.Slurm(jobid)
    job_info.get_job_nodes()
    proc_table = craysdb.CraySdbProc(cache_file=cache_file)
    node_positions = []
    if 'node_names' not in job_info or len(job_info['node_names']) == 0:
        warnings.warn("no valid job_info received from slurm.Slurm")
        return {}
    for jobnode in job_info['node_names']:
        if not jobnode.startswith('nid'):
            warnings.warn("unable to parse jobnode '%s' for jobid %s" % (jobnode, jobid))
            return {}
        nid_num = int(jobnode.lstrip('nid'))
        node_x = proc_table[nid_num]['x_coord']
        node_y = proc_table[nid_num]['y_coord']
        node_z = proc_table[nid_num]['z_coord']
        node_positions.append((node_x, node_y, node_z))

    # Three dimensional topology
    center = [0.0, 0.0, 0.0] 
    for node_position in node_positions: 
        center[0] += node_position[0]
        center[1] += node_position[1]
        center[2] += node_position[2]

    center[0] /= float(len(node_positions))
    center[1] /= float(len(node_positions))
    center[2] /= float(len(node_positions))

    min_r = 10000.0
    max_r = 0.0
    avg_r = 0.0
    for node_position in node_positions: 
        r2 = (node_position[0] - center[0])**2.0
        r2 += (node_position[1] - center[1])**2.0
        r2 += (node_position[2] - center[2])**2.0
        r = math.sqrt(r2)
        if r < min_r:
            min_r = r
        if r > max_r:
            max_r = r
        avg_r += r

    return { 
        "job_min_radius": min_r,
        "job_max_radius": max_r,
        "job_avg_radius": avg_r / float(len(node_positions)),
    }
