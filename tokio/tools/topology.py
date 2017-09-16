#!/usr/bin/env python

import math
import warnings
from ..connectors import slurm, craysdb

def get_job_diameter(jobid=None, craysdb_cache_file=None, slurm_cache_file=None):
    """
    An extremely crude way to reduce a job's node allocation into a scalar
    metric

    """
    job_info = slurm.Slurm(jobid=jobid, cache_file=slurm_cache_file)
    node_list = job_info.get_job_nodes()
    proc_table = craysdb.CraySdbProc(cache_file=craysdb_cache_file)
    node_positions = []
    if len(node_list) == 0:
        warnings.warn("no valid job_info received from slurm.Slurm")
        return {}
    for jobnode in node_list:
        if not jobnode.startswith('nid'):
            job_ids = job_info.get_jobids()
            warnings.warn("unable to parse jobnode '%s' for jobid '%s'" % (jobnode, ','.join(job_ids)))
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
