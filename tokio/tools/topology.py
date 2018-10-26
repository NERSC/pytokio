#!/usr/bin/env python
"""Perform operations based on the mapping of a job to network topology
"""

import math
import warnings
import tokio.connectors.craysdb as craysdb
from . import jobinfo as jobinfo

def get_job_diameter(jobid, nodemap_cache_file=None, jobinfo_cache_file=None):
    """Calculate the diameter of a job

    An extremely crude way to reduce a job's node allocation into a scalar
    metric.  Assumes nodes are equally capable and fall on a 3D network;
    calculates the center of mass of the job's node positions.

    Args:
        jobid (str): A logical job id from which nodes are determined and their
            topological placement is determined
        nodemap_cache_file (str): Full path to the file containing the cached
            contents to be used to determine the node position map
        jobinfo_cache_file (str): Full path to the file containing the cached
            contents to be used to convert the job id into a node list

    Returns:
        dict: Contains three keys representing three ways in which a job's
        radius can be expressed.  Keys are:

            * job_min_radius: The smallest distance between the job's center of
              mass and a job node
            * job_max_radius: The largest distance between the job's center of
              mass and a job node
            * job_avg_radius: The average distance between the job's center of
              mass and all job nodes

    """
    node_list = jobinfo.get_job_nodes(jobid=jobid, cache_file=jobinfo_cache_file)
    proc_table = craysdb.CraySdbProc(cache_file=nodemap_cache_file)
    node_positions = []
    if len(node_list) == 0:
        warnings.warn("no valid job_info received from jobinfo.get_job_nodes()")
        return {}
    for jobnode in node_list:
        if not jobnode.startswith('nid'):
            warnings.warn("unable to parse jobnode '%s' for jobid '%s'" % (jobnode, jobid))
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
