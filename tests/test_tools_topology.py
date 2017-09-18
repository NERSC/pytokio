#!/usr/bin/env python
"""
Test topology tool interface
"""

import os
import tokio.tools.topology

SAMPLE_XTDB2PROC_FILE = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                     'inputs',
                                     'sample.xtdb2proc')
SAMPLE_SLURM_CACHE = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                                  'inputs',
                                  'sample.slurm')

def test_basic():
    """
    tools.topology.get_job_diameter

    """
    topo = tokio.tools.topology.get_job_diameter(
        slurm_cache_file=SAMPLE_SLURM_CACHE,
        craysdb_cache_file=SAMPLE_XTDB2PROC_FILE)
    assert len(topo) > 0
    for expected_key in 'job_min_radius', 'job_max_radius', 'job_avg_radius':
        assert expected_key in topo
        assert topo[expected_key] > 0

#   try:
#       ...
#   except OSError as error:
#       # sacct is not available
#       raise nose.SkipTest(error)
#   except KeyError as error:
#       # sacct is available, but job's nodes aren't in our CraySdb cache
#       raise nose.SkipTest(error)
