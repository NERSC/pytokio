#!/usr/bin/env python
"""
Test topology tool interface
"""

import tokiotest
import tokio.tools.topology

def test_basic():
    """
    tools.topology.get_job_diameter

    """
    topo = tokio.tools.topology.get_job_diameter(
        jobid_cache_file=tokiotest.SAMPLE_SLURM_CACHE_FILE,
        nodemap_cache_file=tokiotest.SAMPLE_XTDB2PROC_FILE)
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
