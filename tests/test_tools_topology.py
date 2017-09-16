#!/usr/bin/env python

import os
import nose
import tokio.tools.topology

SAMPLE_XTDB2PROC_FILE = os.path.join(os.getcwd(), 'inputs', 'sample.xtdb2proc')
SAMPLE_SLURM_JOBID = 2000000

def test_basic():
    """
    Basic tools.topology functionality

    """
    try:
        tokio.tools.topology.get_job_diameter(SAMPLE_SLURM_JOBID, cache_file=SAMPLE_XTDB2PROC_FILE)
    except OSError as error:
        # sacct is not available
        raise nose.SkipTest(error)
    except KeyError as error:
        # sacct is available, but job's nodes aren't in our CraySdb cache
        raise nose.SkipTest(error)
