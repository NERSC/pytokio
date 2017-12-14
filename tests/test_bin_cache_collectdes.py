#!/usr/bin/env python
"""
Test the cache_collectd*.py tools.  These are temporary implementations which
need to be integrated into TOKIO more completely.
"""

import os
import subprocess
import tokiotest

BINARY = os.path.join(tokiotest.BIN_DIR, 'cache_collectdes.py')

def test_bin_cache_collectdes():
    """
    bin/cache_collectdes.py
    """
    raise NotImplementedError
    subprocess.check_output([BINARY])
