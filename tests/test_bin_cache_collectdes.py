#!/usr/bin/env python
"""
Test the cache_collectd*.py tools.  These are temporary implementations which
need to be integrated into TOKIO more completely.
"""

import os
import subprocess
import tokiotest

BINARY = os.path.join(tokiotest.BIN_DIR, 'cache_collectdes.py')

#
#  TESTING APPEND/UPDATE FUNCTIONALITY
#
#  The current test strategy is to 
#
#  1. initialize a new HDF5 and pull down a large window
#  2. pull down a complete subset of that window to this newly minted HDF5
#  3. ensure that the hdf5 between #1 and #2 doesn't change using summarize_bbhdf5.py
#
# For example,
#
# rm -v output.hdf5; ./bin/cache_collectdes.py --init-start 2017-12-13T00:00:00 --init-end 2017-12-14T00:00:00 --debug --input-json output.json --num-bbnodes 288 --timestep 10 2017-12-13T00:00:00 2017-12-13T01:00:00
# ./bin/summarize_bbhdf5.py output.hdf5
# ./bin/cache_collectdes.py --debug --input-json output.json 2017-12-13T00:15:00 2017-12-13T00:30:00 
# ./bin/summarize_bbhdf5.py output.hdf5
#
# Assert that the two instances of summarize_bbhdf5.py return identical results
#
   
def test_bin_cache_collectdes():
    """
    bin/cache_collectdes.py
    """
    raise NotImplementedError
    subprocess.check_output([BINARY])

