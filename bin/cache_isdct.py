#!/usr/bin/env python
"""
Parse and cache an ISDCT dump to simply reanalysis and sharing its data.
"""

import sys
import json
import tokio.connectors.nersc_isdct

if __name__ == "__main__":
    input_file = sys.argv[1]

    # Read from a cache file
    isdct_data = tokio.connectors.nersc_isdct.NerscIsdct(sys.argv[1])

    # Serialize the object, then re-read it and verify it
    cache_file = None
    for known_suffix in ('.tgz', '.tar.gz'):
        if input_file.endswith(known_suffix):
            cache_file = sys.argv[1].replace(known_suffix, '.json')
    if cache_file is None:
        cache_file = "%s.json" % input_file

    print "Caching to %s" % cache_file

    isdct_data.save_cache(cache_file)
