#!/usr/bin/env python
"""
Parse and cache an ISDCT dump to simply reanalysis and sharing its data.
"""

import sys
import json
import argparse
import tokio.connectors.nersc_isdct

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("isdctfile", type=str, help="darshan logs to process")
    parser.add_argument("-c", "--csv", action="store_true", help="return output in CSV format")
    parser.add_argument("-o", "--output", type=str, default=None, help="output file")
    args = parser.parse_args()

    input_file = args.isdctfile

    # Read from a cache file
    isdct_data = tokio.connectors.nersc_isdct.NerscIsdct(args.isdctfile)

    # Serialize the object
    cache_file = args.output
    if cache_file is None: # try to come up with our own output file name
        if args.csv:
            output_suffix = ".csv"
        else:
            output_suffix = ".json"
        for known_suffix in ('.tgz', '.tar.gz'):
            if input_file.endswith(known_suffix):
                cache_file = sys.argv[1].replace(known_suffix, output_suffix)
    if cache_file is None:
        cache_file = "%s%s" % (input_file, output_suffix)

    print "Caching to %s" % cache_file

    if args.csv:
        isdct_data.to_dataframe().to_csv(cache_file)
    else:
        isdct_data.save_cache(cache_file)
