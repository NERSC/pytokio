#!/usr/bin/env python
"""
Parse and cache a Slurm job record to simply reanalysis and sharing its data.
"""

import os
import sys
import json
import argparse
import tokio.connectors.slurm

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("slurm_jobid", type=str, help="slurm jobid to process")
    parser.add_argument("-n", "--native", action="store_true", default=True, help="return output in native format")
    parser.add_argument("-j", "--json", action="store_true", help="return output in JSON format")
    parser.add_argument("-c", "--csv", action="store_true", help="return output in CSV format")
    parser.add_argument("-o", "--output", type=str, default=None, help="output file")
    args = parser.parse_args()

    jobid = args.slurm_jobid

    # the following assumption only gets goofy if you have a file named the same
    # thing as a jobid
    if os.path.isfile(jobid):
        # treat the jobid as a cache file if it exists
        slurm_data = tokio.connectors.slurm.Slurm(cache_file=jobid)
    else:
        # treat the jobid as a slurm jobid and call sacct
        slurm_data = tokio.connectors.slurm.Slurm(jobid)

    # Serialize the object
    cache_file = args.output
    if cache_file is not None:
        print "Caching to %s" % cache_file

    if args.csv:
        if cache_file is None:
            print slurm_data.to_dataframe().to_csv()
        else:
            slurm_data.to_dataframe().to_csv(cache_file)
    elif args.json:
        if cache_file is None:
            print slurm_data.to_json(indent=4, sort_keys=True)
        else:
            with open(cache_file, 'w') as fp:
                fp.write(slurm_data.to_json())
    elif args.native:
        if cache_file is None:
            print slurm_data.save_cache()
        else:
            slurm_data.save_cache(cache_file)
    else:
        raise Exception("No output format specified")
