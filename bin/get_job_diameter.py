#!/usr/bin/env python

import json
import argparse
import tokio.tools

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cache-file", type=str, default=None, help="path to xtdb2proc output file" )
    parser.add_argument("jobid", help="jobid of interest")
    args = parser.parse_args()
    print json.dumps(tokio.tools.topology.get_job_diameter(args.jobid, args.cache_file),
                     indent=4,
                     sort_keys=True)
