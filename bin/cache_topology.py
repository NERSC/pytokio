#!/usr/bin/env python
"""
Simple CLI wrapper around the tools.topology interface
"""

import json
import argparse
import tokio.tools.topology

def main(argv=None):
    """
    Take either a jobid or a Slurm cache file and return the summary provided by
    the topology tool
    """
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--slurm-cache", default=None, type=str,
                       help="path to slurm job cache file")
    parser.add_argument("--craysdb-cache", default=None, type=str,
                        help="path to xtdb2proc cache file")
    parser.add_argument("-o", "--output", type=str, default=None, help="output file")
    group.add_argument("jobid", nargs='?', default=None, help="Slurm job id of interest")
    args = parser.parse_args(argv)

    topology_result = tokio.tools.topology.get_job_diameter(jobid=args.jobid,
                                                            craysdb_cache_file=args.craysdb_cache,
                                                            slurm_cache_file=args.slurm_cache)
    # Serialize the object
    cache_file = args.output
    if cache_file is None:
        print json.dumps(topology_result, indent=4, sort_keys=True)
    else:
        print "Caching to %s" % cache_file
        json.dump(topology_result, open(cache_file, 'w'))

if __name__ == "__main__":
    main()
