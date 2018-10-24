"""
Provides CLI interfaces for
:meth:`tokio.connectors.slurm.Slurm.to_dataframe` and
:meth:`tokio.connectors.slurm.Slurm.to_json`.
"""

import os
import argparse
import tokio.connectors.slurm

def main(argv=None):
    """Entry point for the CLI interface
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("slurm_jobid", type=str, help="slurm jobid to process")
    parser.add_argument("-n", "--native", action="store_true", default=True,
                        help="return output in native format")
    parser.add_argument("-j", "--json", action="store_true", help="return output in JSON format")
    parser.add_argument("-c", "--csv", action="store_true", help="return output in CSV format")
    parser.add_argument("-o", "--output", type=str, default=None, help="output file")
    args = parser.parse_args(argv)

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
        print("Caching to %s" % cache_file)

    if cache_file is None:
        if args.csv:
            print((slurm_data.to_dataframe()).to_csv())
        elif args.json:
            print(slurm_data.to_json(indent=4, sort_keys=True))
        elif args.native:
            print(str(slurm_data))
        else:
            raise Exception("No output format specified")
    else:
        if args.csv:
            (slurm_data.to_dataframe()).to_csv(cache_file)
        elif args.json:
            with open(cache_file, 'w') as cache_fd:
                cache_fd.write(slurm_data.to_json())
        elif args.native:
            with open(cache_file, 'w') as cache_fd:
                cache_fd.write(str(slurm_data))
        else:
            raise Exception("No output format specified")
