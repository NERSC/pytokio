"""
Provides CLI interfaces into the :mod:`tokio.tools.lfsstatus` tool's
:meth:`tokio.tools.lfsstatus.get_failures` and
:meth:`tokio.tools.lfsstatus.get_fullness` methods.
"""

import argparse
import datetime
import json
import tokio.tools.lfsstatus as lfsstatus

def main(argv=None):
    """Entry point for the CLI interface
    """
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--fullness", nargs='?', const="", type=str,
                       help="path to lfs_df text file; summarize fullness of OSTs ")
    group.add_argument("--failure", nargs='?', const="", type=str,
                       help="path to ost_map text file; summarize failure state of OSSes and OSTs")
    parser.add_argument("-o", "--output", type=str, default=None, help="output file")
    parser.add_argument("filesystem", help="logical file system name (e.g., cscratch)")
    parser.add_argument("datetime", help="date and time of interest in YYYY-MM-DDTHH:MM:SS format")
    args = parser.parse_args(argv)

    target_datetime = datetime.datetime.strptime(args.datetime, "%Y-%m-%dT%H:%M:%S")
    if args.failure is not None:
        results = lfsstatus.get_failures(
            args.filesystem,
            target_datetime,
            cache_file=args.failure if args.failure != "" else None)
    elif args.fullness is not None:
        results = lfsstatus.get_fullness(
            args.filesystem,
            target_datetime,
            cache_file=args.fullness if args.fullness != "" else None)
    else:
        raise Exception('Neither --fullness nor --failure were specified')

    # Serialize the object
    cache_file = args.output
    if cache_file is None:
        print(json.dumps(results, indent=4, sort_keys=True))
    else:
        print("Caching to %s" % cache_file)
        json.dump(results, open(cache_file, 'w'))


