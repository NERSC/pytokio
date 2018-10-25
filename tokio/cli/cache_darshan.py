"""
Expose several methods of :mod:`tokio.connectors.darshan` via a
command-line interface.
"""

import json
import argparse
import tokio.connectors.darshan

def main(argv=None):
    """Entry point for the CLI interface
    """
    parser = argparse.ArgumentParser(description='parse a darshan log')
    parser.add_argument('logfile', metavar='N', type=str, help='darshan log file to parse')
    parser.add_argument('--base', action='store_true', help='darshan log field data [default]')
    parser.add_argument('--total', action='store_true', help='aggregated darshan field data')
    parser.add_argument('--perf', action='store_true', help='derived perf data')
    parser.add_argument("-o", "--output", type=str, default=None, help="output file")
    args = parser.parse_args(argv)

    darshan = tokio.connectors.darshan.Darshan(args.logfile)
    if args.total:
        darshan.darshan_parser_total()

    if args.perf:
        darshan.darshan_parser_perf()

    if args.base or (not args.perf and not args.total):
        darshan.darshan_parser_base()

    # Serialize the object
    cache_file = args.output
    if cache_file is None:
        print(json.dumps(darshan, indent=4, sort_keys=True))
    else:
        print("Caching to %s" % cache_file)
        json.dump(darshan, open(cache_file, 'w'))
