#!/usr/bin/env python

import tokio.connectors.darshan

import json
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='parse a darshan log')
    parser.add_argument('logfile', metavar='N', type=str, help='darshan log file to parse')
    parser.add_argument('--base', action='store_true', help='darshan log field data [default]')
    parser.add_argument('--total', action='store_true', help='aggregated darshan field data')
    parser.add_argument('--perf', action='store_true', help='derived perf data')
    args = parser.parse_args()
    darshan = tokio.connectors.darshan.Darshan(args.logfile)
    if args.total:
        darshan_data = darshan.darshan_parser_total()
    elif args.perf:
        darshan_data = darshan.darshan_parser_perf()
    else:
        darshan_data = darshan.darshan_parser_base()

    print json.dumps(darshan_data, indent=4, sort_keys=True)
