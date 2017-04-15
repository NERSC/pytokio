#!/usr/bin/env python

import os
import datetime
import argparse

import tokio
import tokio.es

_DATE_FMT = "%Y-%m-%d %H:%M:%S"

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('tstart',
                        type=str,
                        help="lower bound of time to scan, in YYYY-mm-dd HH:MM:SS format")
    parser.add_argument('tstop',
                        type=str,
                        help="upper bound of time to scan, in YYYY-mm-dd HH:MM:SS format")
    parser.add_argument('--debug',
                        action='store_true',
                        help="produce debug messages")
    args = parser.parse_args()
    if not (args.tstart and args.tstop):
        parser.print_help()
        sys.exit(1)
    if args.debug:
        tokio.DEBUG = True

    try:
        t_start = datetime.datetime.strptime(args.tstart, _DATE_FMT)
        t_stop = datetime.datetime.strptime(args.tstop, _DATE_FMT)
    except ValueError:
        sys.stderr.write("Start and end times must be in format %s\n" % _DATE_FMT)
        raise

    esdb = tokio.es.connect()

    esdb.get_rw_data( t_start, t_stop, 5.0 )
