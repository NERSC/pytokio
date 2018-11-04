"""
Provides CLI interface for the :meth:`tokio.tools.darshan.load_darshanlogs` tool
which locates darshan logs in the system-wide repository.
"""

import sys
import json
import argparse
import datetime
import tokio.tools.darshan

DATE_FMT = "%Y-%m-%d"
DATE_FMT_PRINT = "YYYY-MM-DD"

def main(argv=None):
    """Entry point for the CLI interface
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start', type=str, default=None,
                        help='date to begin search for logs, in %s format ' % DATE_FMT_PRINT
                        + '(default: same as start)')
    parser.add_argument('-e', '--end', type=str, default=None,
                        help='date to end search for logs (inclusive),'
                        + ' in %s format (default: same as start)' % DATE_FMT_PRINT)
    parser.add_argument("-u", "--username", type=str, default=None,
                        help="username of Darshan log owner")
    parser.add_argument("-j", "--jobid", type=str, default=None,
                        help="jobid of Darshan log")
    parser.add_argument("-l", "--load", type=str, default=None,
                         help='load each Darshan log; must be {base[,total][,perf]}')
    parser.add_argument('--logdir', type=str,
                        help='path to DARSHAN_LOG_DIR, exclusive of dated subdirectories (default: use site config value)')
    parser.add_argument('--host', type=str, default=None,
                        help="hostname; only required if --logdir is not specified and site config contains multiple darshan_log_dirs")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Enable debug messages")
    args = parser.parse_args(argv)

    start = None
    end = None
    try:
        if args.start:
            start = datetime.datetime.strptime(args.start, DATE_FMT)
    
        if args.end:
            end = datetime.datetime.strptime(args.end, DATE_FMT)

    except ValueError:
        sys.stderr.write("Start and end times must be in format %s\n" % DATE_FMT_PRINT)
        raise

    # Basic input bounds checking
    if end and not start:
        start = end
    if start and not end:
        end = start
    if start and start > end:
        raise ValueError('query_start >= query_end')
    elif not start and not args.jobid:
        raise ValueError("Either --start/--end or --jobid must be defined")

    if args.verbose:
        tokio.debug.DEBUG = True

    if args.load:
        results = tokio.tools.darshan.load_darshanlogs(datetime_start=start,
                                                       datetime_end=end,
                                                       username=args.username,
                                                       jobid=args.jobid,
                                                       which=args.load,
                                                       log_dir=args.logdir,
                                                       system=args.host)
        print(json.dumps(results, indent=4, sort_keys=True))

    else:
        for logfile in tokio.tools.darshan.find_darshanlogs(datetime_start=start,
                                                            datetime_end=end,
                                                            username=args.username,
                                                            jobid=args.jobid,
                                                            log_dir=args.logdir,
                                                            system=args.host):
            print(logfile)
