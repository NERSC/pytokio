#!/usr/bin/env python
"""Command-line wrapper for the tokio.tools.darshan function
"""

import sys
import argparse
import datetime
import tokio.tools.darshan

DATE_FMT = "%Y-%m-%d"
DATE_FMT_PRINT = "YYYY-MM-DD"

def main(argv=None):
    """Find darshan logs in the system-wide repository
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
    parser.add_argument('logdir', type=str,
                        help='path to DARSHAN_LOG_DIR (exclusive of dated subdirectories)')
    args = parser.parse_args(argv)

    try:
        if args.start:
            start = datetime.datetime.strptime(args.start, DATE_FMT)
        else:
            start = None
        if args.end:
            end = datetime.datetime.strptime(args.end, DATE_FMT)
        else:
            end = start
    except ValueError:
        sys.stderr.write("Start and end times must be in format %s\n" % DATE_FMT_PRINT)
        raise

    # Basic input bounds checking
    if start > end:
        raise Exception('query_start >= query_end')

    for logfile in tokio.tools.darshan.find_darshanlogs(datetime_start=start,
                                                        datetime_end=end,
                                                        username=args.username,
                                                        jobid=args.jobid,
                                                        darshan_log_dir=args.logdir):
        print logfile

if __name__ == "__main__":
    main()
