"""
Provides CLI interfaces for :meth:`tokio.connectors.nersc_jobsdb.NerscJobsDb.get_concurrent_jobs`
"""

import os
import time
import datetime
import argparse
import tokio.connectors.nersc_jobsdb

def main(argv=None):
    """Entry point for the CLI interface
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("start", type=str, help="start time in YYYY-MM-DDTHH:MM:SS format")
    parser.add_argument("end", type=str, help="end time in YYYY-MM-DDTHH:MM:SS format")
    parser.add_argument("host", type=str, help="return jobs running on this NERSC host")
    parser.add_argument("-i", "--input", type=str, default=None, help="input cache db file")
    parser.add_argument("-o", "--output", type=str, default=None, help="output file")
    args = parser.parse_args(argv)

    start = datetime.datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%S")
    end = datetime.datetime.strptime(args.end, "%Y-%m-%dT%H:%M:%S")

    if args.input is not None:
        nerscjobsdb = tokio.connectors.nersc_jobsdb.NerscJobsDb(cache_file=args.input)
    else:
        nerscjobsdb = tokio.connectors.nersc_jobsdb.NerscJobsDb()
    nerscjobsdb.get_concurrent_jobs(
        int(time.mktime(start.timetuple())),
        int(time.mktime(end.timetuple())),
        args.host)

    cache_file = args.output
    if cache_file is None:
        i = 0
        while True:
            cache_file = "nersc_jobsdb-%d.sqlite" % i
            if os.path.exists(cache_file):
                i += 1
            else:
                break
    print("Caching to %s" % cache_file)
    nerscjobsdb.save_cache(cache_file)
