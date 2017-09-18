#!/usr/bin/env python
"""
Parse and cache parts of the NERSC jobs database.
"""

import os
import time
import datetime
import argparse
import tokio.connectors.nersc_jobsdb

def cache_nersc_jobsdb():
    """
    CLI wrapper around NerscJobsDb object's I/O methods
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("start", type=str, help="start time in YYYY-MM-DDTHH:MM:SS format")
    parser.add_argument("end", type=str, help="end time in YYYY-MM-DDTHH:MM:SS format")
    parser.add_argument("host", type=str, help="return jobs running on this NERSC host")
    parser.add_argument("-o", "--output", type=str, default=None, help="output file")
    args = parser.parse_args()

    start = datetime.datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%S")
    end = datetime.datetime.strptime(args.end, "%Y-%m-%dT%H:%M:%S")

    nerscjobsdb = tokio.connectors.nersc_jobsdb.NerscJobsDb()
    nerscjobsdb.get_concurrent_jobs(
        long(time.mktime(start.timetuple())),
        long(time.mktime(end.timetuple())),
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
    print "Caching to %s" % cache_file
    nerscjobsdb.save_cache(cache_file)

if __name__ == "__main__":
    cache_nersc_jobsdb()
