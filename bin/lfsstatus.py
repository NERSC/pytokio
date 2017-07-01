#!/usr/bin/env python

import argparse
import datetime
import json
import tokio.tools.lfsstatus as lfsstatus

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--fullness", help="summarize fullness of OSTs (default)", action="store_true")
    parser.add_argument("--failure", help="summarize failure state of OSSes and OSTs", action="store_true")
    parser.add_argument("filesystem", help="file system identifier (e.g., snx11168)")
    parser.add_argument("txt_file", help="path of lfs-df.txt file or ost-map.txt")
    parser.add_argument("datetime", help="date and time of interest in YYYY-MM-DD HH:MM:SS format")
    args = parser.parse_args()

    if args.failure:
        print json.dumps(lfsstatus.get_failures_at_datetime(args.filesystem, datetime.datetime.strptime(args.datetime, "%Y-%m-%d %H:%M:%S")), indent=4, sort_keys=True)
    else:
        print json.dumps(lfsstatus.get_fullness_at_datetime(args.filesystem, datetime.datetime.strptime(args.datetime, "%Y-%m-%d %H:%M:%S")), indent=4, sort_keys=True)
