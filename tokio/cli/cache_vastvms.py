"""
Provide a CLI interface for :meth:`tokio.connectors.vastvms.VastManagementSystem`.

Accepts a number of arguments; ``query_type`` is the only required argument and
can be any query type supported by VastManagementSystem.  One or more ``-q``
arguments can be passed to specify additional key=value parameters to be added
to the query object issued to the REST interface.  For example::

    cache_vastvms monitor 61 -q time_frame=10m -q object_ids=[42,43]

will query the monitors resource for monitor number 61, and pass the following
arguments along with the query::

    {
        "time_frame": "10m",
        "object_ids": [
            42,
            43
        ]
    }

The key=value handles values that are either scalars or json-formatted lists.
"""

import sys
import json
import datetime
import argparse
import tokio.connectors.vastvms
import tokio.config

DATE_FMT = "%Y-%m-%dT%H:%M:%S"
DATE_FMT_PRINT = "YYYY-MM-DDTHH:MM:SS"

def main(argv=None):
    """Entry point for the CLI interface
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--start", type=str,
                        help="start time of query in %s format" % DATE_FMT_PRINT)
    parser.add_argument("-e", "--end", type=str,
                        help="end time of query in %s format" % DATE_FMT_PRINT)
    parser.add_argument("-j", "--json", action="store_true", default=True,
                        help="return output in JSON format")
    parser.add_argument("-c", "--csv", action="store_true",
                        help="return output in CSV format")
    parser.add_argument("-i", "--input", type=str, default=None,
                        help="read input from this JSON instead of accessing REST API")
    parser.add_argument("-o", "--output", type=str, default=None,
                        help="output file")
    parser.add_argument("--url", type=str, default="https://localhost/api", help="rest API endpoint (default: https://localhost/api)")
    parser.add_argument("-u", "--username", type=str, default="admin", help="username for VMS authentication")
    parser.add_argument("-p", "--password", type=str, default="123456", help="password for VMS username")
    parser.add_argument("-q", "--query-args", type=str, nargs='*', default=None, help="additional query args in key=value form; can be specified multiple times")
    parser.add_argument("query_type", type=str, nargs='+',
                        help='query type ("dashboard" or "monitor <#>")')
    args = parser.parse_args(argv)

    if args.end and not args.start:
        parser.error("--start must be specified with --end")
    try:
        if args.start:
            start = datetime.datetime.strptime(args.start, DATE_FMT)
        if args.end:
            end = datetime.datetime.strptime(args.end, DATE_FMT)
    except ValueError:
        raise ValueError("Start and end times must be in format %s\n" % DATE_FMT_PRINT)

    if not args.start:
        start = datetime.datetime.now() - datetime.timedelta(hours=1)
    if not args.end:
        end = start + datetime.timedelta(hours=1)

    # Basic input bounds checking
    if start >= end:
        raise ValueError('--start >= --end')

    if args.input:
        raise NotImplementedError()

    # Initialize query interface
    vms = tokio.connectors.vastvms.VastManagementSystem(
        endpoint=args.url,
        username=args.username,
        password=args.password)

    # Additional query arguments
    query_args = {}
    if args.query_args is not None:
        for query_arg in args.query_args:
            key, value = query_arg.split("=", 1)
            if value[0] in ('{', '['):
                value = json.loads(value)
            query_args[key] = value

    if args.query_type[0] == "monitor":
        payload = vms.get_monitor(args.query_type[1], **query_args)
    elif args.query_type[0] == "dashboard":
        payload = vms.get_dashboard(**query_args)
    else:
        raise ValueError("unknown query type")

    # Serialize the object
    cache_file = args.output
    if cache_file is not None:
        print("Caching to %s" % cache_file)

    if args.csv:
        frame = tokio.connectors.vastvms.vms2df(payload)
        if cache_file is None:
            print(frame.to_csv())
        else:
            frame.to_csv(cache_file)
    else:
        if cache_file is not None:
            json.dump(payload, open(cache_file, 'w'))
        else:
            print(json.dumps(payload, indent=4, sort_keys=True))
