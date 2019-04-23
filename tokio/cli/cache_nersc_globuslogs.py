"""Command-line interface into the nersc_globuslogs connector
"""

import sys
import gzip
import json
import time
import datetime
import argparse
import warnings
import mimetypes

import tokio.debug
import tokio.connectors.nersc_globuslogs

DATE_FMT = "%Y-%m-%dT%H:%M:%S"

def main(argv=None):
    """Entry point for the CLI interface
    """
    warnings.simplefilter('always', UserWarning) # One warning per invalid file

    # Parse CLI options
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("query_start", type=str,
                        help="start time of query in %s format" % DATE_FMT)
    parser.add_argument("query_end", type=str,
                        help="end time of query in %s format" % DATE_FMT)
    parser.add_argument('--debug', action='store_true',
                        help="produce debug messages")
    parser.add_argument('--timeout', type=int, default=30,
                        help='ElasticSearch timeout time (default: 30)')
    parser.add_argument('--input', type=str, default=None,
                        help="use cached output from previous ES query")
    parser.add_argument("-o", "--output", type=str, default=None,
                        help="output file")
    parser.add_argument('-h', '--host', type=str, default="localhost",
                        help="hostname of ElasticSearch endpoint (default: localhost)")
    parser.add_argument('-p', '--port', type=int, default=9200,
                        help="port of ElasticSearch endpoint (default: 9200)")
    parser.add_argument('-i', '--index', type=str, default='dtn-dtn-log*',
                        help='ElasticSearch index to query (default:dtn-dtn-log*)')
    parser.add_argument("-c", "--csv", action="store_true", help="return output in CSV format")
    parser.add_argument('--user', type=str, default=None,
                        help='limit results to transfers owned by this user')
    parser.add_argument('--type', type=str, default=None,
                        help='limit results to either STOR, RETR, or NLST')
    args = parser.parse_args(argv)

    if args.debug:
        tokio.DEBUG = True

    # Convert CLI options into datetime
    try:
        query_start = datetime.datetime.strptime(args.query_start, DATE_FMT)
        query_end = datetime.datetime.strptime(args.query_end, DATE_FMT)
    except ValueError:
        sys.stderr.write("Start and end times must be in format %s\n" % DATE_FMT)
        raise

    # Basic input bounds checking
    if query_start >= query_end:
        raise Exception('query_start >= query_end')

    # Read input from a cached json file (generated previously) or by querying
    # Elasticsearch directly
    must = []
    if args.input is None:
        ### Try to connect
        esdb = tokio.connectors.nersc_globuslogs.NerscGlobusLogs(
            host=args.host,
            port=args.port,
            index=args.index,
            timeout=args.timeout)

        if args.user:
            must.append({"term": {"USER": args.user}})
        if args.type:
            must.append({"term": {"TYPE": args.type.upper()}})

        tokio.debug.debug_print("Loaded results from %s:%s" % (args.host, args.port))
    else:
        esdb = tokio.connectors.nersc_globuslogs.NerscGlobusLogs.from_cache(args.input)
        tokio.debug.debug_print("Loaded results from %s" % args.input)

    esdb.query(query_start, query_end, must=must)

    # Write output
    cache_file = args.output
    if cache_file is not None:
        print("Caching to %s" % cache_file)

    if args.csv:
        if cache_file is None:
            print(esdb.to_dataframe().to_csv())
        else:
            esdb.to_dataframe().to_csv(cache_file)
    else:
        esdb.save_cache(cache_file)
