"""
Dump a lot of data out of ElasticSearch using the Python API and native
scrolling support.

Instantiates a :class:`tokio.connectors.collectd_es.CollectdEs` object and
relies on the :meth:`tokio.connectors.collectd_es.CollectdEs.query_timeseries`
method to populate a data structure that is then serialized to JSON.
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
import tokio.connectors.collectd_es

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
    parser.add_argument('-i', '--index', type=str, default='cori-collectd-*',
                        help='ElasticSearch index to query (default:cori-collectd-*)')
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

    # Read input from a cached json file (generated previously via the --json
    # option) or by querying ElasticSearch?
    if args.input is None:
        ### Try to connect
        esdb = tokio.connectors.collectd_es.CollectdEs(
            host=args.host,
            port=args.port,
            index=args.index,
            timeout=args.timeout)

        pages = None
        for plugin_query in [tokio.connectors.collectd_es.QUERY_CPU_DATA,
                             tokio.connectors.collectd_es.QUERY_DISK_DATA,
                             tokio.connectors.collectd_es.QUERY_MEMORY_DATA]:
            esdb.query_timeseries(plugin_query,
                            query_start,
                            query_end)
            if pages is None:
                pages = esdb.scroll_pages
            else:
                pages += esdb.scroll_pages

        tokio.debug.debug_print("Loaded results from %s:%s" % (args.host, args.port))
    else:
        _, encoding = mimetypes.guess_type(args.input)
        if encoding == 'gzip':
            input_file = gzip.open(args.input, 'r')
        else:
            input_file = open(args.input, 'r')
        pages = json.load(input_file)
        input_file.close()
        tokio.debug.debug_print("Loaded results from %s" % args.input)

    # Write output
    if args.output:
        _, encoding = mimetypes.guess_type(args.output)
        output_file = gzip.open(args.output, 'w') if encoding == 'gzip' else open(args.output, 'w')
        json.dump(pages, output_file)
        output_file.close()
        print("Wrote output to %s" % args.output)
    else:
        print(json.dumps(pages, indent=4, sort_keys=True))
