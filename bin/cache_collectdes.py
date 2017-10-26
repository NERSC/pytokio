#!/usr/bin/env python
"""
Dump a lot of data out of ElasticSearch using the Python API and native
scrolling support
"""

import sys
import copy
import json
import gzip
import time
import datetime
import argparse

try:
    import cPickle as pickle
except:
    import pickle

import tokio.connectors.collectd_es

# Maximum number of documents to serialize into a single output file
MAX_DOC_BUNDLE = 50000

DEFAULT_PAGE_SIZE = 10000

_DATE_FMT = "%Y-%m-%d %H:%M:%S"

_ES_INDEX = "cori-collectd-*"

_QUERY_OST_DATA = {
    "query": {
        "bool": {
            "must": {
                "query_string": {
                    "query": "hostname:bb* AND plugin:disk AND plugin_instance:nvme* AND collectd_type:disk_octets",
### doing a catch-all query over large swaths of time (e.g., one hour) seems to
### lose a lot of information--not sure if there are degenerate document ids or
### what.  TODO: debug this
#                   "query": "hostname:bb* AND (plugin:memory OR plugin:disk OR plugin:cpu OR plugin:interface)",
                    "analyze_wildcard": True,
                },
            },
        },
    },
# uncomment following to get a field with @timestamp represented as milliseconds since epoch
#   "docvalue_fields": ["@timestamp"],
}
_SOURCE_FIELDS = [
    '@timestamp',
    'hostname',
    'plugin',
    'collectd_type',
    'type_instance',
    'plugin_instance',
    'value',
    'longterm',
    'midterm',
    'shortterm',
    'majflt',
    'minflt',
    'if_octets',
    'if_packets',
    'if_errors',
    'rx',
    'tx',
    'read',
    'write',
    'io_time',
]

def serialize_bundle_json(es_obj):
    """
    save our bundled pages into gzipped json
    """
    output_file = "%s.%08d.json.gz" % (es_obj.index.replace('-*', ''), es_obj.num_flushes)
    t0 = datetime.datetime.now()
    with gzip.open(filename=output_file, mode='w', compresslevel=1) as fp:
        json.dump(es_obj.scroll_pages, fp)
    print "  Serialization took %.2f seconds" % (datetime.datetime.now() - t0).total_seconds()
    print "  Bundled %d pages into %s" % (len(es_obj.scroll_pages), output_file)
    es_obj.scroll_pages = []

def serialize_bundle_pickle(es_obj):
    """
    save our bundled pages into gzipped pickle
    """
    output_file = "%s.%08d.json.gz" % (es_obj.index.replace('-*', ''), es_obj.num_flushes)
    t0 = datetime.datetime.now()
    with gzip.open(filename=output_file.replace('json', 'pickle'), mode='w', compresslevel=1) as fp:
        pickle.dump(es_obj.scroll_pages, fp, pickle.HIGHEST_PROTOCOL)
    print "  Serialization took %.2f seconds" % (datetime.datetime.now() - t0).total_seconds()
    print "  Bundled %d documents into %s" % (len(es_obj.scroll_pages), output_file)
    es_obj.scroll_pages = []

def build_timeseries_query(orig_query, start, end):
    """
    Given a query dict and a start/end datetime object, return a new query
    object with the correct time ranges bounded.
    """
    query = copy.deepcopy(orig_query)

    # Create the appropriate timeseries filter if it doesn't exist
    this_node = query
    for node_name in 'query', 'bool', 'filter', 'range', '@timestamp':
        if node_name not in this_node:
            this_node[node_name] = {}
        this_node = this_node[node_name]

    # Update the timeseries filter
    this_node['gte'] = long(time.mktime(start.timetuple()))
    this_node['lt'] = long(time.mktime(end.timetuple()))
    this_node['format'] = "epoch_second"

    return query

if __name__ == '__main__':
    # Parse CLI options
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('tstart',
                        type=str,
                        help="lower bound of time to scan, in YYYY-mm-dd HH:MM:SS format")
    parser.add_argument('tstop',
                        type=str,
                        help="upper bound of time to scan, in YYYY-mm-dd HH:MM:SS format")
    parser.add_argument('--debug',
                        action='store_true',
                        help="produce debug messages")
    parser.add_argument('--pickle',
                        action='store_true',
                        help="use pickle instead of json")
    parser.add_argument('-h', '--host', type=str, default="localhost", help="hostname of ElasticSearch endpoint")
    parser.add_argument('-p', '--port', type=int, default=9200, help="port of ElasticSearch endpoint")
    args = parser.parse_args()
    if not (args.tstart and args.tstop):
        parser.print_help()
        sys.exit(1)

    if args.debug:
        tokio.DEBUG = True

    # Convert CLI options into datetime
    try:
        t_start = datetime.datetime.strptime(args.tstart, _DATE_FMT)
        t_stop = datetime.datetime.strptime(args.tstop, _DATE_FMT)
    except ValueError:
        sys.stderr.write("Start and end times must be in format %s\n" % _DATE_FMT)
        raise

    query = build_timeseries_query(_QUERY_OST_DATA, t_start, t_stop)

    # Print query
    if args.debug:
        print json.dumps(query,indent=4)

    # Try to connect
    es_obj = tokio.connectors.collectd_es.CollectdEs(
        host='localhost',
        port=9200,
        index='cori-collectd-*')

    # Run query
    if args.pickle:
        flush_function = serialize_bundle_pickle
    else:
        flush_function = serialize_bundle_json

    es_obj.query_and_scroll(
        query=query,
        source_filter=_SOURCE_FIELDS,
        filter_function=lambda x: x['hits']['hits'],
        flush_every=MAX_DOC_BUNDLE,
        flush_function=flush_function
    )

    if len(es_obj.scroll_pages) > 0:
        flush_function(es_obj)
