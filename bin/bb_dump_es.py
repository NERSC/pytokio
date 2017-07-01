#!/usr/bin/env python
"""
Dump a lot of data out of ElasticSearch using the Python API and native
scrolling support
"""

import copy
import json
import gzip
import datetime
import argparse

try:
    import cPickle as pickle
except:
    import pickle

import elasticsearch
import elasticsearch.helpers

### maximum number of documents to serialize into a single output file
MAX_DOC_BUNDLE = 50000

DEFAULT_PAGE_SIZE = 10000

_DATE_FMT = "%Y-%m-%d %H:%M:%S"

_ES_INDEX = "cori-collectd-*"

_QUERY_OST_DATA = {
    "query": {
        "bool": {
            "must": {
                "query_string": {
#                   "query": "hostname:bb*",
#                   "query": "hostname:bb* AND plugin:disk AND plugin_instance:nvme* AND collectd_type:disk_octets",
                    "query": "hostname:bb* AND (plugin:memory OR plugin:disk OR plugin:cpu OR plugin:interface)",
                    "analyze_wildcard": True,
                },
            },
        },
    },
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

def serialize_bundle_json(bundle, output_file):
    """
    save our bundled pages into gzipped json
    """
    t0 = datetime.datetime.now()
    with gzip.open( filename=output_file, mode='w', compresslevel=1 ) as fp:
        json.dump(bundle, fp)
    print "  Serialization took %.2f seconds" % (datetime.datetime.now() - t0).total_seconds()

def serialize_bundle_pickle(bundle, output_file):
    """
    save our bundled pages into gzipped pickle
    """
    t0 = datetime.datetime.now()
    with gzip.open( filename=output_file.replace('json', 'pickle'), mode='w', compresslevel=1 ) as fp:
        pickle.dump(bundle, fp, pickle.HIGHEST_PROTOCOL)
    print "  Serialization took %.2f seconds" % (datetime.datetime.now() - t0).total_seconds()

def query_and_page( esdb, query, index=_ES_INDEX, scroll='1m', size=DEFAULT_PAGE_SIZE, _source=_SOURCE_FIELDS, serialize_bundle=serialize_bundle_json ):
    t_begin = datetime.datetime.now()

    ### Get first set of results and a scroll id
    result = esdb.search(
        index=index,
        body=query,
        scroll=scroll,
        size=size,
        sort='@timestamp',
        _source=_source,
    )

    print "Fetching first page took %.2f seconds" % (datetime.datetime.now() - t_begin).total_seconds()

    num_bundles = 0
    total_retrieved = 0
    bundled_pages = []
    ### Keep scrolling until we have all the data
    while len(result['hits']['hits']) > 0:
        t_iterate = datetime.datetime.now()
        sid = result['_scroll_id']

        docs_retrieved = len(result['hits']['hits'])
        total_retrieved += docs_retrieved
        print "  Retrieved %d hits, %d to go (%d total)" % (docs_retrieved,
                                                          result['hits']['total'] - total_retrieved,
                                                          result['hits']['total'])

        ### If this page will overflow the bundle, flush the bundle first
        if (len(bundled_pages) + docs_retrieved) > MAX_DOC_BUNDLE:
            output_file = "%s.%08d.json.gz" % (_ES_INDEX.replace('-*', ''), num_bundles)
            serialize_bundle(bundled_pages, output_file)
            print "  Bundled %d documents into %s" % (len(bundled_pages), output_file)
            bundled_pages = []
            num_bundles += 1

        bundled_pages += result['hits']['hits']

        ### Fetch a new page
        t0 = datetime.datetime.now()
        result = esdb.scroll(scroll_id=sid, scroll='1m')
        tf = datetime.datetime.now()
        print "  Fetching data took %.2f seconds" % (tf - t0).total_seconds()
        print "Total cycle took %.2f seconds" % (tf - t_iterate).total_seconds()

    ### flush the last bundle if it's not empty
    if len(bundled_pages) > 0:
        output_file = "%s.%08d.json.gz" % (_ES_INDEX.replace('-*', ''), num_bundles)
        serialize_bundle(bundled_pages, output_file)
        print "Bundled %d documents into %s" % (len(bundled_pages), output_file)

    t_wall = (datetime.datetime.now() - t_begin).total_seconds()
    print "Packaged %d documents in %.2f seconds (%.2f docs/sec)" % (total_retrieved,
                                                                     t_wall,
                                                                     total_retrieved/t_wall)
                                                                     
if __name__ == '__main__':
    ### Parse CLI options
    parser = argparse.ArgumentParser( add_help=False)
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

    ### Convert CLI options into datetime
    try:
        t_start = datetime.datetime.strptime(args.tstart, _DATE_FMT)
        t_stop = datetime.datetime.strptime(args.tstop, _DATE_FMT)
    except ValueError:
        sys.stderr.write("Start and end times must be in format %s\n" % _DATE_FMT)
        raise

    query = copy.deepcopy( _QUERY_OST_DATA )

    ### Create the appropriate timeseries filter if it doesn't exist
    this_node = query
    for node_name in 'query', 'bool', 'filter', 'range', '@timestamp':
        if node_name not in this_node:
            this_node[node_name] = {}
        this_node = this_node[node_name]
    ### Update the timeseries filter
    this_node['gte'] = t_start.strftime("%s")
    this_node['lt'] = t_stop.strftime("%s")
    this_node['format'] = "epoch_second"

    ### Print query
    print json.dumps(query,indent=4)

    ### Try to connect
    esdb = elasticsearch.Elasticsearch([{
        'host': args.host,
        'port': args.port, }])

    ### Run query
    if args.pickle:
        query_and_page(esdb, query, serialize_bundle=serialize_bundle_pickle)
    else:
        query_and_page(esdb, query)
