#!/usr/bin/env python

import datetime
import nose.plugins.skip
import elasticsearch.exceptions
import tokio.connectors.collectd_es
# import logging

### the ElasticSearch python library can be VERY noisy on failures
# logging.getLogger('elasticsearch').setLevel(logging.WARNING)

### PAGE_SIZE is number of documents to return per page
PAGE_SIZE = 200 

### QUERY_WINDOW is how long of a time period to search over
QUERY_WINDOW = datetime.timedelta(seconds=10)

SAMPLE_INDEX = 'cori-collectd-*'

SAMPLE_QUERY = {
    "query": {
        "bool": {
            "must": {
                "query_string": tokio.connectors.collectd_es.QUERY_DISK_BYTES_RW['query']['query_string'],
            },
            "filter": {
                "range": {
                    "@timestamp": {},
                },
            },
        },
    },
}

FLUSH_STATE = {
    'pages': []
}

def flush_function(es_obj):
    """
    save our bundled pages into json
    """
    global FLUSH_STATE

    ### assert es_obj.page_size == flush_every as passed to es_obj.query_and_scroll
    assert len(es_obj.scroll_pages) == 1

    ### "flush" to the global state
    FLUSH_STATE['pages'].append(es_obj.scroll_pages[0])

    ### empty the page buffer
    es_obj.scroll_pages = []

def to_epoch(dt):
    """
    could do strftime("%s") but that is not actually explicitly supported by
    Python standard and may not work on non-Linux systems
    """
    epoch_stamp = long((dt - datetime.datetime(1970, 1, 1)).total_seconds())
    print epoch_stamp
    return epoch_stamp


def test_flush_function_correctness():
    """
    CollectdEs flush function correctness
    """

    # Define start/end time.  Because we don't know what's in the remote server,
    # we can't really make this deterministic; just use a five second window
    # ending now.
    t_stop = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
    t_start = t_stop - QUERY_WINDOW

    # note that strftime("%s") is not standard POSIX; this may not work 
    SAMPLE_QUERY['query']['bool']['filter']['range']['@timestamp'] = {
        'gte': to_epoch(t_start),
        'lt': to_epoch(t_stop),
        'format': "epoch_second",
    }

    # Connect
    try:
        es_obj = tokio.connectors.collectd_es.CollectdEs(
            host='localhost',
            port=9200,
            index=SAMPLE_INDEX,
            page_size=PAGE_SIZE)
    except elasticsearch.exceptions.ConnectionError as e:
        raise nose.plugins.skip.SkipTest(e)

    ############################################################################
    # Accumulate results using a flush_function
    ############################################################################
    es_obj.query_and_scroll(
        query=SAMPLE_QUERY,
        flush_every=PAGE_SIZE,
        flush_function=flush_function
    )
    # Run flush function on the last page
    if len(es_obj.scroll_pages) > 0:
        flush_function(es_obj)

    if not len(FLUSH_STATE['pages']) > 2:
        raise nose.plugins.skip.SkipTest("time range only got a single page; cannot test flush function")

    ############################################################################
    # Accumulate results on the object without a flush function
    ############################################################################
    es_obj.query_and_scroll(SAMPLE_QUERY)

    ############################################################################
    # Ensure that both return identical hits
    ############################################################################
    flush_function_hits = set([])
    for page in FLUSH_STATE['pages']:
        flush_function_hits |= set([ x['_id'] for x in page['hits']['hits'] ])
    print "flush_function populated %d documents" % len(flush_function_hits)

    no_flush_hits = set([])
    for page in es_obj.scroll_pages:
        no_flush_hits |= set([ x['_id'] for x in page['hits']['hits'] ])
    print "no flush function populated %d documents" % len(no_flush_hits)

    # Ensure that our ground-truth query actually returned something
    assert len(no_flush_hits) > 0

    # Catch obviously wrong number of returned results
    assert len(no_flush_hits) == len(flush_function_hits) 

    # Catch cases where mismatching results are returned
    assert flush_function_hits == no_flush_hits
