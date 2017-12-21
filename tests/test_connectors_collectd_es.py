#!/usr/bin/env python
"""
Test the ElasticSearch collectd connector.  Some of these tests will essentially
pass only at NERSC because of the assumptions built into the indices.
"""

import datetime
import nose.plugins.skip
import elasticsearch.exceptions
import tokio.connectors.collectd_es
import tokiotest
# import logging

### the ElasticSearch python library can be VERY noisy on failures
# logging.getLogger('elasticsearch').setLevel(logging.WARNING)

### PAGE_SIZE is number of documents to return per page
PAGE_SIZE = 200

### QUERY_WINDOW is how long of a time period to search over
QUERY_WINDOW = datetime.timedelta(seconds=10)

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

def test_flush_function_correctness():
    """
    CollectdEs flush function correctness
    """

    # Define start/end time.  Because we don't know what's in the remote server,
    # we can't really make this deterministic; just use a five second window
    # ending now.
    t_stop = datetime.datetime.now() - datetime.timedelta(hours=1)
    t_start = t_stop - QUERY_WINDOW

    # note that strftime("%s") is not standard POSIX; this may not work
    query = tokio.connectors.collectd_es.build_timeseries_query(
        tokiotest.SAMPLE_COLLECTDES_QUERY,
        t_start,
        t_stop)

    # Connect
    try:
        es_obj = tokio.connectors.collectd_es.CollectdEs(
            host='localhost',
            port=9200,
            index=tokiotest.SAMPLE_COLLECTDES_INDEX,
            page_size=PAGE_SIZE)
    except elasticsearch.exceptions.ConnectionError as error:
        raise nose.plugins.skip.SkipTest(error)

    ############################################################################
    # Accumulate results using a flush_function
    ############################################################################
    es_obj.query_and_scroll(
        query=query,
        flush_every=PAGE_SIZE,
        flush_function=flush_function
    )
    # Run flush function on the last page
    if len(es_obj.scroll_pages) > 0:
        flush_function(es_obj)

    # If no pages were found, there is a problem
    assert len(FLUSH_STATE['pages']) > 0

    if not len(FLUSH_STATE['pages']) > 2:
        raise nose.plugins.skip.SkipTest("time range only got %d pages; cannot test flush function"
                                         % len(FLUSH_STATE['pages']))

    ############################################################################
    # Accumulate results on the object without a flush function
    ############################################################################
    es_obj.query_and_scroll(query)

    ############################################################################
    # Ensure that both return identical hits
    ############################################################################
    flush_function_hits = set([])
    for page in FLUSH_STATE['pages']:
        flush_function_hits |= set([x['_id'] for x in page['hits']['hits']])
    print "flush_function populated %d documents" % len(flush_function_hits)

    no_flush_hits = set([])
    for page in es_obj.scroll_pages:
        no_flush_hits |= set([x['_id'] for x in page['hits']['hits']])
    print "no flush function populated %d documents" % len(no_flush_hits)

    # Ensure that our ground-truth query actually returned something
    assert len(no_flush_hits) > 0

    # Catch obviously wrong number of returned results
    assert len(no_flush_hits) == len(flush_function_hits)

    # Catch cases where mismatching results are returned
    assert flush_function_hits == no_flush_hits
