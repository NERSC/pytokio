#!/usr/bin/env python
"""
Test the ElasticSearch collectd connector.  Some of these tests will essentially
pass only at NERSC because of the assumptions built into the indices.
"""

import copy
import datetime
import tokio.connectors.es

FLUSH_STATE = {'pages': []}
PAGE_SIZE = 100
NUM_PAGES = 10

FAKE_TIMESERIES_QUERIES = [
    {
        '@timestamp': {},
    },
    {
        "blah": {
            'hello': 'blah',
            'world': 'blah',
            '@timestamp': {},
        },
    },
    {
        "blah": [
            {
                'hello': 'world',
                '1': 2,
            },
            {
                '@timestamp': {},
            },
        ],
    },
]

def make_fake_pages(num_pages=NUM_PAGES, page_size=PAGE_SIZE):
    """Create a set of fake pages
    """
    fake_pages = []
    scroll_id = 0
    for scroll_id in range(num_pages):
        fake_page = {
            '_scroll_id': str(scroll_id),
            'hits': {
                'hits': []
            }
        }
        for hit_id in range(page_size):
            fake_page['hits']['hits'].append({
                '_id': str(scroll_id * page_size + hit_id),
                'payload': str(scroll_id * hit_id),
            })
        fake_pages.append(fake_page)

    # append a terminal empty page (this is what elasticsearch does to signal
    # the end of a scrolling query)
    fake_pages.append({'_scroll_id': str(scroll_id), 'hits': {'hits': []}})
    return fake_pages

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
    """connectors.es flush function correctness
    """
    es_obj = tokio.connectors.es.EsConnection(host=None, port=None)
    es_obj.local_mode = True
    es_obj.fake_pages = make_fake_pages()

    ############################################################################
    # Accumulate results using a flush_function
    ############################################################################
    es_obj.query_and_scroll(
        query="",
        flush_every=PAGE_SIZE,
        flush_function=flush_function
    )

    # Run flush function on the last page
    if len(es_obj.scroll_pages) > 0:
        flush_function(es_obj)

    # If no pages were found, there is a problem
    assert len(FLUSH_STATE['pages']) > 0

    assert len(FLUSH_STATE['pages']) == NUM_PAGES

    ############################################################################
    # Accumulate results on the object without a flush function
    ############################################################################
    es_obj.fake_pages = make_fake_pages()
    es_obj.query_and_scroll("")

    ############################################################################
    # Ensure that both return identical hits
    ############################################################################
    flush_function_hits = set([])
    for page in FLUSH_STATE['pages']:
        flush_function_hits |= set([x['_id'] for x in page['hits']['hits']])
    print("flush_function populated %d documents" % len(flush_function_hits))

    no_flush_hits = set([])
    for page in es_obj.scroll_pages:
        no_flush_hits |= set([x['_id'] for x in page['hits']['hits']])
    print("no flush function populated %d documents" % len(no_flush_hits))

    # Ensure that our ground-truth query actually returned something
    assert len(no_flush_hits) > 0

    # Catch obviously wrong number of returned results
    assert len(no_flush_hits) == len(flush_function_hits)

    # Catch cases where mismatching results are returned
    assert flush_function_hits == no_flush_hits

def test_build_timeseries_query():
    """connectors.es.build_timeseries_query()
    """
    end_time = datetime.datetime.now()
    start_time = end_time - datetime.timedelta(hours=1)

    for query in FAKE_TIMESERIES_QUERIES:
        # doesn't matter _what_ the datetime is
        ret = tokio.connectors.es.build_timeseries_query(query, start_time, end_time)
        print(ret)
        assert ret

        # now use the time range version of the call
        ret_ref = copy.deepcopy(ret)
        ret = tokio.connectors.es.build_timeseries_query(
            query,
            start_time,
            end_time,
            start_key='@timestamp',
            end_key='@timestamp')
        print(ret)
        assert ret == ret_ref

        # and make sure that the test inputs weren't throwing false positives
        ret = tokio.connectors.es.build_timeseries_query(
            query,
            start_time,
            end_time,
            start_key='@timestamp',
            end_key='INVALID KEY')
        assert ret != ret_ref
