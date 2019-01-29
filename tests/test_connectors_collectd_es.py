#!/usr/bin/env python
"""
Test the ElasticSearch collectd connector.  Not much that can be done since most
functionality is dictated by the behavior of Elasticsearch.
"""

import datetime
import copy
import nose
import tokio.connectors.es
import tokio.connectors.collectd_es
import tokiotest
try:
    import elasticsearch.exceptions
    _HAVE_ELASTICSEARCH = True
except ImportError:
    _HAVE_ELASTICSEARCH = False

FAKE_PAGES = [
    {
        '_scroll_id': 1,
        'hits': {
            'hits': [{'payload': 'results'}],
        },
    },
    {
        '_scroll_id': 1,
        'hits': {
            'hits': [],
        },
    },
]

### the ElasticSearch python library can be VERY noisy on failures
# import logging
# logging.getLogger('elasticsearch').setLevel(logging.WARNING)

def test_query_interfaces():
    """test_connectors_collectd_es.query_*
    """
    global _HAVE_ELASTICSEARCH

    end = datetime.datetime.now() - datetime.timedelta(hours=1)
    start = end - datetime.timedelta(seconds=10)

    esdb = None
    if _HAVE_ELASTICSEARCH:
        esdb = tokio.connectors.collectd_es.CollectdEs(
            host=tokiotest.SAMPLE_COLLECTDES_HOST,
            port=tokiotest.SAMPLE_COLLECTDES_PORT,
            index=tokiotest.SAMPLE_COLLECTDES_INDEX)
        try:
            esdb.query({"query": {"match": {"message": {"query": "test connection", "operator": "and"}}}})
        except elasticsearch.exceptions.ConnectionError:
            esdb = None

    if not esdb:
        tokio.connectors.es.LOCAL_MODE = True
        assert tokio.connectors.es.LOCAL_MODE
        esdb = tokio.connectors.collectd_es.CollectdEs(host=None, port=None, index=None)
        esdb.local_mode = True
        print("Testing in local mode")
    else:
        print("Testing in remote mode")

    if tokio.connectors.es.LOCAL_MODE:
        esdb.fake_pages = copy.deepcopy(FAKE_PAGES)
    esdb.query_disk(start, end)
    assert esdb.scroll_pages
    esdb.scroll_pages = []

    if tokio.connectors.es.LOCAL_MODE:
        esdb.fake_pages = copy.deepcopy(FAKE_PAGES)
    esdb.query_memory(start, end)
    assert esdb.scroll_pages
    esdb.scroll_pages = []

    if tokio.connectors.es.LOCAL_MODE:
        esdb.fake_pages = copy.deepcopy(FAKE_PAGES)
    esdb.query_cpu(start, end)
    assert esdb.scroll_pages
    esdb.scroll_pages = []
