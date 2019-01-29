#!/usr/bin/env python
"""Tests the Elasticsearch Globus log connector

Not much that can be done since most functionality is dictated by the behavior
of Elasticsearch.
"""

import gzip
import json
import copy
import datetime

import tokio.connectors.es
import tokio.connectors.nersc_globuslogs

import tokiotest
try:
    import elasticsearch.exceptions
    _HAVE_ELASTICSEARCH = True
except ImportError:
    _HAVE_ELASTICSEARCH = False

FAKE_PAGES = json.load(gzip.open(tokiotest.SAMPLE_GLOBUSLOGS))

### the ElasticSearch python library can be VERY noisy on failures
# import logging
# logging.getLogger('elasticsearch').setLevel(logging.WARNING)

def test_query_interfaces():
    """connectors.nersc_globuslogs.NerscGlobusLogs.query_*
    """
    global _HAVE_ELASTICSEARCH

    end = datetime.datetime.now() - datetime.timedelta(hours=1)
    start = end - datetime.timedelta(hours=1)

    esdb = None
    remote_results = None
    if _HAVE_ELASTICSEARCH:
        esdb = tokio.connectors.nersc_globuslogs.NerscGlobusLogs(
            host=tokiotest.SAMPLE_COLLECTDES_HOST,
            port=tokiotest.SAMPLE_COLLECTDES_PORT,
            index=tokiotest.SAMPLE_GLOBUSLOGS_INDEX)
        try:
            esdb.query(start, end)
        except elasticsearch.exceptions.ConnectionError:
            esdb = None

        if esdb and esdb.scroll_pages:
            remote_results = esdb.scroll_pages[0]

    if not esdb or not remote_results:
        tokio.connectors.es.LOCAL_MODE = True
        assert tokio.connectors.es.LOCAL_MODE
        esdb = tokio.connectors.nersc_globuslogs.NerscGlobusLogs(host=None, port=None, index=None)
        esdb.local_mode = True
        print("Testing in local mode")
    else:
        print("Testing in remote mode")

    if tokio.connectors.es.LOCAL_MODE:
        esdb.fake_pages = copy.deepcopy(FAKE_PAGES)
        esdb.query_user(start, end, tokiotest.SAMPLE_GLOBUSLOGS_USERS[0])
        assert esdb.scroll_pages
    else: # remote_results is defined
        target_user = remote_results[0]['_source']['USER']
        print("Searching for user %s" % target_user)
        esdb.query_user(start, end, target_user)
        assert esdb.scroll_pages
        print("Got %d pages" % len(esdb.scroll_pages))
        num_recs = sum([len(page) for page in esdb.scroll_pages])
        print("Got %d records" % num_recs)

        valid_records = 0
        for page in esdb.scroll_pages:
            for rec in page:
                assert rec['_source']['USER'] == target_user
                valid_records += 1
        print("Validated %d records" % valid_records)

    esdb.scroll_pages = []

    if tokio.connectors.es.LOCAL_MODE:
        esdb.fake_pages = copy.deepcopy(FAKE_PAGES)
        esdb.query_type(start, end, tokiotest.SAMPLE_GLOBUSLOGS_TYPES[0])
        assert esdb.scroll_pages
    else:
        target_type = remote_results[0]['_source']['TYPE']
        print("Searching for type %s" % target_type)
        esdb.query_type(start, end, target_type)
        assert esdb.scroll_pages
        print("Got %d pages" % len(esdb.scroll_pages))
        num_recs = sum([len(page) for page in esdb.scroll_pages])
        print("Got %d records" % num_recs)

        valid_records = 0
        for page in esdb.scroll_pages:
            for rec in page:
                assert rec['_source']['TYPE'] == target_type
                valid_records += 1
        print("Validated %d records" % valid_records)


    esdb.scroll_pages = []
