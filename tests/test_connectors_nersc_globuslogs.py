#!/usr/bin/env python
"""Tests the Elasticsearch Globus log connector

Ensures that the connector-specific queries are working when Elasticsearch is
available.  Will require modifying tokiotest.SAMPLE_GLOBUSLOGS and
tokiotest.SAMPLE_COLLECTDES_* (For connection info) to work at non-NERSC sites.
"""

import gzip
import json
import copy
import datetime

try:
    from imp import reload
except ImportError:
    # Python 2
    pass

import tokio.connectors.es
reload(tokio.connectors.es) # required because es maintains local-mode state
import tokio.connectors.nersc_globuslogs

import tokiotest
try:
    import elasticsearch.exceptions
    _HAVE_ELASTICSEARCH = True
except ImportError:
    _HAVE_ELASTICSEARCH = False

# elasticsearch can be VERY noisy on failures
# import logging
# logging.getLogger('elasticsearch').setLevel(logging.WARNING)

# Create fake pages and add decorations that real queries return
FAKE_PAGES = []
for fake_page in json.load(gzip.open(tokiotest.SAMPLE_GLOBUSLOGS)):
    FAKE_PAGES.append({
        '_scroll_id': 1,
        'hits': {
            'hits': fake_page
        }
    })
# never forget to terminate fake pages with an empty page
FAKE_PAGES.append({'_scroll_id': 1, 'hits': {'hits': []}})

def test_query_interfaces():
    """connectors.nersc_globuslogs.NerscGlobusLogs.query_*
    """
    global _HAVE_ELASTICSEARCH

    end = datetime.datetime.now() - datetime.timedelta(hours=1)
    # adjust the timedelta to match the data source's data generate rates
    start = end - datetime.timedelta(hours=1)

    esdb = None
    remote_results = [[{}]]
    if _HAVE_ELASTICSEARCH:
        esdb = tokio.connectors.nersc_globuslogs.NerscGlobusLogs(
            host=tokiotest.SAMPLE_COLLECTDES_HOST,
            port=tokiotest.SAMPLE_COLLECTDES_PORT,
            index=tokiotest.SAMPLE_GLOBUSLOGS_INDEX)
        try:
            esdb.fake_pages = copy.deepcopy(FAKE_PAGES)
            esdb.query(start, end)
        except elasticsearch.exceptions.ConnectionError:
            esdb = None

        # we need to save remote_results to discover valid values to query
        # from which we can assert that our connector's specific queries are
        # producing valid response data
        if esdb and esdb.scroll_pages:
            remote_results = esdb.scroll_pages

    if not esdb or not remote_results:
        tokio.connectors.es.LOCAL_MODE = True
        assert tokio.connectors.es.LOCAL_MODE
        esdb = tokio.connectors.nersc_globuslogs.NerscGlobusLogs(host=None, port=None, index=None)
        esdb.local_mode = True
        print("Testing in local mode")
    else:
        print("Testing in remote mode")

#   validate_es_query_method(esdb=esdb,
#                            method=esdb.query_user,
#                            field='USER',
#                            start=start,
#                            end=end,
#                            target_val=remote_results[0][0]['_source'].get('USER', ""))
    test_func = validate_es_query_method
    test_func.description = "connectors.nersc_globuslogs.NerscGlobusLogs.query_user()"
    yield test_func, esdb, esdb.query_user, 'USER', start, end, \
          remote_results[0][0]['_source'].get('USER', "")

#    validate_es_query_method(esdb=esdb,
#                             method=esdb.query_type,
#                             field='TYPE',
#                             start=start,
#                             end=end,
#                             target_val=remote_results[0][0]['_source'].get('TYPE', ""))
    test_func.description = "connectors.nersc_globuslogs.NerscGlobusLogs.query_type()"
    yield test_func, esdb, esdb.query_type, 'TYPE', start, end, \
          remote_results[0][0]['_source'].get('TYPE', "")

def validate_es_query_method(esdb, method, field, start, end, target_val):
    """Tests a connector-specific query method

    Exercises a connector-specific query method in a way that fully validates
    functionality against a remote Elasticsearch service that contains arbitrary
    data (but in the correct format) if available.  If a remote Elasticsearch
    service is not available, still validates that the API works.

    Args:
        esdb: Instance of an object derived from connectors.es.EsConnection
            to test
        method: query method of ``esdb`` that should be called
        field (str): key corresponding to the _source field that ``method`` is
            querying.  Required to verify that ``method`` returns only those
            records that match this field.
        start (datetime.datetime): Passed as ``method``'s ``start`` argument
        end (datetime.datetime): Passed as ``method``'s ``end`` argument
        target_val: a value of ``field`` that will return more than zero results
            when issued to a remote Elasticsearch service.  If operating in
            local mode, this can be any value.
    """
    if tokio.connectors.es.LOCAL_MODE:
        esdb.fake_pages = copy.deepcopy(FAKE_PAGES)
        method(start, end, target_val)
#       method(start, end)
        assert esdb.scroll_pages
    else:
        print("Searching for %s" % target_val)
        method(start, end, target_val)
#       method(start, end)
        assert esdb.scroll_pages
        print("Got %d pages" % len(esdb.scroll_pages))
        num_recs = sum([len(page) for page in esdb.scroll_pages])
        print("Got %d records" % num_recs)

        valid_records = 0
        for page in esdb.scroll_pages:
            for rec in page:
                if rec['_source'][field] != target_val:
                    print("%s != %s" % (rec['_source'][field], target_val))
                    assert rec['_source'][field] == target_val
                valid_records += 1
        print("Validated %d records" % valid_records)

    esdb.scroll_pages = []
