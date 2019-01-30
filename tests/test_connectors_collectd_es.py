#!/usr/bin/env python
"""Test the Elasticsearch collectd connector

Ensures that the connector-specific queries are working when Elasticsearch is
available.  Will require modifying tokiotest.SAMPLE_COLLECTDES_* to work at
non-NERSC sites.
"""

import datetime
import copy

import tokio.connectors.es
import tokio.connectors.collectd_es
import tokiotest
try:
    import elasticsearch.exceptions
    _HAVE_ELASTICSEARCH = True
except ImportError:
    _HAVE_ELASTICSEARCH = False

# elasticsearch can be VERY noisy on failures
# import logging
# logging.getLogger('elasticsearch').setLevel(logging.WARNING)

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
            'hits': []
        }
    }
]

def test_query_interfaces():
    """connectors.collectd_es.CollectdEs.query_*
    """
    global _HAVE_ELASTICSEARCH

    end = datetime.datetime.now() - datetime.timedelta(hours=1)
    # adjust the timedelta to match the data source's data generate rates
    start = end - datetime.timedelta(seconds=10)

    esdb = None
    if _HAVE_ELASTICSEARCH:
        esdb = tokio.connectors.collectd_es.CollectdEs(
            host=tokiotest.SAMPLE_COLLECTDES_HOST,
            port=tokiotest.SAMPLE_COLLECTDES_PORT,
            index=tokiotest.SAMPLE_COLLECTDES_INDEX)
        try:
            esdb.fake_pages = copy.deepcopy(FAKE_PAGES)
            esdb.query(tokio.connectors.collectd_es.BASE_QUERY)
        except elasticsearch.exceptions.ConnectionError:
            esdb = None

    if not esdb:
        esdb = tokio.connectors.collectd_es.CollectdEs(host=None, port=None, index=None)
        esdb.local_mode = True
        print("Testing in local mode")
    else:
        print("Testing in remote mode")

#   validate_es_query_method(esdb=esdb,
#                            method=esdb.query_disk,
#                            field='plugin',
#                            start=start,
#                            end=end,
#                            target_val='disk')
    test_func = validate_es_query_method
    test_func.description = "connectors.collectd_es.CollectdEs.query_disk()"
    yield test_func, esdb, esdb.query_disk, 'plugin', start, end, 'disk'


#   validate_es_query_method(esdb=esdb,
#                            method=esdb.query_cpu,
#                            field='plugin',
#                            start=start,
#                            end=end,
#                            target_val='cpu')
    test_func = validate_es_query_method
    test_func.description = "connectors.collectd_es.CollectdEs.query_cpu()"
    yield test_func, esdb, esdb.query_cpu, 'plugin', start, end, 'cpu'

#   validate_es_query_method(esdb=esdb,
#                            method=esdb.query_memory,
#                            field='plugin',
#                            start=start,
#                            end=end,
#                            target_val='memory')
    test_func = validate_es_query_method
    test_func.description = "connectors.collectd_es.CollectdEs.query_memory()"
    yield test_func, esdb, esdb.query_memory, 'plugin', start, end, 'memory'

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
    if esdb.local_mode:
        esdb.fake_pages = copy.deepcopy(FAKE_PAGES)
#       method(start, end, target_val)
        method(start, end)
        assert esdb.scroll_pages
    else:
        print("Searching for %s" % target_val)
#       method(start, end, target_val)
        method(start, end)
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

def test_to_csv():
    """connectors.collectd_es.CollectdEs.to_csv()
    """
    esdb = tokio.connectors.collectd_es.CollectdEs(host=None, port=None, index=None)
    esdb.local_mode = True
    esdb.fake_pages = copy.deepcopy(FAKE_PAGES)
    esdb.query_disk(datetime.datetime.now(), datetime.datetime.now())
    csv = esdb.to_csv()
    print(csv)
    csv_lines = csv.splitlines()
    assert len(csv_lines) > 1
    assert csv_lines[-1].strip(',')
