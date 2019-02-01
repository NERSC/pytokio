#!/usr/bin/env python
"""Retrieve Globus transfer logs from NERSC's Elasticsearch infrastructure

Connects to NERSC's OMNI service and retrieves Globus transfer logs.
"""

import copy
from . import es

QUERY = {
    "query": {
        "constant_score": {
            "filter": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "start_date": {}
                            }
                        },
                        {
                            "range": {
                                "end_date": {}
                            }
                        }
                    ]
                }
            }
        }
    }
}

### Only return the following _source fields
SOURCE_FILTER = [
    '@timestamp',
    'BLOCK',
    'BUFFER',
    'CODE',
    'DATE',
    'DEST',
    'DESTIP',
    'FILE',
    'HOST',
    'NBYTES',
    'START',
    'STREAMS',
    'STRIPES',
    'TASKID',
    'TYPE',
    'USER',
    'VOLUME',
    'bandwidth_mbps',
    'duration',
    'start_date',
    'end_date',
    'host',
]

class NerscGlobusLogs(es.EsConnection):
    """Connection handler for NERSC Globus transfer logs
    """
    def __init__(self, *args, **kwargs):
        super(NerscGlobusLogs, self).__init__(*args, **kwargs)
        self.filter_function = lambda x: x['hits']['hits']
        self.flush_every = 50000
        self.flush_function = lambda x: x

    @classmethod
    def from_cache(cls, *args, **kwargs):
        instance = super(NerscGlobusLogs, cls).from_cache(*args, **kwargs)
        instance.filter_function = lambda x: x['hits']['hits']
        instance.flush_every = 50000
        instance.flush_function = lambda x: x
        return instance

    def query(self, start, end, must=None):
        """Queries Elasticsearch for Globus logs

        Accepts a start time, end time, and an optional "must" field which can
        be used to apply additional term queries.  For example, ``must`` may be

            [
                {
                    "term": {
                        "TASKID": "none"
                    }
                },
                {
                    "term: {
                        "TYPE": "STOR"
                    }
                }
            ]

        which would return only those queries that have no associated TASKID and
        were sending (storing) data.

        Args:
            start (datetime.datetime): lower bound for query (inclusive)
            end (datetime.datetime): upper bound for query (exclusive)
            must (list or None): list of dictionaries to be inserted as
                additional term-level query parameters.
        """
        if must:
            query = copy.deepcopy(QUERY)
            query['query']['constant_score']['filter']['bool']['must'] += must
        else:
            # no need to deepcopy since query_timeseries() will do that
            query = QUERY

        self.query_timeseries(query, start, end)

    def query_user(self, start, end, user):
        """Wraps query() with a user restriction

        Convenience method to constrain a query to a specific user.

        Args:
            start (datetime.datetime): lower bound for query (inclusive)
            end (datetime.datetime): upper bound for query (exclusive)
            user (str): constrain results to this username
        """
        self.query(start=start, end=end, must=[{"term": {"USER": user}}])

    def query_type(self, start, end, xfer_type):
        """Wraps query() with a type restriction

        Convenience method to constrain a query to a specific transfer type.

        Args:
            start (datetime.datetime): lower bound for query (inclusive)
            end (datetime.datetime): upper bound for query (exclusive)
            xfer_type (str): constrain results to this transfer type (STOR,
                RETR, etc).  Case sensitive.
        """
        self.query(start=start, end=end, must=[{"term": {"TYPE": xfer_type}}])

    def query_timeseries(self, query_template, start, end):
        """Craft and issue query that returns all overlapping records

        Args:
            query_template (dict): a query object containing at least one
                ``@timestamp`` field
            start (datetime.datetime): lower bound for query (inclusive)
            end (datetime.datetime): upper bound for query (exclusive)
            source_filter (bool or list): Return all fields contained in each
                document's _source field if True; otherwise, only return source
                fields contained in the provided list of str.
            filter_function (function, optional): Function to call before each
                set of results is appended to the ``scroll_pages`` attribute; if
                specified, return value of this function is what is appended.
            flush_every (int or None): trigger the flush function once the
                number of docs contained across all ``scroll_pages`` reaches
                this value.  If None, do not apply `flush_function`.
            flush_function (function, optional): function to call when
                `flush_every` docs are retrieved.
        """
        query = es.build_timeseries_query(
            query_template,
            start,
            end,
            start_key='start_date',
            end_key='end_date')

        self.query_and_scroll(
            query=query,
            source_filter=SOURCE_FILTER,
            filter_function=self.filter_function,
            flush_every=self.flush_every,
            flush_function=self.flush_function)

    def to_dataframe(self):
        """Converts self.scroll_pages to a DataFrame

        Returns:
            pandas.DataFrame: Contents of the last query's pages
        """
        return super(NerscGlobusLogs, self).to_dataframe(fields=SOURCE_FILTER)
