#!/usr/bin/env python
"""Retrieve Globus transfer logs from NERSC's Elasticsearch infrastructure

Connects to NERSC's OMNI service and retrieves Globus transfer logs.
"""

from . import es

QUERY = {
    "query": {
        "constant_score": {
            "filter": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {}
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

    def query(self, start, end):
        """Queries Elasticsearch for Globus logs

        Args:
            start (datetime.datetime): lower bound for query (inclusive)
            end (datetime.datetime): upper bound for query (exclusive)
        """
        self._query_timeseries(QUERY, start, end)

    def _query_timeseries(self, query_template, start, end):
        """Map connection-wide attributes to self.query_timeseries arguments

        Args:
            query_template (dict): a query object containing at least one
                ``@timestamp`` field
            start (datetime.datetime): lower bound for query (inclusive)
            end (datetime.datetime): upper bound for query (exclusive)
        """
        return self.query_timeseries(query_template=query_template,
                                     start=start,
                                     end=end,
                                     source_filter=SOURCE_FILTER,
                                     filter_function=self.filter_function,
                                     flush_every=self.flush_every,
                                     flush_function=self.flush_function)
