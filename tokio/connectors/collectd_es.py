#!/usr/bin/env python
"""Retrieve data generated by collectd and stored in Elasticsearch
"""

from . import es

QUERY_DISK_DATA = {
    "query": {
        "constant_score": {
            "filter": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {}
                            }
                        },
                        {
                            "prefix": {
                                "hostname": "bb"
                            }
                        },
                        {
                            "prefix": {
                                "plugin_instance": "nvme"
                            }
                        },
                        {
                            "regexp": {
                                "collectd_type": "disk_(octets|ops)"
                            }
                        },
                        {
                            "term": {
                                "plugin": "disk"
                            }
                        }
                    ]
                }
            }
        }
    }
}
QUERY_CPU_DATA = {
    "query": {
        "constant_score": {
            "filter": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {}
                            }
                        },
                        {
                            "prefix": {
                                "hostname": "bb"
                            }
                        },
                        {
                            "term": {
                                "plugin": "cpu"
                            }
                        },
                        {
                            "regexp": {
                                "type_instance": "(idle|user|system)"
                            }
                        },
                    ]
                }
            }
        }
    }
}
QUERY_MEMORY_DATA = {
    "query": {
        "constant_score": {
            "filter": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {}
                            }
                        },
                        {
                            "prefix": {
                                "hostname": "bb"
                            }
                        },
                        {
                            "term": {
                                "plugin": "memory"
                            }
                        },

                    ]
                }
            }
        }
    }
}


### Only return the following _source fields
COLLECTD_SOURCE_FILTER = [
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


class CollectdEs(es.EsConnection):
    """collectd-Elasticsearch connection handler
    """
    def __init__(self, *args, **kwargs):
        super(CollectdEs, self).__init__(*args, **kwargs)
        self.filter_function = lambda x: x['hits']['hits']
        self.flush_every = 50000
        self.flush_function = lambda x: x

    def query_disk(self, start, end):
        """Query Elasticsearch for collectd disk plugin data.

        Args:
            start (datetime.datetime): lower bound for query (inclusive)
            end (datetime.datetime): upper bound for query (exclusive)
        """
        self._query_timeseries(QUERY_DISK_DATA, start, end)

    def query_memory(self, start, end):
        """Query Elasticsearch for collectd memory plugin data.

        Args:
            start (datetime.datetime): lower bound for query (inclusive)
            end (datetime.datetime): upper bound for query (exclusive)
        """
        self._query_timeseries(QUERY_MEMORY_DATA, start, end)

    def query_cpu(self, start, end):
        """Query Elasticsearch for collectd cpu plugin data.

        Args:
            start (datetime.datetime): lower bound for query (inclusive)
            end (datetime.datetime): upper bound for query (exclusive)
        """
        self._query_timeseries(QUERY_CPU_DATA, start, end)

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
                                     source_filter=COLLECTD_SOURCE_FILTER,
                                     filter_function=self.filter_function,
                                     flush_every=self.flush_every,
                                     flush_function=self.flush_function)
