#!/usr/bin/env python
"""
Tool to calculate the total bytes written according to two ISDCT dumps,
then compare this value to the total bytes written according to
collectd/ElasticSearch.  Used to validate collectd data.

Specify a start and end ISDCT dumpfile as command-line arguments.
"""

import os
import sys
import json
import datetime
import warnings
import pandas
import tokio.connectors.collectd_es
import tokio.connectors.nersc_isdct

COLLECTD_TIMESTEP = 10 ### ten second polling interval

ES_HOST = 'localhost'
ES_PORT = 9200
ES_INDEX = 'cori-collectd-*'

AGG_READS = "1"
AGG_WRITES = "2"
AGG_HOST = "3"
AGG_INSTANCE = "4"

QUERY_SUM_PER_NVME_NID = {
    "size": 0,
    "query": {
        "bool": {
            "must": {
                "query_string": {
                    "query": "hostname:bb* AND collectd_type:disk_octets AND plugin_instance:nvme*",
                    "analyze_wildcard": True,
                },
            },
            "filter": {
                "range": {
                    "@timestamp": {},
                },
            },
        },
    },
    "aggs": {
        AGG_HOST: {
            "terms": {
                "field": "host",
                "size": 500,
                "order": {
                    "_term": "desc"
                }
            },
            "aggs": {
                AGG_INSTANCE: {
                    "terms": {
                        "field": "plugin_instance",
                        "size": 5,
                        "order": {
                            "_term": "desc"
                        }
                    },
                    "aggs": {
                        AGG_READS: {
                            "sum": {
                                "field": "read"
                            }
                        },
                        AGG_WRITES: {
                            "sum": {
                                "field": "write"
                            },
                        },
                    },
                },
            },
        },
    },
}

def to_epoch(dt):
    """
    could do strftime("%s") but that is not actually explicitly supported by
    Python standard and may not work on non-Linux systems
    """
    epoch_stamp = long((dt - datetime.datetime(1970, 1, 1)).total_seconds())
    return epoch_stamp


def query_sum_rw(t_start, t_stop):
    ### Set the timestamp of the query
    QUERY_SUM_PER_NVME_NID['query']['bool']['filter']['range']['@timestamp'] = {
        'gte': to_epoch(t_start),
        'lt': to_epoch(t_stop),
        'format': "epoch_second",
    }
    ### Create connection to ElasticSearch
    es_obj = tokio.connectors.collectd_es.CollectdEs(
        host=ES_HOST,
        port=ES_PORT,
        index=ES_INDEX)

    ### Issue the query
    es_obj.query(QUERY_SUM_PER_NVME_NID)

    return es_obj.page

if __name__ == "__main__":
    ### Decode the first and second ISDCT dumps
    isdct0 = tokio.connectors.nersc_isdct.NerscIsdct(sys.argv[1])
    isdct1 = tokio.connectors.nersc_isdct.NerscIsdct(sys.argv[2])

    ### Determine the time each dump was taken so that we can bound our
    ### ElasticSearch query with it
    t_start = datetime.datetime.fromtimestamp(isdct0.itervalues().next()['timestamp'])
    t_stop = datetime.datetime.fromtimestamp(isdct1.itervalues().next()['timestamp'])

    if t_start >= t_stop:
        raise Exception('t_start >= t_stop')

    ### Calculate the increase in host bytes written from ISDCT
    deltas = {}
    for serial_no, counters in isdct1.iteritems():
        for key in ['smart_host_bytes_written_bytes']:
            new_key = "%s:%s" % (counters['NodeName'], counters['device_path'].split(os.sep)[-1])
            deltas[new_key] = {
                key: isdct1[serial_no][key] - isdct0[serial_no][key],
                'serial': serial_no,
            }

    ### Calculate the total bytes written over the whole day according to
    ### ElasticSearch
    es_data = query_sum_rw(t_start, t_stop)
    for host_bucket in es_data['aggregations'][AGG_HOST]['buckets']:
        for instance_bucket in host_bucket[AGG_INSTANCE]['buckets']:
            read_bytes = long(instance_bucket[AGG_READS]['value']) * COLLECTD_TIMESTEP
            write_bytes = long(instance_bucket[AGG_WRITES]['value']) * COLLECTD_TIMESTEP
            key = "%s:%s" % (host_bucket['key'], instance_bucket['key'])

            if key not in deltas:
                ### This will happen if a BB server is unreachable during the
                ### ISDCT dump creation but is still sending collectd data out
                warnings.warn("Could not find key %s" % key)
            else:
                deltas[key]['es_write_bytes'] = write_bytes
                deltas[key]['es_read_bytes'] = read_bytes

    ### Convert dict to DataFrame and calculate intermediate values
    df = pandas.DataFrame.from_dict(deltas, orient='index')
    df['es_isdct_ratio'] = df['es_write_bytes'] / df['smart_host_bytes_written_bytes']
    df.index.name = "node:device"

    print df.to_csv()
