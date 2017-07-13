#!/usr/bin/env python
"""
Extract data info about the Burst Buffer from Elastic Search.
Working only on Cori 

"""
import json
import datetime
from gzip import open
from elasticsearch import Elasticsearch
import datetime

_DATE_FMT = "%Y-%m-%d %H:%M:%S"
MAX_DOC_BUNDLE = 50000
DEFAULT_PAGE_SIZE = 10000
_DATE_FMT = "%Y-%m-%d %H:%M:%S"
_ES_INDEX = "cori-collectd-*"
_QUERY_OST_DATA = {
    "query": {
        "query_string": {
            "query": "hostname:bb* AND (plugin:memory OR plugin:disk OR plugin:cpu OR plugin:interface)",
            "analyze_wildcard": True,
                },
    },
}

_SOURCE_FIELDS = ['@timestamp', 'hostname', 'plugin','collectd_type',
           'type_instance','plugin_instance','value','longterm',
           'midterm','shortterm','majflt','minflt','if_octets',
           'if_packets','if_errors','rx','tx','read','write',
           'io_time']


class NerscCollectd(object):
    def __init__(self, host='localhost', port='9200', output_file=None):
        self.client = None
        self.page = None
        self.num_bundle = 0
        self.output_file = output_file
        self.last_index = None
        self.connect()

    def connect(self, host='localhost', port='9200', timeout=30):
        self.client = Elasticsearch(host=host, 
                                    timeout=timeout,
                                    port=port
        )

    def close(self):
        if self.client:
            self.client.close()
            self.client = None
            self.num_bundle = 0
               
    def save(self, bundle, output_file=None):
        self._save_bundle(bundle, output_file)

    #===========================================#

    def _save_bundle(self, bundle, output_file):
        """
        Save our bundled pages into gzipped json
        """
        if output_file is None: 
                output_file = "%s.%08d.json.gz" % (self.last_index, self.num_bundle)
        with open(filename=output_file, mode='w', compresslevel=1) as fp:
            json.dump(bundle, fp)
        self.num_bundle += 1
    
    def query(self, t_start, t_stop, index=_ES_INDEX, scroll='1m', query = _QUERY_OST_DATA,
              sort='@timestamp', _source=_SOURCE_FIELDS, size=DEFAULT_PAGE_SIZE):
        t_start = datetime.datetime.strptime(t_start, _DATE_FMT)
        t_stop = datetime.datetime.strptime(t_stop, _DATE_FMT)
        if index == _ES_INDEX:
            self.last_index = index.replace('-*', '')
        else:
            self.last_index = index
        t0 = datetime.datetime.now()
        self.page = self.client.search(index=index,body=query,
                                       scroll=scroll,size=size,
                                       sort=sort,_source=_source)
        print("Fetching page took %.2f seconds" % 
              (datetime.datetime.now() - t0).total_seconds())
        return self.page

    def scroll(self, scrollid=None, scroll='1m'):
        if self.page:
            if scrollid is None:
                sid = self.page['_scroll_id']
            else:
                sid = scrollid
            t0 = datetime.datetime.now()
            self.page = self.client.scroll(scroll_id=sid, scroll=scroll)
            print("Scrolling took %.2f seconds" 
                  % (datetime.datetime.now() - t0).total_seconds())
            print("I scrolled %s page" % scroll)
            return self.page
            
    # def scroll_all(self, scrollid=None, scroll='1m', save=False, output_file=None, bundled=False):
    #     bundle = []
    #     while(self.page):
    #         self.page = self.scroll(scrollid, scroll=scroll)
    #         if save:
    #             if not bundled:
    #                 save(self.page, output_file)
    #             else:
    #                 if (len(bundle) + self.page  > MAX_DOC_BUNDLE):
    #                     save(bundle, output_file)
    #                     bundle = self.page
    #                 else:
    #                     bundle += self.page
    #     if save and bundled:
    #         save(bundle, output_file)
    #     print("I scrolled all the data") 


class NerscBb(NerscCollectd):
    def __init__(self):
        super(NerscBb, self).__init__(self)
    
    
