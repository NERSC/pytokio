#!/usr/bin/env python
"""Retrieve data stored in Elasticsearch

This module provides a wrapper around the Elasticsearch connection handler and
methods to query, scroll, and process pages of scrolling data.
"""

import copy
import time
import json
import mimetypes
import gzip
import warnings
import pandas
from .. import debug
try:
    import elasticsearch
    HAVE_ES_PKG = False
except ImportError:
    HAVE_ES_PKG = True

BASE_QUERY = {
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

class EsConnection(object):
    """Elasticsearch connection handler.

    Wrapper around an Elasticsearch connection context that provides simpler
    scrolling functionality for very long documents and callback functions to be
    run after each page is retrieved.
    """
    def __init__(self, host, port, index=None, scroll_size='1m', page_size=10000, timeout=30):
        """Configure and connect to an Elasticsearch endpoint.

        Args:
            host (str): hostname for the Elasticsearch REST endpoint
            port (int): port where Elasticsearch REST endpoint is bound
            index (str): name of index against which queries will be issued
            scroll_size (str): how long to keep the scroll search context open
                (e.g., "1m" for 1 minute)
            page_size (int): how many documents should be returned per scrolled
                page (e.g., 10000 for 10k docs per scroll)
            timeout (int): how many seconds to wait for a response from
                Elasticsearch before the query should time out

        Attributes:
            client: Elasticsearch connection handler
            page (dict): last page retrieved by a query
            scroll_pages (list): dictionary of pages retrieved by query
            index (str): name of index against which queries will be issued
            connect_host (str): hostname for Elasticsearch REST endpoint
            connect_port (int): port where Elasticsearch REST endpoint is bound
            connect_timeout (int): seconds before query should time out
            page_size (int): max number of documents returned per page
            scroll_size (int): duration to keep scroll search context open
            scroll_id: identifier for the scroll search context currently in use
            sort_by (str): field by which Elasticsearch should sort results
                before returning them as query results
            fake_pages (list): A list of ``page`` structures that should be
                returned by self.scroll() when the elasticsearch module is not
                actually available.  Used only for debugging.
            local_mode (bool): If True, retrieve query results from
                self.fake_pages instead of attempting to contact an
                Elasticsearch server
        """
        # retain state of Elasticsearch client
        self.client = None
        self.page = None
        self.scroll_pages = []
        self.index = index
        # connection info
        self.connect_host = host
        self.connect_port = port
        self.connect_timeout = timeout
        # for the scroll API
        self.page_size = page_size
        self.scroll_size = scroll_size
        self.scroll_id = None
        # for query_and_scroll
        self._num_flushes = 0
        self._filter_function = None
        self._flush_every = None
        self._flush_function = None
        self._total_hits = 0
        self._hits_since_flush = 0
        # hidden parameters to refine how Elasticsearch queries are issued
        self.sort_by = ''
        # for debugging
        self.fake_pages = []
        # if elasticsearch package is not available, we MUST run in local mode.
        # But this can also be changed at runtime if, e.g., elasticsearch _is_
        # available, but a connection cannot be made to the Elasticsearch
        # service.
        self.local_mode = HAVE_ES_PKG

        if host and port:
            self.connect()

    @classmethod
    def from_cache(cls, cache_file):
        """Initializes an EsConnection object from a cache file.

        This path is designed to be used for testing.

        Args:
            cache_file (str): Path to the JSON formatted list of pages
        """
        _, encoding = mimetypes.guess_type(cache_file)
        if encoding == 'gzip':
            input_fp = gzip.open(cache_file, 'rt')
        else:
            input_fp = open(cache_file, 'r')

        # convert cached hits into something resembling a real Elasticsearch response
        pages = []
        for hits in json.load(input_fp):
            pages.append({
                '_scroll_id': '0',
                'hits': {
                    'hits': hits
                }
            })

        # never forget to terminate fake pages with an empty page
        pages.append({'_scroll_id': 0, 'hits': {'hits': []}})
        input_fp.close()

        instance = cls(host=None, port=None, index=None)
        instance.local_mode = True
        instance.fake_pages = pages
        return instance

    def save_cache(self, output_file=None):
        """Persist the response of the last query to a file

        This is a little different from other connectors' save_cache() methods
        in that it only saves down the state of the last query's results.  It
        does not save any connection information and does not restore the state
        of a previous EsConnection object.

        Its principal intention is to be used with testing.

        Args:
            output_file (str or None): Path to file to which json should be
                written.  If None, write to stdout.  Default is None.
        """
        # write out pages to a file
        if output_file is None:
            print(json.dumps(self.scroll_pages, indent=4))
        else:
            _, encoding = mimetypes.guess_type(output_file)
            output_file = gzip.open(output_file, 'w') if encoding == 'gzip' else open(output_file, 'w')
            json.dump(self.scroll_pages, output_file)
            output_file.close()

    def _process_page(self):
        """Remove a page from the incoming queue and append it

        Takes the last received page (self.page), updates the internal state of
        the scroll operation, updates some internal counters, calls the flush
        function if applicable, and applies the filter function.  Then appends
        the results to self.scroll_pages.

        Returns:
            bool: True if hits were appended or not
        """
        if not self.page['hits']['hits']:
            return False

        self.scroll_id = self.page.get('_scroll_id')
        num_hits = len(self.page['hits']['hits'])
        self._total_hits += num_hits

        # if this page will push us over flush_every, flush it first
        if self._flush_function is not None \
        and self._flush_every \
        and (self._hits_since_flush + num_hits) > self._flush_every:
            self._flush_function(self)
            self._hits_since_flush = 0
            self._num_flushes += 1

        # increment hits since flush only after we've (possibly) flushed
        self._hits_since_flush += num_hits

        # if a filter function exists, use its output as the page to append
        if self._filter_function is None:
            filtered_page = self.page
        else:
            filtered_page = self._filter_function(self.page)

        # finally append the page
        self.scroll_pages.append(filtered_page)
        return True


    def _pop_fake_page(self):
        if not self.fake_pages:
            warn_str = "fake_pages is empty on a query/scroll; this means either"
            warn_str += "\n\n"
            warn_str += "1. You forgot to set self.fake_pages before issuing the query, or\n"
            warn_str += "2. You forgot to terminate self.fake_pages with an empty page"
            warnings.warn(warn_str)
        self.page = self.fake_pages.pop(0)

    def connect(self):
        """Instantiate a connection and retain the connection context.
        """
        if self.local_mode:
            warnings.warn("operating in local mode; elasticsearch queries will not be issued")
        else:
            self.client = elasticsearch.Elasticsearch(host=self.connect_host,
                                                      port=self.connect_port,
                                                      timeout=self.connect_timeout)
    def close(self):
        """Close and invalidate the connection context.
        """
        if self.client:
            self.client = None

    def query(self, query):
        """Issue an Elasticsearch query.

        Issues a query and returns the resulting page.  If the query included a
        scrolling request, the `scroll_id` attribute is set so that scrolling
        can continue.

        Args:
            query (dict): Dictionary representing the query to issue

        Returns:
            dict: The page resulting from the issued query.
        """
        if self.local_mode:
            self._pop_fake_page()
        else:
            if not self.client:
                # allow lazy connect
                self.connect()
            self.page = self.client.search(body=query, index=self.index, sort=self.sort_by)

        self.scroll_id = self.page.get('_scroll_id')

        return self.page

    def scroll(self):
        """Request the next page of results.

        Requests the next page of results for a query that is scrolling.
        This can only be performed if the `scroll_id` attribute is set (e.g.,
        by the ``query()`` method).

        Returns:
            dict: The next page in the scrolling context.
        """
        if self.scroll_id is None:
            raise Exception('no scroll id')

        if self.local_mode:
            self._pop_fake_page()
        else:
            if not self.client:
                # allow lazy connect
                self.connect()
            self.page = self.client.scroll(scroll_id=self.scroll_id, scroll=self.scroll_size)

        return self.page

    def query_and_scroll(self, query, source_filter=True, filter_function=None,
                         flush_every=None, flush_function=None):
        """Issue a query and retain all results.

        Issues a query and scrolls through every resulting page, optionally
        applying in situ logic for filtering and flushing.  All resulting pages
        are appended to the ``scroll_pages`` attribute of this object.

        The ``scroll_pages`` attribute must be wiped by whatever is consuming it;
        if this does not happen, `query_and_scroll()` will continue appending
        results to the results of previous queries.

        Args:
            query (dict): Dictionary representing the query to issue
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
        ### Print query
        debug.debug_print(json.dumps(query, indent=4))

        ### Run query
        time0 = time.time()

        # initialize the scroll state
        self.scroll_pages = []
        self._filter_function = filter_function
        self._flush_every = flush_every
        self._flush_function = flush_function
        self._total_hits = 0
        self._hits_since_flush = 0

        # Get first set of results and a scroll id
        if self.local_mode:
            self._pop_fake_page()
        else:
            if not self.client:
                # allow lazy connect
                self.connect()
            self.page = self.client.search(
                index=self.index,
                body=query,
                scroll=self.scroll_size,
                size=self.page_size,
                _source=source_filter,
            )

        more = self._process_page()

        # Get remaining pages
        while more:
            self.page = self.scroll()
            more = self._process_page()
        debug.debug_print("Elasticsearch query took %s seconds" % (time.time() - time0))

    def query_timeseries(self, query_template, start, end, source_filter=True,
                         filter_function=None, flush_every=None,
                         flush_function=None):
        """Craft and issue query bounded by time

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
        query = build_timeseries_query(query_template, start, end)

        self.query_and_scroll(
            query=query,
            source_filter=source_filter,
            filter_function=filter_function,
            flush_every=flush_every,
            flush_function=flush_function)

    def to_dataframe(self, fields):
        """Converts self.scroll_pages to CSV

        Returns:
            str: Contents of the last query's pages in CSV format
        """
        to_df = []
        for page in self.scroll_pages:
            for record in page:
                record_dict = {}
                for field in fields:
                    record_dict[field] = record['_source'].get(field)
                to_df.append(record_dict)
        return pandas.DataFrame(to_df)

def build_timeseries_query(orig_query, start, end, start_key='@timestamp', end_key=None):
    """Create a query object with time ranges bounded.

    Given a query dict and a start/end datetime object, return a new query
    object with the correct time ranges bounded.  Relies on `orig_query`
    containing at least one ``@timestamp`` field to indicate where the time
    ranges should be inserted.

    If orig_query is querying records that contain both a "start" and "end"
    time (e.g., a job) rather than a discrete point in time (e.g., a sampled
    metric), ``start_key`` and ``end_key`` can be used to modify the query to
    return all records that overlapped with the interval specified by
    ``start_time`` and ``end_time``.

    Args:
        orig_query (dict): A query object containing at least one ``@timestamp``
            field.
        start (datetime.datetime): lower bound for query (inclusive)
        end (datetime.datetime): upper bound for query (exclusive)
        start_key (str): The key containing a timestamp against which a time
            range query should be applied.
        end_key (str): The key containing a timestamp against which the upper
            bound of the time range should be applied.  If None, treat
            ``start_key`` as a single point in time rather than the start of
            a recorded process.

    Returns:
        dict: A query object with all instances of ``@timestamp`` bounded by
        `start` and `end`.
    """
    def map_item(obj, target_key, map_function, **kwargs):
        """
        Recursively walk a hierarchy of dicts and lists, searching for a
        matching key.  For each match found, apply map_function to that key's
        value.
        """
        if isinstance(obj, list):
            iterator = enumerate
            if target_key in obj:
                map_function(obj[target_key], **kwargs)
                return
        elif isinstance(obj, dict):
            iterator = dict.items
            if target_key in obj:
                map_function(obj[target_key], **kwargs)
                return
        else:
            # hit a dead end without a match
            return

        # if this isn't a dead end, search down each iterable element
        for _, value in iterator(obj):
            if isinstance(value, (list, dict)):
                # dive down any discovered rabbit holes
                map_item(value, target_key, map_function, **kwargs)
        return

    def set_time_range(time_range_obj, start_time, end_time, time_format="epoch_second"):
        """Set the upper and lower bounds of a time range
        """
        time_range_obj['gte'] = int(time.mktime(start_time.timetuple()))
        time_range_obj['lt'] = int(time.mktime(end_time.timetuple()))
        time_range_obj['format'] = time_format
        remaps[0] += 1

    def set_time(time_range_obj, operator, time_val, time_format="epoch_second"):
        """Set a single time filter
        """
        time_range_obj[operator] = int(time.mktime(time_val.timetuple()))
        time_range_obj['format'] = time_format
        remaps[0] += 1

    # note we use a single-element list so that remaps becomes mutable
    remaps = [0]

    query = copy.deepcopy(orig_query)

    if end_key is None:
        map_item(query, start_key, set_time_range, start_time=start, end_time=end)
    else:
        map_item(query, start_key, set_time, operator='lt', time_val=end)
        map_item(query, end_key, set_time, operator='gte', time_val=start)

    if not remaps[0]:
        raise RuntimeError("unable to locate timestamp in query")

    return query

def mutate_query(mutable_query, field, value, term="term"):
    """Inserts a new condition into a query object

    See https://www.elastic.co/guide/en/elasticsearch/reference/current/term-level-queries.html
    for complete documentation.

    Args:
        mutable_query (dict): a query object to be modified
        field (str): the field to which a term query will be applied
        value: the value to match for the term query
        term (str): one of the following: term, terms, terms_set, range, exists,
            prefix, wildcard, regexp, fuzzy, type, ids.

    Returns:
        Nothing.  ``mutable_query`` is updated in place.
    """
    mutable_query['query']['constant_score']['filter']['bool']['must'].append({
        term: {field: value}
    })
