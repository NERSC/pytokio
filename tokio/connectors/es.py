#!/usr/bin/env python
"""Retrieve data stored in Elasticsearch

This module provides a wrapper around the Elasticsearch connection handler and
methods to query, scroll, and process pages of scrolling data.
"""

import copy
import time
import json
import warnings
import tokio
try:
    import elasticsearch
    LOCAL_MODE = False
except ImportError:
    LOCAL_MODE = True

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
        self.local_mode = LOCAL_MODE

        if host and port and not self.local_mode:
            self.connect()

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
            self.page = self.fake_pages.pop(0)
        else:
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
            self.page = self.fake_pages.pop(0)
        else:
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

        # initialize the scroll state
        self.scroll_pages = []
        self._filter_function = filter_function
        self._flush_every = flush_every
        self._flush_function = flush_function
        self._total_hits = 0
        self._hits_since_flush = 0

        # Get first set of results and a scroll id
        if self.local_mode:
            self.page = self.fake_pages.pop(0)
        else:
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

    def query_timeseries(self, query_template, start, end, source_filter=True,
                         filter_function=None, flush_every=None,
                         flush_function=None):
        """Query Elasticsearch for collectd plugin data.

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

        ### Print query
        tokio.debug.debug_print(json.dumps(query, indent=4))

        ### Run query
        time0 = time.time()
        self.query_and_scroll(
            query=query,
            source_filter=source_filter,
            filter_function=filter_function,
            flush_every=flush_every,
            flush_function=flush_function)
        tokio.debug.debug_print("Elasticsearch query took %s seconds" % (time.time() - time0))

def build_timeseries_query(orig_query, start, end):
    """Create a query object with time ranges bounded.

    Given a query dict and a start/end datetime object, return a new query
    object with the correct time ranges bounded.  Relies on `orig_query`
    containing at least one ``@timestamp`` field to indicate where the time
    ranges should be inserted.

    Args:
        orig_query (dict): A query object containing at least one ``@timestamp``
            field.
        start (datetime.datetime): lower bound for query (inclusive)
        end (datetime.datetime): upper bound for query (exclusive)

    Returns:
        dict: A query object with all instances of ``@timestamp`` bounded by
        `start` and `end`.
    """
    def map_item(obj, target_key, map_function):
        """
        Recursively walk a hierarchy of dicts and lists, searching for a
        matching key.  For each match found, apply map_function to that key's
        value.
        """
        if isinstance(obj, list):
            iterator = enumerate
            if target_key in obj:
                return obj[target_key]
        elif isinstance(obj, dict):
            iterator = dict.items
            if target_key in obj:
                return obj[target_key]
        else:
            # hit a dead end without a match
            return None
        # if this isn't a dead end, search down each iterable element
        for _, value in iterator(obj):
            if isinstance(value, (list, dict)):
                # dive down any discovered rabbit holes
                item = map_item(value, target_key, map_function)
                if item is not None:
                    map_function(item)
        return None

    def set_time_range(time_range_obj, time_format="epoch_second"):
        """
        Set the upper and lower bounds of a time range
        """
        time_range_obj['gte'] = int(time.mktime(start.timetuple()))
        time_range_obj['lt'] = int(time.mktime(end.timetuple()))
        time_range_obj['format'] = time_format
        remaps[0] += 1

    # note we use a single-element list so that remaps becomes mutable
    remaps = [0]

    query = copy.deepcopy(orig_query)

    map_item(query, '@timestamp', set_time_range)

    if not remaps:
        raise RuntimeError("unable to locate timestamp in query")

    return query
