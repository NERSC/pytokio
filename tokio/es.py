#!/usr/bin/env python

import json
import copy
import StringIO
import pycurl
import time
import datetime
import tokio
import numpy as np

import argparse
import os

_ES_TIMESTEP = 10.0

_DATE_FMT = "%Y-%m-%d %H:%M:%S"

_ES_FETCHMANY_LIMIT = 10000

_API_ENDPOINT = 'http://%s/%s/_search'

_QUERY_OST_DATA = {
    "size": _ES_FETCHMANY_LIMIT,
    "query": {
        "filtered": {
            "query": {
                "query_string": {
                    "query": "hostname:bb* AND plugin:disk AND plugin_instance:nvme* AND collectd_type:disk_octets",
                    "analyze_wildcard": True
                }
            },
            "filter": {
                "bool": {
                    "must": {
                        "range": {
                            "@timestamp": {
#                               "gte": "1465300800000",
#                               "lt":  "1465304400000",
                                "format": "epoch_millis"
                            }
                        }
                    }
                }
            }
        }
    },
    "fields": [ "@timestamp", "hostname", "plugin_instance", "write", "read" ]
}

def connect(*args, **kwargs):
    return ESDB( *args, **kwargs )

class ESDB(object):
    def __init__( self, dbhost=None, dbuser=None, dbpassword=None, dbindex=None ):
        if dbhost is None:
            self.dbhost = os.environ.get('ES_HOST')
        else:
            self.dbhost = dbhost
        if dbuser is None:
            self.dbuser = os.environ.get('ES_USER')
        else:
            dbuser = dbuser
        if dbpassword is None:
            self.dbpassword = os.environ.get('ES_PASSWORD')
        else:
            self.dbpassword = dbpassword
        if dbindex is None:
            self.dbindex = os.environ.get('ES_INDEX')
        else:
            self.dbindex = dbindex

        ### create a pyCurl connection
        self.db = pycurl.Curl()
        self.db.setopt(self.db.URL, _API_ENDPOINT % ( self.dbhost, self.dbindex ) )

        ### populate st names here, or after query?
        self.st_names = []

    def __enter__(self):
        return self

    def __die__(self):
        if self.db:
            self.db.close()
            sys.stderr.write('closing DB connection\n')

    def __exit__(self, exc_type, exc_value, traceback):
        if self.db:
            self.db.close()
            sys.stderr.write('closing DB connection\n')

    def close( self ):
        if self.db:
            self.db.close()
            sys.stderr.write('closing DB connection\n')

    def get_rw_data( self, t_start, t_stop, timestep=_ES_TIMESTEP ):
        """
        Return a tuple of two objects:
            1. a M*N matrix of int64s that encode the total read bytes for N STs over M timesteps
            2. a M*N matrix of int64s that encode the total write bytes for N STs over M timesteps
        """

        ### get the total number of hits so we can determine a sensible time
        ### range to query
        query = copy.deepcopy( _QUERY_OST_DATA )
        query['size'] = 0
        query_time_range = query['query']['filtered']['filter']['bool']['must']['range']['@timestamp']
        query_time_range['gte'] = long(time.mktime( t_start.timetuple() ) * 1000.0)
        query_time_range['lt'] = long(time.mktime( t_stop.timetuple() ) * 1000.0)
        response, error = self._query_es( query )

        ### assume hits are equally distributed over time, then divide our
        ### inputted time range into a sensible set of time ranges
        expected_hits = response['hits']['total'] 
        if error:
            tokio.error("initial size query returned unexpected results")
            tokio.error(json.dumps( response))
            return None, None

        total_time_range = t_stop - t_start
        num_subqueries = int(expected_hits / _ES_FETCHMANY_LIMIT) + 1
        ### the odds are that our assumption that our hits will be perfectly
        ### evenly distributed over time will not be correct.  if the number of
        ### subqueries is large, we don't want to have to sub-divide and
        ### re-issue queries for all those sub-queries that are slightly over the 
        ### _ES_FETCHMANY_liMIT.  So, we use the following heuristic to narrow
        ### our subqueries just a little to minimize the number of sub-divided
        ### sub-queries we have to issue
        if num_subqueries > 5:
            num_subqueries = int(1.05 * num_subqueries)
        time_per_subquery = total_time_range / num_subqueries
        list_of_times = [ t_start ]
        t = t_start + time_per_subquery
        ### create a list of cut points that bound sub-queries
        while t < t_stop:
            list_of_times.append( t )
            t += time_per_subquery
        list_of_times.append( t_stop )

        ### Initialize our output matrices
        ### Get the number of timesteps (# rows)
        ts_ct = int((t_stop - t_start).total_seconds() / timestep)
        t0 = int(time.mktime(t_start.timetuple()))

        ### Get the number of OSTs and their names (# cols)
        st_ct = len(self.st_names)

        ### Initialize everything to -0.0; we use the signed zero to distinguish
        ### the absence of data from a measurement of zero
        buf_r = np.full(shape=(ts_ct, st_ct), fill_value=-0.0, dtype='f8')
        buf_w = np.full(shape=(ts_ct, st_ct), fill_value=-0.0, dtype='f8') 

        tot_read = 0.0
        tot_written = 0.0
        ### issue query for each calculated time range
        processed_hits = 0
        error_state = False
        i = 0
        while i < len(list_of_times)-1:
            subq_t_start = list_of_times[i]
            subq_t_stop = list_of_times[i+1]
            response = self._get_rw_data( subq_t_start, subq_t_stop )
            if '_tokio_error' in response:
                tokio.error('received error response for time range %s to %s' 
                    % (subq_t_start, subq_t_stop))
                ### throttle dumping of full responses if error is cascading
                if not error_state:
                    tokio.error(json.dumps(response, indent=4, sort_keys=True))
            ### if we get an incomplete response, sub-divide the sub-query by
            ### cutting the time range in half, then re-submit the query
            if '_tokio_incomplete' in response:
                list_of_times.insert( i+1,
                    subq_t_start + (subq_t_stop - subq_t_start)/2 )
                print "Reducing to %s - %s - %s" % (
                    subq_t_start,
                    list_of_times[i+1],
                    subq_t_stop)
                continue
            processed_hits += len(response['hits']['hits'])

            ### for each hit in the response, insert these into a numpy matrix
            # do that here

            sum_bytes_read = sum([x['fields']['read'][0] for x in response['hits']['hits']])
            sum_bytes_written = sum([ x['fields']['write'][0] for x in response['hits']['hits']])

            ### assume that collectd only reports a single scalar for both read/write
            assert len(response['hits']['hits'][0]['fields']['write']) == 1
            assert len(response['hits']['hits'][0]['fields']['read']) == 1

            print "%s - %s yields %d results; %.1f GiB read, %.1f GiB written" % (
                subq_t_start,
                subq_t_stop,
                response['hits']['total'],
                sum_bytes_read * _ES_TIMESTEP / 2**30,
                sum_bytes_written * _ES_TIMESTEP / 2**30)
            tot_read += sum_bytes_read * _ES_TIMESTEP
            tot_written += sum_bytes_written * _ES_TIMESTEP
            i += 1

        print "TOTAL READ:    %.1f GiB" % (tot_read / 2.0**30)
        print "TOTAL WRITTEN: %.1f GiB" % (tot_written / 2.0**30)

        ### do some sanity checking of our results
        if processed_hits > expected_hits:
            ### we are working in a time range where data is still being
            ### appended; this is in the danger zone as our database may
            ### not fully consistent yet, so we may miss data
            tokio.warn('processed_hits > expected_hits (%d > %d)' %
                (processed_hits, expected_hits))
        elif processed_hits < expected_hits:
            ### data somehow vanished between the beginning and end of this
            ### process, or our time ranges had temporal gaps
            tokio.error('processed_hits < expected_hits (%d < %d);' %
                (processed_hits, expected_hits) 
                + ' not all data extracted!')


    def _get_rw_data( self, t_start, t_stop, size=_ES_FETCHMANY_LIMIT ):
        """
        Attempt to get some results from ElasticSearch.  If anything but a full
        set of results is returned, pass back nothing but an error so the query
        can be revised and re-submitted.
        """
        query = copy.deepcopy( _QUERY_OST_DATA )
        query['size'] = size
        query_time_range = query['query']['filtered']['filter']['bool']['must']['range']['@timestamp']
        query_time_range['gte'] = long(time.mktime( t_start.timetuple() ) * 1000.0)
        query_time_range['lt'] = long(time.mktime( t_stop.timetuple() ) * 1000.0)

        response, err = self._query_es( query )

        if err:
            tokio.error("Received an error response")
            tokio.error(json.dumps(response, sort_keys=True, indent=4))
            return { '_tokio_error': True }
        elif _is_response_incomplete( response ):
            return { '_tokio_incomplete': True }
        else:
            return response


    def _query_es( self, query_dict ):
        """
        Convert an input dict describing an ElasticSearch query into json, POST
        it to the ES cluster, then convert the response json into a dict
        """
        out_buffer = StringIO.StringIO()
        self.db.setopt(self.db.WRITEFUNCTION, out_buffer.write)
        self.db.setopt(self.db.POST, 1)
        self.db.setopt(self.db.POSTFIELDS, json.dumps(query_dict))
        self.db.perform()

        response = json.loads( out_buffer.getvalue() )

        err = _is_response_error( response )

        return response, err


def _is_response_error( response ):
    """
    Does the response from ElasticSearch indicate an error we don't know how
    to handle?
    {
      "error" : {
        "root_cause" : [ { "type" : "query_phase_execution_exception", "reason" : "Result window is too large ..." } ],
        "type" : "search_phase_execution_exception",
        "reason" : "all shards failed",
        "phase" : "query",
        "grouped" : true,
        "failed_shards" : [ { "shard" : 0, "index" : "cori-collectd-2016.05.17", "node" : "zzP5I-rCTgWem7SDtqbWPg", "reason" : { "type" : "...", "reason" : "..." } } ]
      },
      "status" : 500
    }"""

    ### specifically filter out "too many results" errors, because this is a
    ### recoverable error
    if (response.get('status', 200) == 500
    and response.get('error',{}) \
                .get('root_cause',[{}])[0] \
                .get('reason',"") \
                .startswith('Result window is too large')):
        return False
    elif ('error' in response
    or response.get('status', 200) >= 400
    or 'hits' not in response):
        return True
    else:
        return False


def _is_response_incomplete( response ):
    """
    Does the response from ElasticSearch contain only a subset of the total
    results?
    {
      "took" : 1809,
      "timed_out" : false,
      "_shards" : {
        "total" : 318,
        "successful" : 318,
        "failed" : 0
      },
      "hits" : {
        "total" : 31005905,
        "max_score" : 6.889569,
        "hits" : [ {
          "_index" : "cori-collectd-2016.06.11",
          "_type" : "collectd",
          "_id" : "AVU9UhO2F8Y2Z4TQg07n",
          "_score" : 6.889569,
          "fields" : {
            "hostname" : [ "bb142" ],
            "@timestamp" : [ "2016-06-11T02:36:08.589Z" ],
            "plugin_instance" : [ "nvme1n1" ],
            "read" : [ 0.0 ],
            "write" : [ 0.0 ]
          }
        }, ...
    """
    if response['hits']['total'] != len(response['hits']['hits']):
        return True
    else:
        return False


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('tstart',
                        type=str,
                        help="lower bound of time to scan, in YYYY-mm-dd HH:MM:SS format")
    parser.add_argument('tstop',
                        type=str,
                        help="upper bound of time to scan, in YYYY-mm-dd HH:MM:SS format")
    parser.add_argument('--debug',
                        action='store_true',
                        help="produce debug messages")
    args = parser.parse_args()
    if not (args.tstart and args.tstop):
        parser.print_help()
        sys.exit(1)
    if args.debug:
        tokio.DEBUG = True

    try:
        t_start = datetime.datetime.strptime(args.tstart, _DATE_FMT)
        t_stop = datetime.datetime.strptime(args.tstop, _DATE_FMT)
    except ValueError:
        sys.stderr.write("Start and end times must be in format %s\n" % _DATE_FMT)
        raise

    esdb = connect()

    esdb.get_rw_data( t_start, t_stop, _ES_TIMESTEP )
