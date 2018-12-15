"""Provides interfaces into ESnet's SNMP REST API

Documentation for the REST API is here:

    http://es.net/network-r-and-d/data-for-researchers/snmp-api/
"""

import time
import json
import datetime
import warnings

import requests
import pandas

from .. import config
from . import common


class EsnetSnmp(common.CacheableDict):
    """Container for ESnet SNMP counters
    """
    def get_interface_counters(self, start, end, endpoint, interface, direction, agg_func=None, interval=None):
        """Retrieves data rate data for an ESnet endpoint

        Args:
            start (datetime.datetime): Start of interval to retrieve, inclusive
            end (datetime.datetime): End of interval to retrieve, inclusive
            direction (str): "in" or "out" to signify data input into ESnet or
                data output from ESnet
            agg_func (str or None): Specifies the reduction operator to be
                applied over each interval; must be one of "average," "min," or
                "max."  If None, uses the ESnet default.
            interval (int or None): Resolution, in seconds, of the data to be
                returned.  If None, uses the ESnet default.
        """
        # TODO: check begin/end parameter validity

        # TODO: check in/out parameter validity

        uri = config.CONFIG.get('esnet_snmp_uri')
        if uri is None:
            warnings.warn("no esnet_snmp_uri configured")
            return {}
        uri += '/%s/interface/%s/%s' % (endpoint, interface, direction)

        params = {
            'begin': int(time.mktime(start.timetuple())),
            'end': int(time.mktime(end.timetuple())),
        }
        if agg_func is not None:
            params['calc_func'] = agg_func
        if interval is not None:
            params['calc'] = interval

        request = requests.get(uri, params=params)
        response_data = json.loads(request.text)
        self.update(response_data)
        return response_data

    def to_dataframe(self):
        """Return data as a Pandas DataFrame
        """
        if 'data' not in self:
            return None

        indices = [datetime.datetime.fromtimestamp(x[0]) for x in self.get('data', [])]
        values = [datetime.datetime.fromtimestamp(x[1]) for x in self.get('data', [])]

        return pandas.DataFrame(values, index=indices)
