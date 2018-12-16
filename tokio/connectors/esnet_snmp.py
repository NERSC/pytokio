"""Provides interfaces into ESnet's SNMP REST API

Documentation for the REST API is here:

    http://es.net/network-r-and-d/data-for-researchers/snmp-api/

Relies either on the 'esnet_snmp_uri' configuration value being set in the
pytokio configuration or the PYTOKIO_ESNET_SNMP_URI being defined in the
environment.
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

    Dictionary with structure::

        {
            "endpoint0": {
                "interface_x": {
                    "in": {
                        timestamp1: value1,
                        timestamp2: value2,
                        timestamp3: value3,
                        ...
                    },
                    "out": { ... }
                },
                "interface_y": { ... }
            },
            "endpoint1": { ... }
        }

    Various methods are provided to access the data of interest.
    """
    def __init__(self,
                 start,
                 end,
                 endpoint=None,
                 interface=None,
                 direction=None,
                 agg_func=None,
                 interval=None):
        """Retrieves data rate data for an ESnet endpoint

        Initializes the object with a start and end time.  Optionally runs a
        REST API query if endpoint, interface, and direction are provided.
        Assumes that once the start/end time have been specified, they should
        not be changed.

        Args:
            start (datetime.datetime): Start of interval to retrieve, inclusive
            end (datetime.datetime): End of interval to retrieve, inclusive
            endpoint (str, optional): Name of the ESnet endpoint whose data is
                being retrieved
            interface (str, optional): Name of the ESnet endpoint interface on
                the specified endpoint
            direction (str, optional): "in" or "out" to signify data input into
                ESnet or data output from ESnet
            agg_func (str, optional ): Specifies the reduction operator to be
                applied over each interval; must be one of "average," "min," or
                "max."  If None, uses the ESnet default.
            interval (int, optional ): Resolution, in seconds, of the data to be
                returned.  If None, uses the ESnet default.

        Attributes:
            start (datetime.datetime): Start of interval represented by this object, inclusive
            end (datetime.datetime): End of interval represented by this object, inclusive
            start_epoch (int): Seconds since epoch for self.start
            end_epoch (int): Seconds since epoch for self.end
        """

        super(EsnetSnmp, self).__init__()

        if start >= end:
            raise RuntimeError("Start must be greater than or equal to end")

        # input parameters
        self.start = start
        self.end = end
        self.start_epoch = int(time.mktime(self.start.timetuple()))
        self.end_epoch = int(time.mktime(self.end.timetuple()))
        self.requested_timestep = interval
        self.requested_endpoint = endpoint
        self.requested_interface = interface
        self.requested_direction = direction
        self.requested_agg_func = agg_func

        # output parameters
        self.last_response = None
        self.timestep = None

        if endpoint and interface and direction:
            self.get_interface_counters(endpoint=endpoint,
                                        interface=interface,
                                        direction=direction,
                                        agg_func=agg_func,
                                        interval=interval)

    def _insert_result(self):
        """Parse the raw output of the REST API and update self

        ESnet's REST API will return an object like::

            {
                "agg": "30",
                "begin_time": 1517471100,
                "end_time": 1517471910,
                "cf": "average",
                "data": [
                    [
                        1517471100,
                        5997486471.266666
                    ],
            ...
                    [
                        1517471910,
                        189300026.8
                    ]
                ]
            }

        Args:
            result (dict): JSON structure returned by the ESnet REST API

        Returns:
            bool: True if data inserted without errors; False otherwise
        """

        result = self.last_response
        if not result:
            print("no result; bailing")
            return False

        # set the timestep if not previously set
        interval = _get_interval_result(result)
        if interval:
            if not self.timestep:
                self.timestep = interval
            elif self.timestep != interval:
                warnings.warn("received timestep %d from an object with timestep %d"
                              % (interval, self.timestep))

        if self.requested_endpoint not in self:
            self[self.requested_endpoint] = {}
        if self.requested_interface not in self[self.requested_endpoint]:
            self[self.requested_endpoint][self.requested_interface] = {}
        if self.requested_direction not in self[self.requested_endpoint][self.requested_interface]:
            self[self.requested_endpoint][self.requested_interface][self.requested_direction] = {}

        data = result.get('data')
        if not data:
            warnings.warn("No data in result")

        for timestamp, value in data:
            self[self.requested_endpoint][self.requested_interface][self.requested_direction][timestamp] = value

        return True

    def get_interface_counters(self, endpoint, interface, direction, agg_func=None, interval=None):
        """Retrieves data rate data for an ESnet endpoint

        Args:
            interface (str, optional): Name of the ESnet endpoint interface
            direction (str): "in" or "out" to signify data input into ESnet or
                data output from ESnet
            agg_func (str or None): Specifies the reduction operator to be
                applied over each interval; must be one of "average," "min," or
                "max."  If None, uses the ESnet default.
            interval (int or None): Resolution, in seconds, of the data to be
                returned.  If None, uses the ESnet default.

        Returns:
           dict: raw return from the REST API call
        """
        if direction not in ['in', 'out']:
            raise RuntimeError("direction must be either in or out")

        self.requested_endpoint = endpoint
        self.requested_interface = interface
        self.requested_direction = direction
        self.requested_agg_func = agg_func
        self.requested_timestep = interval

        if self.requested_timestep is not None \
        and self.timestep is not None \
        and self.requested_timestep != self.timestep:
            warnings.warn("received timestep %d from an object with timestep %d"
                          % (self.requested_timestep, self.timestep))

        uri = config.CONFIG.get('esnet_snmp_uri')
        if uri is None:
            raise requests.exceptions.ConnectionError("no esnet_snmp_uri configured")
        uri += '/%s/interface/%s/%s' % (endpoint, interface, direction)

        params = {
            'begin': self.start_epoch,
            'end': self.end_epoch,
        }
        if agg_func is not None:
            params['calc_func'] = agg_func
        if interval is not None:
            params['calc'] = interval

        request = requests.get(uri, params=params)
        self.last_response = json.loads(request.text)

        self._insert_result()

        return self.last_response

    def to_dataframe(self, multiindex=False):
        """Return data as a Pandas DataFrame

        Args:
            multiindex (bool): If True, return a DataFrame indexed by timestamp,
                endpoint, interface, and direction
        """

        to_df = []

        for endpoint, interfaces in self.items():
            for interface, directions in interfaces.items():
                for direction, data in directions.items():
                    for timestamp, value in data.items():
                        to_df.append({
                            'endpoint': endpoint,
                            'interface': interface,
                            'direction': direction,
                            'timestamp': datetime.datetime.fromtimestamp(timestamp),
                            'data_rate': value,
                        })

        dataframe = pandas.DataFrame.from_records(to_df)
        if multiindex:
            return dataframe.set_index(['timestamp', 'endpoint', 'interface', 'direction'])

        return dataframe

def _get_interval_result(result):
    """Parse the raw output of the REST API output and return the timestep

    Args:
        result (dict): the raw JSON output of the ESnet REST API
    """
    value = result.get('agg')
    if value:
        return int(value)

    value = result.get('calc')
    if 'calc' in result:
        return int(value)

    return None
