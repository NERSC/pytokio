"""Provides interfaces into ESnet's SNMP REST API

Documentation for the REST API is here:

    http://es.net/network-r-and-d/data-for-researchers/snmp-api/
"""

class EsnetSnmp(dict):
    """Container for ESnet SNMP counters
    """

    def __init__(*args, **kwargs):
        """
        """
        super(EsnetSnmp, self).__init__(*args, **kwargs)
        # do some other stuff
        pass

    def get_interface_counters(self, start, end, interface, direction, agg_func=None, interval=None):
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
        pass
