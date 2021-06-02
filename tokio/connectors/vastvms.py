import requests
import urllib3

import pandas
import dateutil

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class VastManagementSystem():
    """Object to interact with VAST Management System REST API.

    Unlike most other connectors, this object is not self-populating; rather, it
    represents an object that is a Python interface into the VAST REST API
    itself.

    Args:
        endpoint (str): Base URL for VMS REST API (e.g., https://vms/api)
        username (str): Username to authenticate to VMS
        password (str): Password to authenticate to VMS
    """
    def __init__(self, endpoint, username=None, password=None):
        self.endpoint = endpoint
        self.auth = requests.auth.HTTPBasicAuth(username, password)
        self.resources = {
            "dashboard": "/dashboard/monitoring/",
            "monitors": "/monitors/{}/query/",
            "ad_hoc_query": "/monitors/ad_hoc_query/",
        }

    def get_generic(self, resource, *args, **kwargs):
        url = "/".join([self.endpoint, self.resources[resource].format(*args)])
        response = requests.get(
            url,
            headers={"accept": "application/json"},
            auth=self.auth,
            verify=False,
            params=kwargs)

        return response.json()

    def get_monitor(self, monitorid, **kwargs):
        """Queries a predefined VMS analytics monitor

        Args:
            monitorid (int or str): ID for monitor to query
            kwargs (dict): Passed on to the REST query

        Returns:
            dict: REST response
        """
        return self.get_generic("monitors", str(monitorid), **kwargs)

    def get_dashboard(self, **kwargs):
        return self.get_generic("dashboard", **kwargs)

    def ad_hoc_query(self, **kwargs):
        return self.get_generic("ad_hoc_query", **kwargs)

    def get_blockio_data(self, media, metric, *args, **kwargs):
        """Semantically friendly interface into blockio stats monitors

        Args:
            media (str): ssd or nvram
            metric (str): bw|mbs|iops|bandwidth|iops|b|m|w; bw and mbs are
                syonyms

        Returns:
            dict: raw results returned by VMS REST API for given monitor
        """
        metric = _normalize_metric(metric)
        monitor_map = {
            "ssd": {
                "mbs": 61,
                "iops": 63,
            },
            "nvram": {
                "mbs": 62,
                "iops": 64,
            },
        }
        monitorid = monitor_map[media.lower()][metric.lower()]
        return self.get_monitor(monitorid, *args, **kwargs)

    def get_blockio_frame(self, metric, access, *args, **kwargs):
        """Returns dataframe containing blockio stats

        Args:
            metric (str): bw|mbs|iops|bandwidth|iops|b|m|w; bw and mbs are
                syonyms
            access (str): read|write|r|w
            *args, **kwargs: Passed to REST query

        Returns:
            pandas.DataFrame: Contains timeseries of per-device read/write bandwidth/iops.
              * index is epoch timestamps as integers
              * columns are device names of form nvram1, ssd10, etc
              * values are either iops or bytes/sec
        """
        access = _normalize_access(access)
        metric = _normalize_metric(metric)

        key = "Hardware,component=disk,{}_{}".format(access, metric)

        query_params = {}
        query_params.update(kwargs)
        query_params.update({
            "prop_list": [ key ],
        })

        frame = None
        for media in "ssd", "nvram":
            raw_data = self.get_blockio_data(media, metric, *args, **kwargs)
            this_frame = vms2df(raw_data)
            this_frame['media'] = media
            this_frame['device'] = this_frame.apply(
                lambda x: "{}{}".format(x['media'], x['object_id']),
                axis=1)
            this_frame = this_frame[['timestamp', 'device', key]]

            if frame is None:
                frame = this_frame
            else:
                frame = pandas.concat((frame, this_frame))

        frame = pandas.pivot_table(frame, values=key, index='timestamp', columns='device')
        return frame[sorted(frame.columns, key=_sort_devicename)]

def _sort_devicename(device):
    """Sorts device names

    Args:
        device (str): of the form (nvram|ssd)[0-9]+

    Returns:
        str: of the form (nvram|ssd)00000XX
    """
    if not device.startswith('nvram') and not device.startswith('ssd'):
        return device
    media = device.rstrip('0123456789')
    deviceid = int(device[len(media):])
    return "{}{:012d}".format(media, deviceid)


def _normalize_access(access):
    valid_accesses = {
        "w": "w",
        "r": "r",
    }
    access = valid_accesses.get(access.lower()[0])
    if not access:
        raise ValueError("invalid access; must be one of {}".format(str(valid_accesses.keys())))
    return access

def _normalize_metric(metric):
    valid_metrics = {
        "i": "iops",
        "b": "mbs",
        "m": "mbs",
    }
    metric = valid_metrics.get(metric.lower()[0])
    if not metric:
        raise ValueError("invalid metric; must be one of {}".format(str(valid_metrics.keys())))
    return metric

def _normalize_media(media):
    valid_media = {
        "n": "nvram",
        "s": "ssd",
    }
    media = valid_media.get(media.lower()[0])
    if not media :
        raise ValueError("invalid media; must be one of {}".format(str(valid_media.keys())))
    return media

def vms2df(vms_data):
    """Converts results of a VMS query into a dataframe

    Args:
        vms_data (dict): contains raw output of a VMS REST query.  At minimum,
            must contain the following keys:
            - ``timestamp`` - of format 2021-06-02T12:25:00Z
            - ``prop_list`` - list containing column names
            - ``data` - list containing records corresponding to ``prop_list``

    Returns:
        pandas.DataFrame: Dataframe indexed by timestamp and with columns
        determined from ``prop_list``.
    """
    frame = pandas.DataFrame(vms_data['data'], columns=vms_data['prop_list'])
    frame['timestamp'] = frame['timestamp'].apply(
        lambda x: int(dateutil.parser.parse(x).timestamp()))
    return frame
