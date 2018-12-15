"""Test the ESnet SNMP REST API connector
"""

import datetime
import time
import json

import requests
import nose

import tokio.connectors.esnet_snmp

def validate_interface_counters(esnetsnmp, start=None, end=None, agg_func=None, interval=None):
    """Ensure that the REST API returns a known structure

    Ensures that all return fields are present based on what we input.  Note
    that we do not check for the existence of return data because the production
    API endpoint seems to sometimes throttle requests in a way that returns an
    empty 'data' member.

    Args:
        esnetsnmp (tokio.connectors.esnet_snmp.EsnetSnmp): the data structure to
            validate
        start (int): the requested start time, in seconds since epoch
        end (int): the requested end time, in seconds since epoch
        agg_func (str or None): the agg_func passed to the API endpoint, if any
        interval (int or None): the interval passed to the API endpoint, if any

    """
    assert esnetsnmp
    print(json.dumps(esnetsnmp, indent=4, sort_keys=True))
    assert 'end_time' in esnetsnmp
    assert 'begin_time' in esnetsnmp
    assert 'data' in esnetsnmp
    if agg_func is None:
        assert 'agg' in esnetsnmp
    else:
        assert 'calc' in esnetsnmp
    if interval is None:
        assert 'cf' in esnetsnmp
    else:
        assert 'calc_func' in esnetsnmp

    if start is not None:
        print("begin_time: %s <= %s? %s" % (
            esnetsnmp['begin_time'],
            int(time.mktime(start.timetuple())),
            (esnetsnmp['begin_time'] <= int(time.mktime(start.timetuple())))))
        assert esnetsnmp['begin_time'] <= int(time.mktime(start.timetuple()))

    if end is not None:
        print("end_time: %s >= %s? %s" % (
            esnetsnmp['end_time'],
            int(time.mktime(end.timetuple())),
            (esnetsnmp['end_time'] >= int(time.mktime(end.timetuple())))))
        assert esnetsnmp['end_time'] >= int(time.mktime(end.timetuple()))

def test_esnetsnmp_get_interface_counters():
    """EsnetSnmp.get_interface_counters() basic functionality
    """
    endpoints = {
        'sunn-cr5': ['5_2_1'],
        'sacr-cr5': ['4_2_1'],
        'nersc-mr2': ['xe-0_1_0', 'xe-7_1_0'],
    }
    end = datetime.datetime.now()
    start = end - datetime.timedelta(hours=1)
    endpoint = list(endpoints)[0]
    interface = endpoints[endpoint][0]

    # test bare minimum functionality, direction=='in'
    esnetsnmp = tokio.connectors.esnet_snmp.EsnetSnmp()
    try:
        esnetsnmp.get_interface_counters(start=start,
                                         end=end,
                                         endpoint=endpoint,
                                         interface=interface,
                                         direction='in')
    except requests.exceptions.ConnectionError as error:
        raise nose.SkipTest(error)

    validate_interface_counters(esnetsnmp, start=start, end=end)

    # test bare minimum functionality, direction=='out'
    esnetsnmp = tokio.connectors.esnet_snmp.EsnetSnmp()
    try:
        esnetsnmp.get_interface_counters(start=start,
                                         end=end,
                                         endpoint=endpoint,
                                         interface=interface,
                                         direction='out')
    except requests.exceptions.ConnectionError as error:
        raise nose.SkipTest(error)

    validate_interface_counters(esnetsnmp, start=start, end=end)

    # test specifying all parameters
    agg_func = 'max'
    interval = 60
    esnetsnmp = tokio.connectors.esnet_snmp.EsnetSnmp()
    try:
        esnetsnmp.get_interface_counters(start=start,
                                         end=end,
                                         endpoint=endpoint,
                                         interface=interface,
                                         direction='out',
                                         agg_func=agg_func,
                                         interval=interval)
    except requests.exceptions.ConnectionError as error:
        raise nose.SkipTest(error)

    validate_interface_counters(esnetsnmp,
                                start=start,
                                end=end,
                                agg_func=agg_func,
                                interval=interval)

    print("calc: %s == %s? %s" % (
        int(esnetsnmp['calc']),
        interval,
        (int(esnetsnmp['calc']) == interval)))
    assert int(esnetsnmp['calc']) == interval

    print("calc_func: %s == %s? %s" % (
        esnetsnmp['calc_func'].lower(),
        agg_func.lower(),
        esnetsnmp['calc_func'].lower() == agg_func.lower()))
    assert esnetsnmp['calc_func'].lower() == agg_func.lower()
