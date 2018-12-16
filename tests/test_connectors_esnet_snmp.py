"""Test the ESnet SNMP REST API connector
"""

import datetime
import json

import requests
import nose

import tokio.connectors.esnet_snmp

def validate_interface_counters(esnetsnmp):
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
    print(json.dumps(esnetsnmp.last_response, indent=4, sort_keys=True))
#   assert esnetsnmp
    assert 'end_time' in esnetsnmp.last_response
    assert 'begin_time' in esnetsnmp.last_response
    assert 'data' in esnetsnmp.last_response

    if esnetsnmp.requested_agg_func is None:
        assert 'agg' in esnetsnmp.last_response
    else:
        assert 'calc' in esnetsnmp.last_response

    if esnetsnmp.requested_timestep is None:
        assert 'cf' in esnetsnmp.last_response
    else:
        assert 'calc_func' in esnetsnmp.last_response

    if esnetsnmp.start is not None:
        print("begin_time: %s <= %s? %s" % (
            esnetsnmp.last_response['begin_time'],
            esnetsnmp.start_epoch,
            (esnetsnmp.last_response['begin_time'] <= esnetsnmp.start_epoch)))
        assert esnetsnmp.last_response['begin_time'] <= esnetsnmp.start_epoch

    if esnetsnmp.end is not None:
        print("end_time: %s >= %s? %s" % (
            esnetsnmp.last_response['end_time'],
            esnetsnmp.end_epoch,
            (esnetsnmp.last_response['end_time'] >= esnetsnmp.end_epoch)))
        assert esnetsnmp.last_response['end_time'] >= esnetsnmp.end_epoch

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
    esnetsnmp = tokio.connectors.esnet_snmp.EsnetSnmp(start=start, end=end)
    try:
        esnetsnmp.get_interface_counters(endpoint=endpoint,
                                         interface=interface,
                                         direction='in')
    except requests.exceptions.ConnectionError as error:
        raise nose.SkipTest(error)

    validate_interface_counters(esnetsnmp)

    # test bare minimum functionality, direction=='out'
    esnetsnmp = tokio.connectors.esnet_snmp.EsnetSnmp(start=start, end=end)
    try:
        esnetsnmp.get_interface_counters(endpoint=endpoint,
                                         interface=interface,
                                         direction='out')
    except requests.exceptions.ConnectionError as error:
        raise nose.SkipTest(error)

    validate_interface_counters(esnetsnmp)

    # test specifying all parameters
    agg_func = 'max'
    interval = 60
    esnetsnmp = tokio.connectors.esnet_snmp.EsnetSnmp(start=start, end=end)
    try:
        esnetsnmp.get_interface_counters(endpoint=endpoint,
                                         interface=interface,
                                         direction='out',
                                         agg_func=agg_func,
                                         interval=interval)
    except requests.exceptions.ConnectionError as error:
        raise nose.SkipTest(error)

    validate_interface_counters(esnetsnmp)

    print("calc: %s == %s? %s" % (
        int(esnetsnmp.last_response['calc']),
        interval,
        (int(esnetsnmp.last_response['calc']) == interval)))
    assert int(esnetsnmp.last_response['calc']) == interval

    print("calc_func: %s == %s? %s" % (
        esnetsnmp.last_response['calc_func'].lower(),
        agg_func.lower(),
        esnetsnmp.last_response['calc_func'].lower() == agg_func.lower()))
    assert esnetsnmp.last_response['calc_func'].lower() == agg_func.lower()
