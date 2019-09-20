"""Test the ESnet SNMP REST API connector
"""

import gzip
import json
import datetime

HAVE_REQUESTS = True
try:
    from requests.exceptions import Timeout, ConnectionError, HTTPError
except ImportError:
    HAVE_REQUESTS = False

import nose

import tokiotest
import tokio.connectors.esnet_snmp

ESNETSNMP_ENDPOINTS = {
    'sunn-cr5': ['5_2_1'],
    'sacr-cr5': ['4_2_1'],
    'nersc-mr2': ['xe-0_1_0', 'xe-7_1_0'],
}
ESNETSNMP_END = datetime.datetime.now()
ESNETSNMP_START = ESNETSNMP_END - datetime.timedelta(hours=1)

def validate_last_response(esnetsnmp):
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
    print(json.dumps(esnetsnmp.last_response))
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
    if not HAVE_REQUESTS:
        raise nose.SkipTest("requests library not available")

    endpoint = list(ESNETSNMP_ENDPOINTS)[0]
    interface = ESNETSNMP_ENDPOINTS[endpoint][0]

    # tests:
    #  - minimum required inputs
    #  - direction=='in'
    #  - two-step initialize and populate
    esnetsnmp = tokio.connectors.esnet_snmp.EsnetSnmp(
        start=ESNETSNMP_START, end=ESNETSNMP_END)
    try:
        esnetsnmp.get_interface_counters(endpoint=endpoint,
                                         interface=interface,
                                         direction='in',
                                         timeout=5)
    except (ConnectionError, Timeout, HTTPError) as error:
        raise nose.SkipTest(error)

    validate_last_response(esnetsnmp)

    # tests:
    #  - minimum required inputs
    #  - direction=='out'
    #  - single-step initialize and populate
    try:
        esnetsnmp = tokio.connectors.esnet_snmp.EsnetSnmp(
            start=ESNETSNMP_START,
            end=ESNETSNMP_END,
            endpoint=endpoint,
            interface=interface,
            direction='out')
    except (ConnectionError, Timeout, HTTPError) as error:
        raise nose.SkipTest(error)

    validate_last_response(esnetsnmp)

def test_all_parameters():
    """EsnetSnmp.gen_url()
    """
    endpoint = list(ESNETSNMP_ENDPOINTS)[0]
    interface = ESNETSNMP_ENDPOINTS[endpoint][0]
    agg_func = 'max'
    interval = 60

    esnetsnmp = tokio.connectors.esnet_snmp.EsnetSnmp(
        start=ESNETSNMP_START,
        end=ESNETSNMP_END)

    kwargs = esnetsnmp.gen_url(
                endpoint=endpoint,
                interface=interface,
                direction='out',
                agg_func=agg_func,
                interval=interval)

    assert 'url' in kwargs
    assert 'params' in kwargs
    assert 'begin' in kwargs['params']
    assert 'end' in kwargs['params']
    assert 'calc_func' in kwargs['params'] and kwargs['params']['calc_func'] == agg_func
    assert 'calc' in kwargs['params'] and kwargs['params']['calc'] == interval

def test_to_dataframe():
    """EsnetSnmp.to_dataframe()
    """
    esnetsnmp = tokio.connectors.esnet_snmp.EsnetSnmp(
        start=ESNETSNMP_START,
        end=ESNETSNMP_END)

    esnetsnmp.load_json(tokiotest.SAMPLE_ESNET_SNMP_FILE)
    endpoint = next(iter(esnetsnmp.keys()))
    interface = next(iter(esnetsnmp[endpoint].keys()))
    direction = next(iter(esnetsnmp[endpoint][interface].keys()))
    expected_rows = len(esnetsnmp[endpoint][interface][direction])

    assert esnetsnmp
    print("Loaded esnetsnmp data:")
    print(esnetsnmp)

    # Test simple to_dataframe
    # dataframe.columns = [endpoint, interface, direction, timestamp, data_rate]
    dataframe = esnetsnmp.to_dataframe()
    assert len(dataframe)
    print("Generated dataframe:")
    print(dataframe)

    # filter only one (endpoint, interface, direction) combination and make sure
    # that all of the data from the original dict-like object is represented in
    # the dataframe
    filt = dataframe['endpoint'] == endpoint
    filt &= dataframe['interface'] == interface
    filt &= dataframe['direction'] == direction

    print("dataframe has %d rows; raw result had %d rows" % (
        len(dataframe[filt]),
        expected_rows))
    assert len(dataframe[filt]) == expected_rows 

    # just make sure the multiindex=True parameter works and produces something
    dataframe = esnetsnmp.to_dataframe(multiindex=True)
    assert len(dataframe)
