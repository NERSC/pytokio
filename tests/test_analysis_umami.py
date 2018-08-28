#!/usr/bin/env python
"""
Test the UMAMI tool interface
"""

import json
import random
import datetime
import nose
import matplotlib.pyplot
import tokiotest
import tokio.analysis.umami

# prevent the test from throwing DISPLAY errors
matplotlib.pyplot.switch_backend('agg')

# keep it deterministic
random.seed(0)

# arbitrary but fixed start time
SAMPLE_TIMES = [datetime.datetime.fromtimestamp(1505345992 + n*86400) for n in range(5)]

# procedurally generated garbage data to plot
SAMPLE_DATA = [
    [random.randrange(0, 100.0) for n in range(5)],
    [random.randrange(0, 1000.0) for n in range(5)],
    [random.randrange(-1000.0, 1000.0) for n in range(5)],
]

def verify_umami_fig(axes):
    """
    Verify basic UMAMI correctness
    """
    ### more correctness assertions?
    assert len(axes) == 2*len(SAMPLE_DATA)

def build_umami_from_sample():
    """
    Construct an Umami object from the test's constants
    """
    umami = tokio.analysis.umami.Umami()
    for index, sample_data in enumerate(SAMPLE_DATA):
        umami['test_metric_%d' % index] = tokio.analysis.umami.UmamiMetric(
            timestamps=SAMPLE_TIMES,
            values=sample_data,
            label="Test Metric %d" % index,
            big_is_good=True)
    return umami

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_umami_plot_to_file():
    """
    Ensure that basic UMAMI plot can be generated
    """
    umami = build_umami_from_sample()
    fig = umami.plot(output_file=tokiotest.TEMP_FILE.name)
    print "Wrote output to %s" % tokiotest.TEMP_FILE.name
    verify_umami_fig(fig)

def test_umami_to_dict():
    """
    Umami.to_dict() correctness
    """
    umami = build_umami_from_sample()
    umami_dict = umami.to_dict()
    print umami_dict

    for metric, measurement in umami_dict.iteritems():
        # get the corresponding SAMPLE_DATA row number from the metric name,
        # which should be test_metric_XX
        row_num = int(metric.split('_')[-1])

        # walk list of values to ensure they are correct
        for index, value in enumerate(measurement['values']):
            print row_num, index, value, SAMPLE_DATA[row_num][index]
            assert value == SAMPLE_DATA[row_num][index]

        # walk list of timestamps
        for index, value in enumerate(measurement['timestamps']):
            assert value == SAMPLE_TIMES[index]
        row_num += 1

def test_umami_to_json():
    """
    Umami.to_json() functionality
    """
    # Don't bother checking correctness.  Just make sure json.dumps doesn't fail
    umami = build_umami_from_sample()
    print umami.to_json()

def test_umami_to_df():
    """
    Umami.to_dataframe() correctness
    """
    umami = build_umami_from_sample()
    umami_df = umami.to_dataframe()
    print umami_df
    for metric in umami_df:
        # get the corresponding SAMPLE_DATA row number from the metric name,
        # which should be test_metric_XX
        row_num = int(metric.split('_')[-1])
        index = 0
        for timestamp, value in umami_df[metric].iteritems():
            assert timestamp == SAMPLE_TIMES[index]
            assert value == SAMPLE_DATA[row_num][index]
            index += 1

def test_umamimetric_pop():
    """
    UmamiMetric pop functionality
    """
    umami = build_umami_from_sample()
    row_num = 0
    for umami_metric in umami.itervalues():
        index = -1
        while len(umami_metric.values) > 0:
            # prevent an infinite loop...
            assert index >= (-1*len(SAMPLE_DATA[row_num]))
            timestamp, value = umami_metric.pop()
            # make sure the value we popped off is what we expect
            assert value == SAMPLE_DATA[row_num][index]
            # also make sure the timestamp is what we expect
            assert timestamp == SAMPLE_TIMES[index]
            index -= 1
        row_num += 1

def test_umamimetric_append():
    """
    UmamiMetric append functionality
    """
    umami_metrics = []
    for index, sample_data in enumerate(SAMPLE_DATA):
        umami_metric = tokio.analysis.umami.UmamiMetric(
            timestamps=[],
            values=[],
            label="Test Metric %d" % index,
            big_is_good=True)
        print "%d: sample_data is %d units long (%s)" % (index,
                                                         len(sample_data),
                                                         json.dumps(sample_data))
        for jndex, sample_datum in enumerate(sample_data):
            umami_metric.append(SAMPLE_TIMES[jndex], sample_datum)
        umami_metrics.append(umami_metric)

    umami = tokio.analysis.umami.Umami()
    for index, umami_metric in enumerate(umami_metrics):
        umami["test_metric_%d" % index] = umami_metric

    fig = umami.plot()
    verify_umami_fig(fig)

def test_umamimetric_to_json():
    """
    Umami.to_json() functionality
    """
    # Don't bother checking correctness.  Just make sure json.dumps doesn't fail
    umami = build_umami_from_sample()
    for umamimetric in umami.itervalues():
        print umamimetric.to_json()
