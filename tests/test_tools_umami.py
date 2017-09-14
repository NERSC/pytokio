#!/usr/bin/env python

import matplotlib
matplotlib.use('agg')

import nose
import os
import random
import tempfile
import tokio.tools.umami

# keep it deterministic
random.seed(0)

TEMP_FILE = None

# arbitrary but fixed start time
SAMPLE_TIMES = [ 1505345992 + n*86400 for n in range(5) ]

# procedurally generated garbage data to plot
SAMPLE_DATA = [
    [ random.randrange(0, 100.0) for n in range(5) ],
    [ random.randrange(0, 1000.0) for n in range(5) ],
    [ random.randrange(-1000.0, 1000.0) for n in range(5) ],
]

def teardown_tmpfile():
    global TEMP_FILE
    os.unlink(TEMP_FILE.name)

def setup_tmpfile():
    global TEMP_FILE
    TEMP_FILE = tempfile.NamedTemporaryFile(delete=False)

@nose.tools.with_setup(setup_tmpfile, teardown_tmpfile)
def test_umami_plot():
    """
    Ensure that basic UMAMI plot can be generated
    """
    umami = tokio.tools.umami.Umami()
    for index, sample_data in enumerate(SAMPLE_DATA):
        umami['test_metric_%d' % index] = tokio.tools.umami.UmamiMetric(
            timestamps=SAMPLE_TIMES,
            values=sample_data,
            label="Test Metric %d" % index,
            big_is_good=True)

    fig = umami.plot(output_file=TEMP_FILE)

    ### more correctness assertions?
    assert(len(fig.axes) == 2*len(SAMPLE_DATA))

def test_umamimetric_pop():
    """
    UmamiMetric pop functionality
    """
    raise nose.SkipTest

def test_umamimetric_append():
    """
    UmamiMetric append functionality
    """
    raise nose.SkipTest
