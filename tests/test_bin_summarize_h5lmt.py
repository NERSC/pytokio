#!/usr/bin/env python

import os
import json
import subprocess
import nose.plugins.skip

SAMPLE_INPUT = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'inputs', 'sample.h5lmt')
BINARY = os.path.join('..', 'bin', 'summarize_h5lmt.py')

def test_basic():
    """
    Basic functionality of default settings
    """
    p = subprocess.Popen([BINARY, SAMPLE_INPUT], stdout=subprocess.PIPE)
    output_str = p.communicate()[0]
    assert p.returncode == 0

def test_json():
    """
    Basic functionality of json output--NOT YET IMPLEMENTED
    """
    p = subprocess.Popen([BINARY, '--json', SAMPLE_INPUT], stdout=subprocess.PIPE)
    output_str = p.communicate()[0]
    try:
        output_json = json.loads(output_str)
    except ValueError as e:
        raise nose.plugins.skip.SkipTest(e)
    assert p.returncode == 0
