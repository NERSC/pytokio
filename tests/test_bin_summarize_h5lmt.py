#!/usr/bin/env python
"""
Test the bin/summarize_h5lmt.py tool
"""

import os
import json
import subprocess
import nose

SAMPLE_INPUT = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'inputs', 'sample.h5lmt')
BINARY = os.path.join('..', 'bin', 'summarize_h5lmt.py')

def verify_json(output):
    """
    Verify the contents of summarize_h5lmt.py when encoded as json
    """
    assert output

def test_basic():
    """
    Basic functionality of default settings
    """
    subprocess.check_output([BINARY, SAMPLE_INPUT])

@nose.SkipTest
def test_json():
    """
    Basic functionality of json output--NOT YET IMPLEMENTED
    """
    output_str = subprocess.check_output([BINARY, '--json', SAMPLE_INPUT])
    output_json = json.loads(output_str)
    verify_json(output_json)
