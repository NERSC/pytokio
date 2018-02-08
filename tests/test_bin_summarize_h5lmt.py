#!/usr/bin/env python
"""
Test the bin/summarize_h5lmt.py tool
"""

import os
import json
import subprocess
import nose
import tokiotest

BINARY = os.path.join(tokiotest.BIN_DIR, 'summarize_h5lmt.py')

def verify_json(output):
    """
    Verify the contents of summarize_h5lmt.py when encoded as json
    """
    assert output

def test_basic():
    """
    bin/summarize_h5lmt.py
    """
    subprocess.check_output([BINARY, tokiotest.SAMPLE_H5LMT_FILE])

@nose.SkipTest
def test_json():
    """
    bin/summarize_h5lmt.py --json
    """
    output_str = subprocess.check_output([BINARY, '--json', tokiotest.SAMPLE_H5LMT_FILE])
    output_json = json.loads(output_str)
    verify_json(output_json)
