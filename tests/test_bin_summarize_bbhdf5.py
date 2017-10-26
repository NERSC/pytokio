#!/usr/bin/env python
"""
Test the bin/summarize_bbhdf5.py tool.  This will eventually be incorporated
into bin/summarize_h5lmt.py
"""

import os
import json
import subprocess
import nose
import tokiotest

BINARY = os.path.join(tokiotest.BIN_DIR, 'summarize_bbhdf5.py')

def verify_json(output):
    """
    Verify the contents of summarize_h5lmt.py when encoded as json
    """
    assert output

def test_basic():
    """
    bin/test_bin_summarize_bbhdf5.py
    """
    raise NotImplementedError
    subprocess.check_output([BINARY, tokiotest.SAMPLE_H5LMT_FILE])

def test_json():
    """
    bin/test_bin_summarize_bbhdf5.py --json
    """
    output_str = subprocess.check_output([BINARY, '--json', tokiotest.SAMPLE_TOKIOTS_FILE])
    output_json = json.loads(output_str)
    verify_json(output_json)
