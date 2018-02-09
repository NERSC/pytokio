#!/usr/bin/env python
"""
Test the bin/summarize_h5lmt.py tool
"""

import json
import nose
import tokiotest
import tokiobin.summarize_h5lmt

def verify_json(output):
    """
    Verify the contents of summarize_h5lmt.py when encoded as json
    """
    assert json.loads(output)

def test_basic():
    """
    bin/summarize_h5lmt.py
    """
    output_str = tokiotest.run_bin(tokiobin.summarize_h5lmt, [tokiotest.SAMPLE_H5LMT_FILE])
    assert output_str > 0

@nose.SkipTest
def test_json():
    """
    bin/summarize_h5lmt.py --json
    """
    output_str = tokiotest.run_bin(tokiobin.summarize_h5lmt,
                                   ['--json', tokiotest.SAMPLE_H5LMT_FILE])
    assert output_str > 0

    output_json = json.loads(output_str)
    verify_json(output_json)
