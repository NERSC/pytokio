#!/usr/bin/env python
"""
Test the bin/summarize_tts_hdf5.py tool.
"""

import json
import tokiotest
import tokiobin.summarize_tts_hdf5

INPUT_ARGS = [
    [],
    ["--json"],
    ["--columns"],
    ["--timesteps"],
    ["--columns", "--json"],
    ["--timesteps", "--json"],
]

def verify_json(output):
    """
    Verify the validity of json
    """
    assert output

def test_tts():
    """
    bin/summarize_tts_hdf5.py
    """
    for input_type, input_file in tokiotest.SAMPLE_TIMESERIES_FILES.items():
        for args in INPUT_ARGS:
            func = exec_cmd
            func.description = "bin/summarize_tts_hdf5.py %s (%s)" % (" ".join(args), input_type)
            full_args = args + [input_file]
            yield func, full_args

def exec_cmd(argv):
    """
    Execute the command with some arguments
    """
    print("Executing %s" % " ".join(argv))
    output_str = tokiotest.run_bin(tokiobin.summarize_tts_hdf5, argv)

    assert len(output_str) > 0

    if 'json' in ''.join(argv):
        output_json = json.loads(output_str)
        verify_json(output_json)
