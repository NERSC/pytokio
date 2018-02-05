#!/usr/bin/env python
"""
Test the bin/summarize_tts_hdf5.py tool.
"""

import os
import json
import subprocess
import nose
import tokiotest

BINARY = os.path.join(tokiotest.BIN_DIR, 'summarize_tts_hdf5.py')

INPUT_TYPES = {
    "TOKIO HDF5": tokiotest.SAMPLE_TOKIOTS_FILE,
    "pylmt HDF5": tokiotest.SAMPLE_H5LMT_FILE,
}
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
    for input_type, input_file in INPUT_TYPES.iteritems():
        for args in INPUT_ARGS:
            func = exec_cmd
            func.description = "bin/summarize_tts_hdf5.py %s (%s)" % (" ".join(args), input_type)
            full_args = args + [input_file]
            yield func, full_args

def exec_cmd(args):
    """
    Execute the command with some arguments
    """
    full_cmd = [BINARY] + args
    print "Executing", " ".join(full_cmd)
    output_str = subprocess.check_output(full_cmd)

    assert len(output_str) > 0

    if 'json' in ''.join(args):
        output_json = json.loads(output_str)
        verify_json(output_json)
