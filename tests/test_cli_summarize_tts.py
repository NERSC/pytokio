#!/usr/bin/env python
"""
Test the cli.summarize_tts tool.
"""

import json
import tokiotest
import tokio.cli.summarize_tts

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
    cli.summarize_tts
    """
    for input_type, input_file in tokiotest.SAMPLE_TIMESERIES_FILES.items():
        for args in INPUT_ARGS:
            func = exec_cmd
            func.description = "cli.summarize_tts %s (%s)" % (" ".join(args), input_type)
            full_args = args + [input_file]
            yield func, full_args

def exec_cmd(argv):
    """
    Execute the command with some arguments
    """
    print("Executing %s" % " ".join(argv))
    output_str = tokiotest.run_bin(tokio.cli.summarize_tts, argv)

    assert len(output_str) > 0

    if 'json' in ''.join(argv):
        output_json = json.loads(output_str)
        verify_json(output_json)
