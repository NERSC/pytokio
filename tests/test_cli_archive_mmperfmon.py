#!/usr/bin/env python
"""
Test the archive_mmperfmon.py tool
"""

import os
import time
import nose
import h5py
import tokio
import tokio.cli.archive_mmperfmon
import tokiotest

def generate_tts(output_file,
                 input_file=tokiotest.SAMPLE_MMPERFMON_MULTI,
                 init_start=tokiotest.SAMPLE_MMPERFMON_MULTI_START,
                 init_end=tokiotest.SAMPLE_MMPERFMON_MULTI_END):
    """Create a TokioTimeSeries output file
    """
    argv = [
        '--init-start', init_start,
        '--init-end', init_end,
        '--timestep', str(tokiotest.SAMPLE_MMPERFMON_TIMESTEP),
        '--output', output_file,
        input_file
    ]
    print("Running [%s]" % ' '.join(argv))
    tokio.cli.archive_mmperfmon.main(argv)
    print("Created %s" % output_file)

def update_tts(output_file):
    """
    Append to an existing tts file
    """
    assert os.path.isfile(output_file) # must update an existing file

    argv = [
        '--output', output_file,
        tokiotest.SAMPLE_MMPERFMON_MULTI_SUBSET,
    ]

    print("Running [%s]" % ' '.join(argv))
    tokio.cli.archive_mmperfmon.main(argv)
    print("Updated %s" % output_file)

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_bin_archive_mmperfmon_overlaps():
    """cli.archive_mmperfmon: write + overwrite correctness

    1. initialize a new HDF5 and pull down a large window
    2. pull down a complete subset of that window to this newly minted HDF5
    3. ensure that the hdf5 between #1 and #2 doesn't change
    """
    tokiotest.TEMP_FILE.close()

    # initialize a new TimeSeries, populate it, and write it out as HDF5
    generate_tts(tokiotest.TEMP_FILE.name)
    h5_file = h5py.File(tokiotest.TEMP_FILE.name, 'r')
    summary0 = tokiotest.summarize_hdf5(h5_file)
    h5_file.close()

    time.sleep(1.5)

    # append an overlapping subset of data to the same HDF5
    update_tts(tokiotest.TEMP_FILE.name)
    h5_file = h5py.File(tokiotest.TEMP_FILE.name, 'r')
    summary1 = tokiotest.summarize_hdf5(h5_file)
    h5_file.close()

    tokiotest.identical_datasets(summary0, summary1)
