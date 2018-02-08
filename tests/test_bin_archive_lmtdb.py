#!/usr/bin/env python
"""
Test the archive_lmtdb.py tool
"""

import os
import time
import subprocess
import datetime
import nose
import h5py
import tokiotest

BINARY = os.path.join(tokiotest.BIN_DIR, 'archive_lmtdb.py')

#
#  TESTING APPEND/UPDATE FUNCTIONALITY
#
#  The current test strategy is to
#
#  1. initialize a new HDF5 and pull down a large window
#  2. pull down a complete subset of that window to this newly minted HDF5
#  3. ensure that the hdf5 between #1 and #2 doesn't change
#
DATE_FMT = "%Y-%m-%dT%H:%M:%S"

TEST_RANGES = [
    (0.00, 1.00),
    (0.00, 0.50),
    (0.50, 1.00),
    (0.25, 0.75),
]

def generate_tts(output_file):
    """
    Create a TokioTimeSeries output file
    """
    init_start = datetime.datetime.fromtimestamp(tokiotest.SAMPLE_LMTDB_START).strftime(DATE_FMT)
    init_end = datetime.datetime.fromtimestamp(tokiotest.SAMPLE_LMTDB_END).strftime(DATE_FMT)
    cmd = [BINARY,
           '--init-start', init_start,
           '--init-end', init_end,
           '--input', tokiotest.SAMPLE_LMTDB_FILE,
           '--timestep', str(tokiotest.SAMPLE_LMTDB_TIMESTEP),
           '--output', output_file,
           '--debug',
           init_start,
           init_end]
    print "Running [%s]" % ' '.join(cmd)
    subprocess.check_output(cmd)
    print "Created", output_file

def update_tts(output_file, q_start, q_end):
    """
    Append to an existing tts file
    """
    assert os.path.isfile(output_file) # must update an existing file

    cmd = [BINARY,
           '--input', tokiotest.SAMPLE_LMTDB_FILE,
           '--output', output_file,
           '--debug',
           q_start.strftime(DATE_FMT),
           q_end.strftime(DATE_FMT)]

    print "Running [%s]" % ' '.join(cmd)
    subprocess.check_output(cmd)
    print "Updated", output_file

def summarize_hdf5(hdf5_file):
    """
    Return some summary metrics of an hdf5 file in a mostly content-agnostic way
    """
    # characterize the h5file in a mostly content-agnostic way
    summary = {
        'sums': {},
        'shapes': {},
        'updates': {},
    }

    def characterize_object(obj_name, obj_data):
        """retain some properties of each dataset in an hdf5 file"""
        if isinstance(obj_data, h5py.Dataset):
            summary['shapes'][obj_name] = obj_data.shape
            # note that this will break if the hdf5 file contains non-numeric datasets
            if len(obj_data.shape) == 1:
                summary['sums'][obj_name] = obj_data[:].sum()
            elif len(obj_data.shape) == 2:
                summary['sums'][obj_name] = obj_data[:, :].sum()
            elif len(obj_data.shape) == 3:
                summary['sums'][obj_name] = obj_data[:, :, :].sum()
            summary['updates'][obj_name] = obj_data.attrs.get('updated')

    hdf5_file.visititems(characterize_object)

    return summary

def compare_hdf5(summary0, summary1):
    """
    compare the contents of two HDF5s for similarity
    """
    # ensure that updating the overlapping data didn't change the contents of the TimeSeries
    num_compared = 0
    for metric in 'sums', 'shapes':
        for key, value in summary0[metric].iteritems():
            num_compared += 1
            assert key in summary1[metric]
            print "%s->%s->[%s] == [%s]?" % (metric, key, summary1[metric][key], value)
            assert summary1[metric][key] == value

    # check to make sure summary1 was modified after summary0 was generated
    for obj, update in summary0['updates'].iteritems():
        if update is not None:
            result = summary1['updates'][obj] > summary0['updates'][obj]
            print "%s newer than %s? %s" % (summary1['updates'][obj],
                                            summary0['updates'][obj],
                                            result)
            assert result

    assert num_compared > 0

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_bin_archive_lmtdb():
    """
    bin/archive_lmtdb.py
    """
    tokiotest.TEMP_FILE.close()

    start = datetime.datetime.fromtimestamp(tokiotest.SAMPLE_LMTDB_START)
    end = datetime.datetime.fromtimestamp(tokiotest.SAMPLE_LMTDB_END)
    delta = (end - start).total_seconds()

    for test_range in TEST_RANGES:
        # initialize a new TimeSeries, populate it, and write it out as HDF5
        generate_tts(tokiotest.TEMP_FILE.name)
        h5_file = h5py.File(tokiotest.TEMP_FILE.name, 'r')
        summary0 = summarize_hdf5(h5_file)
        h5_file.close()

        q_start = start + datetime.timedelta(seconds=int(test_range[0] * delta))
        q_end = start + datetime.timedelta(seconds=int(test_range[1] * delta))
        print "Overwriting %s to %s" % (q_start, q_end)
        time.sleep(1.5)

        # append an overlapping subset of data to the same HDF5
        update_tts(tokiotest.TEMP_FILE.name, q_start, q_end)
        h5_file = h5py.File(tokiotest.TEMP_FILE.name, 'r')
        summary1 = summarize_hdf5(h5_file)
        h5_file.close()

        func = compare_hdf5
        func.description = ("bin/archive_lmtdb.py overlap %s" % str(test_range))
#       yield func, summary0, summary1
        func(summary0, summary1)
