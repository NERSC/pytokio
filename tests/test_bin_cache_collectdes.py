#!/usr/bin/env python
"""
Test the cache_collectdes.py tool, summarize_tts.py tool, and the
TokioTimeSeries class
"""

import os
import h5py
import subprocess
import nose
import tokiotest

BINARY = os.path.join(tokiotest.BIN_DIR, 'cache_collectdes.py')

#
#  TESTING APPEND/UPDATE FUNCTIONALITY
#
#  The current test strategy is to 
#
#  1. initialize a new HDF5 and pull down a large window
#  2. pull down a complete subset of that window to this newly minted HDF5
#  3. ensure that the hdf5 between #1 and #2 doesn't change using summarize_bbhdf5.py
#
# For example,
#
# rm -v output.hdf5
# ./bin/cache_collectdes.py --init-start 2017-12-13T00:00:00 \
#                           --init-end 2017-12-14T00:00:00 \
#                           --input-json output.json \
#                           --num-bbnodes 288 \
#                           --timestep 10 \
#                           2017-12-13T00:00:00 2017-12-13T01:00:00
# ./bin/summarize_bbhdf5.py output.hdf5
# ./bin/cache_collectdes.py --debug --input-json output.json 2017-12-13T00:15:00 2017-12-13T00:30:00 
# ./bin/summarize_bbhdf5.py output.hdf5
#
# Assert that the two instances of summarize_bbhdf5.py return identical results
#

def generate_tts(output_file):
    """
    Create a TokioTimeSeries output file
    """
    cmd = [BINARY,
           '--init-start', tokiotest.SAMPLE_COLLECTDES_START,
           '--init-end', tokiotest.SAMPLE_COLLECTDES_END,
           '--input-json', tokiotest.SAMPLE_COLLECTDES_FILE,
           '--num-bbnodes', str(tokiotest.SAMPLE_COLLECTDES_NUMNODES),
           '--timestep', str(tokiotest.SAMPLE_COLLECTDES_TIMESTEP),
           '--output', output_file,
           '--debug',
           tokiotest.SAMPLE_COLLECTDES_START,
           tokiotest.SAMPLE_COLLECTDES_END]
    print "Running [%s]" % ' '.join(cmd)
    subprocess.check_output(cmd)
    print "Created", output_file

def update_tts(output_file):
    """
    Append to an existing tts file
    """
    assert os.path.isfile(output_file) # must update an existing file

    cmd = [BINARY,
           '--input-json', tokiotest.SAMPLE_COLLECTDES_FILE2,
           '--output', output_file,
           '--debug',
           tokiotest.SAMPLE_COLLECTDES_START2,
           tokiotest.SAMPLE_COLLECTDES_END2]

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
        'shapes': {}
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

    hdf5_file.visititems(characterize_object)

    return summary

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_bin_cache_collectdes():
    """
    bin/cache_collectdes.py
    """
    tokiotest.TEMP_FILE.close()
    generate_tts(tokiotest.TEMP_FILE.name)

    h5_file = h5py.File(tokiotest.TEMP_FILE.name, 'r')
    summary0 = summarize_hdf5(h5_file)
    h5_file.close()

    subprocess.call(['cp', tokiotest.TEMP_FILE.name, 'poop1.hdf5'])

    update_tts(tokiotest.TEMP_FILE.name)

    h5_file = h5py.File(tokiotest.TEMP_FILE.name, 'r')
    summary1 = summarize_hdf5(h5_file)
    h5_file.close()

    subprocess.call(['cp', tokiotest.TEMP_FILE.name, 'poop2.hdf5'])

    num_compared = 0
    for metric in 'sums', 'shapes':
        for key, value in summary0[metric].iteritems():
            num_compared += 1
            assert key in summary1[metric]
            print "%s->%s->[%s] == [%s]?" % (metric, key, summary1[metric][key], value)
            assert summary1[metric][key] == value

    assert num_compared > 0
