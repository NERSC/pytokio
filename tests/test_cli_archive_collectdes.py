#!/usr/bin/env python
"""
Test the archive_collectdes.py tool
"""

import os
import datetime
import warnings
import nose
import h5py
import tokio.connectors.hdf5
import tokiotest
import tokio.cli.archive_collectdes

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
# ./bin/archive_collectdes.py --init-start 2017-12-13T00:00:00 \
#                           --init-end 2017-12-14T00:00:00 \
#                           --input output.json \
#                           --num-bbnodes 288 \
#                           --timestep 10 \
#                           2017-12-13T00:00:00 2017-12-13T01:00:00
# ./bin/summarize_bbhdf5.py output.hdf5
# ./bin/archive_collectdes.py --debug --input output.json 2017-12-13T00:15:00 2017-12-13T00:30:00
# ./bin/summarize_bbhdf5.py output.hdf5
#
# Assert that the two instances of summarize_bbhdf5.py return identical results
#

def generate_tts(output_file,
                 init_start=tokiotest.SAMPLE_COLLECTDES_START,
                 init_end=tokiotest.SAMPLE_COLLECTDES_END,
                 input_file=tokiotest.SAMPLE_COLLECTDES_FILE,
                 query_start=tokiotest.SAMPLE_COLLECTDES_START,
                 query_end=tokiotest.SAMPLE_COLLECTDES_END):
    """Create a TokioTimeSeries output file
    """
    argv = ['--init-start', init_start,
            '--init-end', init_end,
            '--input', input_file,
            '--num-nodes', str(tokiotest.SAMPLE_COLLECTDES_NUMNODES),
            '--ssds-per-node', str(tokiotest.SAMPLE_COLLECTDES_SSDS_PER),
            '--timestep', str(tokiotest.SAMPLE_COLLECTDES_TIMESTEP),
            '--output', output_file,
            query_start,
            query_end]
    print("Running [%s]" % ' '.join(argv))
    tokio.cli.archive_collectdes.main(argv)
    print("Created %s" % output_file)

def update_tts(output_file,
               input_file=tokiotest.SAMPLE_COLLECTDES_FILE2,
               query_start=tokiotest.SAMPLE_COLLECTDES_START2,
               query_end=tokiotest.SAMPLE_COLLECTDES_END2):
    """Append to an existing tts file
    """
    assert os.path.isfile(output_file) # must update an existing file

    argv = ['--input', input_file,
            '--output', output_file,
            query_start,
            query_end]

    print("Running [%s]" % ' '.join(argv))
    tokio.cli.archive_collectdes.main(argv)
    print("Updated %s" % output_file)

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
            summary['sums'][obj_name] = obj_data[...].sum()
            print("dataset %s version = %s" % (obj_name, hdf5_file.get_version(obj_name)))

    hdf5_file.visititems(characterize_object)

    return summary

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_idempotency():
    """
    cli.archive_collectdes cpuload idempotency
    """
    tokiotest.TEMP_FILE.close()

    # initialize a new TimeSeries, populate it, and write it out as HDF5
    generate_tts(output_file=tokiotest.TEMP_FILE.name,
                 init_start=tokiotest.SAMPLE_COLLECTDES_START,
                 init_end=tokiotest.SAMPLE_COLLECTDES_END,
                 input_file=tokiotest.SAMPLE_COLLECTDES_CPULOAD,
                 query_start=tokiotest.SAMPLE_COLLECTDES_START,
                 query_end=tokiotest.SAMPLE_COLLECTDES_END)

    h5_file = tokio.connectors.hdf5.Hdf5(tokiotest.TEMP_FILE.name, 'r')
    summary0 = summarize_hdf5(h5_file)
    h5_file.close()

    # append an overlapping subset of data to the same HDF5
    update_tts(output_file=tokiotest.TEMP_FILE.name,
               input_file=tokiotest.SAMPLE_COLLECTDES_CPULOAD,
               query_start=tokiotest.SAMPLE_COLLECTDES_START,
               query_end=tokiotest.SAMPLE_COLLECTDES_END)
    h5_file = tokio.connectors.hdf5.Hdf5(tokiotest.TEMP_FILE.name, 'r')
    summary1 = summarize_hdf5(h5_file)
    h5_file.close()

    # ensure that updating the overlapping data didn't change the contents of the TimeSeries
    num_compared = 0
    for metric in 'sums', 'shapes':
        for key, value in summary0[metric].items():
            num_compared += 1
            assert key in summary1[metric]
            print("%s->%s->[%s] == [%s]?" % (metric, key, summary1[metric][key], value))
            assert summary1[metric][key] == value

    assert num_compared > 0



@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_out_of_bounds():
    """
    cli.archive_collectdes with out-of-bounds
    """
    tokiotest.TEMP_FILE.close()

    # Calculate new bounds that are a subset of the actual data that will be returned
    orig_start_dt = datetime.datetime.strptime(tokiotest.SAMPLE_COLLECTDES_START,
                                               "%Y-%m-%dT%H:%M:%S")
    orig_end_dt = datetime.datetime.strptime(tokiotest.SAMPLE_COLLECTDES_END,
                                             "%Y-%m-%dT%H:%M:%S")
    orig_delta = orig_end_dt - orig_start_dt
    new_start_dt = orig_start_dt + orig_delta/3
    new_end_dt = orig_end_dt - orig_delta/3

    argv = ['--init-start', new_start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            '--init-end', new_end_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            '--input', tokiotest.SAMPLE_COLLECTDES_FILE,
            '--num-nodes', str(tokiotest.SAMPLE_COLLECTDES_NUMNODES),
            '--ssds-per-node', str(tokiotest.SAMPLE_COLLECTDES_SSDS_PER),
            '--timestep', str(tokiotest.SAMPLE_COLLECTDES_TIMESTEP),
            '--output', tokiotest.TEMP_FILE.name,
            tokiotest.SAMPLE_COLLECTDES_START,
            tokiotest.SAMPLE_COLLECTDES_END]
    print("Running [%s]" % ' '.join(argv))
    with warnings.catch_warnings(record=True) as warn:
        warnings.simplefilter("always")
        tokio.cli.archive_collectdes.main(argv)
        print("Caught %d warnings" % len(warn))
        assert len(warn) > 0
