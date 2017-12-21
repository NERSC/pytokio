#!/usr/bin/env python
"""
tokio.timeseries.TimeSeries methods
"""

import sys
import random
import shutil
import warnings
import h5py
import nose
import numpy 
import tokio
import tokiotest

def compare_timeseries(timeseries1, timeseries2, verbose=False):
    """
    Compare two TimeSeries objects' datasets column by column
    """
    for timeseries1_index, column in enumerate(list(timeseries1.columns)):
        timeseries2_index = timeseries2.column_map[column]
        if verbose:
            col_sum1 = timeseries1.dataset[:, timeseries1_index].sum()
            col_sum2 = timeseries2.dataset[:, timeseries2_index].sum()
            print "%-14s: %.16e vs. %.16e" % (column, col_sum1, col_sum2)
        assert numpy.array_equal(timeseries1.dataset[:, timeseries1_index], timeseries2.dataset[:, timeseries2_index])

def generate_timeseries(file_name=tokiotest.SAMPLE_COLLECTDES_HDF5):
    """
    Return a TimeSeries object that's initialized against the tokiotest sample
    input
    """
    output_hdf5 = h5py.File(file_name, 'r')
    target_dset_name = tokiotest.SAMPLE_COLLECTDES_DSET
    target_group_name = '/'.join(tokiotest.SAMPLE_COLLECTDES_DSET.split('/')[0:-1])
    target_group = output_hdf5[target_group_name]
    timeseries = tokio.timeseries.TimeSeries(group=target_group)
    timeseries.attach_dataset(dataset=output_hdf5[target_dset_name])
    return timeseries

def test_rearrange():
    """
    TimeSeries.rearrange_columns()
    """
    timeseries1 = generate_timeseries()
    timeseries2 = generate_timeseries()

    # test random reordering
    new_col_order = list(timeseries2.columns[:])
    print new_col_order
    random.shuffle(new_col_order)
    print new_col_order
    timeseries2.rearrange_columns(new_col_order)

    compare_timeseries(timeseries2, timeseries1, verbose=True)

def test_sort():
    """
    TimeSeries.sort_columns()
    """
    timeseries1 = generate_timeseries()
    timeseries2 = generate_timeseries()

    timeseries2.sort_columns()
    compare_timeseries(timeseries2, timeseries1, verbose=True)

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_commit_dataset():
    """
    TimeSeries.commit_dataset()
    """
    tokiotest.TEMP_FILE.close()

    # Connect to the sample input file
    timeseries1 = generate_timeseries()
    hdf5_file = h5py.File(tokiotest.TEMP_FILE.name, 'w')
    # Write the output out as a new HDF5 file
    timeseries1.commit_dataset(hdf5_file)
    hdf5_file.close()

    # Read that newly generated HDF5 file back in
    timeseries2 = generate_timeseries(file_name=tokiotest.TEMP_FILE.name)

    # Compare the original to the reprocessed
    compare_timeseries(timeseries2, timeseries1, verbose=True)

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_uneven_columns():
    """
    TimeSeries.attach_dataset2d with uneven columns
    """

    # We expect to trigger some warnings here
    warnings.filterwarnings('ignore')

    tokiotest.TEMP_FILE.close()
    # copy the input file into a new temporary file with which we can tinker
    with open(tokiotest.SAMPLE_COLLECTDES_HDF5, 'r') as input_file:
        with open(tokiotest.TEMP_FILE.name, 'w') as output_file:
            shutil.copyfileobj(input_file, output_file)

    # ensure that the input dataset has even column lengths before we make them uneven
    h5_file = h5py.File(tokiotest.TEMP_FILE.name, 'r+')
    dataset = h5_file[tokiotest.SAMPLE_COLLECTDES_DSET]
    orig_col_names = list(dataset.attrs[tokio.timeseries.COLUMN_NAME_KEY])
    print len(orig_col_names), "==", dataset.shape[1], "?"
    assert len(orig_col_names) == dataset.shape[1]

    # corrupt the columns of a dataset by adding too many column names
    extra_columns = orig_col_names + ['argle', 'bargle', 'fffff']
    dataset.attrs[tokio.timeseries.COLUMN_NAME_KEY] = extra_columns

    # load the file with its messed up columns and verify
    timeseries = generate_timeseries(file_name=tokiotest.TEMP_FILE.name)
    print len(timeseries.columns), "==", timeseries.dataset.shape[1], "?"
    assert len(timeseries.columns) == timeseries.dataset.shape[1]

    # corrupt the columns of a dataset by deleting some names
    fewer_columns = orig_col_names[0:-(3*len(orig_col_names)/4)]
    dataset.attrs[tokio.timeseries.COLUMN_NAME_KEY] = fewer_columns

    # load the file with its messed up columns and verify
    timeseries = generate_timeseries(file_name=tokiotest.TEMP_FILE.name)
    print len(timeseries.columns), "==", timeseries.dataset.shape[1], "?"
    assert len(timeseries.columns) == timeseries.dataset.shape[1]
