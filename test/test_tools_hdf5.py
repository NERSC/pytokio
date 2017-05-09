#!/usr/bin/env python

import os
import datetime
import tokio.tools.hdf5
import tokio.connectors.hdf5
from test_connectors_hdf5 import DATASETS_1D, DATASETS_2D

import nose.tools

tokio.tools.hdf5.H5LMT_BASE = os.path.join(os.getcwd(), 'inputs' )

SAMPLE_INPUT = 'sample.hdf5'

with tokio.connectors.hdf5.HDF5(os.path.join(tokio.tools.hdf5.H5LMT_BASE, SAMPLE_INPUT)) as fp:
    t00 = fp['FSStepsGroup/FSStepsDataSet'][0]
    dt = int(fp['FSStepsGroup/FSStepsDataSet'][1] - t00)

### tuple of (offset relative to start of first day, duration)
TIME_OFFSETS = [
    (datetime.timedelta(hours=3, minutes=25, seconds=45), datetime.timedelta(days=1, hours=2, minutes=35, seconds=15)),
    (datetime.timedelta(hours=3, minutes=25, seconds=45), datetime.timedelta(days=0, hours=20, minutes=34, seconds=15)),
    (datetime.timedelta(hours=0, minutes=0, seconds=0),   datetime.timedelta(days=1, hours=0, minutes=0, seconds=dt)),
]

def check_get_files_and_indices(start_offset, duration):
    start_time = datetime.datetime.fromtimestamp(t00) + start_offset
    end_time = start_time + duration

    ### make sure we're touching at least two files
    assert (end_time.date() - start_time.date()).days == 1

    files_and_indices = tokio.tools.hdf5.get_files_and_indices(SAMPLE_INPUT, start_time, end_time)
    for count, (file_name, istart, iend) in enumerate(files_and_indices):
        with tokio.connectors.HDF5(file_name, mode='r') as f:
            derived_start = datetime.datetime.fromtimestamp(f['FSStepsGroup/FSStepsDataSet'][istart])
            derived_end = datetime.datetime.fromtimestamp(f['FSStepsGroup/FSStepsDataSet'][iend])

            assert (derived_start == start_time) \
                or (count > 0 and istart == 0)
            assert (derived_end == end_time - datetime.timedelta(seconds=dt)) \
                or (count < (len(files_and_indices)-1) and iend == -1) 

def check_get_dataframe_from_time_range(dataset_name, start_offset, duration):
    start_time = datetime.datetime.fromtimestamp(t00) + start_offset
    end_time = start_time + duration

    ### make sure we're touching at least two files
    assert (end_time.date() - start_time.date()).days == 1

    result = tokio.tools.hdf5.get_dataframe_from_time_range(SAMPLE_INPUT, dataset_name, start_time, end_time)

    assert result.index[0] == start_time
    assert result.index[-1] == end_time - datetime.timedelta(seconds=dt)

def test():
    for (start_offset, duration) in TIME_OFFSETS:
        yield check_get_files_and_indices, start_offset, duration

        for dataset_name in DATASETS_1D + DATASETS_2D:
            yield check_get_dataframe_from_time_range, dataset_name, start_offset, duration
