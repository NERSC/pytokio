#!/usr/bin/env python

import os
import tokio.tools.hdf5
import tokio.connectors.hdf5
from test_connectors_hdf5 import DATASETS_1D, DATASETS_2D
from datetime import datetime, timedelta

tokio.tools.hdf5.H5LMT_BASE = os.path.join(os.getcwd(), 'inputs' )
SAMPLE_INPUT = 'sample.h5lmt'
DATASET_NAME = 'FSStepsGroup/FSStepsDataSet'
with tokio.connectors.Hdf5(os.path.join(tokio.tools.hdf5.H5LMT_BASE, SAMPLE_INPUT)) as fp:
    t0 = fp[DATASET_NAME][0]
    dt = int(fp[DATASET_NAME][1] - t0)

# Tuple of (offset relative to start of first day, duration) 
# Make sure these always touch exactly two calendar days
TIME_OFFSETS = [
    # > 24 hours, but still two calendar days
    (timedelta(hours=3, minutes=25, seconds=45), 
     timedelta(days=1, hours=2, minutes=35, seconds=15)),
    # < 24 hours landing exactly on the first record of the second calendar day
    (timedelta(hours=3, minutes=25, seconds=45), 
     timedelta(days=0, hours=20, minutes=34, seconds=15)),
    # > 24 hours, starting on the first record of the first day
    (timedelta(hours=0, minutes=0, seconds=0),  
     timedelta(days=1, hours=0, minutes=0, seconds=dt)),
    # << 24 hours but straddling two days
    (timedelta(hours=21, minutes=35, seconds=25), 
     timedelta(days=0, hours=12, minutes=0, seconds=0)),
]

def check_get_files_and_indices(start_offset, duration):
    start_time = datetime.fromtimestamp(t0) + start_offset
    end_time = start_time + duration
    # Make sure we're touching at least two files
    assert (end_time.date() - start_time.date()).days == 1

    files_and_indices = tokio.tools.hdf5.get_files_and_indices(SAMPLE_INPUT, start_time, end_time)
    for count, (file_name, istart, iend) in enumerate(files_and_indices):
        with tokio.connectors.Hdf5(file_name, mode='r') as file:
            derived_start = datetime.fromtimestamp(file[DATASET_NAME][istart])
            derived_end = datetime.fromtimestamp(file[DATASET_NAME][iend])
            assert (derived_start == start_time) or istart == 0
            assert (derived_end == end_time - timedelta(seconds=dt)) or iend == -1 

def check_get_dataframe_from_time_range(dataset_name, start_offset, duration):
    start_time = datetime.fromtimestamp(t0) + start_offset
    end_time = start_time + duration
    # Make sure we're touching at least two files
    assert (end_time.date() - start_time.date()).days == 1
    result = tokio.tools.hdf5.get_dataframe_from_time_range(SAMPLE_INPUT, dataset_name, start_time, end_time)
    assert result.index[0] == start_time
    assert result.index[-1] == end_time - timedelta(seconds=dt)

def test():
    for (start_offset, duration) in TIME_OFFSETS:
        yield check_get_files_and_indices, start_offset, duration
        for dataset_name in DATASETS_1D + DATASETS_2D:
            yield check_get_dataframe_from_time_range, dataset_name, start_offset, duration
