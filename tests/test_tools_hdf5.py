#!/usr/bin/env python
"""
Test HDF5 tools interfaces
"""

import os
import datetime
import tokiotest
import tokio.config
import tokio.tools.hdf5
import tokio.connectors.hdf5
from test_connectors_hdf5 import DATASETS_1D, DATASETS_2D

### For tokio.tools.hdf5, which is used by summarize_job.py
tokio.config.H5LMT_BASE_DIR = os.path.join(tokiotest.INPUT_DIR, "%Y-%m-%d")
tokio.config.LFSSTATUS_BASE_DIR = os.path.join(tokiotest.INPUT_DIR, "%Y-%m-%d")

SAMPLE_H5LMT_FILE_BN = os.path.basename(tokiotest.SAMPLE_H5LMT_FILE)
TIMESTAMPS_DATASET = 'FSStepsGroup/FSStepsDataSet'

TIME_0, TIME_1 = tokio.connectors.Hdf5(tokiotest.SAMPLE_H5LMT_FILE)[TIMESTAMPS_DATASET][0:2]
LMT_TIMESTEP = int(TIME_1 - TIME_0)

# Tuple of (offset relative to start of first day, duration)
# Make sure these always touch exactly two calendar days
TIME_OFFSETS = [
    ('> 24 hr between two days',
     datetime.timedelta(hours=3, minutes=25, seconds=45),
     datetime.timedelta(days=1, hours=2, minutes=35, seconds=15)),
    ('< 24 hr ending on 1st record of 2nd day',
     datetime.timedelta(hours=3, minutes=25, seconds=45),
     datetime.timedelta(days=0, hours=20, minutes=34, seconds=15)),
    ('> 24 hr starting on 1st record of 1st day',
     datetime.timedelta(hours=0, minutes=0, seconds=0),
     datetime.timedelta(days=1, hours=0, minutes=0, seconds=LMT_TIMESTEP)),
    ('<< 24 hr still straddling two days',
     datetime.timedelta(hours=21, minutes=35, seconds=25),
     datetime.timedelta(days=0, hours=12, minutes=0, seconds=0)),
]

def check_get_files_and_indices(dataset_name, start_offset, duration):
    """
    Enumerate input files from time range
    """
    start_time = datetime.datetime.fromtimestamp(TIME_0) + start_offset
    end_time = start_time + duration
    # Make sure we're touching at least two files
    assert (end_time.date() - start_time.date()).days == 1

    files_and_indices = tokio.tools.hdf5.get_files_and_indices(SAMPLE_H5LMT_FILE_BN,
                                                               dataset_name,
                                                               start_time,
                                                               end_time)
    for (file_name, istart, iend) in files_and_indices:
        with tokio.connectors.Hdf5(file_name, mode='r') as hdf_file:
            derived_start = datetime.datetime.fromtimestamp(hdf_file[TIMESTAMPS_DATASET][istart])
            derived_end = datetime.datetime.fromtimestamp(hdf_file[TIMESTAMPS_DATASET][iend])
            assert (derived_start == start_time) or istart == 0
            assert (derived_end == end_time - datetime.timedelta(seconds=LMT_TIMESTEP)) \
                or iend == -1

def check_get_df_from_time_range(dataset_name, start_offset, duration):
    """
    Retrieve DataFrame from time range
    """
    start_time = datetime.datetime.fromtimestamp(TIME_0) + start_offset
    end_time = start_time + duration
    # Make sure we're touching at least two files
    assert (end_time.date() - start_time.date()).days == 1
    result = tokio.tools.hdf5.get_dataframe_from_time_range(SAMPLE_H5LMT_FILE_BN,
                                                            dataset_name,
                                                            start_time,
                                                            end_time)
    assert result.index[0] == start_time
    assert result.index[-1] == end_time - datetime.timedelta(seconds=LMT_TIMESTEP)

def test():
    """
    Correctness of tools.hdf5 edge cases
    """
    for (description, start_offset, duration) in TIME_OFFSETS:
        for dataset_name in DATASETS_1D + DATASETS_2D:
            func = check_get_files_and_indices
            func.description = "tools.hdf5.get_files_and_indices(%s): %s" % (dataset_name, description)
            yield func, dataset_name, start_offset, duration

            func = check_get_df_from_time_range
            func.description = "tools.hdf5.get_df_from_time_range(%s): %s" % (dataset_name, description)
            yield func, dataset_name, start_offset, duration
