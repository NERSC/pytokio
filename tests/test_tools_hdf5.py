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


SAMPLE_H5LMT_FILE_BN = os.path.basename(tokiotest.SAMPLE_H5LMT_FILE)
FAKE_FSNAME = 'fakefs'

TIMESTAMPS = tokio.connectors.hdf5.Hdf5(tokiotest.SAMPLE_H5LMT_FILE).get_timestamps('/datatargets/readbytes')[...]
TIME_0, TIME_1 = TIMESTAMPS[0:2]
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
    print("TIME_0 =", TIME_0)
    print("TIME_1 =", TIME_1)
    print("LMT_TIMESTEP=", LMT_TIMESTEP)

    start_time = datetime.datetime.fromtimestamp(TIME_0) + start_offset
    end_time = start_time + duration
    print("Getting [%s to %s)" % (start_time, end_time))
    # Make sure we're touching at least two files
    assert (end_time.date() - start_time.date()).days == 1

    files_and_indices = tokio.tools.hdf5.get_files_and_indices(
        fsname=FAKE_FSNAME,
        dataset_name=dataset_name,
        datetime_start=start_time,
        datetime_end=end_time)

    assert len(files_and_indices) > 0

    for (file_name, istart, iend) in files_and_indices:
        with tokio.connectors.hdf5.Hdf5(file_name, mode='r') as hdf_file:
            timestamps = hdf_file.get_timestamps('/datatargets/readbytes')[...]
            derived_start = datetime.datetime.fromtimestamp(timestamps[istart])
            derived_end = datetime.datetime.fromtimestamp(timestamps[iend])

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

    result = tokio.tools.hdf5.get_dataframe_from_time_range(
        fsname=FAKE_FSNAME,
        dataset_name=dataset_name,
        datetime_start=start_time,
        datetime_end=end_time)

    assert len(result) > 0

    assert result.index[0] == start_time
    assert result.index[-1] == end_time - datetime.timedelta(seconds=LMT_TIMESTEP)

def test():
    """
    Correctness of tools.hdf5 edge cases
    """
    for (description, start_offset, duration) in TIME_OFFSETS:
        for dataset_name in tokiotest.TIMESERIES_DATASETS_MOST:
            func = check_get_files_and_indices
            func.description = "tools.hdf5.get_files_and_indices(%s): %s" % (dataset_name,
                                                                             description)
            yield func, dataset_name, start_offset, duration

            func = check_get_df_from_time_range
            func.description = "tools.hdf5.get_df_from_time_range(%s): %s" % (dataset_name,
                                                                              description)
            yield func, dataset_name, start_offset, duration
