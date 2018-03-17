#!/usr/bin/env python

import os
import datetime
import tempfile
import subprocess
import numpy as np
import common
from .. import connectors, config
from ..debug import debug_print as _debug_print

def enumerate_h5lmts(file_name, datetime_start, datetime_end):
    """
    Given a starting datetime and (optionally) an ending datetime, return all
    H5LMT files that contain data inside of that date range (inclusive).
    
    """   
    h5lmt_files = common.enumerate_dated_dir(config.H5LMT_BASE_DIR,
                                             datetime_start,
                                             datetime_end,
                                             file_name=file_name)
    return h5lmt_files

def get_files_and_indices(file_name, dataset_name, datetime_start, datetime_end):
    """
    Given the name of an Hdf5 file and a start/end date+time, returns a list of
    tuples containing
    """
    if datetime_end is None:
        datetime_end_local = datetime_start
    else:
        datetime_end_local = datetime_end
    h5lmt_files = enumerate_h5lmts(file_name, datetime_start, datetime_end)
    output = []

    for h5lmt_file in h5lmt_files:
        hdf5 = connectors.hdf5.Hdf5(h5lmt_file, mode="r")

        i_0 = 0
        timestamps = hdf5.get_timestamps(dataset_name)
        if datetime.datetime.fromtimestamp(timestamps[0]) <= datetime_start:
            i_0 = hdf5.get_index(dataset_name, datetime_start) # This is the first day's hdf5

        i_f = -1
        if datetime.datetime.fromtimestamp(timestamps[-1]) >= datetime_end:
            # This is the last day's hdf5
            i_f = hdf5.get_index(dataset_name, datetime_end) - 1  
            # -1 because datetime_end should be exclusive
            #
            # If the last timestamp is on the first datapoint of a new day,
            # just drop the whole day to maintain exclusivity of the last
            # timestamp
            if i_f < 0:
                continue

        hdf5.close()
        output.append((h5lmt_file, i_0, i_f))
    return output

def get_dataframe_from_time_range(file_name, dataset_name, datetime_start, datetime_end):
    """
    Returns the same content as get_group_data_from_time_range into a dataframe
    """
    files_and_indices = get_files_and_indices(file_name, dataset_name, datetime_start, datetime_end)
    if not files_and_indices:
        raise IOError("No relevant hdf5 files found in %s" % config.H5LMT_BASE_DIR)
    result = None

    for h5file in enumerate_h5lmts(file_name, datetime_start, datetime_end):
        with connectors.hdf5.Hdf5(h5file, mode='r') as f:
            df_slice = f.to_dataframe(dataset_name)
            df_slice = df_slice[(df_slice.index >= datetime_start) 
                                & (df_slice.index < datetime_end)]
            if result is None:
                result = df_slice
            else:
                ### append a copy--I think this is memory-inefficient
                # result = result.append(df_slice)
                # concat ?
                ### append in place--maybe more efficient than .append??
                result = result.reindex(result.index.union(df_slice.index))
                result.loc[df_slice.index] = df_slice
    return result.sort_index()
