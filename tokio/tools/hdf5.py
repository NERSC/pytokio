#!/usr/bin/env python
"""Retrieve data from TOKIO Time Series files using time as inputs

Provides a mapping between dates and times and a site's time-indexed repository
of TOKIO Time Series HDF5 files.
"""

import datetime
import tokio.tools.common
import tokio.connectors.hdf5

def enumerate_h5lmts(fsname, datetime_start, datetime_end):
    """Return all time-indexed HDF5 files falling between a time range

    Given a starting datetime and (optionally) an ending datetime, return all
    HDF5 files that contain data inside of that date range (inclusive).
    """
    return tokio.tools.common.enumerate_dated_files(start=datetime_start,
                                                    end=datetime_end,
                                                    template=tokio.config.CONFIG['hdf5_files'],
                                                    lookup_key=fsname,
                                                    match_first=True)

def get_files_and_indices(fsname, dataset_name, datetime_start, datetime_end):
    """
    Given the name of an Hdf5 file and a start/end date+time, returns a list of
    tuples containing
    """
    if datetime_end is None:
        datetime_end = datetime_start
    else:
        datetime_end = datetime_end
    h5lmt_files = enumerate_h5lmts(fsname, datetime_start, datetime_end)
    output = []

    for h5lmt_file in h5lmt_files:
        with tokio.connectors.hdf5.Hdf5(h5lmt_file, mode="r") as hdf5:
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

        output.append((h5lmt_file, i_0, i_f))
    return output

def get_dataframe_from_time_range(fsname, dataset_name, datetime_start, datetime_end):
    """
    Returns the same content as get_group_data_from_time_range into a dataframe
    """
    files_and_indices = get_files_and_indices(fsname, dataset_name, datetime_start, datetime_end)
    result = None

    if not files_and_indices:
#       raise IOError("No relevant hdf5 files found in %s between %s - %s" % (
#           tokio.config.CONFIG['h5lmt_file'],
#           datetime_start,
#           datetime_end))
        return result

    for hdf_filename in enumerate_h5lmts(fsname, datetime_start, datetime_end):
        with tokio.connectors.hdf5.Hdf5(hdf_filename, mode='r') as hdf_file:
            df_slice = hdf_file.to_dataframe(dataset_name)
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
