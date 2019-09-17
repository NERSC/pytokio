#!/usr/bin/env python
"""Retrieve data from TOKIO Time Series files using time as inputs

Provides a mapping between dates and times and a site's time-indexed repository
of TOKIO Time Series HDF5 files.
"""

import datetime
import tokio.tools.common
import tokio.connectors.hdf5


def enumerate_h5lmts(fsname, datetime_start, datetime_end):
    """Alias for :meth:`tokio.tools.hdf5.enumerate_hdf5`"""
    return enumerate_hdf5(fsname, datetime_start, datetime_end)

def enumerate_hdf5(fsname, datetime_start, datetime_end):
    """Returns all time-indexed HDF5 files falling between a time range

    Given a starting and ending datetime, returns the names of all HDF5 files
    that should contain data falling within that date range (inclusive).

    Args:
        fsname (str): Logical file system name; should match a key within
            the ``hdf5_files`` config item in ``site.json``.
        datetime_start (datetime.datetime): Begin including files corresponding
            to this start date, inclusive.
        datetime_end (datetime.datetime): Stop including files with timestamps
            that follow this end date.  Resulting files _will_ include this
            date.

    Returns:
        list: List of strings, each describing a path to an existing HDF5 file
        that should contain data relevant to the requested start and end
        dates.
    """
    return tokio.tools.common.enumerate_dated_files(start=datetime_start,
                                                    end=datetime_end,
                                                    template=tokio.config.CONFIG['hdf5_files'],
                                                    lookup_key=fsname,
                                                    match_first=True)

def get_files_and_indices(fsname, dataset_name, datetime_start, datetime_end):
    """Retrieve filenames and indices within files corresponding to a date range

    Given a logical file system name and a dataset within that file system's
    TOKIO Time Series files, return a list of all file names and the indices
    within those files that fall within the specified date range.

    Args:
        fsname (str): Logical file system name; should match a key within
            the ``hdf5_files`` config item in ``site.json``.
        dataset_name (str): Name of a TOKIO Time Series dataset name
        datetime_start (datetime.datetime): Begin including files corresponding
            to this start date, inclusive.
        datetime_end (datetime.datetime): Stop including files with timestamps
            that follow this end date.  Resulting files _will_ include this
            date.

    Returns:
        list: List of three-item tuples of types (str, int, int), where

        * element 0 is the path to an existing HDF5 file
        * element 1 is the first index (inclusive) of ``dataset_name`` within
          that file containing data that falls within the specified date range
        * element 2 is the last index (exclusive) of ``dataset_name`` within
          that file containing data that falls within the specified date range

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
    """Generate a dataframe containing all relevant data within a date range

    Given a logical file system name and a dataset within that file system's
    TOKIO Time Series files, return a dataframe containing all relevant data
    falling within the given time range from that dataset.  Spans multiple HDF5
    files if necessary.

    Args:
        fsname (str): Logical file system name; should match a key within
            the ``hdf5_files`` config item in ``site.json``.
        dataset_name (str): Name of a TOKIO Time Series dataset name
        datetime_start (datetime.datetime): Begin including files corresponding
            to this start date, inclusive.
        datetime_end (datetime.datetime): Stop including files with timestamps
            that follow this end date.  Resulting files _will_ include this
            date.

    Returns:
        pandas.DataFrame or None: DataFrame, indexed in time, containing all of
        the relevant data from ``dataset_name`` starting at ``datetime_start``
        (inclusive) and ending at ``datetime_end`` (exclusive)
    """
    result = None

    hdf5_filenames = enumerate_h5lmts(fsname, datetime_start, datetime_end)
    if not hdf5_filenames:
        return result

    for hdf_filename in hdf5_filenames:
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
    return result
