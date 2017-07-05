#!/usr/bin/env python

import os
import datetime
import numpy as np
from .. import connectors
from ..debug import debug_print as _debug_print
# For repack_h5lmt
import subprocess
import tempfile
import ConfigParser

cfg = ConfigParser.ConfigParser()
cfg.read(os.path.join('..', 'tokio', 'tokio.cfg'))
H5LMT_BASE = eval(cfg.get('tokio', 'H5LMT_BASE'))
if H5LMT_BASE is None:
    H5LMT_BASE = eval(cfg.get('tokio', 'H5LMT_BASE_NERSC'))


def enumerate_h5lmts(file_name, datetime_start, datetime_end):
    """
    Given a starting datetime and (optionnally) an ending datetime, return all
    H5LMT files that contain data inside of that date range (inclusive).

    # :param str file_name: The basename of the H5LMT files you want to find (e.g., cori_snx11168.h5lmt)
    # :param datetime_start: The first day of H5LMT data you want to collect
    # :type datetime_start: datetime.datetime
    # :param datetime_end: The last day of H5LMT data you want to collect
    # :type datetime_end: datetime.datetime or None
    # :return: fully qualified paths to existing H5LMT files
    # :rtype: list(str)
    
    """   
    if datetime_start is None:
        raise Exception('A start time must be provided')
    if datetime_end is None: 
        datatime_end = datetime.datetime.day()
    day = datetime_start
    h5lmt_files = []
    while day.date() <= datetime_end.date():
        h5lmt_file = os.path.join(H5LMT_BASE, day.strftime("%Y-%m-%d"), file_name)
        if os.path.isfile(h5lmt_file):
            h5lmt_files.append(h5lmt_file)
        day += datetime.timedelta(days=1)
    return h5lmt_files


def get_files_and_indices(file_name, datetime_start, datetime_end):
    """
    Given the name of an HDF5 file and a start/end date+time, returns a list of
    tuples containing
    
    """
    if datetime_end is None:
        datetime_end_local = datetime_start
    else:
        datetime_end_local = datetime_end
    h5lmt_files = enumerate_h5lmts(file_name, datetime_start, datetime_end)
    output = []

    for h5lmt_file in h5lmt_files:
        hdf5 = connectors.hdf5.HDF5(h5lmt_file, mode="r")
        if hdf5.first_timestamp is None or hdf5.last_timestamp is None:
            raise Exception("uninitialized tokio.hdf5 object")
        i_0 = 0
        if hdf5.first_timestamp <= datetime_start:
            i_0 = hdf5.get_index(datetime_start) # This is the first day's hdf5
        i_f = -1
        if hdf5.last_timestamp >= datetime_end:
            # This is the last day's hdf5
            i_f = hdf5.get_index(datetime_end) - 1  
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

def get_metadata_from_time_range(file_name, datetime_start, datetime_end):
    """
    This routine returns a dict with a few specific metadata
    attributes in it which can be stored by the calling application.
    
    Because pytokio returns data in a numpy array, certain metadata gets lost.
    This is particularly bothersome for MDSOpsDataSet, where the 'OpNames'
    attribute is required to understand what each column in the np.array
    correspond to.  
    
    """
    result = {}
    for (h5file, i_0, i_f) in get_files_and_indices(file_name, datetime_start, datetime_end):
        with connectors.hdf5.HDF5(h5file, mode='r') as f:
            # Copy over MDS op names
            op_names = list(f['MDSOpsGroup/MDSOpsDataSet'].attrs['OpNames'])
            if 'OpNames' in result:
                if op_names != result['OpNames']:
                    raise Exception("Inconsistent OpNames found across different H5LMT files")
            else:
                result['OpNames'] = op_names

            # Copy over OST names
            ost_names = list(f['OSTReadGroup/OSTBulkReadDataSet'].attrs['OSTNames'])
            if 'OSTNames' in result:
                if ost_names != result['OSTNames']:
                    raise Exception("Inconsistent OSTNames found across different H5LMT files")
            else:
                result['OSTNames'] = ost_names
    return result

def get_group_data_from_time_range(file_name, group_name, datetime_start, datetime_end):
    """
    Returns a numpy array containing all the data from the given group 
    across all of the files of the given filename during the time 
    range specified.
    
    """    
    files_and_indices = get_files_and_indices(file_name, 
                                              datetime_start, 
                                              datetime_end)
    result = None

    for (h5file, i_0, i_f) in files_and_indices:
        with connectors.hdf5.HDF5(h5file, mode='r') as f:
            # Version : depending on the O.S /
            version = f['/'].attrs.get('version', 1)
            if len(file[group_name].shape) == 1:
                dataset_slice = file[group_name][i_0:i_f]
                axis = 0
            elif len(f[group_name].shape) == 2:
                if version == 1:
                    dataset_slice = f[group_name][:,i_0:i_f]
                    axis = 1
                else:
                    dataset_slice = f[group_name][i_0:i_f,:]
                    axis = 0
            elif len(f[group_name].shape) == 3:
                if version == 1:
                    ### no idea if this is correct
                    dataset_slice = f[group_name][:,i_0:i_f,:]
                    axis = 1
                else:
                    dataset_slice = f[group_name][i_0:i_f,:,:]
                    axis = 0
            else:
                raise Exception("Dimensions of %s in %s are greater than 3" 
                                % (group_name, h5file))

            if result is None:
                result = dataset_slice
            else:
                result = np.concatenate([result, dataset_slice], axis=axis)
            _debug_print("added %s from %5d to %5d" % (h5file, i_0, i_f))
    return result

def get_dataframe_from_time_range(file_name, group_name, datetime_start, datetime_end):
    """
    Returns the same content as get_group_data_from_time_range 
    into a dataframe
    
    """
    files_and_indices = get_files_and_indices(file_name, datetime_start, datetime_end)
    if not files_and_indices:
        raise Exception("no relevant hdf5 files found")
    result = None

    for h5file in enumerate_h5lmts(file_name, datetime_start, datetime_end):
        with connectors.hdf5.HDF5(h5file, mode='r') as f:
            df_slice = f.to_dataframe(group_name)
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


def repack_h5lmt(src, dest, datasets):
    """
    Take a source hdf5 file and a set of datasets and produce a dest hdf5 file
    that contains only those datasets and that has been repacked.

    # :param str src: Path to input H5LMT file
    # :param str dest: Path to the intended output H5LMT file
    # :param datasets: Datasets to copy from src to dest
    # :type datasets: list(str)
    # :return: the last nonzero exit code
    # :rtype: int
    """
    if not os.path.isfile(src):
        return -1
    ret = 0

    with tempfile.NamedTemporaryFile() as temp:
        # Copy the relevant datasets
        for dset in datasets:
            cmd_args = ["h5copy", "-i", src, "-o", temp.name, "-s", dset, "-d", dset, "-p"]
            ret_tmp = subprocess.call(cmd_args)
            # Only copy nonzero exit codes into return value
            if ret_tmp: ret = ret_tmp
   
        cmd_args = ["h5repack", "-L", "-v", "-f", "GZIP=1", temp.name, dest]
        ret_tmp = subprocess.call(cmd_args)
        # Only copy nonzero exit codes into return value
        if ret_tmp: ret = ret_tmp
    return ret

