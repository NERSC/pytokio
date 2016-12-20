#!/usr/bin/env python
"""
Miscellaneous tools that are functionally handy for dealing with files and data
sources used by the TOKIO framework.  This particular package is not intended to
have a stable API; as different functions prove themselves here, they will be
incorporated into the formal TOKIO package API.
"""

import tokio

import os

import datetime
import h5py

### for repack_h5lmt
import subprocess
import tempfile


if os.environ.get("H5LMT_BASE"):
    H5LMT_BASE = os.environ.get("H5LMT_BASE")
else:
    ### default location at NERSC
    H5LMT_BASE = "/global/project/projectdirs/pma/www/daily"

def enumerate_h5lmts( file_name, datetime_start, datetime_end=None ):
    """
    Given a starting datetime and (optionally) an ending datetime, return all
    H5LMT files that contain data inside of that date range (inclusive).

    :param str file_name: The basename of the H5LMT files you want to find (e.g., cori_snx11168.h5lmt)
    :param datetime_start: The first day of H5LMT data you want to collect
    :type datetime_start: datetime.datetime
    :param datetime_end: The last day of H5LMT data you want to collect
    :type datetime_end: datetime.datetime or None
    :return: fully qualified paths to existing H5LMT files (if datetime_end is not None), a single fully qualified path to an H5LMT file (if datetime_end is None), or None (if datetime_end is None)
    :rtype: list(str) or str or None
    """
    if datetime_end is None:
        datetime_end_local = datetime_start
    else:
        datetime_end_local = datetime_end

    today = datetime_start
    h5lmt_files = []
    while today <= datetime_end_local:
        h5lmt_file = os.path.join( H5LMT_BASE, today.strftime("%Y-%m-%d"), file_name )
        if os.path.isfile( h5lmt_file ):
            h5lmt_files.append( h5lmt_file )

        today += datetime.timedelta(days=1)

    if datetime_end is None:
        if len(h5lmt_files) == 0:
            return None
        else:
            return h5lmt_files[0]
    else:
        return h5lmt_files

def repack_h5lmt( src, dest, datasets ):
    """
    Take a source hdf5 file and a set of datasets and produce a dest hdf5 file
    that contains only those datasets and that has been repacked.

    :param str src: Path to input H5LMT file
    :param str dest: Path to the intended output H5LMT file
    :param datasets: Datasets to copy from src to dest
    :type datasets: list(str)
    :return: the last nonzero exit code
    :rtype: int
    """
    if not os.path.isfile(src):
        return -1

    ret = 0
    with tempfile.NamedTemporaryFile() as temp:
        ### copy the relevant datasets
        for dset in datasets:
            cmd_args = ["h5copy", "-i", src, "-o", temp.name, "-s", dset, "-d", dset, "-p"]
            ret_tmp = subprocess.call( cmd_args )
            ### only copy nonzero exit codes into return value
            if ret_tmp != 0: ret = ret_tmp

        cmd_args = ["h5repack", "-L", "-v", "-f", "GZIP=1", temp.name, dest]
        ret_tmp = subprocess.call( cmd_args )
        ### only copy nonzero exit codes into return value
        if ret_tmp != 0: ret = ret_tmp

    return ret

if __name__ == '__main__':
    pass
