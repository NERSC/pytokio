#!/usr/bin/env python
"""
Demonstrate the pandas interface to TOKIO HDF5 files by loading multiple days'
worth of data at once and presenting them.
"""

import argparse
import tokio.tools
import datetime

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--hdf5file", default='cori_snx11168.h5lmt', help="file system h5lmt file name (e.g., cori_snx11168.h5lmt)")
    parser.add_argument("-d", "--dataset", default='OSTWriteGroup/OSTBulkWriteDataSet', help="path of lfs-df.txt file or ost-map.txt")
    parser.add_argument("-s", "--start", default=None, help="start date and time in YYYY-MM-DD HH:MM:SS format")
    parser.add_argument("-e", "--end", default=None, help="end date and time in YYYY-MM-DD HH:MM:SS format")
    args = parser.parse_args()

    if args.end is None:
        t_stop = datetime.datetime.now()
    else:
        t_stop = datetime.datetime.strptime(args.end, "%Y-%m-%d %H:%M:%S")

    if args.start is None:
        t_start = t_stop - datetime.timedelta(days=2)
    else:
        t_start = datetime.datetime.strptime(args.start, "%Y-%m-%d %H:%M:%S")

    assert(t_stop > t_start)

    print tokio.tools.get_dataframe_from_time_range(
        args.hdf5file,
        args.dataset,
        t_start,
        t_stop
    ).describe()
