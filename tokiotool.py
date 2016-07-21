#!/usr/bin/env python
################################################################################
#
#  A generic CLI for the tokio python package.  Very much a work in progress.
#
#  Glenn K. Lockwood, Lawrence Berkeley National Laboratory            July 2016
#
################################################################################

import tokio
import tokio.hdf5
import tokio.lmt
import argparse
import datetime
import time
import numpy as np
import sys
import os

_DATE_FMT = "%Y-%m-%d %H:%M:%S"
_LMT_TIMESTEP = 5
_FILENAME = 'testfile.hdf5'

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('tstart',
                        type=str,
                        help="lower bound of time to scan, in YYY-mm-dd HH:MM:SS format")
    parser.add_argument('tstop',
                        type=str,
                        help="upper bound of time to scan, in YYY-mm-dd HH:MM:SS format")
    parser.add_argument('--debug',
                        action='store_true',
                        help="produce debug messages")
    args = parser.parse_args()
    if not (args.tstart and args.tstop):
        parser.print_help()
        sys.exit(1)
    if args.debug:
        tokio.DEBUG = True

    try:
        t_start = datetime.datetime.strptime(args.tstart, _DATE_FMT)
        t_stop = datetime.datetime.strptime(args.tstop, _DATE_FMT)
    except ValueError:
        sys.stderr.write("Start and end times must be in format %s\n" % _DATE_FMT)
        raise

    lmtdb = tokio.lmt.connect()

    ### initialize hdf5 file - always init to a full day, then populate later
    ost_ct = len(lmtdb.ost_names)
    init_start = t_start.replace(hour=0,minute=0,second=0,microsecond=0)
    init_stop = t_start + datetime.timedelta(days=1)
    ts_ct = int((init_stop - init_start).total_seconds() / _LMT_TIMESTEP)

    if os.path.isfile( _FILENAME ):
        os.unlink( _FILENAME )
    h5f = tokio.hdf5.connect(_FILENAME)
    h5f.init_datasets(ost_ct, ts_ct, host='cori', filesystem='snx11168')
    h5f.init_timestamps(init_start, init_stop)

    ### now populate the slice of time input by user
    idx_0 = h5f.get_index( t_start )
    idx_f = h5f.get_index( t_stop )
    ts_ct = int((t_stop - t_start).total_seconds() / _LMT_TIMESTEP)

    ### add +1 row to hold the data immediately before our time range
    buf_r = np.full(shape=(ts_ct+1, ost_ct), fill_value=-0.0, dtype='f8')
    buf_w = np.full(shape=(ts_ct+1, ost_ct), fill_value=-0.0, dtype='f8')

    ### populate all but the first row with the data from our time range of
    ### interest
    buf_r[1:,:], buf_w[1:,:] = lmtdb.get_rw_data(t_start, t_stop, _LMT_TIMESTEP)
    ### populate the first row with the data immediately prior to our time
    ### range of interest
    buf_r[0,:],  buf_w[0,:], prev_t = lmtdb.get_last_rw_data_before( t_start )

    ### write the raw data for the time range of interest
    h5f['ost_bytes_read'][idx_0:idx_f, :] = buf_r[1:,:]
    h5f['ost_bytes_written'][idx_0:idx_f, :] = buf_w[1:,:]
    ### subtract every row from the row before it
    h5f['OSTReadGroup/OSTBulkReadDataSet'][:, idx_0:idx_f] = (buf_r[1:,:] - buf_r[:-1,:]).T / float(_LMT_TIMESTEP)
    h5f['OSTWriteGroup/OSTBulkWriteDataSet'][:, idx_0:idx_f] = (buf_w[1:,:] - buf_w[:-1,:]).T / float(_LMT_TIMESTEP)

    h5f.close()
    lmtdb.close()
