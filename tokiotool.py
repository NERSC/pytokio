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
import h5py
import numpy as np
import json

_DATE_FMT = "%Y-%m-%d %H:%M:%S"

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument( 'tstart', type=str, help="lower bound of time to scan, in YYY-mm-dd HH:MM:SS format" )
    parser.add_argument( 'tstop', type=str, help="upper bound of time to scan, in YYY-mm-dd HH:MM:SS format" )
    parser.add_argument( '--read-pickle',  action='store_true', help="attempt to read from pickle instead of MySQL" )
    parser.add_argument( '--write-pickle', action='store_true', help="write output to pickle" )
    parser.add_argument( '--debug', action='store_true', help="produce debug messages" )
    args = parser.parse_args()
    if not ( args.tstart and args.tstop ):
        parser.print_help()
        sys.exit(1)
    if args.read_pickle and args.write_pickle:
        sys.stderr.write("--read-pickle and --write-pickle are mutually exclusive\n" )
        sys.exit(1)
    if args.debug:
        tokio.DEBUG = True

    try:
        t_start = datetime.datetime.strptime( args.tstart, _DATE_FMT )
        t_stop = datetime.datetime.strptime( args.tstop, _DATE_FMT )
    except ValueError:
        sys.stderr.write("Start and end times must be in format %s\n" % _DATE_FMT );
        raise

    if args.read_pickle:
        lmtdb = tokio.lmt.connect( pickles=True )
    else:
        lmtdb = tokio.lmt.connect()

    if args.write_pickle:
        lmtdb.pickle_ost_data( t_start, t_stop )

    ### initialize hdf5 file
    ost_ct = 248
    ts_ct = 86400/int(tokio.LMT_TIMESTEP)
    h5f = tokio.hdf5.connect( 'testfile.hdf5' )
    h5f.init_datasets( ost_ct, ts_ct, host='cori', filesystem='snx11168' )
    h5f.init_timestamps( t_start, t_stop )

    ### populate read/write bytes data
    ost_names = []
    prev_data = {}

    ### figure out how to map indices to timestamps
    if 'first_timestamp' in h5f.attrs and 'timestep' in h5f.attrs:  # ver 2
        t0 = h5f.attrs['first_timestamp']
        dt = h5f.attrs['timestep']
    else:                                                           # ver 1
        t0 = h5f['FSStepsGroup/FSStepsDataSet'][0]
        dt = h5f['FSStepsGroup/FSStepsDataSet'][1] - h5f['FSStepsGroup/FSStepsDataSet'][0]

    buf_wrate = np.full( shape=(ost_ct, ts_ct), fill_value=-0.0, dtype='f8' ) #v1
    buf_rrate = np.full( shape=(ost_ct, ts_ct), fill_value=-0.0, dtype='f8' ) #v1
    buf_w = np.full( shape=(ts_ct, ost_ct), fill_value=-0.0, dtype='f8' ) #v2
    buf_r = np.full( shape=(ts_ct, ost_ct), fill_value=-0.0, dtype='f8' ) #v2
    for tup_out in lmtdb.get_ost_data( t_start, t_stop ):
        timestamp = tup_out[0]
        ost_name = tup_out[1]
        read_bytes = tup_out[2]
        write_bytes = tup_out[3]

        ts_idx = ( timestamp - t0 ) / dt

        if ost_name not in ost_names:
            ost_idx = len(ost_names)
            ost_names.append( tup_out[1] )
        else:
            ost_idx = ost_names.index( ost_name )

        if ost_name in prev_data:
            assert( ts_idx >= 0)
            buf_r[ts_idx, ost_idx] = read_bytes
            buf_w[ts_idx, ost_idx] = write_bytes
            buf_rrate[ost_idx, ts_idx] = (read_bytes - prev_data[ost_name]['read_bytes']) / tokio.LMT_TIMESTEP
            buf_wrate[ost_idx, ts_idx] = (write_bytes - prev_data[ost_name]['write_bytes']) / tokio.LMT_TIMESTEP

        if ost_name not in prev_data:
            prev_data[ost_name] = { }
        prev_data[ost_name]['read_bytes'] = read_bytes
        prev_data[ost_name]['write_bytes'] = write_bytes
    h5f['ost_bytes_read'][:, :] = buf_r
    h5f['ost_bytes_written'][:, :] = buf_w
    h5f['OSTReadGroup/OSTBulkReadDataSet'][:, :] = buf_rrate
    h5f['OSTWriteGroup/OSTBulkWriteDataSet'][:, :] = buf_wrate

    h5f.close()
    lmtdb.close()
