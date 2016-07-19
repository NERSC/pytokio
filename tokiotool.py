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

    tbench0 = time.time()
    ### initialize hdf5 file
    ost_ct = 248
    ts_ct = 86400/int(tokio.LMT_TIMESTEP)
    h5f = tokio.hdf5.connect( 'testfile.hdf5' )
    h5f.init_datasets( ost_ct, ts_ct )
    tokio._debug_print("HDF5 datasets initalized in %f sec" % (time.time() - tbench0))

    tbench0 = time.time()
    ### initialize hdf5 timestamps in given range.  must track the actual
    ### indices observed because there may be timestamps that are entirely
    ### missing from the LMT DB
    ts_map = np.empty( shape=(ts_ct,), dtype='i8' )
    min_ts_idx = None
    max_ts_idx = None
    for tup_out in lmtdb.get_timestamp_map( t_start, t_stop ):
        ts_idx = tokio.hdf5.get_index_from_time( datetime.datetime.fromtimestamp( tup_out[0] ) )
        ts_map[ ts_idx  ] = tup_out[0]
        if min_ts_idx is None or ts_idx < min_ts_idx:
            min_ts_idx = ts_idx
        if max_ts_idx is None or ts_idx > max_ts_idx:
            max_ts_idx = ts_idx
    h5f['FSStepsGroup/FSStepsDataSet'][min_ts_idx:max_ts_idx] = ts_map[min_ts_idx:max_ts_idx]
    tokio._debug_print("HDF5 timestamps initalized in %f sec" % (time.time() - tbench0))

    tbench0 = time.time()
    ### populate read/write bytes data
    ost_names = []
    prev_data = {}

    buf_wrate = np.empty( shape=(ost_ct, ts_ct), dtype='f8' )
    buf_rrate = np.empty( shape=(ost_ct, ts_ct), dtype='f8' )
    buf_w = np.empty( shape=(ost_ct, ts_ct), dtype='f4' )
    buf_r = np.empty( shape=(ost_ct, ts_ct), dtype='f4' )
    for tup_out in lmtdb.get_ost_data( t_start, t_stop ):
        timestamp = datetime.datetime.fromtimestamp( tup_out[0] )
        ost_name = tup_out[1]
        read_bytes = tup_out[2]
        write_bytes = tup_out[3]
        ts_idx = tokio.hdf5.get_index_from_time(timestamp)

        if ost_name not in ost_names:
            ost_idx = len(ost_names)
            ost_names.append( tup_out[1] )
        else:
            ost_idx = ost_names.index( ost_name )

        if ost_name in prev_data:
            buf_r[ost_idx, ts_idx] = read_bytes - prev_data[ost_name]['read']
            buf_w[ost_idx, ts_idx] = write_bytes - prev_data[ost_name]['write']
            buf_rrate[ost_idx, ts_idx] = (read_bytes - prev_data[ost_name]['read']) / tokio.LMT_TIMESTEP
            buf_wrate[ost_idx, ts_idx] = (write_bytes - prev_data[ost_name]['write']) / tokio.LMT_TIMESTEP

        if ost_name not in prev_data:
            prev_data[ost_name] = { }
        prev_data[ost_name]['read'] = read_bytes
        prev_data[ost_name]['write'] = write_bytes
    h5f['ost_bytes_read'][:, :] = buf_r
    h5f['ost_bytes_written'][:, :] = buf_w
    h5f['OSTReadGroup/OSTBulkReadDataSet'][:, :] = buf_rrate
    h5f['OSTWriteGroup/OSTBulkWriteDataSet'][:, :] = buf_wrate

    tokio._debug_print("Data populated in  %f sec" % (time.time() - tbench0))

    h5f.close()
    lmtdb.close()
