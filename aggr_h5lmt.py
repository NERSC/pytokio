#!/usr/bin/env python

import datetime
import h5py
import sys
import os
import argparse
import tokio
import tokio.tools
import warnings

_BYTES_TO_GIB = 2.0**(-30.0)

def print_datum( datum=None ):
    """Take a json bag and print out relevant fields"""
    if datum is None:
        return "%28s %12s %12s %5s %5s %5s %5s %5s\n" % ( "date/time", "gibs read", "gibs writ", "ossav", "ossmx", "mdsav", "mdsmx", "mssng" )
    else:
        print_str = "%19s-%8s " % (datum['t0'].strftime("%Y-%m-%d %H:%M:%S"), datum['tf'].strftime("%H:%M:%S"))
        print_str += "%(gibs_read)12.2f " % datum
        print_str += "%(gibs_writ)12.2f " % datum
        print_str += "%(ave_oss_cpu)5.1f " % datum
        print_str += "%(max_oss_cpu)5.1f " % datum
        print_str += "%(ave_mds_cpu)5.1f " % datum
        print_str += "%(max_mds_cpu)5.1f" % datum
        print_str += "%(frac_missing)5.1f" % datum
        return print_str + "\n"

def print_data_summary( data ):
    totals = {
        'n': 0,
        'tot_bytes_read': 0,
        'tot_bytes_write': 0,
        'oss_ave': 0.0,
        'oss_max': 0.0,
        'mds_ave': 0.0,
        'mds_max': 0.0,
        'tot_missing': 0,
        'tot_present': 0,
    }

    for datum in data:
        points_in_bin = (datum['if'] - datum['i0'])
        totals['tot_bytes_read'] += datum['bytes_read']
        totals['tot_bytes_write'] += datum['bytes_write']
        totals['oss_ave'] += datum['ave_oss_cpu'] * points_in_bin
        totals['mds_ave'] += datum['ave_mds_cpu'] * points_in_bin
        totals['n'] += points_in_bin 
        totals['oss_max'] = datum['max_oss_cpu'] if datum['max_oss_cpu'] > totals['oss_max'] else totals['oss_max']
        totals['mds_max'] = datum['max_mds_cpu'] if datum['max_mds_cpu'] > totals['mds_max'] else totals['mds_max']
        totals['tot_missing'] += datum['tot_missing']
        totals['tot_present'] += datum['tot_present']

    ### derived values
    totals['ave_bytes_read_per_dt'] = totals['tot_bytes_read'] / totals['n']
    totals['ave_bytes_write_per_dt'] = totals['tot_bytes_write'] / totals['n']
    totals['oss_ave'] = totals['oss_ave'] / totals['n']
    totals['mds_ave'] = totals['mds_ave'] / totals['n']
    totals['tot_gibs_read'] = totals['tot_bytes_read'] * _BYTES_TO_GIB
    totals['tot_gibs_write'] = totals['tot_bytes_write'] * _BYTES_TO_GIB
    totals['ave_gibs_read_per_dt'] = totals['tot_gibs_read'] / totals['n']
    totals['ave_gibs_write_per_dt'] = totals['tot_gibs_write'] / totals['n']
    totals['frac_missing'] = 100.0 * totals['tot_missing'] / (totals['tot_missing'] + totals['tot_present'])

    print_str = ""
    print_str += "Total read:  %(tot_gibs_read)14.2f GiB\n" % totals
    print_str += "Total write: %(tot_gibs_write)14.2f GiB\n" % totals
    print_str += "Average OSS CPU: %(oss_ave)6.2f%%\n" % totals
    print_str += "Max OSS CPU:     %(oss_max)6.2f%%\n" % totals
    print_str += "Average MDS CPU: %(mds_ave)6.2f%%\n" % totals
    print_str += "Max MDS CPU:     %(mds_max)6.2f%%\n" % totals
    print_str += "Missing Data:    %(frac_missing)6.2f%%\n" % totals

    return print_str

def bin_h5lmt(h5lmt_file):
    f = tokio.HDF5(h5lmt_file)
    if 'version' in f['/'].attrs and f['/'].attrs['version'] > 1:
        raise Exception("TOKIOfile version > 1 not supported")

    return bin_h5lmt_like_object(f, f.timestep)

def bin_h5lmt_like_object(f, timestep):
    if (f['FSStepsGroup/FSStepsDataSet'].shape[0] - 1) % args.bins > 0:
        warnings.warn("Bin count %d does not evenly divide into FSStepsDataSet size %d" % (args.bins, (f['FSStepsGroup/FSStepsDataSet'].shape[0] - 1)) )
    dt_per_bin = int((f['FSStepsGroup/FSStepsDataSet'].shape[0] - 1) / args.bins)

    bin_data = []
    for bin in range(args.bins):
        i_0 = bin * dt_per_bin
        i_f = (bin+1) * dt_per_bin
        t_0 = datetime.datetime.fromtimestamp(f['FSStepsGroup/FSStepsDataSet'][i_0])
        t_f = datetime.datetime.fromtimestamp(f['FSStepsGroup/FSStepsDataSet'][i_f])

        bin_datum = {
            "i0": i_0,
            "if": i_f,
            "bytes_read": f['OSTReadGroup/OSTBulkReadDataSet'][:, i_0:i_f].sum() * timestep,
            "bytes_write": f['OSTWriteGroup/OSTBulkWriteDataSet'][:, i_0:i_f].sum() * timestep,
            "ave_oss_cpu": f['OSSCPUGroup/OSSCPUDataSet'][:, i_0:i_f].mean(),
            "max_oss_cpu": f['OSSCPUGroup/OSSCPUDataSet'][:, i_0:i_f].max(),
            "ave_mds_cpu": f['MDSCPUGroup/MDSCPUDataSet'][i_0:i_f].mean(),
            "max_mds_cpu": f['MDSCPUGroup/MDSCPUDataSet'][i_0:i_f].max(),
            "tot_missing": f['FSMissingGroup/FSMissingDataSet'][:,i_0:i_f].sum(),
        }

        ### derived values
        bin_datum['t0'] = datetime.datetime.fromtimestamp(f['FSStepsGroup/FSStepsDataSet'][bin_datum['i0']])
        bin_datum['tf'] = datetime.datetime.fromtimestamp(f['FSStepsGroup/FSStepsDataSet'][bin_datum['if']])
        bin_datum['gibs_read'] = bin_datum['bytes_read'] * _BYTES_TO_GIB
        bin_datum['gibs_writ'] = bin_datum['bytes_write'] * _BYTES_TO_GIB
        bin_datum['tot_present'] = f['FSMissingGroup/FSMissingDataSet'][:,i_0:i_f].shape[0] * (i_f - i_0) - bin_datum['tot_missing']
        bin_datum['frac_missing'] = 100.0 * bin_datum['tot_missing'] / (bin_datum['tot_missing'] + bin_datum['tot_present'])
        bin_data.append(bin_datum)

    return bin_data

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='aggregate bytes in/out from h5lmt file every hour')
    parser.add_argument('h5lmt', metavar='N', type=str, nargs='+', help='h5lmt file to examine')
    parser.add_argument('--brief', dest='brief', action='store_true', help='print a single line of output per h5lmt')
    parser.add_argument('--summary', dest='summary', action='store_true', help='print a summary of all output')
    parser.add_argument('--bytes', dest='bytes', action='store_true', help='print bytes, not GiB')
    parser.add_argument('--bins', dest='bins', type=int, default=24, help="number of bins per day")
    parser.add_argument('--start', dest='start', type=str, help="date/time to start in YYYY-MM-DD HH:MM:SS format")
    parser.add_argument('--end', dest='end', type=str, help="date/time to end in YYYY-MM-DD HH:MM:SS format")
    args = parser.parse_args()

    sys.stdout.write(print_datum(None))
    all_binned_data = []

    if args.start is not None and args.end is not None:
        tstart = datetime.datetime.strptime(args.start, "%Y-%m-%d %H:%M:%S")
        tstop = datetime.datetime.strptime(args.end, "%Y-%m-%d %H:%M:%S")

        for h5lmt_file in args.h5lmt:
            h5lmt_like_object = {}
            for dataset_string in ["OSTReadGroup/OSTBulkReadDataSet",
                                   "OSTWriteGroup/OSTBulkWriteDataSet",
                                   "MDSCPUGroup/MDSCPUDataSet",
                                   "OSSCPUGroup/OSSCPUDataSet",
                                   "FSMissingGroup/FSMissingDataSet",
                                   "FSStepsGroup/FSStepsDataSet"]:
                h5lmt_like_object[dataset_string] = tokio.tools.get_group_data_from_time_range(
                                                      h5lmt_file, dataset_string, tstart, tstop)
            bin_data = bin_h5lmt_like_object(h5lmt_like_object, 5) ### hard-code 5 second timestep here
            for bin_datum in bin_data:
                sys.stdout.write(print_datum(bin_datum))
            all_binned_data = all_binned_data + bin_data
    
            if args.summary:
                sys.stdout.write(print_data_summary(all_binned_data))
    else:
        for h5lmt_file in args.h5lmt:
            bin_data = bin_h5lmt(h5lmt_file)

            for bin_datum in bin_data:
                sys.stdout.write(print_datum(bin_datum))
            all_binned_data = all_binned_data + bin_data

        if args.summary:
            sys.stdout.write(print_data_summary(all_binned_data))
