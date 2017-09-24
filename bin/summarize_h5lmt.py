#!/usr/bin/env python
"""
Generate summary metrics from an h5lmt file
"""

import sys
import json
import datetime
import argparse
import warnings
import tokio
import tokio.tools

_BYTES_TO_GIB = 2.0**(-30.0)
INTERESTING_MDS_OPS = [
    'open',
    'close',
    'getattr',
    'unlink',
    'setattr',
    'getxattr',
    'rmdir',
    'rename',
    'mkdir'
]
D_TIMESTEPS = '/FSStepsGroup/FSStepsDataSet'
D_OSSCPU = '/OSSCPUGroup/OSSCPUDataSet'
D_MDSCPU = '/MDSCPUGroup/MDSCPUDataSet'
D_MDSOPS = '/MDSOpsGroup/MDSOpsDataSet'
D_READS = '/OSTReadGroup/OSTBulkReadDataSet'
D_WRITES = '/OSTWriteGroup/OSTBulkWriteDataSet'
D_MISSING = '/FSMissingGroup/FSMissingDataSet'

def print_datum(datum=None):
    """
    Take a json bag and print out relevant fields
    """
    if isinstance(datum, dict):
        print_str = "%19s-%8s " % (datum['t0'].strftime("%Y-%m-%d %H:%M:%S"),
                                   datum['tf'].strftime("%H:%M:%S"))
        print_str += "%(gibs_read)12.2f " % datum
        print_str += "%(gibs_writ)12.2f " % datum
        print_str += "%(ave_oss_cpu)5.1f " % datum
        print_str += "%(max_oss_cpu)5.1f " % datum
        print_str += "%(ave_mds_cpu)5.1f " % datum
        print_str += "%(max_mds_cpu)5.1f" % datum
        print_str += "%(frac_missing)5.1f" % datum
        return print_str + "\n"

    return "%28s %12s %12s %5s %5s %5s %5s %5s\n" % ("date/time", "gibs read",
                                                     "gibs writ", "ossav",
                                                     "ossmx", "mdsav",
                                                     "mdsmx", "mssng")

def summarize_data(data):
    """
    Take a list of LMT data sets and return summaries of each relevant key
    """
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
        'tot_zeros': 0,
    }

    for datum in data:
        points_in_bin = (datum['if'] - datum['i0'])
        totals['tot_bytes_read'] += datum['bytes_read']
        totals['tot_bytes_write'] += datum['bytes_write']
        totals['oss_ave'] += datum['ave_oss_cpu'] * points_in_bin
        totals['mds_ave'] += datum['ave_mds_cpu'] * points_in_bin
        totals['n'] += points_in_bin
        if datum['max_oss_cpu'] > totals['oss_max']:
            totals['oss_max'] = datum['max_oss_cpu']
        else:
            totals['oss_max'] = totals['oss_max']
        if datum['max_mds_cpu'] > totals['mds_max']:
            totals['mds_max'] = datum['max_mds_cpu']
        else:
            totals['mds_max'] = totals['mds_max']
        totals['tot_missing'] += datum['tot_missing']
        totals['tot_present'] += datum['tot_present']
        totals['tot_zeros'] += datum['tot_zeros']
        for operation in INTERESTING_MDS_OPS:
            key = 'ops_%ss' % operation
            if key not in totals:
                totals[key] = 0
            totals[key] += datum[key]

    # Derived values
    totals['ave_bytes_read_per_dt'] = totals['tot_bytes_read'] / totals['n']
    totals['ave_bytes_write_per_dt'] = totals['tot_bytes_write'] / totals['n']
    totals['oss_ave'] = totals['oss_ave'] / totals['n']
    totals['mds_ave'] = totals['mds_ave'] / totals['n']
    totals['tot_gibs_read'] = totals['tot_bytes_read'] * _BYTES_TO_GIB
    totals['tot_gibs_write'] = totals['tot_bytes_write'] * _BYTES_TO_GIB
    totals['ave_gibs_read_per_dt'] = totals['tot_gibs_read'] / totals['n']
    totals['ave_gibs_write_per_dt'] = totals['tot_gibs_write'] / totals['n']
    totals['frac_missing'] = 100.0 * totals['tot_missing'] \
        / (totals['tot_missing'] + totals['tot_present'])
    totals['frac_zeros'] = 100.0 * totals['tot_zeros'] \
        / (totals['tot_missing'] + totals['tot_present'])

    return totals

def print_data_summary(data, use_json=False):
    """
    Print the output of the summarize_data function in a human-readable format
    """
    totals = summarize_data(data)
    if use_json:
        return json.dumps(totals, indent=4, sort_keys=True)

    print_str = ""
    print_str += "Total read:  %(tot_gibs_read)14.2f GiB\n" % totals
    print_str += "Total write: %(tot_gibs_write)14.2f GiB\n" % totals
    print_str += "Average OSS CPU: %(oss_ave)6.2f%%\n" % totals
    print_str += "Max OSS CPU:     %(oss_max)6.2f%%\n" % totals
    print_str += "Average MDS CPU: %(mds_ave)6.2f%%\n" % totals
    print_str += "Max MDS CPU:     %(mds_max)6.2f%%\n" % totals
    print_str += "Missing Data:    %(frac_missing)6.2f%%\n" % totals
    return print_str

def bin_h5lmt(h5_filename):
    """
    Wrap h5lmt_like_object to feed it an H5LMT file
    """
    h5_file = tokio.connectors.hdf5.Hdf5(h5_filename)
    if 'version' in h5_file['/'].attrs and h5_file['/'].attrs['version'] > 1:
        raise Exception("TOKIOfile version > 1 not supported")

    return bin_h5lmt_like_object(h5_file, h5_file.timestep)

def bin_h5lmt_like_object(h5_file, timestep, num_bins=24):
    """
    Take an H5LMT file and convert it into bins of reduced data (e.g., bin by
    hourly totals)
    """
    dset_steps = h5_file[D_TIMESTEPS]
    if (dset_steps.shape[0] - 1) % num_bins > 0:
        warnings.warn("Bin count %d does not evenly divide into FSStepsDataSet size %d" %
                      (num_bins, (dset_steps.shape[0] - 1)))
    dt_per_bin = int((dset_steps.shape[0] - 1) / num_bins)

    bin_data = []
    for bin_num in range(num_bins):
        idx0 = bin_num * dt_per_bin
        idxf = (bin_num+1) * dt_per_bin

        bin_datum = {
            "i0": idx0,
            "if": idxf,
            "bytes_read": h5_file[D_READS][:, idx0:idxf].sum() * timestep,
            "bytes_write": h5_file[D_WRITES][:, idx0:idxf].sum() * timestep,
            "ave_oss_cpu": h5_file[D_OSSCPU][:, idx0:idxf].mean(),
            "max_oss_cpu": h5_file[D_OSSCPU][:, idx0:idxf].max(),
            "ave_mds_cpu": h5_file[D_MDSCPU][idx0:idxf].mean(),
            "max_mds_cpu": h5_file[D_MDSCPU][idx0:idxf].max(),
            "tot_missing": h5_file[D_MISSING][:, idx0:idxf].sum(),
            ### tot_zeros is a bit crazy --count up # elements that reported
            ### read, write, AND CPU load as zero since these are likely missing
            ### UDP packets, not true zero activity
            "tot_zeros": ((h5_file[D_READS][:, idx0:idxf] == 0.0)
                          & (h5_file[D_WRITES][:, idx0:idxf] == 0.0)).sum()}

        mds_dset = h5_file[D_MDSOPS]
        if '__metadata' in h5_file:
            ops = h5_file['__metadata']['OpNames']
        else:
            ops = list(mds_dset.attrs['OpNames'])
        for operation in INTERESTING_MDS_OPS:
            bin_datum['ops_%ss' % operation] = \
                mds_dset[ops.index(operation), idx0:idxf].sum() * timestep

        # Derived values
        bin_datum['t0'] = datetime.datetime.fromtimestamp(
            dset_steps[bin_datum['i0']])
        bin_datum['tf'] = datetime.datetime.fromtimestamp(
            dset_steps[bin_datum['if']])
        bin_datum['gibs_read'] = bin_datum['bytes_read'] * _BYTES_TO_GIB
        bin_datum['gibs_writ'] = bin_datum['bytes_write'] * _BYTES_TO_GIB
        bin_datum['tot_present'] = \
            h5_file['FSMissingGroup/FSMissingDataSet'][:, idx0:idxf].shape[0] * \
            (idxf - idx0) - bin_datum['tot_missing']
        bin_datum['frac_missing'] = 100.0 * bin_datum['tot_missing'] \
            / (bin_datum['tot_missing'] + bin_datum['tot_present'])
        bin_datum['frac_zeros'] = 100.0 * bin_datum['tot_zeros'] \
            / (bin_datum['tot_missing'] + bin_datum['tot_present'])
        bin_data.append(bin_datum)

    return bin_data

def summarize_h5lmt():
    """
    CLI tool to summarize the contents of an H5LMT file
    """
    parser = argparse.ArgumentParser(
        description='aggregate bytes in/out from h5lmt file every hour')
    parser.add_argument('h5lmt', metavar='N', type=str, nargs='+',
                        help='h5lmt file to examine')
    parser.add_argument('--summary', dest='summary', action='store_true',
                        help='print a summary of all output')
    parser.add_argument('--bytes', dest='bytes', action='store_true',
                        help='print bytes, not GiB')
    parser.add_argument('--bins', dest='bins', type=int, default=24,
                        help="number of bins per day")
    parser.add_argument('--start', dest='start', type=str,
                        help="date/time to start in YYYY-MM-DD HH:MM:SS format")
    parser.add_argument('--end', dest='end', type=str,
                        help="date/time to end in YYYY-MM-DD HH:MM:SS format")
    parser.add_argument('--json', dest='json', action='store_true',
                        help='return json output')
    args = parser.parse_args()

    sys.stdout.write(print_datum(None))
    all_binned_data = []

    if args.start is not None and args.end is not None:
        tstart = datetime.datetime.strptime(args.start, "%Y-%m-%d %H:%M:%S")
        tstop = datetime.datetime.strptime(args.end, "%Y-%m-%d %H:%M:%S")

        for h5lmt_file in args.h5lmt:
            h5lmt_like_object = {}
            for dataset_string in [D_READS, D_WRITES, D_MDSCPU, D_OSSCPU, D_MISSING, D_TIMESTEPS]:
                h5lmt_like_object[dataset_string] = tokio.tools.get_group_data_from_time_range(
                    h5lmt_file, dataset_string, tstart, tstop)
            bin_data = bin_h5lmt_like_object(
                h5lmt_like_object,
                tokio.config.LMT_TIMESTEP,
                args.bins)
            for bin_datum in bin_data:
                sys.stdout.write(print_datum(bin_datum))
            all_binned_data = all_binned_data + bin_data

            if args.summary:
                sys.stdout.write(print_data_summary(all_binned_data, args.json))
    else:
        for h5lmt_file in args.h5lmt:
            bin_data = bin_h5lmt(h5lmt_file)

            for bin_datum in bin_data:
                sys.stdout.write(print_datum(bin_datum))
            all_binned_data = all_binned_data + bin_data

        if args.summary:
            sys.stdout.write(print_data_summary(all_binned_data, args.json))

if __name__ == '__main__':
    summarize_h5lmt()
