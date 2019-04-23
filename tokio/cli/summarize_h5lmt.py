"""
Generate summary metrics from an h5lmt file.  Will be eventually replaced by the
summarize_tts command-line tool.
"""

import os
import sys
import json
import time
import math
import datetime
import argparse
import warnings
import tokio.common
import tokio.connectors.hdf5
import tokio.tools.hdf5

DATASETS_TO_BIN_KEYS = {
    'datatargets/readbytes': 'ost_read',
    'datatargets/writebytes': 'ost_write',
    'dataservers/cpuload': 'oss_cpu',
    'mdservers/cpuload': 'mds_cpu',
}

BYTES_TO_GIB = 2.0**30
DATE_FMT = "%Y-%m-%d"
DATE_FMT_STR = "YYYY-MM-DDTHH:MM:SS"

def summarize_reduced_data(data):
    """
    Take a list of LMT data sets and return summaries of each relevant key
    """
    totals = {
        'n': 0,
        'sum_bytes_read': 0,
        'sum_bytes_write': 0,
        'oss_ave': -0.0,
        'oss_max': -0.0,
        'mds_ave': -0.0,
        'mds_max': -0.0,
        'missing_ost_read': 0,
        'missing_ost_write': 0,
        'missing_oss_cpu': 0,
        'missing_mds_cpu': 0,
        'num_ost_read': 0,
        'num_ost_write': 0,
        'num_oss_cpu': 0,
        'num_mds_cpu': 0,
    }

    for timestamp in sorted(data.keys()):
        datum = data[timestamp]
        points_in_bin = (datum['indexf'] - datum['index0'])
        totals['sum_bytes_read'] += datum['sum_ost_read']
        totals['sum_bytes_write'] += datum['sum_ost_write']
        totals['n'] += points_in_bin

        if 'ave_oss_cpu' in datum:
            totals['oss_ave'] += datum['ave_oss_cpu'] * points_in_bin
        if 'ave_mds_cpu' in datum:
            totals['mds_ave'] += datum['ave_mds_cpu'] * points_in_bin

        if 'max_oss_cpu' in datum:
            if datum['max_oss_cpu'] > totals['oss_max']:
                totals['oss_max'] = datum['max_oss_cpu']
            else:
                totals['oss_max'] = totals['oss_max']

        if 'max_mds_cpu' in datum:
            if datum['max_mds_cpu'] > totals['mds_max']:
                totals['mds_max'] = datum['max_mds_cpu']
            else:
                totals['mds_max'] = totals['mds_max']

        totals['missing_ost_read'] += datum.get('missing_ost_read', 0)
        totals['missing_ost_write'] += datum.get('missing_ost_write', 0)
        totals['missing_oss_cpu'] += datum.get('missing_oss_cpu', 0)
        totals['missing_mds_cpu'] += datum.get('missing_mds_cpu', 0)
        totals['num_ost_read'] += datum.get('num_ost_read', 0)
        totals['num_ost_write'] += datum.get('num_ost_write', 0)
        totals['num_oss_cpu'] += datum.get('num_oss_cpu', 0)
        totals['num_mds_cpu'] += datum.get('num_mds_cpu', 0)

    # Derived values
    totals['ave_bytes_read_per_dt'] = totals['sum_bytes_read'] / totals['n']
    totals['ave_bytes_write_per_dt'] = totals['sum_bytes_write'] / totals['n']
    totals['sum_kibs_read'] = totals['sum_bytes_read'] / 1024.0
    totals['sum_kibs_write'] = totals['sum_bytes_write'] / 1024.0
    totals['sum_mibs_read'] = totals['sum_kibs_read'] / 1024.0
    totals['sum_mibs_write'] = totals['sum_kibs_write'] / 1024.0
    totals['sum_gibs_read'] = totals['sum_mibs_read'] / 1024.0
    totals['sum_gibs_write'] = totals['sum_mibs_write'] / 1024.0
    totals['sum_tibs_read'] = totals['sum_gibs_read'] / 1024.0
    totals['sum_tibs_write'] = totals['sum_gibs_write'] / 1024.0
    totals['ave_gibs_read_per_dt'] = totals['sum_gibs_read'] / totals['n']
    totals['ave_gibs_write_per_dt'] = totals['sum_gibs_write'] / totals['n']
    totals['oss_ave'] = totals['oss_ave'] / totals['n']
    totals['mds_ave'] = totals['mds_ave'] / totals['n']

    # Missing fractions
    for base in ['ost_read', 'ost_write', 'oss_cpu', 'mds_cpu']:
        if totals['num_%s' % base] > 0:
            totals['frac_missing_%s' % base] = float(totals['missing_%s' % base]) \
                                               / totals['num_%s' % base]

    # For convenience
    totals['ave_tibs_read_per_dt'] = totals['sum_gibs_read'] / totals['n']
    totals['ave_tibs_write_per_dt'] = totals['sum_gibs_write'] / totals['n']

    return totals

def bin_datasets(hdf5_file, dataset_names, orient='columns', num_bins=24):
    """Group many timeseries datasets into bins

    Takes a TOKIO HDF file and converts it into bins of reduced data (e.g., bin
    by hourly totals)

    Args:
        hdf5_file (connectors.Hdf5): HDF5 file from where data should be retrieved
        dataset_names (list of str): dataset names to be aggregated
        columns (str): either 'columns' or 'index'; same semantic meaning as
            pandas.DataFrame.from_dict
        num_binds (int): number of bins to generate per day

    Returns:
        Dictionary of lists.  Keys are metrics, and values (lists) are the
        aggregated value of that metric in a single timestep bin.  For example::

            {
                "sum_some_metric":      [  0,   2,   3,   1],
                "sum_someother_metric": [9.9, 2.3, 5.1, 0.2],
            }

    """
    bins_by_columns = {}
    bins_by_index = {}
    found_metrics = {}
    for dataset_key in dataset_names:
        # binned_dataset is a list of dictionaries.  Each list element
        # corresponds to a single time bin, and its key-value pairs are
        # individual metrics aggregated over that bin.
        #
        # binned_dataset = {
        #     "ave_mds_cpu": [
        #         0.86498,
        #         0.80356,
        #         ...
        #     ],
        #     "ave_oss_cpu": [
        #         39.7725,
        #         39.2170,
        #         ...
        #     ],
        #     ...
        # }

        binned_dataset = bin_dataset(hdf5_file, dataset_key, num_bins)

        # loop over time bins
        for index, counters in enumerate(binned_dataset):
            # loop over aggregate metrics in a single bin
            #   key = ave_mds_cpu
            #   value = { 0.86498, 0.80356 }
            for key, value in counters.items():
                # look for keys that appear in multiple datasets.  only record
                # values for the first dataset in which the key is encountered
                if key in found_metrics:
                    if found_metrics[key] != dataset_key:
                        if bins_by_columns[key][index] != value:
                            raise Exception("Not all timesteps are the same in HDF5")
                        continue
                else:
                    found_metrics[key] = dataset_key

                # append the value to its metric counter in bins_by_columns
                if key in bins_by_columns:
                    bins_by_columns[key].append(value)
                else:
                    bins_by_columns[key] = [value]

            # append the value to its metric counter in bins_by_index
            if 'tstart' in counters:
                index_key = int(time.mktime(counters['tstart'].timetuple()))
                if index_key not in bins_by_index:
                    bins_by_index[index_key] = {}
                bins_by_index[index_key].update(counters)

    if orient == 'columns':
        return bins_by_columns
    elif orient == 'index':
        return bins_by_index
    else:
        raise ValueError('only recognize index or columns for orient')

def bin_dataset(hdf5_file, dataset_name, num_bins):
    """Group timeseries dataset into bins

    Args:
        dataset (h5py.Dataset): dataset to be binned up

    Returns:
        list of dictionaries corresponding to bins.  Each dictionary contains
        data summarized over that bin's time interval.
    """

    base_key = DATASETS_TO_BIN_KEYS.get(dataset_name.lstrip('/'))
    if not base_key:
        raise KeyError("Cannot bin unknown dataset %s" % dataset_name)
    missing_key = "missing_" + base_key
    total_key = "num_" + base_key

    dataset = hdf5_file.get(dataset_name)
    if dataset is None:
        return []

    timestamps = hdf5_file.get_timestamps(dataset_name)[...]
    columns = hdf5_file.get_columns(dataset_name)

    if hdf5_file.schema:
        if not (timestamps.shape[0] % num_bins) == 0:
            warnings.warn("Bin count %d does not evenly divide into FSStepsDataSet size %d" %
                          (num_bins, (timestamps.shape[0])))
        dt_per_bin = int(timestamps.shape[0] / num_bins)
    else:
        # no schema means h5lmt file, which has + 1 extra timestep
        if not ((timestamps.shape[0] - 1) % num_bins) == 0:
            warnings.warn("Bin count %d does not evenly divide into FSStepsDataSet size %d" %
                          (num_bins, (timestamps.shape[0] - 1)))
        dt_per_bin = int((timestamps.shape[0] - 1) / num_bins)

    # we generate the missing data matrix for each dataset only once--it's very
    # expensive to do this multiple times
    missing_dataset = hdf5_file.get_missing(dataset_name)

    # create a list of dictionaries, where each list element is a bin
    binned_data = []
    for bin_num in range(num_bins):
        index0 = bin_num * dt_per_bin
        indexf = (bin_num+1) * dt_per_bin

        bin_datum = {
            "index0": index0,
            "indexf": indexf,
            "tstart": datetime.datetime.fromtimestamp(timestamps[index0]),
            'tend': datetime.datetime.fromtimestamp(timestamps[indexf - 1]),
        }

        bin_datum["max_" + base_key] = dataset[index0:indexf, :].max()
        bin_datum["min_" + base_key] = dataset[index0:indexf, :].min()
        bin_datum["sum_" + base_key] = dataset[index0:indexf, :].sum()
        bin_datum["ave_" + base_key] = bin_datum["sum_" + base_key] / float(indexf - index0)
        bin_datum["ave_" + base_key] /= columns.shape[0]

        bin_datum[missing_key] = missing_dataset[index0:indexf, :].sum()

        bin_datum[total_key] = (indexf - index0) * columns.shape[0]
        if bin_datum[total_key]:
            bin_datum["frac_" + missing_key] = float(bin_datum[missing_key]) / bin_datum[total_key]
        else:
            bin_datum["frac_" + missing_key] = 1.0

        if base_key.startswith('ost_'):
            for agg_key in 'max', 'min', 'ave', 'sum':
                root = "%s_%s" % (agg_key, base_key)
                bin_datum['%s_bytes' % root] = bin_datum[root]
                bin_datum['%s_kibs' % root] = bin_datum[root] / 1024.0
                bin_datum['%s_mibs' % root] = bin_datum['%s_kibs' % root] / 1024.0
                bin_datum['%s_gibs' % root] = bin_datum['%s_mibs' % root] / 1024.0
                bin_datum['%s_tibs' % root] = bin_datum['%s_gibs' % root] / 1024.0

        binned_data.append(bin_datum)

    return binned_data

def print_datum(datum=None, units='GiB'):
    """
    Print out the relevant fields from a bin containing aggregated values

    Args:
        datum (dict): the results of bin_datum
        units (str): units to print (KiB, MiB, GiB, TiB)
    """

    if datum is None:
        return "%28s %12s %12s %5s %5s %5s %5s %5s\n" % ("date/time",
                                                         "%ss read" % units.lower(),
                                                         "%ss writ" % units.lower(),
                                                         "ossav", "ossmx", "mdsav",
                                                         "mdsmx", "mssng")

    # The per-line "mssng" only counts read/write bytes, not CPU data
    frac_missing = float(datum.get('missing_ost_read', 0) + datum.get('missing_ost_write', 0)) \
                   / (datum.get('num_ost_read', 0) + datum.get('num_ost_write', 0))

    print_str = "%19s-%8s " % (datum['tstart'].strftime("%Y-%m-%d %H:%M:%S"),
                               datum['tend'].strftime("%H:%M:%S"))
    print_str += ("%%(sum_ost_read_%ss)12.2f " % units.lower()) % datum
    print_str += ("%%(sum_ost_write_%ss)12.2f " % units.lower()) % datum

    # The following data may not always be present; leave them blank if missing
    if 'ave_oss_cpu' in datum:
        print_str += "%(ave_oss_cpu)5.1f " % datum
    else:
        print_str += "%5s " % ''
    if 'max_oss_cpu' in datum:
        print_str += "%(max_oss_cpu)5.1f " % datum
    else:
        print_str += "%5s " % ''
    if 'ave_mds_cpu' in datum:
        print_str += "%(ave_mds_cpu)5.1f " % datum
    else:
        print_str += "%5s " % ''
    if 'max_mds_cpu' in datum:
        print_str += "%(max_mds_cpu)5.1f" % datum
    else:
        print_str += "%5s" % ''

    print_str += "%6.1f" % (100.0 * frac_missing)

    return print_str + "\n"

def print_data_summary(data, units='TiB'):
    """
    Print the output of the summarize_reduced_data function in a human-readable format
    """
    totals = summarize_reduced_data(data)

    print_str = ""

    # Data target summary
    print_str += ("Total read:  %%(sum_%ss_read)14.2f %s" % (units.lower(), units)) % totals
    if 'frac_missing_ost_read' in totals:
        print_str += ", %5.1f%% missing\n" % (100.0 * totals['frac_missing_ost_read'])
    else:
        print_str += "\n"
    print_str += ("Total write: %%(sum_%ss_write)14.2f %s" % (units.lower(), units)) % totals
    if 'frac_missing_ost_write' in totals:
        print_str += ", %5.1f%% missing\n" % (100.0 * totals['frac_missing_ost_write'])
    else:
        print_str += "\n"

    # Data server summary
    if math.copysign(1, totals.get('oss_max', -0.0)) > 0.0:
        print_str += "Average OSS CPU:        %(oss_ave)6.2f%%" % totals
        if 'frac_missing_oss_cpu' in totals:
            print_str += ", %5.1f%% missing\n" % (100.0 * totals['frac_missing_oss_cpu'])
        else:
            print_str += "\n"
        print_str += "Max OSS CPU:            %(oss_max)6.2f%%\n" % totals

    # Metadata server summary
    if math.copysign(1, totals.get('mds_max', -0.0)) > 0.0:
        print_str += "Average MDS CPU:        %(mds_ave)6.2f%%" % totals
        if 'frac_missing_mds_cpu' in totals:
            print_str += ", %5.1f%% missing\n" % (100.0 * totals['frac_missing_mds_cpu'])
        else:
            print_str += "\n"
        print_str += "Max MDS CPU:            %(mds_max)6.2f%%\n" % totals

    return print_str

def main(argv=None):
    """Entry point for the CLI interface
    """
    parser = argparse.ArgumentParser(
        description='aggregate bytes in/out from h5lmt file every hour')
    parser.add_argument('h5lmt', type=str, nargs='+',
                        help='h5lmt file or basename to examine')
    parser.add_argument('--summary', action='store_true', help='print a summary of all output')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--bytes', action='store_true', help='print bytes')
    group.add_argument('--kibs', action='store_true', help='print in units of KiBs')
    group.add_argument('--mibs', action='store_true', help='print in units of MiBs')
    group.add_argument('--gibs', action='store_true',
                       help='print in units of GiBs (default)')
    group.add_argument('--tibs', action='store_true', help='print in units of TiBs')
    parser.add_argument('--bins', type=int, default=24,
                        help="number of bins per day")
    parser.add_argument('--start', type=str, default=None,
                        help="date/time to start in %s format" % DATE_FMT_STR)
    parser.add_argument('--end', type=str, default=None,
                        help="date/time to end in %s format" % DATE_FMT_STR)
    parser.add_argument('--json', action='store_true',
                        help='return json output')
    args = parser.parse_args(argv)

    if args.bytes:
        units = 'byte'
    elif args.kibs:
        units = 'KiB'
    elif args.mibs:
        units = 'MiB'
    elif args.tibs:
        units = 'TiB'
    else: # default units
        units = 'GiB'

    # Figure out the list of HDF5 files to open
    if args.start and args.end:
        hdf5_basename = os.path.basename(args.h5lmt[0])
        if len(args.h5lmt) > 1:
            warnings.warn("multiple h5lmt files specified with --start/--end; only using "
                          + hdf5_basename)
        hdf5_filenames = [x[0] for x in tokio.tools.hdf5.get_files_and_indices(
            hdf5_basename,
            list(DATASETS_TO_BIN_KEYS.keys())[0],
            datetime.datetime.strptime(args.start, DATE_FMT),
            datetime.datetime.strptime(args.end, DATE_FMT))]
    else:
        hdf5_filenames = args.h5lmt

    # Process each HDF5 file and append its bins to a global dictionary
    all_binned_data = {}
    if not args.json:
        sys.stdout.write(print_datum(None, units=units))
    for hdf5_filename in hdf5_filenames:
        hdf5_file = tokio.connectors.hdf5.Hdf5(hdf5_filename, 'r')
        binned_data = bin_datasets(hdf5_file=hdf5_file,
                                   dataset_names=list(DATASETS_TO_BIN_KEYS.keys()),
                                   orient='index',
                                   num_bins=args.bins)

        if not args.json:
            for timestamp in sorted(binned_data.keys()):
                print(print_datum(binned_data[timestamp], units=units).strip())

        all_binned_data.update(binned_data)

    # Print binned results in the desired format
    if args.json:
        if args.summary:
            to_serialize = {
                'bins': all_binned_data,
                'summary': summarize_reduced_data(all_binned_data),
            }
        else:
            to_serialize = {'bins': all_binned_data}
        print(json.dumps(to_serialize,
                         sort_keys=True,
                         indent=4,
                         cls=tokio.common.JSONEncoder))
    elif args.summary:
        sys.stdout.write(print_data_summary(all_binned_data, units=units))
