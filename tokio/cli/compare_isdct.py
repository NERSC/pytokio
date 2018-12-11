"""
Compare two NERSC ISDCT dumps and report
    1. the devices that appeared or were removed
    2. the numeric counters whose values changed
    3. the string counters whose contents changed
"""

import json
import argparse
import warnings
import datetime
import tokio.connectors.nersc_isdct
from tokio.common import isstr

# If the following keys report changes, the drive should be flagged
ERROR_KEYS = [
    'crc_error_count',
    'critical_warnings',
    'end_to_end_error_detection_count',
    'erase_fail_count',
    'media_errors',
    'number_of_error_info_log_entries',
    'pci_link_gen_speed',
    'pci_link_width',
    'program_fail_count',
    'smart_crc_error_count_raw',
    'smart_endto_end_error_detection_count_raw',
    'smart_erase_fail_count_raw',
    'smart_pli_lock_loss_count_raw',
    'smart_program_fail_count_raw',
    'thermal_throttle_count',
]

def reduce_diff(diff_dict):
    """
    Take the raw output of .diff() and aggregate the results of each device
    """
    reduced = {
        'sum': {},
        'min': {},
        'max': {},
        'count': {},
    }
    for counters in diff_dict['devices'].values():
        for counter, value in counters.items():
            if counter not in reduced['count']:
                reduced['count'][counter] = 1
                new = True
            else:
                reduced['count'][counter] += 1
                new = False

            if not isstr(value):
                if new:
                    reduced['sum'][counter] = value
                    reduced['min'][counter] = value
                    reduced['max'][counter] = value
                else:
                    reduced['sum'][counter] += value
                    reduced['min'][counter] = min(value, reduced['min'][counter])
                    reduced['max'][counter] = max(value, reduced['max'][counter])

    result = {}
    for reduction, counters in reduced.items():
        for counter, value in counters.items():
            reduced_key = "%s_%s" % (reduction, counter)
            result[reduced_key] = value
            if reduction == 'sum':
                reduced_key = "%s_%s" % ('ave', counter)
                result[reduced_key] = float(value) / reduced['count'][counter]

    return result

def _convert_counters(counters, conversion_factor, label):
    """
    Convert a single flat dictionary of counters of bytes into another unit
    """
    results = {}

    # convert each relevant key
    for counter, value in counters.items():
        if counter.endswith('_bytes'):
            new_key = counter.replace('_bytes', "_" + label)
            new_value = value * conversion_factor
        else:
            new_key = counter
            new_value = value
        results[new_key] = new_value

    return results

def convert_byte_keys(input_dict, conversion_factor=2.0**(-30.0), label="gibs"):
    """
    Convert all keys ending in _bytes to some other unit.  Accepts either the
    raw diff dict or the reduced dict from reduce_diff()
    """
    results = {}
    # raw diff dict
    if 'devices' in input_dict:
        results = input_dict.copy()
        for serial_no, counters in input_dict['devices'].items():
            results['devices'][serial_no] = _convert_counters(counters, conversion_factor, label)
    else:
        results = _convert_counters(input_dict, conversion_factor, label)

    return results

def summarize_reduced_diffs(reduced_diffs):
    """
    Print a human-readable summary of the relevant reduced diff data
    """
    buf = ""
    ### General summary
    if 'sum_data_units_read_gibs' not in reduced_diffs:
        read_gibs = reduced_diffs.get('sum_data_units_read_bytes', 0) * 2.0**(-40)
        write_gibs = reduced_diffs.get('sum_data_units_written_bytes', 0) * 2.0**(-40)
    else:
        read_gibs = reduced_diffs.get('sum_data_units_read_gibs', 0)
        write_gibs = reduced_diffs.get('sum_data_units_written_gibs', 0)

    buf += "Read:    %10.2f TiB, %10.2f MOps\n" % (
        read_gibs,
        reduced_diffs.get('sum_host_read_commands', 0) / 1000000.0)
    buf += "Written: %10.2f TiB, %10.2f MOps\n" % (
        write_gibs,
        reduced_diffs.get('sum_host_write_commands', 0) / 1000000.0)
    buf += "WAF:     %+10.4f\n" % reduced_diffs.get('max_write_amplification_factor', 0)
    return buf

def summarize_errors(diff_dict, isdct_data):
    """
    Print a human-readable summary of any bad SSDs
    """
    buf = ""
    for serial_no, error_key in discover_errors(diff_dict):
        if buf != "":
            buf += "\n"
        buf += "%s %s %s %s" % (
            isdct_data[serial_no]['node_name'],
            serial_no,
            error_key,
            diff_dict['devices'][serial_no][error_key])
    return buf

def discover_errors(diff_dict):
    """
    Look through all diffs and report serial numbers of devices that show
    changes in counters that may indicate a hardware issue.
    """
    if 'devices' not in diff_dict:
        warnings.warn("No 'devices' found in diff dict")
        return []

    errors = []
    for serial_no, counters in diff_dict['devices'].items():
        for error_key in ERROR_KEYS:
            if error_key in counters:
                errors.append((serial_no, error_key))

    return errors

def print_summary(old_isdctfile, new_isdctfile, diff_dict):
    """Print a human-readable summary of diff_dict
    """
    reduced_diff = reduce_diff(diff_dict)
    diff_buf = summarize_reduced_diffs(reduced_diff)
    err_buf = summarize_errors(diff_dict, new_isdctfile)

    print("=== ISDCT Summary: %s - %s ===" % (
        datetime.datetime.fromtimestamp(next(iter(old_isdctfile.values()))['timestamp']),
        datetime.datetime.fromtimestamp(next(iter(new_isdctfile.values()))['timestamp'])))

    if len(err_buf) > 0:
        print("\n*** Errors Detected! ***")
        print(err_buf)

    if len(diff_dict['removed_devices']) > 0:
        print("\n=== Devices Removed ===")
        for dev in diff_dict['removed_devices']:
            print("%s %s" % (old_isdctfile[dev]['node_name'], dev))

    if len(diff_dict['added_devices']) > 0:
        print("\n=== Devices Installed ===")
        for dev in diff_dict['added_devices']:
            print("%s %s" % (new_isdctfile[dev]['node_name'], dev))

    if len(diff_buf) > 0:
        print("\n=== Workload Statistics ===")
        print(diff_buf)

def main(argv=None):
    """Entry point for the CLI interface
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--all", action='store_true',
                        help='report changes for each device')
    parser.add_argument("-g", "--gibs", action="store_true",
                        help="report in units of GiB")
    parser.add_argument("-s", "--summary", action="store_true",
                        help="print summary of differences")
    parser.add_argument("-z", "--report-zeros", action='store_true',
                        help='include counters that do not change')
    parser.add_argument("old_isdctfile", help="older ISDCT dump file")
    parser.add_argument("new_isdctfile", help="newer ISDCT dump file")
    args = parser.parse_args(argv)

    old_isdctfile = tokio.connectors.nersc_isdct.NerscIsdct(args.old_isdctfile)
    new_isdctfile = tokio.connectors.nersc_isdct.NerscIsdct(args.new_isdctfile)
    diff_dict = new_isdctfile.diff(old_isdctfile, report_zeros=args.report_zeros)

    if args.gibs:
        diff_dict = convert_byte_keys(diff_dict)

    if args.summary:
        print_summary(old_isdctfile, new_isdctfile, diff_dict)
    elif args.all:
        print(json.dumps(diff_dict, indent=4, sort_keys=True))
    else:
        reduced_diff = reduce_diff(diff_dict)
        print(json.dumps(reduced_diff, indent=4, sort_keys=True))
