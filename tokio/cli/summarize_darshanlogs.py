"""
Calculate the bytes read and written per file system for one or more Darshan
logs, then return these data as json.  Can process logs in parallel, but be
warned that this can consume significant amounts of system memory.
"""

import os
import json
import functools
import argparse
import warnings
import multiprocessing
import tokio.connectors.darshan

def process_log(darshan_log, max_mb=0):
    """
    Parse a Darshan log and add up the bytes read and written to each entry in
    the mount table.
    """
    result = {}
    if max_mb and (os.path.getsize(darshan_log) / 1024 / 1024) > max_mb:
        errmsg = "Skipping %s due to size (%d MiB)" % (darshan_log, (os.path.getsize(darshan_log) / 1024 / 1024))
        warnings.warn(errmsg)
        return result

    try:
        darshan_data = tokio.connectors.darshan.Darshan(darshan_log, silent_errors=True)
        darshan_data.darshan_parser_base()
    except:
        errmsg = "Unable to open or parse %s" % darshan_log
        warnings.warn(errmsg)
        return result

    posix_counters = darshan_data.get('counters', {}).get('posix')
    if not posix_counters:
        errmsg = "No counters found in %s" % darshan_log
        warnings.warn(errmsg)
        return result

    # reverse the mount to match the deepest path first and root path last
    mount_list = list(reversed(sorted(darshan_data.get('mounts', {}).keys())))
    if not mount_list:
        errmsg = "No mount table found in %s" % darshan_log
        warnings.warn(errmsg)
        return result

    # Initialize sums
    for mount in mount_list:
        result[mount] = {'read_bytes': 0, 'write_bytes': 0}

    # For each file instrumented, find its mount point and increment that
    # mount's counters
    for posix_file in posix_counters:
        for mount in mount_list:
            if posix_file.startswith(mount):
                for counters in posix_counters[posix_file].values():
                    result[mount]['read_bytes'] += counters.get('BYTES_READ', 0)
                    result[mount]['write_bytes'] += counters.get('BYTES_WRITTEN', 0)
                break # don't apply these bytes to more than one mount

    # Remove all mount points that saw zero I/O
    for key in list(result.keys()):
        if result[key]['read_bytes'] == 0 and result[key]['write_bytes'] == 0:
            result.pop(key, None)

    return result if result else {}

def _process_log_parallel(darshan_log, max_mb):
    """
    Return a tuple containing the Darshan log name and the results of
    process_log() to the parallel orchestrator.
    """
    return (darshan_log, process_log(darshan_log, max_mb))

def sum_bytes_per_fs(log_list, threads=1, max_mb=0, update=None):
    """Calculate the sum bytes read/written

    Given a list of input files, process each as a Darshan log and return a
    dictionary, keyed by logfile name, containing the bytes read/written per
    file system mount point.

    Args:
        log_list (list): paths to Darshan logs to be processed
        threads (int): number of subprocesses to spawn for Darshan log parsing
        max_mb (int): skip logs of size larger than this value
        update (dict): if provided, exclude Darshan logs that are already
            present in this dictionary and return the merger of the new data
            and this dictionary

    Returns:
        dict: The reduced data along different reduction dimensions
    """

    # If only one argument is passed in but it's a directory, enumerate all the
    # files in that directory.  This is a compromise for cases where the CLI
    # length limit prevents all darshan logs from being passed via argv, but
    # prevents every CLI arg from having to be checked to determine if it's a
    # darshan log or directory
    if len(log_list) == 1 and os.path.isdir(log_list[0]):
        new_log_list = []
        for filename in os.listdir(log_list[0]):
            filename = os.path.join(log_list[0], filename)
            if os.path.isfile(filename):
                new_log_list.append(filename)
    else:
        new_log_list = log_list

    if update:
        global_results = update
    else:
        global_results = {}

    # Filter out logs that were already processed
    remove_list = []
    for log_name in new_log_list:
        if log_name in global_results:
            remove_list.append(log_name)
    for log_name in remove_list:
        new_log_list.remove(log_name)

    # Analyze the remaining logs in parallel
    for log_name, result in multiprocessing.Pool(threads).imap_unordered(functools.partial(_process_log_parallel, max_mb=max_mb), new_log_list):
        if result:
            global_results[log_name] = result

    return global_results

def main(argv=None):
    """Entry point for the CLI interface
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("darshanlogs", nargs="+", type=str, help="Darshan logs to process")
    parser.add_argument('-t', '--threads', default=1, type=int,
                        help="Number of concurrent processes")
    parser.add_argument('-i', '--input', type=str, default=None, help='Name of input file to update')
    parser.add_argument('-o', '--output', type=str, default=None, help="Name of output file")
    parser.add_argument('-m', '--max-mb', type=int, default=0, help="Maximum log file size to consider")
    args = parser.parse_args(argv)

    if args.input:
        with open(args.input, 'r') as json_input:
            existing_data = json.load(json_input)
        starting_ct = len(existing_data)
    else:
        existing_data = None
        starting_ct = 0

    global_results = sum_bytes_per_fs(log_list=args.darshanlogs,
                                      threads=args.threads,
                                      max_mb=args.max_mb,
                                      update=existing_data)
    if args.output is None:
        print(json.dumps(global_results, sort_keys=True, indent=4))
    else:
        with open(args.output, 'w') as output_file:
            json.dump(global_results, output_file)
            print("Added %d log summaries to %s" % (len(global_results) - starting_ct, args.output))
