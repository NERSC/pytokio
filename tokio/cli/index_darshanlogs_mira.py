"""
Creates an SQLite database that summarizes the key metrics from a collection of
Darshan logs.  The database is constructed in a way that facilitates the
determination of how much I/O is performed to different file systems.

Full documentation is in the index_darshanlogs module.  This module provides
extended functionality for a very specific use case on ALCF's Mira system.

"""

import os
import time
import sqlite3
import functools
import subprocess
import argparse
import warnings
import multiprocessing
import tokio.connectors.darshan
from . import index_darshanlogs

def summarize_by_fs(darshan_log, max_mb=0.0):
    """
    Dispatches a Darshan log summarizer
    """
    if 0.0 < max_mb <= (os.path.getsize(darshan_log) / 1048576.0):
        return summarize_by_fs_fast(darshan_log)
    return index_darshanlogs.summarize_by_fs_lite(darshan_log)

def summarize_by_fs_fast(darshan_log):
    """Generates summary scalar values for a Darshan log

    Use a special darshan-per-fs-parser C tool to build the per-Darshan log
    index outside of Python.  Fastest option for munging Darshan 2.x logs.
    Requires the darshan-per-fs-parser utility from

    https://xgitlab.cels.anl.gov/carns/darshan/tree/carns/dev-tokio-hack1

    whose CLI output is::

        # WARNING: version 2.05 log format has the following limitations:
        # - CP_F_OPEN_TIMESTAMP marks when the first open completed rather than when the first open started.
        # darshan log version: 2.05
        # size of file statistics: 1328 bytes
        # size of job statistics: 1080 bytes
        # exe: /gpfs/mira-fs0/projects/NumSimFuel_2/kaneko/vh053/00023/./nek5000 <unknown args>
        # uid: 32530
        # jobid: 1760690
        # start_time: 1551319728
        # start_time_asci: Thu Feb 28 02:08:48 2019
        # end_time: 1551321146
        # end_time_asci: Thu Feb 28 02:32:26 2019
        # nprocs: 8192
        # run time: 1419
        # metadata: lib_ver = 2.3.0
        # metadata: h = romio_no_indep_rw=true;cb_nodes=4

        # mounted file systems (device, mount point, and fs type)
        # -------------------------------------------------------
        # mount entry: 7387929357332030708  /projects   nfs
        # mount entry: -6264612118694895410 /bgsys  nfs
        # mount entry: -648807988769344735  /   nfs
        # mount entry: 7201891024794563921  /gpfs/mira-fs0  gpfs
        # mount entry: -8912543917457837569 /gpfs/mira-home gpfs
        # mount entry: 3108106406760417729  /gpfs/mira-fs1  gpfs

        # <fs>  <counter>   <summation>
        /gpfs/mira-fs0  CP_BYTES_READ   241366358
        /gpfs/mira-fs0  CP_BYTES_WRITTEN    1885820558


    This function returns a dictionary of the form::

        {
            "headers": {
                "end_time": 1490000983,
                ...
                "walltime": 117
            },
            "mounts": {
                "/scratch2": "scratch2",
                "UNKNOWN": "UNKNOWN"
            },
            "summaries": {
                "/scratch2": {
                    "bytes_read": 0,
                    ...
                    "writes": 16402
                },
                "UNKNOWN": {
                    "bytes_read": 0,
                    ...
                    "writes": 74
                }
            }
        }

    Args:
        darshan_log (str): Path to a Darshan log file

    Returns:
        dict: Contains three keys (summaries, mounts, and headers) whose values
            are dicts of key-value pairs corresponding to scalar summary values
            from the POSIX module which are reduced over all files sharing a
            common mount point.
    """
    if not os.path.isfile(darshan_log):
        if not QUIET:
            errmsg = "Unable to open %s" % darshan_log
            warnings.warn(errmsg)
        return {}

    # hack in UNKNOWN for the stdio module since it does not appear in the mount table
    mount_list = ["UNKNOWN"]

    logical_mount_names = {} # mapping of mountpoint : logical fs name
    header = {
        'filename': os.path.basename(darshan_log),
    }
    reduced_counters = {} # mounts->counters

    dparser = subprocess.Popen(['darshan-per-fs-parser', darshan_log],
                               stdout=subprocess.PIPE)

    logvers = 3
    while True:
        line = dparser.stdout.readline().decode('utf-8')
        if not line:
            break

        # find header lines
        if line.startswith('# darshan log version:'):
            header['version'] = line.split(":", 1)[-1].strip()
            if header['version'].startswith('2'):
                logvers = 2
            continue
        elif line.startswith('# exe:'):
            header['exe'] = line.split(":", 1)[-1].strip().split()
            continue
        elif line.startswith('# uid:'):
            header['uid'] = int(line.split(":", 1)[-1].strip())
            continue
        elif line.startswith('# jobid:'):
            header['jobid'] = line.split(":", 1)[-1].strip()
            continue
        elif line.startswith('# start_time:'):
            header['start_time'] = int(line.split(":", 1)[-1])
            continue
        elif line.startswith('# end_time:'):
            header['end_time'] = int(line.split(":", 1)[-1])
            continue
        elif line.startswith('# nprocs:'):
            header['nprocs'] = int(line.split(":", 1)[-1])
            continue
        elif line.startswith('# run time:'):
            header['walltime'] = int(line.split(":", 1)[-1])
            continue

        # find mount table lines
        elif line.startswith('# mount entry:'):
            if logvers == 2:
                mountpt = line.split(':', 1)[-1].strip().split(None, 1)[-1].rsplit(None, 1)[0]
            else:
                mountpt = line.split(':', 1)[-1].strip().split(None, 1)[0]
            mount_list.append(mountpt)
            continue

        # find counter lines
        elif not line.startswith('#'):
            fields = line.split()
            if len(fields) != 3:
                continue

            mount = fields[0]
            mount = index_darshanlogs.get_file_mount(mount, mount_list)
            if mount is None:
                continue

            value = None
            reducer = None

            counter = fields[1][3:].lower()
            value = int(fields[2])
            reducer = index_darshanlogs.INTEGER_COUNTERS.get(counter)

            if value is None:
                continue

            mount, logical = mount
            logical_mount_names[mount] = logical
            if mount not in reduced_counters:
                reduced_counters[mount] = {}

            if counter not in reduced_counters[mount]:
                reduced_counters[mount][counter] = value
            elif not reducer:
                reduced_counters[mount][counter] = value
            else:
                reduced_counters[mount][counter] = reducer(reduced_counters[mount][counter], value) if reduced_counters[mount][counter] is not None else value

            continue

    # if the file could be opened and read but contained no valid Darshan data,
    # it will have a valid header but no counters; bail
    if not reduced_counters:
        return {}

    # fix header entries
    header['exe'] = header['exe'][0]
    header['exename'] = os.path.basename(header['exe'])

    # username is resolved here so that it can be indexed without having to mess around
    filename_metadata = tokio.connectors.darshan.parse_filename_metadata(darshan_log)
    header['username'] = filename_metadata.get('username')

    # Populate the mounts data and remove all mount points that were not used
    mountpts = {}
    for key in list(reduced_counters.keys()):
        # check_val works only when all the keys are positive counters
        check_val = sum([reduced_counters[key].get(x, 0) for x in (
            'bytes_read', 'bytes_written', 'reads', 'writes', 'opens', 'stats')])
        if check_val == 0:
            reduced_counters.pop(key, None)
        else:
            mountpts[key] = logical_mount_names.get(key, key)

    # fill in counters that should exist but weren't encountered.  This happens
    # for counters that are not shared between POSIX and STDIO and is just here
    # to make the data structure from this parser identical to the full parser
    for counter in index_darshanlogs.INTEGER_COUNTERS:
        for mount in reduced_counters:
            if counter not in reduced_counters[mount]:
                reduced_counters[mount][counter] = None
    for counter in index_darshanlogs.REAL_COUNTERS:
        for mount in reduced_counters:
            if counter not in reduced_counters[mount]:
                reduced_counters[mount][counter] = None

    for mount in reduced_counters:
        # needed so the log filename can be referenced during updates to the summaries table
        reduced_counters[mount]['filename'] = os.path.basename(darshan_log)

    return {
        'summaries': reduced_counters,
        'headers': header,
        'mounts': mountpts
    }

def index_darshanlogs_mira(log_list, output_file, threads=1, max_mb=0.0, bulk_insert=True):
    """Calculate the sum bytes read/written

    Given a list of input files, parse each as a Darshan log in parallel to
    create a list of scalar summary values correspond to each log and insert
    these into an SQLite database.

    Args:
        log_list (list of str): Paths to Darshan logs to be processed
        output_file (str): Path to a SQLite database file to populate
        threads (int): Number of subprocesses to spawn for Darshan log parsing
        max_mb (float): Skip logs of size larger than this value
        bulk_insert (bool): If False, have each thread update the database
            as soon as it has parsed a log

    Returns:
        dict: Reduced data along different reduction dimensions
    """

    conn = sqlite3.connect(output_file)

    index_darshanlogs.init_mount_to_fsname()

    t_start = time.time()
    new_log_list = index_darshanlogs.process_log_list(conn, log_list)
    index_darshanlogs.vprint("Built log list in %.1f seconds" % (time.time() - t_start), 2)

    # Create tables and indices
    t_start = time.time()
    index_darshanlogs.create_mount_table(conn)
    index_darshanlogs.create_headers_table(conn)
    index_darshanlogs.create_summaries_table(conn)
    index_darshanlogs.vprint("Initialized tables in %.1f seconds" % (time.time() - t_start), 2)

    # Analyze the remaining logs in parallel
    t_start = time.time()
    log_records = []
    mount_points = {}
    if threads == 1:
        for new_log in new_log_list:
            result = summarize_by_fs(new_log, max_mb=max_mb)
            if result:
                log_records.append(result)
                mount_points.update(result['mounts'])
                if not bulk_insert:
                    index_darshanlogs.insert_summary(conn, result)
    else:
        for result in multiprocessing.Pool(threads).imap_unordered(functools.partial(summarize_by_fs, max_mb=max_mb), new_log_list):
            if result:
                log_records.append(result)
                mount_points.update(result['mounts'])
                if not bulk_insert:
                    index_darshanlogs.insert_summary(conn, result)

    index_darshanlogs.vprint("Ingested %d logs in %.1f seconds" % (len(log_records), time.time() - t_start), 2)

    # Insert new data that was collected in parallel
    if bulk_insert:
        t_start = time.time()
        index_darshanlogs.update_mount_table(conn, mount_points)
        index_darshanlogs.vprint("Updated mounts table in %.1f seconds" % (time.time() - t_start), 2)
        t_start = time.time()
        index_darshanlogs.update_headers_table(conn, [x['headers'] for x in log_records])
        index_darshanlogs.vprint("Updated headers table in %.1f seconds" % (time.time() - t_start), 2)
        t_start = time.time()
        index_darshanlogs.update_summaries_table(conn, [x['summaries'] for x in log_records])
        index_darshanlogs.vprint("Updated summaries table in %.1f seconds" % (time.time() - t_start), 2)

    conn.close()
    index_darshanlogs.vprint("Updated %s" % output_file, 1)


def main(argv=None):
    """Entry point for the CLI interface
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("darshanlogs", nargs="+", type=str, help="Darshan logs to process")
    parser.add_argument('-t', '--threads', default=1, type=int,
                        help="Number of concurrent processes (default: 1)")
    parser.add_argument('-o', '--output', type=str, default='darshanlogs.db', help="Name of output file (default: darshanlogs.db)")
    parser.add_argument('-m', '--max-mb', type=float, default=0.0, help="Maximum log file before switching to lite parser (default: 0.0 (disabled))")
    parser.add_argument('-v', '--verbose', action='count', default=0, help="Verbosity level (default: none)")
    parser.add_argument('-q', '--quiet', action='store_true', help="Suppress warnings for invalid Darshan logs")
    parser.add_argument('--no-bulk-insert', action='store_true', help="Insert each log record as soon as it is processed")
    args = parser.parse_args(argv)

    index_darshanlogs.VERBOSITY = args.verbose
    index_darshanlogs.QUIET = args.quiet

    index_darshanlogs_mira(log_list=args.darshanlogs,
                           threads=args.threads,
                           max_mb=args.max_mb,
                           bulk_insert=not args.no_bulk_insert,
                           output_file=args.output)
