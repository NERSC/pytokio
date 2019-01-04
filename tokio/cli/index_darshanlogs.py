"""
TODO

Schemata:

CREATE TABLE summaries (
    log_id INTEGER,
    fs_id INTEGER,

    bytes_read INTEGER,
    bytes_written INTEGER,
    reads INTEGER,
    writes INTEGER,
    file_not_aligned INTEGER,
    consec_reads INTEGER
    consec_writes INTEGER,

    mmaps INTEGER,
    opens INTEGER,
    seeks INTEGER,
    stats INTEGER,
    fdsyncs INTEGER,
    fsyncs INTEGER,

    seq_reads INTEGER,
    seq_writes INTEGER,
    rw_switches INTEGER,

    f_close_start_timestamp REAL,
    f_close_end_timestamp REAL,
    f_open_start_timestamp REAL,
    f_open_end_timestamp REAL,

    f_read_start_timestamp REAL,
    f_read_end_timestamp REAL,

    f_write_start_timestamp REAL,
    f_write_end_timestamp REAL,

    FOREIGN KEY (fs_id) REFERENCES mounts (fs_id),
    FOREIGN KEY (log_id) REFERENCES headers (log_id)
);

CREATE TABLE mounts (
    fs_id INTEGER PRIMARY KEY,
    mountpt CHAR
);

CREATE TABLE headers (
    log_id INTEGER PRIMARY KEY,
    filename CHAR UNIQUE,
    end_time INTEGER,
    exe CHAR,
    jobid CHAR,
    nprocs INTEGER,
    start_time INTEGER,
    uid INTEGER,
    log_version CHAR,
    walltime INTEGER
);
"""

import os
import pprint
import sqlite3
import functools
import argparse
import warnings
import multiprocessing
import tokio.connectors.darshan

INTEGER_COUNTERS = [
    'bytes_read',
    'bytes_written',
    'reads',
    'writes',
    'file_not_aligned',
    'consec_reads',
    'consec_writes',
    'mmaps',
    'opens',
    'seeks',
    'stats',
    'fdsyncs',
    'fsyncs',
    'seq_reads',
    'seq_writes',
    'rw_switches',
]

REAL_COUNTERS = [
    'f_close_start_timestamp',
    'f_close_end_timestamp',
    'f_open_start_timestamp',
    'f_open_end_timestamp',
    'f_read_start_timestamp',
    'f_read_end_timestamp',
    'f_write_start_timestamp',
    'f_write_end_timestamp',
]

HEADER_COUNTERS = [
    'end_time',
    'jobid',
    'nprocs',
    'start_time',
    'uid',
    'version',
    'walltime',
]

MOUNT_TABLE = "mounts"
HEADERS_TABLE = "headers"
SUMMARIES_TABLE = "summaries"

def summarize_by_fs(darshan_log, max_mb=0):
    """
    Parse a Darshan log and add up the bytes read and written to each entry in
    the mount table.
    """
    result = {
        'summaries': {},
        'mounts': set([]),
        'headers': {},
    }
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

    #
    # Populate the summaries data
    #

    # Initialize sums
    for mount in mount_list:
        result['summaries'][mount] = {}
        for counter in INTEGER_COUNTERS:
            result['summaries'][mount][counter] = 0
        for counter in REAL_COUNTERS:
            result['summaries'][mount][counter] = 0.0
        result['summaries'][mount]['filename'] = os.path.basename(darshan_data.log_file)

    # For each file instrumented, find its mount point and increment that
    # mount's counters
    for posix_file in posix_counters:
        for mount in mount_list:
            if posix_file.startswith(mount):
                for counters in posix_counters[posix_file].values():
                    for counter in INTEGER_COUNTERS + REAL_COUNTERS:
                        result['summaries'][mount][counter] += counters.get(counter.upper(), 0)
                break # don't apply these bytes to more than one mount

    #
    # Populate the mounts data and remove all mount points that were not used
    #
    for key in list(result['summaries'].keys()):
        # check_val works only when all the keys are positive counters
        check_val = sum([result['summaries'][key][x] for x in ('bytes_read', 'bytes_written', 'reads', 'writes', 'opens')])
        if check_val == 0:
            result['summaries'].pop(key, None)
        else:
            result['mounts'].add(key)

    #
    # Populate the headers data
    #
    counters = darshan_data.get('header')
    print(darshan_data.keys())
    result['headers']['filename'] = os.path.basename(darshan_data.log_file)
    for counter in HEADER_COUNTERS:
        result['headers'][counter] = counters.get(counter)

    result['headers']['exe'] = counters.get('exe')[0]

    return result

def create_mount_table(conn):
    """Creates the mount table
    """
    cursor = conn.cursor()

    query = """CREATE TABLE IF NOT EXISTS %s (
        fs_id INTEGER PRIMARY KEY,
        mountpt CHAR UNIQUE
    )
    """ % MOUNT_TABLE
    print(query)
    cursor.execute(query)

    cursor.close()
    conn.commit()

def update_mount_table(conn, mount_points):
    """Adds new mount points to the mount table
    """
    cursor = conn.cursor()

    for mount_point in mount_points:
        print("INSERT OR IGNORE INTO %s (mountpt) VALUES (?)" % MOUNT_TABLE)
        print((mount_point,))

    cursor.executemany("INSERT OR IGNORE INTO %s (mountpt) VALUES (?)" % MOUNT_TABLE, [(x,) for x in mount_points])

    cursor.close()
    conn.commit()

def create_headers_table(conn):
    """Creates the headers table
    """
    cursor = conn.cursor()
    query = """CREATE TABLE IF NOT EXISTS %s (
        log_id INTEGER PRIMARY KEY,
        filename CHAR UNIQUE,
        end_time INTEGER,
        exe CHAR,
        jobid CHAR,
        nprocs INTEGER,
        start_time INTEGER,
        uid INTEGER,
        version CHAR,
        walltime INTEGER
    )
    """ % HEADERS_TABLE
    print(query)
    cursor.execute(query)
    cursor.close()
    conn.commit()

def update_headers_table(conn, header_data):
    """Adds new header data to the headers table
    """
    cursor = conn.cursor()

    header_counters = ["filename", "exe"] + HEADER_COUNTERS

    query = "INSERT INTO %s (" % HEADERS_TABLE
    query += ", ".join(header_counters)
    query += ") VALUES (" + ",".join(["?"] * len(header_counters))
    query += ")"

    for header_datum in header_data:
        print(query)
        print(tuple([header_datum[x] for x in header_counters]))
    cursor.executemany(query, [tuple([header_datum[x] for x in header_counters]) for header_datum in header_data])

    cursor.close()
    conn.commit()

def create_summaries_table(conn):
    """Creates the summaries table
    """
    cursor = conn.cursor()
    query = """CREATE TABLE IF NOT EXISTS %s (
        log_id INTEGER,
        fs_id INTEGER,
    """ % SUMMARIES_TABLE

    query += " INTEGER,\n    ".join(INTEGER_COUNTERS) + " INTEGER, \n    "
    query += " REAL,\n    ".join(REAL_COUNTERS) + " REAL,\n    "

    query += """
        FOREIGN KEY (fs_id) REFERENCES mounts (fs_id),
        FOREIGN KEY (log_id) REFERENCES headers (log_id)
    )
    """
    print(query)
    cursor.execute(query)
    cursor.close()
    conn.commit()

def update_summaries_table(conn, summary_data):
    """Adds new summary counters to the summaries table
    """
    cursor = conn.cursor()

    base_query = "INSERT INTO %s (" % SUMMARIES_TABLE
    base_query += "log_id, fs_id, " + ", ".join(INTEGER_COUNTERS + REAL_COUNTERS) + ")\nVALUES\n("
    for per_fs_counters in summary_data:
        for mountpt, summary_datum in per_fs_counters.items():
            query = base_query + "(SELECT log_id from headers where filename = '%s'),\n" % summary_datum['filename']
            query += "(SELECT fs_id from mounts where mountpt = '%s'),\n" % mountpt 
            query += ",\n".join(["?"] * len(INTEGER_COUNTERS + REAL_COUNTERS))
            query += "\n)"
            print(query)
            print(tuple([summary_datum[x] for x in (INTEGER_COUNTERS + REAL_COUNTERS)]))
            cursor.execute(query, tuple([summary_datum[x] for x in (INTEGER_COUNTERS + REAL_COUNTERS)]))

    cursor.close()
    conn.commit()

def _summarize_by_fs_parallel(darshan_log, max_mb):
    """
    Return a tuple containing the Darshan log name and the results of
    summarize_by_fs() to the parallel orchestrator.
    """
    return (darshan_log, summarize_by_fs(darshan_log, max_mb))

def index_darshanlogs(log_list, threads=1, max_mb=0):
    """Calculate the sum bytes read/written

    Given a list of input files, process each as a Darshan log and return a
    dictionary, keyed by logfile name, containing the bytes read/written per
    file system mount point.

    Args:
        log_list (list): paths to Darshan logs to be processed
        threads (int): number of subprocesses to spawn for Darshan log parsing
        max_mb (int): skip logs of size larger than this value

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

    global_results = []

    # Filter out logs that were already processed
    remove_list = []
    # TODO: updated logic based on SQLite
#   for log_name in new_log_list:
#       if log_name in global_results:
#           remove_list.append(log_name)
    for log_name in remove_list:
        new_log_list.remove(log_name)

    # Analyze the remaining logs in parallel
    mount_points = set([])
    for log_name, result in multiprocessing.Pool(threads).imap_unordered(functools.partial(_summarize_by_fs_parallel, max_mb=max_mb), new_log_list):
        if result:
            global_results.append(result)
            mount_points |= result['mounts']

    conn = sqlite3.connect('blah.db')
    create_mount_table(conn)
    create_headers_table(conn)
    create_summaries_table(conn)
    update_mount_table(conn, mount_points)
    update_headers_table(conn, [x['headers'] for x in global_results])
    update_summaries_table(conn, [x['summaries'] for x in global_results])

    return global_results

def main(argv=None):
    """Entry point for the CLI interface
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("darshanlogs", nargs="+", type=str, help="Darshan logs to process")
    parser.add_argument('-t', '--threads', default=1, type=int,
                        help="Number of concurrent processes")
    parser.add_argument('-o', '--output', type=str, default=None, help="Name of output file")
    parser.add_argument('-m', '--max-mb', type=int, default=0, help="Maximum log file size to consider")
    args = parser.parse_args(argv)

    global_results = index_darshanlogs(log_list=args.darshanlogs,
                                       threads=args.threads,
                                       max_mb=args.max_mb)

#   pprinter = pprint.PrettyPrinter(indent=4)
#   pprinter.pprint(global_results)
