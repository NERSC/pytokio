"""
Creates an SQLite database that summarizes the key metrics from a collection of
Darshan logs.  The database is constructed in a way that facilitates the
determination of how much I/O is performed to different file systems.

Schemata::

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
        FOREIGN KEY (log_id) REFERENCES headers (log_id),
        UNIQUE(log_id, fs_id)
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
    """Generates summary scalar values for a Darshan log

    Args:
        darshan_log (str): Path to a Darshan log file
        max_mb (int): Skip logs of size larger than this value

    Returns:
        dict: Contains three keys (summaries, mounts, and headers) whose values
            are dicts of key-value pairs corresponding to scalar summary values
            from the POSIX module which are reduced over all files sharing a
            common mount point.
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

    # Populate the summaries data
    result['summaries'] = {}
    for mount in mount_list:
        result['summaries'][mount] = {}
        for counter in INTEGER_COUNTERS:
            result['summaries'][mount][counter] = 0
        for counter in REAL_COUNTERS:
            result['summaries'][mount][counter] = 0.0
        result['summaries'][mount]['filename'] = os.path.basename(darshan_data.log_file)

    # Reduce each counter (with a sum operator) according to its mount point
    for posix_file in posix_counters:
        for mount in mount_list:
            if posix_file.startswith(mount):
                for counters in posix_counters[posix_file].values():
                    for counter in INTEGER_COUNTERS + REAL_COUNTERS:
                        result['summaries'][mount][counter] += counters.get(counter.upper(), 0)
                break # don't apply these bytes to more than one mount

    # Populate the mounts data and remove all mount points that were not used
    result['mounts'] = set([])
    for key in list(result['summaries'].keys()):
        # check_val works only when all the keys are positive counters
        check_val = sum([result['summaries'][key][x] for x in ('bytes_read', 'bytes_written', 'reads', 'writes', 'opens')])
        if check_val == 0:
            result['summaries'].pop(key, None)
        else:
            result['mounts'].add(key)

    # Populate the headers data
    result['headers'] = {}
    counters = darshan_data.get('header')
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
        print("Parameters: ", (mount_point,))

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
        print("Parameters: ", tuple([header_datum[x] for x in header_counters]))
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
        FOREIGN KEY (log_id) REFERENCES headers (log_id),
        UNIQUE(log_id, fs_id)
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
    base_query += "\n  log_id,\n  fs_id,\n  " + ",\n  ".join(INTEGER_COUNTERS + REAL_COUNTERS) + ") VALUES (\n"
    for per_fs_counters in summary_data:
        for mountpt, summary_datum in per_fs_counters.items():
            query = base_query + "  (SELECT log_id from headers where filename = '%s'),\n" % summary_datum['filename']
            query += "  (SELECT fs_id from mounts where mountpt = '%s'),\n  " % mountpt 
            query += ", ".join(["?"] * len(INTEGER_COUNTERS + REAL_COUNTERS))
            query += ")"
            print(query)
            print("Parameters: ", tuple([summary_datum[x] for x in (INTEGER_COUNTERS + REAL_COUNTERS)]))
            cursor.execute(query, tuple([summary_datum[x] for x in (INTEGER_COUNTERS + REAL_COUNTERS)]))

    cursor.close()
    conn.commit()

def get_existing_logs(conn):
    """Returns list of log files already indexed in db

    Scans the summaries table for existing entries and returns the file names
    corresponding to those entries.  We don't worry about summary rows that
    don't correspond to existing header entries because the schema prevents
    this.  Similarly, each log's summaries are committed as a single transaction
    so we can assume that if a log file has _any_ rows represented in the
    summaries table, it has been fully processed and does not need to be
    updated.

    Args:
        conn (sqlite3.Connection): Connection to database containing existing
            logs 

    Returns:
        list of str: Basenames of Darshan log files represnted in the database
    """
    cursor = conn.cursor()
    # test table existence first
    cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND (name = ? OR name = ?)",
        (SUMMARIES_TABLE, HEADERS_TABLE))
    if len(cursor.fetchall()) < 2:
        return []

    # retrieve all existing logs
    cursor.execute("""SELECT DISTINCT h.filename FROM %s AS s
                      INNER JOIN %s AS h ON s.log_id = h.log_id""" % (SUMMARIES_TABLE, HEADERS_TABLE))

    results = cursor.fetchall()
    return [x[0] for x in results]

def process_log_list(conn, log_list):
    """Expand and filter the list of logs to process

    Takes log_list as input by user and returns a list of Darshan logs that
    should be added to the index database.  It does the following:

    1. Expands log_list from a single-element list pointing to a directory [of
       logs] into a list of log files
    2. Returns the subset of Darshan logs which do not already appear in the
       given database.
    
    Relies on the logic of get_existing_logs() to determine whether a log
    appears in a database or not.

    Args:
        conn (sqlite3.Connection): Database containing log data
        log_list (list of str): List of paths to Darshan logs or a single-element
            list to a directory

    Returns:
        list of str: Subset of log_list that contains only those Darshan logs
            that are not already represented in the database referenced by conn.
    """
    # Filter out logs that were already processed.  NOTE: get_existing_logs()
    # returns basename; exclude_list is path/basename
    exclude_list = get_existing_logs(conn)
    print("%d log files already found in database" % len(exclude_list))

    # If only one argument is passed in but it's a directory, enumerate all the
    # files in that directory.  This is a compromise for cases where the CLI
    # length limit prevents all darshan logs from being passed via argv, but
    # prevents every CLI arg from having to be checked to determine if it's a
    # darshan log or directory
    num_excluded = 0
    if len(log_list) == 1 and os.path.isdir(log_list[0]):
        new_log_list = []
        for filename in os.listdir(log_list[0]):
            filename = os.path.join(log_list[0], filename)
            if os.path.isfile(filename) and os.path.basename(filename) not in exclude_list:
                new_log_list.append(filename)
            else:
                num_excluded += 1
    else:
        for filename in log_list:
            if os.path.basename(filename) not in exclude_list:
                new_log_list.append(filename)
            else:
                num_excluded += 1

    print("Adding %d new logs" % len(new_log_list))
    print("Excluding %d existing logs" % num_excluded)

    return new_log_list

def index_darshanlogs(log_list, threads=1, max_mb=0):
    """Calculate the sum bytes read/written

    Given a list of input files, parse each as a Darshan log in parallel to
    create a list of scalar summary values correspond to each log and insert
    these into an SQLite database.

    Args:
        log_list (list of str): Paths to Darshan logs to be processed
        threads (int): Number of subprocesses to spawn for Darshan log parsing
        max_mb (int): Skip logs of size larger than this value

    Returns:
        dict: Reduced data along different reduction dimensions
    """

    conn = sqlite3.connect('blah.db')

    new_log_list = process_log_list(conn, log_list)

    # Analyze the remaining logs in parallel
    log_records = []
    mount_points = set([])
    for result in multiprocessing.Pool(threads).imap_unordered(functools.partial(summarize_by_fs, max_mb=max_mb), new_log_list):
        if result:
            log_records.append(result)
            mount_points |= result['mounts']

    # Create tables and indices
    create_mount_table(conn)
    create_headers_table(conn)
    create_summaries_table(conn)

    # Insert new data that was collected in parallel
    update_mount_table(conn, mount_points)
    update_headers_table(conn, [x['headers'] for x in log_records])
    update_summaries_table(conn, [x['summaries'] for x in log_records])

    conn.close()

    return

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

    index_darshanlogs(log_list=args.darshanlogs,
                      threads=args.threads,
                      max_mb=args.max_mb)
