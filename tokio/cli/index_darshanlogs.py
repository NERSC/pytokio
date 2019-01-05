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
        exename CHAR,
        jobid CHAR,
        nprocs INTEGER,
        start_time INTEGER,
        uid INTEGER,
        username CHAR,
        log_version CHAR,
        walltime INTEGER
    );
"""

import os
import time
import sqlite3
import operator
import functools
import argparse
import warnings
import multiprocessing
import tokio.connectors.darshan

INTEGER_COUNTERS = {
    'bytes_read': operator.add,
    'bytes_written': operator.add,
    'reads': operator.add,
    'writes': operator.add,
    'file_not_aligned': operator.add,
    'consec_reads': operator.add,
    'consec_writes': operator.add,
    'mmaps': operator.add,
    'opens': operator.add,
    'seeks': operator.add,
    'stats': operator.add,
    'fdsyncs': operator.add,
    'fsyncs': operator.add,
    'flushes': operator.add,
    'seq_reads': operator.add,
    'seq_writes': operator.add,
    'rw_switches': operator.add,
}

REAL_COUNTERS = {
    'f_close_start_timestamp': min,
    'f_close_end_timestamp': max,
    'f_open_start_timestamp': min,
    'f_open_end_timestamp': max,
    'f_read_start_timestamp': min,
    'f_read_end_timestamp': max,
    'f_write_start_timestamp': min,
    'f_write_end_timestamp': max,
}

SUMMARY_COUNTERS = list(INTEGER_COUNTERS.keys()) + list(REAL_COUNTERS.keys())

HEADER_COUNTERS = [
    'end_time',
    'jobid',
    'nprocs',
    'start_time',
    'uid',
    'version',
    'walltime',
]

MOUNTS_TABLE = "mounts"
HEADERS_TABLE = "headers"
SUMMARIES_TABLE = "summaries"

VERBOSITY = 0

def record_in_mount(filename, mount):
    if filename.startswith(mount):
        return True
    elif (filename == "<STDOUT>" or filename == "<STDERR>") and mount == "UNKNOWN":
        return True
    return False;

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

    posix_counters = darshan_data.get('counters', {}).get('posix', {})
    stdio_counters = darshan_data.get('counters', {}).get('stdio', {})
    vprint("Got %d posix records and %d stdio records" % (len(posix_counters), len(stdio_counters)), 3)
    if not posix_counters and not stdio_counters:
        errmsg = "No counters found in %s" % darshan_log
        warnings.warn(errmsg)
        return result

    # reverse the mount to match the deepest path first and root path last
    mount_list = list(reversed(sorted(darshan_data.get('mounts', {}).keys())))
    if not mount_list:
        errmsg = "No mount table found in %s" % darshan_log
        warnings.warn(errmsg)
        return result

    # hack in UNKNOWN for the stdio module since it does not appear in the mount table
    mount_list += ["UNKNOWN"]

    # Populate the summaries data
    result['summaries'] = {}
    for mount in mount_list:
        result['summaries'][mount] = {}
        for counter in INTEGER_COUNTERS:
            result['summaries'][mount][counter] = 0
        for counter in REAL_COUNTERS:
            result['summaries'][mount][counter] = 0.0
        result['summaries'][mount]['filename'] = os.path.basename(darshan_data.log_file)

    # Reduce each counter according to its mount point
    for counter_dict in posix_counters, stdio_counters:
        # record_file is the full path to a file that the application manipulated
        for record_file in counter_dict:
            # only consider files that map to known mount points (or UNKNOWN for
            # stdout/stderr)
            for mount in mount_list:
                if record_in_mount(record_file, mount):
                    # counter_dict contains counters from the Darshan log
                    for counters in counter_dict[record_file].values():
                        # loop over the counters we're interested in and update
                        # them if they exist in the Darshan log's compendium.
                        # if they are NOT present in the Darshan log, do
                        # nothing--don't attempt to reduce with an implicit zero
                        # value, as this can screw up reductions that use min/max!
                        for counter, reduction in INTEGER_COUNTERS.items():
                            logged_val = counters.get(counter.upper())
                            if logged_val is not None:
                                result['summaries'][mount][counter] = reduction(result['summaries'][mount][counter], logged_val)
                        for counter, reduction in REAL_COUNTERS.items():
                            logged_val = counters.get(counter.upper())
                            if logged_val is not None:
                                result['summaries'][mount][counter] = reduction(result['summaries'][mount][counter], logged_val)
                    break # don't apply this record's counters to more than one mountpt

    # Populate the mounts data and remove all mount points that were not used
    result['mounts'] = set([])
    for key in list(result['summaries'].keys()):
        # check_val works only when all the keys are positive counters
        check_val = sum([result['summaries'][key][x] for x in ('bytes_read', 'bytes_written', 'reads', 'writes', 'opens', 'stats')])
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

    # username is resolved here so that it can be indexed without having to mess around
    result['headers']['username'] = darshan_data.filename_metadata.get('username')
    result['headers']['exename'] = os.path.basename(result['headers']['exe'])

    return result

def create_mount_table(conn):
    """Creates the mount table
    """
    cursor = conn.cursor()

    query = """CREATE TABLE IF NOT EXISTS %s (
        fs_id INTEGER PRIMARY KEY,
        mountpt CHAR UNIQUE
    )
    """ % MOUNTS_TABLE
    vprint(query, 3)
    cursor.execute(query)

    cursor.close()
    conn.commit()

def update_mount_table(conn, mount_points):
    """Adds new mount points to the mount table
    """
    cursor = conn.cursor()

    # We force the addition of UNKNOWN so stdio's stdout/stderr counters are
    # still included
    for mount_point in mount_points:
        vprint("INSERT OR IGNORE INTO %s (mountpt) VALUES (?)" % MOUNTS_TABLE, 4)
        vprint("Parameters: %s" % str(mount_point,), 4)

    cursor.executemany("INSERT OR IGNORE INTO %s (mountpt) VALUES (?)" % MOUNTS_TABLE, [(x,) for x in mount_points])

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
        exename CHAR,
        jobid CHAR,
        nprocs INTEGER,
        start_time INTEGER,
        uid INTEGER,
        username CHAR,
        version CHAR,
        walltime INTEGER
    )
    """ % HEADERS_TABLE
    vprint(query, 3)
    cursor.execute(query)
    cursor.close()
    conn.commit()

def update_headers_table(conn, header_data):
    """Adds new header data to the headers table
    """
    cursor = conn.cursor()

    header_counters = ["filename", "exe", "username", "exename"] + HEADER_COUNTERS

#   query = "INSERT OR IGNORE INTO %s (" % HEADERS_TABLE # silently ignore inconsistent data
    query = "INSERT INTO %s (" % HEADERS_TABLE
    query += ", ".join(header_counters)
    query += ") VALUES (" + ",".join(["?"] * len(header_counters))
    query += ")"

    for header_datum in header_data:
        vprint(query, 4)
        vprint("Parameters: %s" % str(tuple([header_datum[x] for x in header_counters])), 4)
        cursor.execute(query, tuple([header_datum[x] for x in header_counters])) # easier debugging
#   cursor.executemany(query, [tuple([header_datum[x] for x in header_counters]) for header_datum in header_data])

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

    query += " INTEGER,\n    ".join(INTEGER_COUNTERS.keys()) + " INTEGER, \n    "
    query += " REAL,\n    ".join(REAL_COUNTERS.keys()) + " REAL,\n    "

    query += """
        FOREIGN KEY (fs_id) REFERENCES mounts (fs_id),
        FOREIGN KEY (log_id) REFERENCES headers (log_id),
        UNIQUE(log_id, fs_id)
    )
    """
    vprint(query, 3)
    cursor.execute(query)
    cursor.close()
    conn.commit()

def update_summaries_table(conn, summary_data):
    """Adds new summary counters to the summaries table
    """
    cursor = conn.cursor()

    base_query = "INSERT INTO %s (" % SUMMARIES_TABLE
    base_query += "\n  log_id,\n  fs_id,\n  " + ",\n  ".join(SUMMARY_COUNTERS) + ") VALUES (\n"
    for per_fs_counters in summary_data:
        for mountpt, summary_datum in per_fs_counters.items():
            query = base_query + "  (SELECT log_id from headers where filename = '%s'),\n" % summary_datum['filename']
            query += "  (SELECT fs_id from mounts where mountpt = '%s'),\n  " % mountpt
            query += ", ".join(["?"] * len(SUMMARY_COUNTERS))
            query += ")"
            vprint(query, 4)
            vprint("Parameters: %s" % str(tuple([summary_datum[x] for x in SUMMARY_COUNTERS])), 4)
            cursor.execute(query, tuple([summary_datum[x] for x in SUMMARY_COUNTERS]))

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
    vprint("%d log files already found in database" % len(exclude_list), 1)

    # If only one argument is passed in but it's a directory, enumerate all the
    # files in that directory.  This is a compromise for cases where the CLI
    # length limit prevents all darshan logs from being passed via argv, but
    # prevents every CLI arg from having to be checked to determine if it's a
    # darshan log or directory
    num_excluded = 0
    new_log_list = []
    if len(log_list) == 1 and os.path.isdir(log_list[0]):
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

    vprint("Adding %d new logs" % len(new_log_list), 1)
    vprint("Excluding %d existing logs" % num_excluded, 1)

    return new_log_list

def index_darshanlogs(log_list, output_file, threads=1, max_mb=0):
    """Calculate the sum bytes read/written

    Given a list of input files, parse each as a Darshan log in parallel to
    create a list of scalar summary values correspond to each log and insert
    these into an SQLite database.

    Current implementation parses all logs and stores their index values in
    memory before beginning the database insert process.  This can be
    memory-intensive if processing many millions of logs at once but avoids
    thread contention on the SQLite database.

    Args:
        log_list (list of str): Paths to Darshan logs to be processed
        output_file (str): Path to a SQLite database file to populate
        threads (int): Number of subprocesses to spawn for Darshan log parsing
        max_mb (int): Skip logs of size larger than this value

    Returns:
        dict: Reduced data along different reduction dimensions
    """

    conn = sqlite3.connect(output_file)

    t_start = time.time()
    new_log_list = process_log_list(conn, log_list)
    vprint("Built log list in %.1f seconds" % (time.time() - t_start), 2)

    # Analyze the remaining logs in parallel
    t_start = time.time()
    log_records = []
    mount_points = set([])
    for result in multiprocessing.Pool(threads).imap_unordered(functools.partial(summarize_by_fs, max_mb=max_mb), new_log_list):
        if result:
            log_records.append(result)
            mount_points |= result['mounts']
    vprint("Ingested %d logs in %.1f seconds" % (len(log_records), time.time() - t_start), 2)

    # Create tables and indices
    t_start = time.time()
    create_mount_table(conn)
    create_headers_table(conn)
    create_summaries_table(conn)
    vprint("Initialized tables in %.1f seconds" % (time.time() - t_start), 2)

    # Insert new data that was collected in parallel
    t_start = time.time()
    update_mount_table(conn, mount_points)
    vprint("Updated mounts table in %.1f seconds" % (time.time() - t_start), 2)
    t_start = time.time()
    update_headers_table(conn, [x['headers'] for x in log_records])
    vprint("Updated headers table in %.1f seconds" % (time.time() - t_start), 2)
    t_start = time.time()
    update_summaries_table(conn, [x['summaries'] for x in log_records])
    vprint("Updated summaries table in %.1f seconds" % (time.time() - t_start), 2)

    conn.close()
    vprint("Updated %s" % output_file, 1)

    return

def vprint(string, level):
    """Print a message if verbosity is enabled

    Args:
        string (str): Message to print
        level (int): Minimum verbosity level required to print
    """

    if VERBOSITY >= level:
        print(string)

def main(argv=None):
    """Entry point for the CLI interface
    """
    global VERBOSITY

    parser = argparse.ArgumentParser()
    parser.add_argument("darshanlogs", nargs="+", type=str, help="Darshan logs to process")
    parser.add_argument('-t', '--threads', default=1, type=int,
                        help="Number of concurrent processes (default: 1)")
    parser.add_argument('-o', '--output', type=str, default='darshalogs.db', help="Name of output file (default: darshanlogs.db)")
    parser.add_argument('-m', '--max-mb', type=int, default=0, help="Maximum log file size to consider (default: 0 (disabled))")
    parser.add_argument('-v', '--verbose', action='count', default=0, help="Verbosity level (default: none)")
    args = parser.parse_args(argv)

    VERBOSITY = args.verbose

    index_darshanlogs(log_list=args.darshanlogs,
                      threads=args.threads,
                      max_mb=args.max_mb,
                      output_file=args.output)
