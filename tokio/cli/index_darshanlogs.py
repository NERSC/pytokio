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
        mountpt CHAR,
        fsname CHAR
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
import re
import time
import sqlite3
import operator
import functools
import subprocess
import argparse
import warnings
#import multiprocessing
import concurrent.futures
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
    'posix_files': operator.add,
    'stdio_files': operator.add,
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
QUIET = False

# precompile regular expressions
MOUNT_TO_FSNAME = {}

def init_mount_to_fsname():
    """Initialize regexes to map mount points to file system names
    """
    global MOUNT_TO_FSNAME
    for rex_str, fsname in tokio.config.CONFIG.get('mount_to_fsname', {}).items():
        MOUNT_TO_FSNAME[re.compile(rex_str)] = fsname

def get_file_mount(filename, mount_list):
    """Return the mount point in which a file is located

    Args:
        filename (str): Fully equalified path to a file or directory
        mount_list (list of str): List of mount points

    Returns:
        tuple of (str, str) or None: The member of mount_list in which filename
            lives; first string is the mount point, and the second is the
            logical file system name.  Returns None if filename does not match
            any mounts
    """
    # always want the most specific mount path to match first
    sorted_mount_list = sorted(mount_list, key=len, reverse=True)

    for mount in sorted_mount_list:
        if filename.startswith(mount):
            logical = mount
            for mount_rex, fsname in MOUNT_TO_FSNAME.items():
                match = mount_rex.match(mount)
                if match:
                    logical = fsname
            return (mount, logical)
        if filename in ("<STDOUT>", "<STDERR>") and mount == "UNKNOWN":
            return mount, mount
        # filename should be a fully qualified path in darshan3 logs; if
        # if it is not, then the log has been anonymized _and_ this record
        # does not correspond to a file.  here we assume it is an
        # anonymized representation of <STDOUT> or <STDERR>
        elif os.sep not in filename:
            return "UNKNOWN", "UNKNOWN"
    return None

def summarize_by_fs(darshan_log, max_mb=0.0):
    """Generates summary scalar values for a Darshan log

    Uses the Darshan connector to generate per-file system summary metrics for
    a Darshan log.  The output is a dictionary of the form::

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
        max_mb (float): For logs of size larger than this value, use the
            embedded (lite) darshan parser which uses minimal memory

    Returns:
        dict: Contains three keys (summaries, mounts, and headers) whose values
            are dicts of key-value pairs corresponding to scalar summary values
            from the POSIX module which are reduced over all files sharing a
            common mount point.
    """
    if 0.0 < max_mb <= (os.path.getsize(darshan_log) / 1048576.0):
#       errmsg = "Using lite parser for %s due to size (%d MiB)" % (darshan_log, (os.path.getsize(darshan_log) / 1024 / 1024))
#       warnings.warn(errmsg)
        return summarize_by_fs_lite(darshan_log)

    try:
        darshan_data = tokio.connectors.darshan.Darshan(darshan_log, silent_errors=True)
        if QUIET:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                darshan_data.darshan_parser_base()
        else:
            darshan_data.darshan_parser_base()
    except:
        if not QUIET:
            errmsg = "Unable to open or parse %s" % darshan_log
            warnings.warn(errmsg)
        return {}

    if not darshan_data:
        if not QUIET:
            errmsg = "Unable to open or parse %s" % darshan_log
            warnings.warn(errmsg)
        return {}

    module_records = {
        'posix': darshan_data.get('counters', {}).get('posix', {}),
        'stdio': darshan_data.get('counters', {}).get('stdio', {}),
    }
    if not module_records['posix'] and not module_records['stdio']:
        if not QUIET:
            errmsg = "No counters found in %s" % darshan_log
            warnings.warn(errmsg)
        return {}

    mount_list = list(darshan_data.get('mounts', {}).keys())
    if not mount_list:
        if not QUIET:
            errmsg = "No mount table found in %s" % darshan_log
            warnings.warn(errmsg)
        return {}

    # hack in UNKNOWN for the stdio module since it does not appear in the mount table
    mount_list += ["UNKNOWN"]

    # Populate the summaries data
    reduced_counters = {}
    for mount in mount_list:
        reduced_counters[mount] = {}
        for counter in INTEGER_COUNTERS:
            reduced_counters[mount][counter] = None
        for counter in REAL_COUNTERS:
            reduced_counters[mount][counter] = None
        # needed so the log filename can be referenced during updates to the summaries table
        reduced_counters[mount]['filename'] = os.path.basename(darshan_data.log_file)

    # Reduce each counter according to its mount point
    logical_mount_names = {}
    for module, log_counters in module_records.items():
        # record_file is the full path to a file that the application manipulated
        for record_file in log_counters:
            # skip file records resident on unknown file systems
            mount = get_file_mount(record_file, mount_list)
            if mount is None:
                continue
            mount, logical = mount
            logical_mount_names[mount] = logical

            # only iterate over values; keys are MPI ranks that we don't use
            for counters in log_counters[record_file].values():
                # Now loop over the counters in our schema and add them if
                # they exist in the Darshan log's compendium.
                for counter, reduction in INTEGER_COUNTERS.items():
                    logged_val = counters.get(counter.upper())
                    if logged_val is not None:
                        reduced_counters[mount][counter] = reduction(reduced_counters[mount][counter], logged_val) if reduced_counters[mount][counter] is not None else logged_val
                    elif (counter == 'posix_files' and module == 'posix') \
                    or (counter == 'stdio_files' and module == 'stdio'):
                        reduced_counters[mount][counter] = reduced_counters[mount][counter] + 1 if reduced_counters[mount][counter] is not None else 1
                    # else:
                        # if counter doesn't exist in Darshan log and isn't one
                        # of our special counters, skip it do nothing--don't
                        # attempt to reduce with an implicit zero value, as this
                        # can screw up reductions that use min/max!

                for counter, reduction in REAL_COUNTERS.items():
                    logged_val = counters.get(counter.upper())
                    if logged_val is not None:
                        reduced_counters[mount][counter] = reduction(reduced_counters[mount][counter], logged_val) if reduced_counters[mount][counter] is not None else logged_val

    # Populate the mounts data and remove all mount points that were not used
    mountpts = {}
    for key in list(reduced_counters.keys()):
        # check_val works only when all the keys are positive counters
        check_val = sum([reduced_counters[key][x] if reduced_counters[key][x] is not None else 0 for x in ('bytes_read', 'bytes_written', 'reads', 'writes', 'opens', 'stats')])
        if check_val == 0:
            reduced_counters.pop(key, None)
        else:
            mountpts[key] = logical_mount_names.get(key, key)

    # Populate the headers data
    header = {}
    counters = darshan_data.get('header')
    header['filename'] = os.path.basename(darshan_data.log_file)
    for counter in HEADER_COUNTERS:
        header[counter] = counters.get(counter)

    # some Darshan logs at NERSC have no exename for some reason
    header['exe'] = counters.get('exe')
    if header['exe']:
        header['exe'] = header['exe'][0]
        header['exename'] = os.path.basename(header['exe'])
    else:
        header['exe'] = None
        header['exename'] = None

    # username is resolved here so that it can be indexed without having to mess around
    header['username'] = darshan_data.filename_metadata.get('username')

    return {
        'summaries': reduced_counters,
        'headers': header,
        'mounts': mountpts
    }

def summarize_by_fs_lite(darshan_log):
    """Generates summary scalar values for a Darshan log

    Unlike the summarize_by_fs() function, this does not use the Darshan
    connector and instead walks through the file serially.  This works around
    memory and performance problems inherent in the Darshan connector for very
    large Darshan logs.

    Its output is a dictionary of the form::

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

    dparser = subprocess.Popen(['darshan-parser', '--base', darshan_log],
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
            if len(fields) < 7:
                continue

            value = None
            reducer = None

            # extract key and value; different field layout for darshan2 logs
            if logvers == 2:
                counter = fields[2].split('_', 1)[-1]
                counter = tokio.connectors.darshan.V2_TO_V3.get(counter, counter).lower()
                if counter in INTEGER_COUNTERS:
                    value = int(fields[3])
                    reducer = INTEGER_COUNTERS.get(counter)
                elif counter in REAL_COUNTERS:
                    value = float(fields[3])
                    reducer = REAL_COUNTERS.get(counter)
            else:
                # only consider POSIX and STDIO modules for v3
                if fields[0] not in ('POSIX', 'STDIO'):
                    continue
                counter = fields[3].split('_', 1)[-1].lower()
                if counter in INTEGER_COUNTERS:
                    value = int(fields[4])
                    reducer = INTEGER_COUNTERS.get(counter)
                elif counter in REAL_COUNTERS:
                    value = float(fields[4])
                    reducer = REAL_COUNTERS.get(counter)

            if value is None:
                continue

            mount = fields[-2]
            mount = get_file_mount(mount, mount_list)
            if mount is None:
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
    for counter in INTEGER_COUNTERS:
        for mount in reduced_counters:
            if counter not in reduced_counters[mount]:
                reduced_counters[mount][counter] = None
    for counter in REAL_COUNTERS:
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

def insert_summary(conn, summary):
    """Inserts the output of summarize_by_fs into database

    This inserts a single summary into a database.  This is useful if you do not
    want to wait until the entirety of the logs are parsed before beginning to
    insert them into the database.  Makes the most sense when the time to parse
    a log far exceeds the time to insert its records.

    Args:
        conn (sqlite3.Connection): Database connection into which record should
            be inserted
        summary (dict): Dictionary of the form returned by the summarize_by_fs()
            function
    """
    cursor = conn.cursor()

    # Update mounts table
    for mountpt, fsname in summary['mounts'].items():
        vprint("INSERT OR IGNORE INTO %s (mountpt, fsname) VALUES (?, ?)" % MOUNTS_TABLE, 4)
        vprint("Parameters: %s" % str((mountpt, fsname)), 4)
        cursor.execute("INSERT OR IGNORE INTO %s (mountpt, fsname) VALUES (?, ?)" % MOUNTS_TABLE, (mountpt, fsname))

    # Update headers table
    header_counters = ["filename", "exe", "username", "exename"] + HEADER_COUNTERS
    query = "INSERT INTO %s (" % HEADERS_TABLE
    query += ", ".join(header_counters)
    query += ") VALUES (" + ",".join(["?"] * len(header_counters))
    query += ")"
    vprint(query, 4)
    vprint("Parameters: %s" % str(tuple([summary['headers'][x] for x in header_counters])), 4)
    cursor.execute(query, tuple([summary['headers'][x] for x in header_counters])) # easier debugging

    # Update summaries table
    base_query = "INSERT INTO %s (" % SUMMARIES_TABLE
    base_query += "\n  log_id,\n  fs_id,\n  " + ",\n  ".join(SUMMARY_COUNTERS) + ") VALUES (\n"
    for mountpt, summary_datum in summary['summaries'].items():
        query = base_query + "  (SELECT log_id from headers where filename = '%s'),\n" % summary_datum['filename']
        query += "  (SELECT fs_id from mounts where mountpt = '%s'),\n  " % mountpt
        query += ", ".join(["?"] * len(SUMMARY_COUNTERS))
        query += ")"
        vprint(query, 4)
        vprint("Parameters: %s" % str(tuple([summary_datum.get(x) for x in SUMMARY_COUNTERS])), 4)
        cursor.execute(query, tuple([summary_datum.get(x) for x in SUMMARY_COUNTERS]))

    cursor.close()
    conn.commit()

def create_mount_table(conn):
    """Creates the mount table
    """
    cursor = conn.cursor()

    query = """CREATE TABLE IF NOT EXISTS %s (
        fs_id INTEGER PRIMARY KEY,
        mountpt CHAR UNIQUE,
        fsname CHAR
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

    for mount_point in mount_points.items():
        vprint("INSERT OR IGNORE INTO %s (mountpt, fsname) VALUES (?, ?)" % MOUNTS_TABLE, 4)
        vprint("Parameters: %s" % str(mount_point), 4)

    cursor.executemany("INSERT OR IGNORE INTO %s (mountpt, fsname) VALUES (?, ?)" % MOUNTS_TABLE, [(x, mount_points[x]) for x in mount_points])

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

    query = "INSERT INTO %s (" % HEADERS_TABLE
    query += ", ".join(header_counters)
    query += ") VALUES (" + ",".join(["?"] * len(header_counters))
    query += ")"

    for header_datum in header_data:
        vprint(query, 4)
        vprint("Parameters: %s" % str(tuple([header_datum[x] for x in header_counters])), 4)
#       cursor.execute(query, tuple([header_datum[x] for x in header_counters])) # easier debugging
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
            vprint("Parameters: %s" % str(tuple([summary_datum.get(x) for x in SUMMARY_COUNTERS])), 4)
            cursor.execute(query, tuple([summary_datum.get(x) for x in SUMMARY_COUNTERS]))

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
    appears in a database or not.  If a database is somehow created where the
    summaries table is fully populated but the headers table is not, this will
    still return log files corresponding to the missing headers and
    potentially result in duplicate summaries entries that have no matching
    header.

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

def index_darshanlogs(log_list, output_file, threads=1, max_mb=0.0, bulk_insert=True):
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
        max_mb (float): Skip logs of size larger than this value
        bulk_insert (bool): If False, have each thread update the database
            as soon as it has parsed a log

    Returns:
        dict: Reduced data along different reduction dimensions
    """

    conn = sqlite3.connect(output_file)

    init_mount_to_fsname()

    t_start = time.time()
    new_log_list = process_log_list(conn, log_list)
    vprint("Built log list in %.1f seconds" % (time.time() - t_start), 2)

    # Create tables and indices
    t_start = time.time()
    create_mount_table(conn)
    create_headers_table(conn)
    create_summaries_table(conn)
    vprint("Initialized tables in %.1f seconds" % (time.time() - t_start), 2)

    # Analyze the remaining logs in parallel
    t_start = time.time()
    log_records = []
    mount_points = {}
    # multiprocessing is super flaky (e.g., it deadlocks on macOS), so provide an escape hatch
    if threads > 1:
        #mpcontext = multiprocessing.get_context('forkserver')
        #with mpcontext.Pool(processes=threads) as pool:
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as pool:
            #results = pool.imap_unordered(functools.partial(summarize_by_fs, max_mb=max_mb), new_log_list)
            results = pool.map(functools.partial(summarize_by_fs, max_mb=max_mb), new_log_list)
    else:
        results = [summarize_by_fs(x, max_mb=max_mb) for x in new_log_list]

    for result in results:
        if result:
            log_records.append(result)
            mount_points.update(result['mounts'])
            if not bulk_insert:
                insert_summary(conn, result)

    vprint("Ingested %d logs in %.1f seconds" % (len(log_records), time.time() - t_start), 2)

    # Insert new data that was collected in parallel
    if bulk_insert:
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
    global QUIET

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

    VERBOSITY = args.verbose
    QUIET = args.quiet

    index_darshanlogs(log_list=args.darshanlogs,
                      threads=args.threads,
                      max_mb=args.max_mb,
                      bulk_insert=not args.no_bulk_insert,
                      output_file=args.output)
