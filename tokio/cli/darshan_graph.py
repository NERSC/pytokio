"""
TODO
"""

import os
import sqlite3
import functools
import argparse
import warnings
import multiprocessing
import tokio.connectors.darshan

VERBOSITY = 0
QUIET = False

CREATE_ACCESSES_TABLE = """
CREATE TABLE IF NOT EXISTS accesses (
    file_id INTEGER,
    job_id INTEGER,
    start_time REAL,
    end_time REAL,
    bytes_read INT,
    bytes_written INT,
    FOREIGN KEY (file_id) REFERENCES files (file_id),
    FOREIGN KEY (job_id) REFERENCES jobs (job_id)
)
"""

CREATE_FILES_TABLE = """
CREATE TABLE IF NOT EXISTS files (
    file_id INTEGER PRIMARY KEY,
    file CHAR UNIQUE
)
"""

CREATE_JOBS_TABLE = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id INTEGER PRIMARY KEY,
    job CHAR UNIQUE
)
"""

def process_log(darshan_log, max_mb=0.0):
    """
    """
    if max_mb > 0.0 and (os.path.getsize(darshan_log) / 1024.0 / 1024.0) > max_mb:
        errmsg = "Skipping %s due to size (%d MiB)" % (darshan_log, (os.path.getsize(darshan_log) / 1024 / 1024))
        warnings.warn(errmsg)
        return {}

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


    # Actually process each file
    accesses = {}
    jobid = darshan_data['header']['jobid']
    start = darshan_data['header']['start_time']
    end = darshan_data['header']['end_time']

    for module, per_rec_counters in module_records.items():
        # record_file is the full path to a file that the application manipulated
        for rec_name, per_rank_counters in per_rec_counters.items():
            for rank, mod_counters in per_rank_counters.items():
                if rec_name not in accesses:
                    accesses[rec_name] = {
                        'jobid': jobid,
                        'start': mod_counters['F_OPEN_START_TIMESTAMP'],
                        'end': mod_counters['F_CLOSE_START_TIMESTAMP'],
                        'abs_start': start,
                        'abs_end': end,
                        'bytes_read': mod_counters['BYTES_READ'],
                        'bytes_written': mod_counters['BYTES_WRITTEN'],
                    }
                else:
                    accesses[rec_name]['bytes_read'] += mod_counters['BYTES_READ']
                    accesses[rec_name]['bytes_written'] += mod_counters['BYTES_WRITTEN']
                    accesses[rec_name]['start'] = min(accesses[rec_name]['start'], mod_counters['F_OPEN_START_TIMESTAMP'])
                    accesses[rec_name]['end'] = max(accesses[rec_name]['end'], mod_counters['F_CLOSE_START_TIMESTAMP'])

    return accesses

def index_darshanlogs(log_list, output_file, threads=1, max_mb=0.0):
    """
    """

    # collect all files opened
    some_results = []
    for result in multiprocessing.Pool(threads).imap_unordered(functools.partial(process_log, max_mb=max_mb), log_list):
        if result:
            some_results.append(result)

    # some_results is a list of dicts of filename:attributes

    conn = sqlite3.connect(output_file)
    cursor = conn.cursor()

    cursor.execute(CREATE_JOBS_TABLE)
    cursor.execute(CREATE_FILES_TABLE)
    cursor.execute(CREATE_ACCESSES_TABLE)

    conn.commit()

    # merge all results
    for result in some_results:
        for key, val in result.items():
            print("Inserting %s, %s" % (key, val['jobid']))
            cursor.execute("INSERT OR IGNORE INTO jobs (job) VALUES (?)", (val['jobid'],))
            cursor.execute("INSERT OR IGNORE INTO files (file) VALUES (?)", (key,))
            cursor.execute("""
            INSERT INTO accesses (file_id, job_id, start_time, end_time, bytes_read, bytes_written)
            VALUES (
                (SELECT file_id FROM files WHERE files.file = '%s'),
                (SELECT job_id FROM jobs WHERE jobs.job = '%s'),
            ?, ?, ?, ?)""" % (key, val['jobid']),
                           (val['abs_start'] + val['start'],
                            val['abs_end'] + val['end'],
                            val['bytes_read'],
                            val['bytes_written']))

    conn.commit()

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
    global QUIET

    parser = argparse.ArgumentParser()
    parser.add_argument("darshanlogs", nargs="+", type=str, help="Darshan logs to process")
    parser.add_argument('-t', '--threads', default=1, type=int,
                        help="Number of concurrent processes (default: 1)")
    parser.add_argument('-o', '--output', type=str, default='fileaccesses.db', help="Name of output file (default: fileaccesses.db)")
    parser.add_argument('-m', '--max-mb', type=int, default=0, help="Maximum log file size to consider")
    parser.add_argument('-v', '--verbose', action='count', default=0, help="Verbosity level (default: none)")
    parser.add_argument('-q', '--quiet', action='store_true', help="Suppress warnings for invalid Darshan logs")
    args = parser.parse_args(argv)

    VERBOSITY = args.verbose
    QUIET = args.quiet

    index_darshanlogs(log_list=args.darshanlogs,
                      threads=args.threads,
                      max_mb=args.max_mb,
                      output_file=args.output)
