"""
Tools to find Darshan logs within a system-wide repository
"""

import os
import glob
from tokio.debug import debug_print
import tokio.tools.common
import tokio.tools.jobinfo
import tokio.connectors.darshan
import tokio.connectors.slurm

DARHSAN_LOG_NAME_STR = "%(username)s_%(exe)s_id%(jobid)s_%(month)s-%(day)s-%(seconds)s-%(logmod)s.darshan"
DARSHAN_LOG_GLOB_FIELDS = {
    "username": "*",
    "exe": "*",
    "jobid": "*",
    "month": "*",
    "day": "*",
    "seconds": "*",
    "logmod": "*",
}

def load_darshanlogs(datetime_start=None, datetime_end=None, username=None,
                     jobid=None, log_dir=None, system=None,
                     which=None, **kwargs):
    """Return parsed Darshan logs matching a set of criteria

    Finds Darshan logs that match the input criteria, loads them, and returns a
    dictionary of connectors.darshan.Darshan objects keyed by the full log file
    paths to the source logs.

    Args:
        datetime_start (datetime.datetime): date to begin looking for Darshan logs
        datetime_end (datetime.datetime): date to stop looking for Darshan logs
        username (str): username of user who generated the log
        jobid (int): jobid corresponding to Darshan log
        log_dir (str): path to Darshan log directory base
        system (str): key to pass to enumerate_dated_files's lookup_key
            when resolving darshan_log_dir
        which (str): 'base', 'total', and/or 'perf' as a comma-delimited string
        kwargs: arguments to pass to the connectors.darshan.Darshan object
            initializer

    Returns:
        dict: keyed by log file name whose values are connectors.darshan.Darshan
            objects

    Todo:
        * Use a default `darshan_log_dir` from `tokio.config`
    """
    if which is None:
        raise TypeError("which must be base, total, and/or perf")

    which_list = [x.lower() for x in which.split(',')]
    if 'base' not in which_list \
    and 'total' not in which_list \
    and 'perf' not in which_list:
        raise TypeError("which must be base, total, and/or perf")

    matching_logfiles = find_darshanlogs(datetime_start=datetime_start,
                                         datetime_end=datetime_end,
                                         username=username,
                                         jobid=jobid,
                                         system=system,
                                         log_dir=log_dir)

    results = {}
    for matching_logfile in matching_logfiles:
        results[matching_logfile] = tokio.connectors.darshan.Darshan(log_file=matching_logfile,
                                                                     **kwargs)
        if 'base' in which_list:
            results[matching_logfile].darshan_parser_base()
        if 'total' in which_list:
            results[matching_logfile].darshan_parser_total()
        if 'perf' in which_list:
            results[matching_logfile].darshan_parser_perf()

    return results

def find_darshanlogs(datetime_start=None, datetime_end=None, username=None, jobid=None,
                     log_dir=None, system=None):
    """Return darshan log file paths matching a set of criteria

    Attempts to find Darshan logs that match the input criteria.

    Args:
        datetime_start (datetime.datetime): date to begin looking for Darshan logs
        datetime_end (datetime.datetime): date to stop looking for Darshan logs
        username (str): username of user who generated the log
        jobid (int): jobid corresponding to Darshan log
        log_dir (str): path to Darshan log directory base
        system (str or None): key to pass to enumerate_dated_files's lookup_key
            when resolving darshan_log_dir

    Returns:
        list: paths of matching Darshan logs as strings

    Todo:
        * Abstract the jobid argument into a generic workload manager jobid tool
          that dispatches the correct workload manager connector.
        * Use a default `darshan_log_dir` from `tokio.config`
    """

    if datetime_start is None:
        if jobid is None:
            raise TypeError("datetime_start must be defined if jobid is not")
        else:
            datetime_start, _ = tokio.tools.jobinfo.get_job_startend(jobid=jobid)

    if datetime_end is None:
        datetime_end = datetime_start

    # first expand the eligible Darshan base directories
    if log_dir is None:
        log_dir = tokio.config.CONFIG.get('darshan_log_dirs')

    base_dirs = tokio.tools.common.enumerate_dated_files(start=datetime_start,
                                                         end=datetime_end,
                                                         template=log_dir,
                                                         lookup_key=system,
                                                         match_first=(system is not None))

    # then run another pass of enumerate_dated_files to resolve the path within
    # the base darshan log directory
    search_dirs = []
    for base_dir in base_dirs:
        search_dirs += tokio.tools.common.enumerate_dated_files(
            start=datetime_start,
            end=datetime_end,
            template=os.path.join(base_dir, "%-Y", "%-m", "%-d"),
            lookup_key=system)

    debug_print("Darshan log search directories are:\n  " + "\n  ".join(search_dirs))

    glob_fields = DARSHAN_LOG_GLOB_FIELDS.copy()
    if jobid is not None:
        glob_fields['jobid'] = jobid
    if username:
        glob_fields['username'] = username

    results = []
    for search_dir in search_dirs:
        results += glob.glob(os.path.join(search_dir, DARHSAN_LOG_NAME_STR % glob_fields))

    return list(set(results))
