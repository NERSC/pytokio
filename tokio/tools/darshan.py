"""
Tools to find Darshan logs within a system-wide repository
"""

import os
import glob
import tokio.tools.common
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

def find_darshanlogs(datetime_start=None, datetime_end=None, username=None, jobid=None,
                     darshan_log_dir=None):
    """Return darshan log file paths matching a set of criteria

    Attempts to find Darshan logs that match the input criteria.

    Args:
        datetime_start (datetime.datetime): date to begin looking for Darshan logs
        datetime_end (datetime.datetime): date to stop looking for Darshan logs
        username (str): username of user who generated the log
        jobid (int): jobid corresponding to Darshan log
        darshan_log_dir (str): path to Darshan log directory base

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
            job_data = tokio.connectors.slurm.Slurm(jobid=jobid)
            datetime_start, _ = job_data.get_job_startend()

    # the following will not work on Windows!
    darshan_dated_dir = os.path.join(darshan_log_dir, "%-Y", "%-m", "%-d")

    search_dirs = tokio.tools.common.enumerate_dated_dir(darshan_dated_dir,
                                                         datetime_start,
                                                         datetime_end)

    glob_fields = DARSHAN_LOG_GLOB_FIELDS.copy()
    if jobid is not None:
        glob_fields['jobid'] = jobid
    if username:
        glob_fields['username'] = username

    results = []
    for search_dir in search_dirs:
        results += glob.glob(os.path.join(search_dir, DARHSAN_LOG_NAME_STR % glob_fields))

    return results
