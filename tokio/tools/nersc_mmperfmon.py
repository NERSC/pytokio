"""Tool that translates datetimes to mmperfmon outputs containing the relevant
data
"""

import datetime
import tokio.tools.common

def enumerate_mmperfmon_txt(fsname, datetime_start, datetime_end):
    """Returns all time-indexed mmperfmon output text files falling between a
    time range.

    Args:
        fsname (str): Logical file system name; should match a key within
            the ``mmperfmon_output_files`` config item in ``site.json``.
        datetime_start (datetime.datetime): Begin including files corresponding
            to this start date, inclusive.
        datetime_end (datetime.datetime): Stop including files with timestamps
            that follow this end date.  Resulting files _will_ include this
            date.

    Returns:
        list: List of strings, each describing a path to an existing file
        that should contain data relevant to the requested start and end
        dates.
    """
    return tokio.tools.common.enumerate_dated_files(
        start=datetime_start,
        end=datetime_end,
        template=tokio.config.CONFIG['mmperfmon_output_files'],
        lookup_key=fsname,
        match_first=False,
        use_glob=True,
        timedelta=datetime.timedelta(hours=1))
