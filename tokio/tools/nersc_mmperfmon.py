"""Tool that translates datetimes to mmperfmon outputs containing the relevant
data
"""

import datetime
import tokio.tools.common

def enumerate_mmperfmon_txt(fsname, datetime_start, datetime_end):
    """Returns all time-indexed mmperfmon output text files falling between a
    time range

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
    # mmperfmon files labeled for HH:00:00 actually don't contain the data point
    # for HH:00:00; you need to look in (HH-1):00:00 to get that first datum
    #
    # TODO: implement above logic
    #
    return tokio.tools.common.enumerate_dated_files(
        start=datetime_start,
        end=datetime_end,
        template=tokio.config.CONFIG['mmperfmon_output_files'],
        lookup_key=fsname,
        match_first=False,
        expand_glob=True)

# TODO: move this into its own test script
def test_enumerate_mmperfmon_txt():
    print(enumerate_mmperfmon_txt(
        fsname='project2',
        datetime_start=datetime.datetime.now() - datetime.timedelta(hours=25),
        datetime_end=datetime.datetime.now() - datetime.timedelta(hours=24)))
