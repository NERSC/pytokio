#!/usr/bin/env pthon

import os
import datetime

def enumerate_dated_dir(base_dir,
                        datetime_start,
                        datetime_end=None,
                        file_name=None,
                        timedelta=datetime.timedelta(days=1)):
    """
    Given a directory template which can be passed to strftime, a starting
    datetime, an ending datetime (optionally), and a file name (optionally),
    return all directories (or files) that contain data inside of that date
    range (inclusive).  Can also provide a timedelta to iterate by more or
    less than one day (default).
    """   

    if datetime_start is None:
        raise Exception('A start time must be provided')

    if datetime_end is None: 
        datetime_end = datetime_start

    if datetime_end < datetime_start:
        raise Exception("datetime_end < datetime_start")

    day = datetime_start
    results = []
    while day.date() <= datetime_end.date():
        # return matching directories
        if file_name is None:
            result = day.strftime(base_dir)
            if os.path.isdir(result):
                results.append(result)
        # return matching directories
        else:
            result = os.path.join(day.strftime(base_dir), day.strftime(file_name))
            if os.path.isfile(result):
                results.append(result)
        day += timedelta

    return results
