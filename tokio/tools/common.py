#!/usr/bin/env pthon
"""Common routines used to apply site-specific info to connectors
"""

import os
import glob
import datetime

def enumerate_dated_files(start, end, template,
                          lookup_key=None, match_first=True,
                          timedelta=datetime.timedelta(days=1),
                          use_glob=False):
    """Locate existing time-indexed files between a start and end time.

    Given a start time, end time, and template data structure that describes a
    pattern by which the files of interest are indexed, locate all existing
    files that fall between the start and end time.

    The template argument (`template`) are paths that are passed through
    `datetime.strftime` and then checked for existence for every `timedelta`
    increment between `start` and `end`, inclusive.  `template` may be one
    of three data structures:

        - str: search for files matching this template
        - list of str: search for files matching each template.  If
          ``match_first`` is True, only the first hit per list item per time
          interval is returned; otherwise, every file matching every template
          in the entire list is returned.
        - dict: use ``lookup_key`` to determine the element in the dictionary
          to use as the template.  That value is treated as a new ``template``
          object and can be of any of these three types.

    Args:
        start (datetime.datetime): Begin including files corresponding to this
            start date, inclusive.
        end (datetime.datetime): Stop including files with timestamps that
            follow this end date.  Resulting files _will_ include this date.
        template (str, list, or dict): Template string(s) which should be passed
            to datetime.strftime to be converted into specific time-delimited
            files.
        lookup_key (str or None): When `type(template)` is dict, use this key to
            identify the key-value to use as template.  If None and `template` is
            a dict, iterate through all values of `template`.
        match_first (bool): If True, only return the first matching file for
            each time increment checked.  Otherwise, return _all_ matching
            files.
        timedelta (datetime.timedelta): Increment to use when iterating between
            `start` and `end` while looking for matching files.
        use_glob (bool): Expand file globs in template

    Returns:
        list: List of strings, each describing a path to an existing HDF5 file
        that should contain data relevant to the requested start and end dates.
    """

    if end < start:
        raise IndexError("datetime_end < datetime_start")

    check_paths = _expand_check_paths(template, lookup_key)

    day = start
    results = []
    while day.date() <= end.date():
        results += _match_files(check_paths, day, match_first, use_glob=use_glob)
        day += timedelta

    return results


def _expand_check_paths(template, lookup_key):
    """Generate paths to examine from a variable-type template.

    `template` may be one of three data structures:

        - str: search for files matching this exact template
        - list of str: search for files matching each template listed.
        - dict: use `lookup_key` to determine the element in the dictionary
          to use as the template.  That value is treated as a new `template`
          object and can be of any of these three types.

    Args:
        template (str, list, or dict): Template string(s) which should be passed
            to datetime.datetime.strftime to be converted into specific
            time-delimited files.
        lookup_key (str or None): When `type(template)` is dict, use this key
            to identify the key-value to use as template.  If None and
            ``template`` is a dict, iterate through all values of `template`.

    Returns:
        list: List of strings, each describing a path to an existing HDF5 file
        that should contain data relevant to the requested start and end dates.
    """
    check_paths = []
    if isinstance(template, dict):
        if lookup_key is None:
            check_paths += _expand_check_paths(list(template.values()), lookup_key)
        else:
            check_paths += _expand_check_paths(template.get(lookup_key, []), lookup_key)
    elif isinstance(template, list):
        for value in template:
            check_paths += _expand_check_paths(value, lookup_key)
    else:
        check_paths += [template]

    return check_paths


def _match_files(check_paths, use_time, match_first, use_glob):
    """Locate file(s) that match a templated file path for a given time

    Args:
        check_paths (list of str): List of templates to pass to strftime
        use_time (datetime.datetime): Time to pass through strftime to generate
            an actual file path to check for existence.
        match_first (bool): If True, only return the first matching file for
            each time increment checked.  Otherwise, return _all_ matching
            files.
        use_glob (bool): Expand file globs in template

    Returns:
        list: List of strings, each describing a path to an existing HDF5 file
        that should contain data relevant to the requested start and end dates.
    """

    matching = []
    for check_path in check_paths:
        match_path = use_time.strftime(check_path)
        expanded_match_paths = glob.glob(match_path) if use_glob else [match_path]
        match = False
        for expanded_match_path in expanded_match_paths:
            if os.path.exists(expanded_match_path):
                matching.append(expanded_match_path)
                match = True
                if match_first:
                    break
        if match and match_first:
            break

    return matching
