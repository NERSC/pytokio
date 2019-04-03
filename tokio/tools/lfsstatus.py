#!/usr/bin/env python
"""
Given a file system and a datetime, return summary statistics about the OST
fullness at that time
"""

import time
import datetime
import tokio.tools.common
import tokio.config
import tokio.connectors.nersc_lfsstate as nersc_lfsstate

DEFAULT_FULLNESS_PROVIDERS = ['hdf5', 'nersc_lfsstate']

def get_fullness(file_system, datetime_target, **kwargs):
    """Get file system fullness

    Is a convenience wrapper for `get_summary`.

    Args:
        file_system (str): Logical name of file system whose data should be
            retrieved (e.g., cscratch)
        datetime_target (datetime.datetime): Time at which requested data
            should be retrieved

    Returns:
        dict: various statistics about the file system fullness

    Raises:
        tokio.ConfigError: When no valid providers are found
    """
    providers = tokio.config.CONFIG.get('lfsstatus_fullness_providers', DEFAULT_FULLNESS_PROVIDERS)
    match = False
    fullness = {}
    for provider in providers:
        if provider == 'hdf5':
            match = True
            try:
                fullness = get_fullness_hdf5(file_system, datetime_target)
            except KeyError:
                # get_fullness_hdf5 throws KeyError if fullness data is not available
                match = False
            if fullness:
                return fullness
        if provider == 'nersc_lfsstate':
            match = True
            fsname = tokio.config.CONFIG.get('fsname_to_backend_name', {}).get(file_system)
            try:
                fullness = get_lfsstate(fsname if fsname else file_system,
                                        datetime_target,
                                        "fullness",
                                        **kwargs)
            except IOError:
                # get_lfsstate raises IOError if fullness data is not available
                match = False
            if fullness:
                return fullness

    if match:
        return fullness

    raise tokio.ConfigError("No valid lfsstatus fullness providers found")


def get_failures(file_system, datetime_target, **kwargs):
    """Get file system failures

    Is a convenience wrapper for `get_summary`.

    Args:
        file_system (str): Logical name of file system whose data should be
            retrieved (e.g., cscratch)
        datetime_target (datetime.datetime): Time at which requested data
            should be retrieved
        cache_file (str): Basename of file to search for the requested data

    Returns:
        dict: various statistics about the file system fullness
    """
    fsname = tokio.config.CONFIG.get('fsname_to_backend_name', {}).get(file_system)
    return get_lfsstate(fsname if fsname else file_system, datetime_target, "failures", **kwargs)


def get_fullness_lfsstate(file_system, datetime_target, cache_file=None):
    """Get file system fullness from nersc_lfsstate connector

    Wrapper around the generic get_lfsstate function.

    Args:
        file_system (str): Lustre file system name of the file system whose
            data should be retrieved (e.g., snx11025)
        datetime_target (datetime.datetime): Time at which requested data
            should be retrieved
        cache_file (str): Basename of file to search for the requested data

    Returns:
        Whatever is returned by :func:`tokio.tools.lfsstatus.get_lfsstate`
    """
    return get_lfsstate(file_system, datetime_target, 'fullness', cache_file)

def get_failures_lfsstate(file_system, datetime_target, cache_file=None):
    """Get file system failures from nersc_lfsstate connector

    Wrapper around the generic get_lfsstate function.

    Args:
        file_system (str): Lustre file system name of the file system whose
            data should be retrieved (e.g., snx11025)
        datetime_target (datetime.datetime): Time at which requested data
            should be retrieved
        cache_file (str): Basename of file to search for the requested data

    Returns:
        Whatever is returned by get_lfsstate
    """
    return get_lfsstate(file_system, datetime_target, 'failures', cache_file)

def get_lfsstate(file_system, datetime_target, metric, cache_file=None):
    """Get file system fullness or failures

    Given a file system name (e.g., snx11168) and a datetime object

        1. locate and load the lfs-df (fullness) or ost map (failures) file
        2. find the sample immediately preceding the datetime (don't find one
           that overlaps it
        3. return summary statistics about the OST fullness or OST failures

    Args:
        file_system (str): Lustre file system name of the file system whose
            data should be retrieved (e.g., snx11025)
        datetime_target (datetime.datetime): Time at which requested data
            should be retrieved
        metric (str): either "fullness" or "failures"
        cache_file (str): Basename of file to search for the requested data

    Returns:
        dict: various statistics about the file system fullness

    Raises:
        ValueError: if ``metric`` does not contain a valid option
        IOError: when no valid data sources can be found for the given date
    """
    if metric == "fullness":
        template_path = tokio.config.CONFIG['lfsstatus_fullness_files']
    elif metric == "failures":
        template_path = tokio.config.CONFIG['lfsstatus_map_files']
    else:
        raise ValueError("unknown metric " + metric)

    if cache_file is None:
        # We assume a 1 day lookbehind.  Very wasteful, but we index on dates in
        # local time using GMT-based unix times so we often need to look back to
        # the previous day's index.  The lookahead can be much more conservative
        # since it only needs to compensate for sampling intervals (15 min in
        # practice at NERSC)
        ost_health_files = tokio.tools.common.enumerate_dated_files(
            start=datetime_target - datetime.timedelta(days=1),
            end=datetime_target + datetime.timedelta(hours=1),
            template=template_path,
            match_first=True)

        for index, df_file in enumerate(ost_health_files):
            ost_health_files[index] = ost_health_files[index]
    else:
        ost_health_files = [cache_file]

    if not ost_health_files:
        raise IOError("No OST health files found in %s for %s" % (
            template_path,
            str(datetime_target)))

    # We can get away with the following because NerscLfsOstFullness,
    # NerscLfsOstMap, and NerscLfsOstMap.get_failovers all have the same
    # structure
    if metric == "fullness":
        ost_health = None
        for df_file in ost_health_files:
            if ost_health is None:
                ost_health = nersc_lfsstate.NerscLfsOstFullness(cache_file=df_file)
            else:
                ost_health.update(nersc_lfsstate.NerscLfsOstFullness(cache_file=df_file))
    elif metric == "failures":
        ost_map = None
        for map_file in ost_health_files:
            if ost_map is None:
                ost_map = nersc_lfsstate.NerscLfsOstMap(cache_file=map_file)
            else:
                ost_map.update(nersc_lfsstate.NerscLfsOstMap(cache_file=map_file))
        ost_health = ost_map.get_failovers()

    timestamps = sorted([int(x) for x in ost_health])

    # Unoptimized walk through to find our timestamp of interest
    target_timestamp = int(time.mktime(datetime_target.timetuple()))
    # If the day's records start after the target time stamp, just report the
    # first record (target_index=0)
    target_index = 0
    for index, timestamp in enumerate(timestamps):
        if timestamp >= target_timestamp:
            if index == 0:
                break
            else:
                target_index = index - 1
                break

    fs_data = ost_health[timestamps[target_index]][file_system]

    if metric == "fullness":
        results = _summarize_fullness(fs_data)
    if metric == "failures":
        results = _summarize_failover(fs_data)

    # In case you want to interpolate--hope is that we have enough data points
    # where OST volumes will not change significantly enough to require
    # interpolation
    if target_index < (len(timestamps) - 1):
        results['ost_next_timestamp'] = timestamps[target_index] + 1
    results.update({
        'ost_actual_timestamp': timestamps[target_index],
        'ost_requested_timestamp': target_timestamp,
    })
    return results


def get_fullness_hdf5(file_system, datetime_target):
    """Get file system fullness from an HDF5 object

    Given a file system name (e.g., snx11168) and a datetime object,
    return summary statistics about the OST fullness.

    Args:
        file_system (str): Name of file system whose data should be retrieved
        datetime_target (datetime.datetime): Time at which requested data
            should be retrieved

    Returns:
        dict: various statistics about the file system fullness

    Raises:
        ValueError: if an OST name is encountered which does not conform to
            a naming convention from which an OST index can be derived
    """
    # For concordance with the lfsstate version, assume a generous lookbehind
    # that captures at least a few timesteps.  Lookahead does not need to be as
    # generous.
    df_bytes = tokio.tools.hdf5.get_dataframe_from_time_range(
        fsname=file_system,
        dataset_name='/fullness/bytes',
        datetime_start=datetime_target - datetime.timedelta(hours=1),
        datetime_end=datetime_target + datetime.timedelta(hours=1))
    df_bytes_tot = tokio.tools.hdf5.get_dataframe_from_time_range(
        fsname=file_system,
        dataset_name='/fullness/bytestotal',
        datetime_start=datetime_target - datetime.timedelta(hours=1),
        datetime_end=datetime_target + datetime.timedelta(hours=1))

    # Bail if nothing is found
    if df_bytes is None or df_bytes_tot is None or len(df_bytes) == 0 or len(df_bytes_tot) == 0:
        return {}

    # NAs must be filled or else fullness/total values will be spuriously low
    df_bytes = df_bytes.fillna(method='ffill').fillna(method='bfill')
    df_bytes_tot = df_bytes_tot.fillna(method='ffill').fillna(method='bfill')

    # Find closest matching timestamp
    iloc = df_bytes.index.get_loc(datetime_target, method='nearest')

    # Build a dictionary that _summarize_fullness will accept as input
    results = {}
    for ostname, value in df_bytes.iloc[iloc].items():
        ostname_key = ostname.split('-')[-1]
        try:
            target_index = int(ostname_key.lower().lstrip('ost'), 16)
        except ValueError as error:
            raise type(error)("Cannot derive OST index from name '%s'" % ostname)
        used_kib = int(value / 1024.0)
        total_kib = int(df_bytes_tot.iloc[iloc][ostname] / 1024.0)
        results[ostname_key] = {
            'used_kib': used_kib,
            'total_kib': total_kib,
            'target_index': target_index,
        }

    summarized_data = _summarize_fullness(results)

    # for interpolation and error bounding
    if iloc < len(df_bytes.index):
        summarized_data['ost_next_timestamp'] = df_bytes.index[iloc + 1].to_pydatetime()

    summarized_data.update({
        'ost_actual_timestamp': df_bytes.index[iloc].to_pydatetime(),
        'ost_requested_timestamp': datetime_target,
    })

    return summarized_data


def _summarize_fullness(fs_data):
    """Summarize fullness data for a single time record

    Given an fs_data dict, generate a dict of summary statistics.  Expects
    fs_data dict of form generated by `nersc_lfsstate.NerscLfsOstFullness`::

        {
             "MDT0000": {
                 "mount_pt": "/scratch1",
                 "remaining_kib": 2147035984,
                 "target_index": 0,
                 "total_kib": 2255453580,
                 "used_kib": 74137712
             },
             "OST0000": {
                 "mount_pt": "/scratch1",
                 "remaining_kib": 28898576320,
                 "target_index": 0,
                 "total_kib": 90767651352,
                 "used_kib": 60894630700
             },
             ...
        }

    Args:
        fs_data (dict): a single timestamp and file system record taken from
            a nersc_lfsstate.NerscLfsOstFullness object

    Returns:
        dict: summary metrics about the state of the file system fullness
    """
    results = {
        'ost_least_full_kib': None,
        'ost_most_full_kib': 0,
        'ost_avg_full_kib': 0,
        'ost_avg_full_pct': 0,
        'ost_count': 0,
    }

    for ost_name, ost_data in fs_data.items():
        # Only care about OSTs, not MDTs or MGTs
        if not ost_name.lower().startswith('ost'):
            continue
        results['ost_count'] += 1
        results['ost_avg_full_kib'] += ost_data['used_kib']
        results['ost_avg_full_pct'] += ost_data['total_kib']
        if results['ost_least_full_kib'] is None \
        or results['ost_least_full_kib'] > ost_data['used_kib']:
            results['ost_least_full_kib'] = ost_data['used_kib']
            results['ost_least_full_name'] = ost_name
            results['ost_least_full_pct'] = 100.0 * ost_data['used_kib'] / ost_data['total_kib']

        if results['ost_most_full_kib'] < ost_data['used_kib']:
            results['ost_most_full_kib'] = ost_data['used_kib']
            results['ost_most_full_name'] = ost_name
            results['ost_most_full_pct'] = 100.0 * ost_data['used_kib'] / ost_data['total_kib']


    # If there are no osts, this will break
    try:
        results['ost_avg_full_kib'] = int(float(results['ost_avg_full_kib']) \
                                          / float(results['ost_count']))
        results['ost_avg_full_pct'] = 100.0 * float(results['ost_avg_full_kib']) \
                                      / float(results['ost_avg_full_pct']) \
                                      * float(results['ost_count'])
    except ZeroDivisionError:
        pass

    results['ost_least_full_id'] = fs_data[results['ost_least_full_name']]['target_index']
    results['ost_most_full_id'] = fs_data[results['ost_most_full_name']]['target_index']
    return results


def _summarize_failover(fs_data):
    """Summarize failover data for a single time record

    Given an fs_data dict, generate a dict of summary statistics. Expects
    fs_data dict of the form generated by parse_lustre_txt.get_failovers::

        {
            "abnormal_ips": {
                "10.100.104.140": [
                    "OST0087",
                    "OST0086",
                    ...
                ],
                "10.100.104.43": [
                    "OST0025",
                    "OST0024",
                    ...
                ]
            },
            "mode": 1
        }

    Args:
        fs_data (dict): a single timestamp and file system record taken from
            the output of `nersc_lfsstate.NerscLfsOstMap.get_failovers`

    Returns:
        dict: summary metrics about the state of failovers on the file system
    """
    num_abnormal_ip = len(fs_data['abnormal_ips'])
    num_abnormal_osts = 0
    if num_abnormal_ip:
        for _, ost_list in fs_data['abnormal_ips'].items():
            num_abnormal_osts += len(ost_list)
        avg_overload = float(num_abnormal_osts) / float(num_abnormal_ip)
        avg_overload_factor = avg_overload / float(fs_data['mode'])
    else:
        avg_overload = 0.0
        avg_overload_factor = 1.0

    return {
        'ost_overloaded_oss_count': num_abnormal_ip,
        'ost_overloaded_ost_count': num_abnormal_osts,
        'ost_avg_overloaded_ost_per_oss': avg_overload,
        'ost_avg_overloaded_overload_factor': avg_overload_factor,
    }
