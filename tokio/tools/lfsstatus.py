#!/usr/bin/env python
"""
Given a file system and a datetime, return summary statistics about the OST
fullness at that time
"""

import os
import datetime
from ..connectors import nersc_lfsstate
import hdf5
import ConfigParser
cfg = ConfigParser.ConfigParser()
cfg.read(os.path.join('..', 'tokio', 'tokio.cfg'))
FILE_BASENAME_FULLNESS = eval(cfg.get('tokio', 'FILE_BASENAME_FULLNESS'))
FILE_BASENAME_FAILURES = eval(cfg.get('tokio', 'FILE_BASENAME_FAILURES'))
FS_TO_H5LMT = eval(cfg.get('tokio', 'FS_TO_H5LMT'))


def get_fullness_at_datetime(file_system, datetime_target, cache_file=None):
    return get_summary_at_datetime(file_system, datetime_target, "fullness", cache_file)

def get_failures_at_datetime(file_system, datetime_target, cache_file=None):
    return get_summary_at_datetime(file_system, datetime_target, "failures", cache_file)

def get_summary_at_datetime(file_system, datetime_target, metric, cache_file):
    """
    Given a file system name (e.g., snx11168), a datetime object, and either
    "fullness" or "failures":
        1. locate and load the lfs-df (fullness) or ost map (failures) file
        2. find the sample immediately preceding the datetime (don't find one
           that overlaps it, as it may contain the effects of the benchmark
           being run)
        3. return summary statistics about the OST fullness or OST failures

    """
    file_system_to_h5lmt = FS_TO_H5LMT
    h5lmt_file = file_system_to_h5lmt[file_system]
    ### TODO: this is terrible; need to not hard-code these names and paths
    if metric == "fullness":
        file_basename = FILE_BASENAME_FULLNESS
    elif metric == "failures":
        file_basename = FILE_BASENAME_FAILURES
    else:
        raise Exception("unknown metric " + metric)

    if cache_file is None:
        # We assume a 1 day lookbehind.  Very wasteful, but we index on dates in
        # local time using GMT-based unix times so we often need to look back to
        # the previous day's index.  The lookahead can be much more conservative
        # since it only needs to compensate for sampling intervals (15 min in
        # practice at NERSC)
        ost_health_files = hdf5.enumerate_h5lmts(h5lmt_file, 
                                                 datetime_target - datetime.timedelta(days=1),
                                                 datetime_target + datetime.timedelta(hours=1))
        for index, df_file in enumerate(ost_health_files):
            ost_health_files[index] = ost_health_files[index].replace(h5lmt_file, file_basename)
    else:
        ost_health_files = [cache_file]

    # TODO : Remove this comment after the package is deleted
    # We can get away with the following because NERSCLFSOSTFullness,
    # NERSCLFSOSTMap, and NERSCLFSOSTMap.get_failovers all have the same
    # structure
    if metric == "fullness":
        ost_health = None
        for df_file in ost_health_files:
            if ost_health is None:
                ost_health = nersc_lfsstate.NERSCLFSOSTFullness(cache_file=df_file)
            else:
                ost_health.update(nersc_lfsstate.NERSCLFSOSTFullness(cache_file=df_file))
    elif metric == "failures":
        ost_map = None
        for map_file in ost_health_files:
            if ost_map is None:
                ost_map = nersc_lfsstate.NERSCLFSOSTMap(cache_file=map_file)
            else:
                ost_map.update(nersc_lfsstate.NERSCLFSOSTMap(cache_file=map_file))
        ost_health = ost_map.get_failovers()

    timestamps = sorted([int(x) for x in ost_health.keys()])

    # Unoptimized walk through to find our timestamp of interest.  gynmastics
    # with fromtimestamp(0) required to convert a datetime (expressed in local
    # time) into a UTC-based epoch
    target_timestamp = int((datetime_target - datetime.datetime.fromtimestamp(0)).total_seconds())
    target_index = None
    for index, timestamp in enumerate(timestamps):
        if timestamp >= target_timestamp:
            if index == 0:
                break
            else:
                target_index = index - 1
                break
    if target_index is None:
        raise Exception("no timestamp of interest not found")

    fs_data = ost_health[timestamps[target_index]][file_system]

    if metric == "fullness":
        results = summarize_df_data(fs_data)
    if metric == "failures":
        results = summarize_maps_data(fs_data)

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

def summarize_maps_data(fs_data):
    """
    Given an fs_data dict, generate a dict of summary statistics. Expects
    fs_data dict of the form generated by parse_lustre_txt.get_failovers:
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

    """
    num_abnormal_ip = len(fs_data['abnormal_ips'].keys())
    num_abnormal_osts = 0
    if num_abnormal_ip:
        for _, ost_list in fs_data['abnormal_ips'].iteritems():
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


def summarize_df_data(fs_data):
    """
    Given an fs_data dict, generate a dict of summary statistics.  Expects
    fs_data dict of form generated by parse_lustre_txt.parse_df:
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

    """
    results = { 
        'ost_least_full_kib': None,
        'ost_most_full_kib': 0,
        'ost_avg_full_kib': 0,
        'ost_avg_full_pct': 0,
        'ost_count': 0,
    }

    for ost_name, ost_data in fs_data.iteritems():
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
        results['ost_avg_full_pct'] = 100.0 * float(results['ost_avg_full_kib']) / float(results['ost_avg_full_pct']) * float(results['ost_count'])
    except ZeroDivisionError:
        pass
    
    results['ost_least_full_id'] = fs_data[results['ost_least_full_name']]['target_index']
    results['ost_most_full_id'] = fs_data[results['ost_most_full_name']]['target_index']
    return results
