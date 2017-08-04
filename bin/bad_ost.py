#!/usr/bin/env python

import sys
import json
import argparse
import numpy as np
import pandas as pd
import scipy.stats
import tokio.connectors.darshan

# OST_NAME_FMT = "OST%05x"
OST_NAME_FMT = "OST#%d"

def analyze_ost_performance(darshan_log, threshold):
    df = darshanlog_to_ost_dataframe(darshan_log)

    results = []
    for ost_name in [ x for x in df.columns if x.startswith('OST') ]:
        coeff, pval = scipy.stats.pearsonr(df['performance'], df[ost_name])
        results.append({ 'ost_name': ost_name,
                         'coefficient': coeff,
                         'p-value': pval })

    for result in sorted(results, key=lambda k: k['coefficient']):
        if result['p-value'] < threshold:
            print "%(ost_name)-12s %(coefficient)10.6f %(p-value)10.4g" % result

def estimate_darshan_file_performance(ranks_data):
    """
    Calculate performance in a sideways fashion: find the longest I/O time
    across any rank for this file, then divide the sum of all bytes
    read/written by this longest io time.  This function expects to receive
    a dict that is keyed by MPI ranks (or a single "-1" key) and whose
    values are dicts corresponding to Darshan POSIX counters.
    """
    max_io_time = 0.0
    sum_bytes = 0.0
    for rank_id, counter_data in ranks_data.iteritems():
        this_io_time = counter_data['F_WRITE_TIME'] + \
                       counter_data['F_READ_TIME'] + \
                       counter_data['F_META_TIME']
        if this_io_time > max_io_time:
            max_io_time = this_io_time
        sum_bytes += counter_data['BYTES_READ'] + \
                     counter_data['BYTES_WRITTEN']
    return sum_bytes / max_io_time

def darshanlog_to_ost_dataframe(darshan_log):
    """
    Given a Darshan log file path, (1) calculate the performance observed when
    accessing each individual file, (2) identify OSTs over which each file was
    striped, and (3) create a dataframe containing each file, its observed
    performance, and a matrix of values corresponding to what fraction of that
    file's contents were probably striped on each OST.
    """
    d = tokio.connectors.darshan.Darshan( darshan_log )
    d.darshan_parser_base()

    if 'counters' not in d:
        raise Exception("Invalid Darshan log")
    if 'lustre' not in d['counters']:
        raise Exception("Darshan log does not contain Lustre module data")

    results = {
        'file_paths': [],
        'performance': [],
        'ost_lists': [],
    }

    for file_path, ranks_data in d['counters']['posix'].iteritems():
        # calculate the file's I/O performance
        performance = estimate_darshan_file_performance(ranks_data)

        # assemble a list of OSTs
        ost_list = set([])
        if file_path not in d['counters']['lustre']:
            continue
        for rank_id, counter_data in d['counters']['lustre'][file_path].iteritems():
            for ost_id in range(counter_data['STRIPE_WIDTH']):
                key = "OST_ID_%d" % ost_id
                ost_list.add(counter_data[key])

        # append findings from this file record to the master dict
        if file_path not in results['file_paths']:
            results['file_paths'].append(file_path)
            results['performance'].append(performance)
            results['ost_lists'].append(list(ost_list))
        else:
            index = results['file_paths'].index(file_path)
            if results['performance'][index] < performance:
                results['performance'][index] = performance
            results['ost_lists'][index] = \
                list(set(results['ost_lists'][index]) | ost_list)

    # build a dataframe from the results dictionary
    num_records = len(results['file_paths'])
    pre_dataframe = {
        'file_paths': results['file_paths'],
        'performance': results['performance'],
    }
    # create one column for each OST
    for ost_id_list in results['ost_lists']:
        for ost_id in ost_id_list:
            ost_name = OST_NAME_FMT % ost_id
            if ost_name not in pre_dataframe:
                pre_dataframe[ost_name] = [ 0.0 ] * num_records

    # for each file record, calculate the fraction it contributed to each OST
    for index, file_path in enumerate(results['file_paths']):
        num_osts = float(len(results['ost_lists'][index]))
        for ost_id in results['ost_lists'][index]:
            ost_name = OST_NAME_FMT % ost_id
            pre_dataframe[ost_name][index] = 1.0 / num_osts

    return pd.DataFrame(pre_dataframe)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action='store_true', help='display all correlation results')
    parser.add_argument("-t", "--threshold", type=float, default=0.05, help="p-value above which correlations will not be displayed")
    parser.add_argument("darshanlog", default=None, help="darshan log to process")
    args = parser.parse_args()
    analyze_ost_performance(args.darshanlog, args.threshold)
