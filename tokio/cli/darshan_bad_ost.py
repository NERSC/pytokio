"""
Given one or more Darshan logs containing both POSIX and Lustre counters,
attempt to determine the performance each file saw and try to correlate poorly
performing files with specific Lustre OSTs.

This tool first estimates per-file I/O bandwidths by dividing the total bytes
read/written to each file by the time the application spent performing I/O to
that file.  It then uses data from Darshan's Lustre module to map these
performance estimates to the OSTs over which each file was striped.
With the list of OSTs and performance measurements corresponding to each OST,
the Pearson correlation coefficient is then calculated between performance and
each individual OST.

Multiple Darshan logs can be passed to increase the number of observations used
for correlation.  This tool does not work unless the Darshan log(s) contain
data from the Lustre module.
"""

import json
import argparse
import warnings
import pandas
import scipy.stats
import tokio.connectors.darshan

# OST_NAME_FMT = "OST%05x"
OST_NAME_FMT = "OST#%d"

def correlate_ost_performance(darshan_logs):
    """
    Generate a DataFrame containing files, performance measurements, and OST
    mappings and attempt to correlate performance with individual OSTs.
    """
    darshan_df = darshanlogs_to_ost_dataframe(darshan_logs)

    # in the unlikely event that all performance measurements are the same (or
    # there is only one), we simply cannot perform correlation analysis
    if len(darshan_df['performance'].unique()) == 1:
        return []

    results = []
    for ost_name in [x for x in darshan_df.columns if x.startswith('OST')]:
        # if all values are the same (or there is only one), we cannot correlate
        if len(darshan_df[ost_name].unique()) == 1:
            continue
        coeff, pval = scipy.stats.pearsonr(darshan_df['performance'],
                                           darshan_df[ost_name])
        results.append({
            'ost_name': ost_name,
            'coefficient': coeff,
            'p-value': pval})
    return results

def estimate_darshan_perf(ranks_data):
    """
    Calculate performance in a sideways fashion: find the longest I/O time
    across any rank for this file, then divide the sum of all bytes
    read/written by this longest io time.  This function expects to receive
    a dict that is keyed by MPI ranks (or a single "-1" key) and whose
    values are dicts corresponding to Darshan POSIX counters.
    """
    max_io_time = 0.0
    sum_bytes = 0.0
    for counter_data in ranks_data.values():
        this_io_time = counter_data['F_WRITE_TIME'] + \
                       counter_data['F_READ_TIME'] + \
                       counter_data['F_META_TIME']
        if this_io_time > max_io_time:
            max_io_time = this_io_time
        sum_bytes += counter_data['BYTES_READ'] + \
                     counter_data['BYTES_WRITTEN']
    return sum_bytes / max_io_time

def summarize_darshan_perf(darshan_logs):
    """
    Given a list of Darshan log file paths, calculate the performance observed
    from each file and identify OSTs over which each file was striped.  Return
    this summary of file performances and stripes.
    """
    results = {
        'file_paths': [],
        'performance': [],
        'ost_lists': [],
    }
    for darshan_log in darshan_logs:
        darshan_data = tokio.connectors.darshan.Darshan(darshan_log)
        darshan_data.darshan_parser_base()
        if 'counters' not in darshan_data:
            warnings.warn("Invalid Darshan log %s" % darshan_log)
            continue
        elif 'posix' not in darshan_data['counters']:
            warnings.warn("Darshan log %s does not contain POSIX module data" % darshan_log)
            continue
        elif 'lustre' not in darshan_data['counters']:
            warnings.warn("Darshan log %s does not contain Lustre module data" % darshan_log)
            continue
        counters = darshan_data['counters']

        for logged_file_path, ranks_data in counters['posix'].items():
            # encode the darshan log's name in addition to the file path in case
            # multiple Darshan logs with identical file paths (but different
            # striping) are being processed
            file_path = "%s@%s" % (darshan_log, logged_file_path)

            # calculate the file's I/O performance
            performance = estimate_darshan_perf(ranks_data)

            # assemble a list of OSTs
            ost_list = set([])
            if logged_file_path not in counters['lustre']:
                continue
            for counter_data in counters['lustre'][logged_file_path].values():
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
    return results

def darshanlogs_to_ost_dataframe(darshan_logs):
    """
    Given a set of Darshan log file paths, create a dataframe containing each
    file, its observed performance, and a matrix of values corresponding to what
    fraction of that file's contents were probably striped on each OST.
    """

    # get a dict of files, their performances, and their stripes
    results = summarize_darshan_perf(darshan_logs)

    # build a dataframe from the results dictionary
    pre_dataframe = {
        'file_paths': results['file_paths'],
        'performance': results['performance'],
    }

    # create one column for each OST
    for ost_id_list in results['ost_lists']:
        for ost_id in ost_id_list:
            ost_name = OST_NAME_FMT % ost_id
            if ost_name not in pre_dataframe:
                pre_dataframe[ost_name] = [0.0] * len(results['file_paths'])

    # for each file record, calculate the fraction it contributed to each OST
    for index in range(len(results['file_paths'])):
        num_osts = float(len(results['ost_lists'][index]))
        for ost_id in results['ost_lists'][index]:
            ost_name = OST_NAME_FMT % ost_id
            pre_dataframe[ost_name][index] = 1.0 / num_osts

    return pandas.DataFrame(pre_dataframe)

def main(argv=None):
    """Entry point for the CLI interface
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-j", "--json", action="store_true", help="return output in JSON format")
    parser.add_argument("-p", "--p-threshold", type=float, default=0.05,
                        help="p-value above which correlations will not be displayed")
    parser.add_argument("-c", "--c-threshold", type=float, default=0.0,
                        help="coefficient below which abs(correlations) will not be displayed")
    parser.add_argument("-C", "--c-warning", type=float, default=0.5,
                        help="print a warning if abs(correlations) is above this threshold")
    parser.add_argument("darshanlogs", nargs="*", default=None, help="darshan logs to process")
    args = parser.parse_args(argv)

    results = correlate_ost_performance(args.darshanlogs)

    if args.json:
        filtered_results = []
        for result in sorted(results, key=lambda k: k['coefficient']):
            if result['p-value'] < args.p_threshold \
            and abs(result['coefficient']) > args.c_threshold:
                filtered_results.append(result)
        print(json.dumps(filtered_results, indent=4, sort_keys=True))
    else:
        print("%-10s %12s %10s" % ("OST Name", "Correlation", "P-Value"))
        for result in sorted(results, key=lambda k: k['coefficient']):
            if result['p-value'] < args.p_threshold \
            and abs(result['coefficient']) > args.c_threshold:
                if abs(result['coefficient']) > args.c_warning:
                    if result['coefficient'] < 0:
                        result['warning'] = " <-- this OST looks unhealthy"
                    else:
                        result['warning'] = " <-- this OST is running unusually quickly"
                else:
                    result['warning'] = ""
                print("%(ost_name)-10s %(coefficient)12.3f %(p-value)10.4g%(warning)s" % result)
