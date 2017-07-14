#!/usr/bin/env python
"""
Return Darshan performance data for each TOKIO-ABC benchmark log file given on
the command line
"""

import re
import argparse
import os
import tokio.connectors.darshan

# key = regex to identify a file system based on an output path
# val = a string that uniquely identifies that file system
_FS_PATTERNS = {
    '^/projects/radix-io': 'mira-fs1',
    '^/scratch1' : 'scratch1',
    '^/scratch2' : 'scratch2',
    '^/scratch3' : 'scratch3',
    '^/global/cscratch1' : 'cscratch',
    '^/var/opt/cray/dws/mounts/.*/ss/' : 'bb-shared',
    '^/var/opt/cray/dws/mounts/.*/ps/' : 'bb-private',
}

# key = the string that uniquely identifies a file system from _FS_PATTERNS
# val = the compute platform associated with that file system
_FS_PATTERN_TO_HOST = {
    'mira-fs1':   "mira",
    'scratch1':   "edison",
    'scratch2':   "edison",
    'scratch3':   "edison",
    'cscratch':   "cori",
    'bb-shared':  "cori",
    'bb-private': "cori",
}

def extract_darshan_perf(darshan_log):

    d = tokio.connectors.darshan.DARSHAN(darshan_log)
    darshan_data = d.darshan_parser_perf()

    exe_cmd_line = darshan_data['header']['exe']
    jobid = darshan_data['header']['jobid']
    start_time = darshan_data['header']['start_time']
    end_time = darshan_data['header']['end_time']

    if 'posix' in darshan_data['counters']:
        total_bytes = darshan_data['counters']['posix']['_perf']['total_bytes']
        slowest_rank_io_time = darshan_data['counters']['posix']['_perf']['slowest_rank_io_time_unique_files']
        time_by_slowest = darshan_data['counters']['posix']['_perf']['time_by_slowest_shared_files']
        agg_perf_by_slowest = darshan_data['counters']['posix']['_perf']['agg_perf_by_slowest']
    else:
        return {}

    darshan_data = d.darshan_parser_total()
    total_opens = darshan_data['counters']['posix']['_total']['OPENS']
    total_rws = (darshan_data['counters']['posix']['_total']['READS'] +
                darshan_data['counters']['posix']['_total']['WRITES'])

    app = 'UNKNOWN'
    api = 'UNKNOWN'
    rw = 'UNKNOWN'
    file_system = 'UNKNOWN'
    platform = 'UNKNOWN'

    # Determine exe name so we know how to identify other benchmark params
    exe = os.path.basename(exe_cmd_line[0])
    exe_args = exe_cmd_line[1:]
    if exe == 'ior':
        app = 'IOR'

        # Use cmdline args to determine api, rw, and file system (and platform)
        for pos, field in enumerate(exe_args):
            if field == '-w':
                rw = 'write'
            elif field == '-r':
                rw = 'read'
            elif field == '-f':
                params_file = exe_args[pos+1]
                if os.path.basename(params_file).startswith('mpiio'):
                    api = 'MPIIO'
                elif os.path.basename(params_file).startswith('posix'):
                    api = 'POSIX'
            elif field == '-o':
                for pattern, fs_key in _FS_PATTERNS.iteritems():
                    match = re.search(pattern, exe_args[pos+1])
                    if match is not None:
                        file_system = fs_key
                        platform = _FS_PATTERN_TO_HOST[file_system]
                        break
    elif exe == 'hacc_io_write':
        app = 'HACC-IO'
        api = 'GLEAN'
        rw  = 'write'

        # Use output file name to determine file system (and platform)
        for pattern, fs_key in _FS_PATTERNS.iteritems():
            match = re.search(pattern, exe_args[-1])
            if match is not None:
                file_system = fs_key
                platform = _FS_PATTERN_TO_HOST[file_system]
                break
    elif exe == 'hacc_io_read':
        app = 'HACC-IO'
        api = 'GLEAN'
        rw  = 'read'

        # Use output file name to determine file system (and platform)
        for pattern, fs_key in _FS_PATTERNS.iteritems():
            match = re.search(pattern, exe_args[-1])
            if match is not None:
                file_system = fs_key
                platform = _FS_PATTERN_TO_HOST[file_system]
                break
    elif exe == 'vpicio_uni':
        app = 'VPIC-IO'
        api = 'H5Part'
        rw  = 'write'

        # Use output file name to determine file system (and platform)
        for pattern, fs_key in _FS_PATTERNS.iteritems():
            match = re.search(pattern, exe_args[0])
            if match is not None:
                file_system = fs_key
                platform = _FS_PATTERN_TO_HOST[file_system]
                break
    elif exe == 'dbscan_read':
        app = 'BD-CATS-IO'
        api = 'H5Part'
        rw  = 'read'

        # Use cmdline args to determine file system (and platform)
        for pos, field in enumerate(exe_args):
            if field == '-f':
                for pattern, fs_key in _FS_PATTERNS.iteritems():
                    match = re.search(pattern, exe_args[pos+1])
                    if match is not None:
                        file_system = fs_key
                        platform = _FS_PATTERN_TO_HOST[file_system]
                        break

    # Total io time is sum of shared and unique file io times
    io_time = float(time_by_slowest) + float(slowest_rank_io_time)

    return {
        'platform': platform,
        'file_system': file_system,
        'app': app,
        'api': api,
        'rw': rw,
        'io_time': io_time,
        'agg_perf_by_slowest': agg_perf_by_slowest,
        'total_bytes': total_bytes,
        'total_opens': total_opens,
        'total_rws': total_rws,
        'start_time': start_time,
        'end_time': end_time,
        'jobid': jobid,
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='summarize performance from a darshan log', add_help=False)
    parser.add_argument("files", nargs='*', help="darshan logs to process")
    parser.add_argument("-h", "--header", action='store_true', help="display column header")
    args = parser.parse_args()

    if args.header:
        print '# %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % \
            ('platform', 'file_sys', 'app', 'api', 'rw', 'io_time', \
            'agg_perf_by_slowest', 'total_bytes', 'total_opens', 'total_rws', \
            'start_time', 'end_time', 'jobid')

    for darshan_log in sorted(args.files, key=os.path.getmtime):
        darshan_perf = extract_darshan_perf(darshan_log)
        if len(darshan_perf.keys()) > 0:
            print '%(platform)s,%(file_system)s,%(app)s,%(api)s,%(rw)s,%(io_time)s,%(agg_perf_by_slowest)s,%(total_bytes)s,%(total_opens)s,%(total_rws)s,%(start_time)s,%(end_time)s,%(jobid)s' % darshan_perf
