#!/usr/bin/env python
"""
Simple tools to parse and index the textfile dumps of osts.txt and ost-map.txt
which are populated with cron jobs that periodically issue the following:

echo "BEGIN $(date +%s)" >> osts.txt
/usr/bin/lfs df >> osts.txt

echo "BEGIN $(date +%s)" >> ost-map.txt
/usr/sbin/lctl dl -t >> ost-map.txt

This infrastructure needs serious work.
"""

import re
import sys
import json
import argparse

### only try to match osc/mdc lines; skip mgc/lov/lmv
# 351 UP osc snx11025-OST0007-osc-ffff8875ac1e7c00 3f30f170-90e6-b332-b141-a6d4a94a1829 5 10.100.100.12@o2ib1
REX_OST_MAP = re.compile('^\s*(\d+)\s+(\S+)\s+(\S+)\s+(snx\d+-\S+)\s+(\S+)\s+(\d+)\s+(\S+@\S+)\s*$')
# snx11035-OST0000_UUID 90767651352 54512631228 35277748388  61% /scratch2[OST:0]
#                             snx000-OST... tot     use     avail      00%    /scra[OST    :0     ]
REX_LFS_DF = re.compile('^\s*(snx\d+-\S+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+).\s+(\S+)\[([^:]+):(\d+)\]\s*$')

def parse_map( ost_map_txt ):
    """
    Parser for ost_map.txt.  Generates a dict of form

        { timestamp(int) : { file_system: { ost_name : { keys: values } } } }

    This is a generally logical structure, although this map is always almost
    fed into a routine that tries to find multiple OSTs on the same OSS (i.e., a
    failover situation)
    """
    this_timestamp = None
    all_data = {}
    with open( ost_map_txt, 'r' ) as fp:
        for line in fp:
            if line.startswith('BEGIN'):
                this_timestamp = int(line.split()[1])
                assert this_timestamp not in all_data
                all_data[this_timestamp] = {}
            else:
                match = REX_OST_MAP.search(line)
                if match is not None:
                    file_system, target_name = (match.group(4).split('-')[0:2])

                    if file_system not in all_data[this_timestamp]:
                        all_data[this_timestamp][file_system] = {}

                    ### duplicates can happen if a file system is doubly mounted
                    if target_name in all_data[this_timestamp][file_system]:
                        print json.dumps(all_data[this_timestamp])
                        raise KeyError("%s already found in timestamp %d" % (target_name, this_timestamp))

                    all_data[this_timestamp][file_system][target_name] = {
                        'index': match.group(1),
                        'role': match.group(3).lower(),
                        'target_ip': match.group(7).split('@')[0],
                    }
    return all_data

def parse_maps(file_list):
    """
    Wrapper for parse_map that accepts a list of input files
    """
    all_data = {}
    for input_file in file_list:
        all_data.update(parse_map(input_file))
    return all_data

def parse_df(lfs_df_txt):
    """
    Parse the output of a file containing concatenated outputs of `lfs df`
    separated by lines of the form `BEGIN 0000` where 0000 is the UNIX epoch
    time.  At NERSC, this file was (foolishly) called osts.txt in the h5lmt
    dump directory.
    """
    this_timestamp = None
    all_data = {}
    with open(lfs_df_txt, 'r') as fp:
        for line in fp:
            if line.startswith('BEGIN'):
                this_timestamp = int(line.split()[1])
                assert this_timestamp not in all_data
                all_data[this_timestamp] = {}
            else:
                match = REX_LFS_DF.search(line)
                if match is not None:
                    file_system, target_name = re.findall('[^-_]+', match.group(1))[0:2]

                    if file_system not in all_data[this_timestamp]:
                        all_data[this_timestamp][file_system] = {}

                    ### duplicates can happen if a file system is doubly mounted
                    if target_name in all_data[this_timestamp][file_system]:
                        print json.dumps(all_data[this_timestamp])
                        raise KeyError("%s already found in timestamp %d" % (target_name, this_timestamp))

                    all_data[this_timestamp][file_system][target_name] = {
                        'total_kib': long(match.group(2)),
                        'used_kib': long(match.group(3)),
                        'remaining_kib': long(match.group(4)),
                        'mount_pt': match.group(6),
                        'target_index': int(match.group(8)),
                    }

    return all_data

def parse_dfs(file_list):
    """
    Wrapper for parse_map that accepts a list of input files
    """
    all_data = {}
    for input_file in file_list:
        all_data.update(parse_df(input_file))
    return all_data

def get_failovers_from_map_list(file_list):
    """
    Wrapper that takes a list of ost map files and returns a failover dict
    """
    map_data = parse_maps(file_list)
    return get_failovers(map_data)

def get_failovers(ost_map):
    """
    Given the output of parse_map, figure out OSTs that are probably failed over
    """
    resulting_data = {}
    for timestamp, fs_data in ost_map.iteritems():
        per_timestamp_data = {}
        for file_system, ost_data in fs_data.iteritems():
            ost_counts = {} # key = ip address, val = ost count
            for ost_name, ost_values in ost_data.iteritems():
                if ost_values['role'] != 'osc': # don't care about mdc, mgc
                    continue
                ip_addr = ost_values['target_ip']
                ost_counts[ip_addr] = ost_counts.get(ip_addr, 0) + 1

            ### get mode of OSTs per OSS to infer what "normal" OST/OSS ratio is
            histogram = {}
            for ip_addr, ost_count in ost_counts.iteritems():
                if ost_count not in histogram:
                    histogram[ost_count] = 1
                else:
                    histogram[ost_count] += 1
            mode = max(histogram, key=histogram.get)

            ### build a dict of { ip_addr: [ ostname1, ostname2, ... ], ... }
            abnormal_ips = {}
            for ost_name, ost_values in ost_data.iteritems():
                if ost_values['role'] != 'osc': # don't care about mdc, mgc
                    continue
                ip_addr = ost_values['target_ip']  
                if ost_counts[ip_addr] != mode:
                    if ip_addr in abnormal_ips:
                        abnormal_ips[ip_addr].append(ost_name)
                    else:
                        abnormal_ips[ip_addr] = [ ost_name ]

            per_timestamp_data[file_system] = {
                'mode': mode,
                'abnormal_ips': abnormal_ips,
            }
        resulting_data[timestamp] = per_timestamp_data
        
    return resulting_data

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--map", help="parse OST map files (from lfs osts)", action="store_true")
    parser.add_argument("-d", "--df", help="parse lustre df files (from lfs df)", action="store_true")
    parser.add_argument("-f", "--failovers", help="identify failovers in a map file", action='store_true')
    parser.add_argument("files", nargs='*', help="output directories to process")
    args = parser.parse_args()

    if args.map and args.df:
        raise Exception("--map and --df are mutually exclusive")

    if args.map:
        parser_func = parse_maps
    elif args.df:
        parser_func = parse_dfs
    elif args.failovers:
        parser_func = get_failovers_from_map_list
    else:
        raise Exception("must specify --map, --df, or --failovers")

    all_data = parser_func(args.files)

    print json.dumps(all_data, indent=4, sort_keys=True)
