#!/usr/bin/env python
"""
Simple tools to parse and index the textfile dumps of osts.txt and ost-map.txt
which are populated with cron jobs that periodically issue the following:

echo "BEGIN $(date +%s)" >> osts.txt
/usr/bin/lfs df >> osts.txt

echo "BEGIN $(date +%s)" >> ost-map.txt
/usr/sbin/lctl dl -t >> ost-map.txt

This infrastructure is deployed at NERSC.
"""

import os
import sys
import gzip
import re
import warnings
import mimetypes

# Only try to match osc/mdc lines; skip mgc/lov/lmv
# 351 UP osc snx11025-OST0007-osc-ffff8875ac1e7c00 3f30f170-90e6-b332-b141-a6d4a94a1829 5 10.100.100.12@o2ib1
#
# snx11035-OST0000_UUID 90767651352 54512631228 35277748388  61% /scratch2[OST:0]
#                             snx000-OST... tot     use     avail      00%    /scra[OST    :0     ]
_REX_OST_MAP = re.compile('^\s*(\d+)\s+(\S+)\s+(\S+)\s+(snx\d+-\S+)\s+(\S+)\s+(\d+)\s+(\S+@\S+)\s*$')
_REX_LFS_DF = re.compile('^\s*(snx\d+-\S+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+).\s+(\S+)\[([^:]+):(\d+)\]\s*$')


class NerscLfsOstMap(dict):
    """
    Parser for ost-map.txt.  Generates a dict of form

        { timestamp(int) : { file_system: { ost_name : { keys: values } } } }

    This is a generally logical structure, although this map is always almost
    fed into a routine that tries to find multiple OSTs on the same OSS (i.e., a
    failover situation)
    """
    def __init__(self, cache_file=None):
        super(NerscLfsOstMap, self).__init__(self)
        self.cache_file = cache_file
        self.load_ost_map_file()

    def __repr__(self):
        """
        Serialize ost-map.txt back out into a format that can be re-loaded
        
        """
        repr_result = ""
        # Iterate over time steps ("BEGIN" lines)
        for timestamp in sorted(self.keys()):
            fs_data = self[timestamp]
            repr_result += "BEGIN %d\n" % timestamp
            # Iterate over file systems within each time step
            for target_name in sorted(fs_data.keys()):
                obd_data = fs_data[target_name]
                for obd_name in sorted(obd_data.keys(), key=lambda x: int(obd_data[x]['index']) ):
                    keyvals = obd_data[obd_name]
                    record_string = \
                        "%(index)3d %(status)2s %(role)3s %(role_id)s %(uuid)s %(ref_count)d %(nid)s\n" % keyvals
                    repr_result += record_string
        return repr_result

    def load_ost_map_file(self):
        """
        Parser for ost-map.txt.  Generates a dict of form

            { timestamp(int) : { file_system: { ost_name : { keys: values } } } }

        This is a generally logical structure, although this map is always almost
        fed into a routine that tries to find multiple OSTs on the same OSS (i.e., a
        failover situation)
        
        """
        _, encoding = mimetypes.guess_type(self.cache_file)
        if encoding == 'gzip':
            fp = gzip.open(self.cache_file, 'r')
        else:
            fp = open(self.cache_file, 'r')

        this_timestamp = None
        degenerate_keys = 0
        for line in fp:
            if line.startswith('BEGIN'):
                if degenerate_keys > 0:
                    warnings.warn("%d degenerate keys found for timestamp %d" % (degenerate_keys, this_timestamp))
                this_timestamp = int(line.split()[1])
                if this_timestamp in self:
                    warnings.warn("degenerate timestamp %d found" % this_timestamp)
                self.__setitem__(this_timestamp, {})
                degenerate_keys = 0
            else:
                match = _REX_OST_MAP.search(line)
                if match is not None:
                    file_system, target_name = (match.group(4).split('-')[0:2])

                    if file_system not in self[this_timestamp]:
                        self[this_timestamp][file_system] = {}

                    # Duplicates can happen if a file system is doubly mounted
                    if target_name in self[this_timestamp][file_system]:
                        degenerate_keys += 1

                    self[this_timestamp][file_system][target_name] = {
                        'index': int(match.group(1)),
                        'status': match.group(2).lower(),
                        'role': match.group(3).lower(),
                        'role_id': match.group(4),
                        'uuid': match.group(5),
                        'ref_count': int(match.group(6)),
                        'target_ip': match.group(7).split('@')[0],
                        'nid': match.group(7),
                    }

        fp.close()

    def save_cache(self, output_file=None):
        """
        Serialize the object in a form compatible with the output of ost-map.txt
        
        """
        if output_file is None:
            self._save_cache(sys.stdout)
        else:
            with open(output_file, 'w') as fp:
                self._save_cache(fp)

    def _save_cache(self, output):
        output.write(str(self))

     #=================================================#
   
    def get_failovers(self):
        """
        Given a NerscLfsOstMap, figure out OSTs that are probably failed over and,
        for each time stamp and file system, return a list of abnormal OSSes and the
        expected number of OSTs per OSS.
        
        """
        resulting_data = {}
        for timestamp, fs_data in self.iteritems():
            per_timestamp_data = {}
            for file_system, ost_data in fs_data.iteritems():
                ost_counts = {} # key = ip address, val = ost count
                for ost_name, ost_values in ost_data.iteritems():
                    if ost_values['role'] != 'osc': # don't care about mdc, mgc
                        continue
                    ip_addr = ost_values['target_ip']
                    ost_counts[ip_addr] = ost_counts.get(ip_addr, 0) + 1

                # Get mode of OSTs per OSS to infer what "normal" OST/OSS ratio is
                histogram = {}
                for ip_addr, ost_count in ost_counts.iteritems():
                    if ost_count not in histogram:
                        histogram[ost_count] = 1
                    else:
                        histogram[ost_count] += 1
                if not histogram:
                    raise KeyError('no OSTs to count')
                mode = max(histogram, key=histogram.get)

                # Build a dict of { ip_addr: [ ostname1, ostname2, ... ], ... }
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

class NerscLfsOstFullness(dict):
    """
    Parser for ost-fullness.txt.  Generates a dict of form

        { timestamp(int) : { file_system: { ost_name : { keys: values } } } }
    
    """
    def __init__(self, cache_file=None):
        super(NerscLfsOstFullness, self).__init__(self)
        self.cache_file = cache_file
        self.load_ost_fullness_file()

    def __repr__(self):
        """
        snx11025-OST0001_UUID 90767651352 63381521692 26424604184  71% /scratch1[OST:1]
    
        """
        repr_result = ""
        for timestamp in sorted(self.keys()):
            repr_result += "BEGIN %d\n" % timestamp
            fs_data = self[timestamp]
            for target_name in sorted(fs_data.keys()):
                obd_data = fs_data[target_name]
                for obd_name in sorted(obd_data.keys(), key=lambda x: obd_data[x]['target_index']):
                    keyvalues = obd_data[obd_name]
                    repr_result += "%s-%s_UUID %ld %ld %ld %3d%% %s[%s:%d]\n" % (
                        target_name,
                        obd_name,
                        keyvalues['total_kib'],
                        keyvalues['used_kib'],
                        keyvalues['remaining_kib'],
                        # Note that lfs dl's percents are not divided by
                        # avail_kib, but rather the sum of used and remaining.
                        round(100.0 * keyvalues['used_kib'] / (keyvalues['remaining_kib'] + keyvalues['used_kib'])),
                        keyvalues['mount_pt'],
                        keyvalues['role'].upper(),
                        keyvalues['target_index'], )

        return repr_result

    def load_ost_fullness_file(self):
        """
        Parse the output of a file containing concatenated outputs of `lfs df`
        separated by lines of the form `BEGIN 0000` where 0000 is the UNIX epoch
        time.  At NERSC, this file was (foolishly) called osts.txt in the h5lmt
        dump directory.

        """
        _, encoding = mimetypes.guess_type(self.cache_file)
        if encoding == 'gzip':
            fp = gzip.open(self.cache_file, 'r')
        else:
            fp = open(self.cache_file, 'r')

        this_timestamp = None
        degenerate_keys = 0
        for line in fp:
            if line.startswith('BEGIN'):
                if degenerate_keys > 0:
                    warnings.warn("%d degenerate keys found for timestamp %d" % (degenerate_keys, this_timestamp))
                this_timestamp = int(line.split()[1])
                if this_timestamp in self:
                    warnings.warn("degenerate timestamp %d found" % this_timestamp)
                self[this_timestamp] = {}
                degenerate_keys = 0
            else:
                match = _REX_LFS_DF.search(line)
                if match is not None:
                    file_system, target_name = re.findall('[^-_]+', match.group(1))[0:2]

                    if file_system not in self[this_timestamp]:
                        self[this_timestamp][file_system] = {}

                    # Duplicates can happen if a file system is doubly mounted
                    if target_name in self[this_timestamp][file_system]:
                        degenerate_keys += 1

                    self[this_timestamp][file_system][target_name] = {
                        'total_kib': long(match.group(2)),
                        'used_kib': long(match.group(3)),
                        'remaining_kib': long(match.group(4)),
                        'mount_pt': match.group(6),
                        'role': match.group(7).lower(),
                        'target_index': int(match.group(8)),
                    }

        fp.close()

    def save_cache(self, output_file=None):
        """
        Serialize the object in a form compatible with the output of
        ost-fullness.txt
        
        """
        if output_file is None:
            self._save_cache(sys.stdout)
        else:
            with open(output_file, 'w') as fp:
                self._save_cache(fp)

    def _save_cache(self, output):
        output.write(str(self))
