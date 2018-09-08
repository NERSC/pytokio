#!/usr/bin/env python
"""
Connectors for the Lustre `lfs df` and `lctl dl -t` commands to determine the
health of Lustre file systems from the clients' perspective.
"""

import re
from tokio.connectors.common import SubprocessOutputDict

# Only try to match osc/mdc lines; skip mgc/lov/lmv
# 351 UP osc snx11025-OST0007-osc-ffff8875ac1e7c00 3f30f170-90e6-b332-b141-a6d4a94a1829 5 10.100.100.12@o2ib1
#
# snx11035-OST0000_UUID 90767651352 54512631228 35277748388  61% /scratch2[OST:0]
#                             snx000-OST... tot     use     avail      00%    /scra[OST    :0     ]
_REX_OST_MAP = re.compile(r'^\s*(\d+)\s+(\S+)\s+(\S+)\s+(snx\d+-\S+)\s+(\S+)\s+(\d+)\s+(\S+@\S+)\s*$')
_REX_LFS_DF = re.compile(r'^\s*(snx\d+-\S+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+).\s+(\S+)\[([^:]+):(\d+)\]\s*$')

LCTL = 'lctl'
LCTL_DL_T = [LCTL, 'dl', '-t']
LFS = 'lfs'
LFS_DF = [LFS, 'df']

class LfsOstMap(SubprocessOutputDict):
    """
    Representation for the lctl dl -t command.  Generates a dict of form

        { file_system: { ost_name : { keys: values } } }

    This is a generally logical structure, although this map is always almost
    fed into a routine that tries to find multiple OSTs on the same OSS (i.e., a
    failover situation)
    """
    def __init__(self, *args, **kwargs):
        super(LfsOstMap, self).__init__(*args, **kwargs)
        self.subprocess_cmd = LCTL_DL_T
        self.load()

    def __repr__(self):
        """Serialize object into an ASCII string

        Returns a string that resembles the input used to initialize this object
        """
        repr_result = ""

        # Iterate over file systems within each time step
        for target_name in sorted(self.keys()):
            obd_data = self[target_name]
            for obd_name in sorted(list(obd_data.keys()), key=lambda x: int(obd_data[x]['index'])):
                keyvals = obd_data[obd_name]
                record_string = \
                    "%(index)3d %(status)2s %(role)3s %(role_id)s %(uuid)s %(ref_count)d %(nid)s\n" % keyvals
                repr_result += record_string

        return repr_result

    def load_str(self, input_str):
        """Parse the output of `lctl dl -t` to initialize self
        """
        degenerate_keys = 0
        for line in input_str.splitlines():
            match = _REX_OST_MAP.search(line)
            if match is not None:
                file_system, target_name = (match.group(4).split('-')[0:2])

                if file_system not in self:
                    self[file_system] = {}

                # Duplicates can happen if a file system is doubly mounted
                if target_name in self[file_system]:
                    degenerate_keys += 1

                self[file_system][target_name] = {
                    'index': int(match.group(1)),
                    'status': match.group(2).lower(),
                    'role': match.group(3).lower(),
                    'role_id': match.group(4),
                    'uuid': match.group(5),
                    'ref_count': int(match.group(6)),
                    'target_ip': match.group(7).split('@')[0],
                    'nid': match.group(7),
                }

    def get_failovers(self):
        """Identify OSSes with an abnormal number of OSTs

        Identify OSTs that are probably failed over and return a list of
        abnormal OSSes and the expected number of OSTs per OSS.
        """
        resulting_data = {}

        for file_system, ost_data in self.items():
            ost_counts = {} # key = ip address, val = ost count
            for ost_name, ost_values in ost_data.items():
                if ost_values['role'] != 'osc': # don't care about mdc, mgc
                    continue
                ip_addr = ost_values['target_ip']
                ost_counts[ip_addr] = ost_counts.get(ip_addr, 0) + 1

            # Get mode of OSTs per OSS to infer what "normal" OST/OSS ratio is
            histogram = {}
            for ip_addr, ost_count in ost_counts.items():
                if ost_count not in histogram:
                    histogram[ost_count] = 1
                else:
                    histogram[ost_count] += 1
            if not histogram:
                raise KeyError('no OSTs to count')
            mode = max(histogram, key=histogram.get)

            # Build a dict of { ip_addr: [ ostname1, ostname2, ... ], ... }
            abnormal_ips = {}
            for ost_name, ost_values in ost_data.items():
                if ost_values['role'] != 'osc': # don't care about mdc, mgc
                    continue
                ip_addr = ost_values['target_ip']
                if ost_counts[ip_addr] != mode:
                    if ip_addr in abnormal_ips:
                        abnormal_ips[ip_addr].append(ost_name)
                    else:
                        abnormal_ips[ip_addr] = [ost_name]

            resulting_data[file_system] = {
                'mode': mode,
                'abnormal_ips': abnormal_ips,
            }

        return resulting_data

class LfsOstFullness(SubprocessOutputDict):
    """
    Representation for the `lfs df` command.  Generates a dict of form

        { file_system: { ost_name : { keys: values } } }

    """
    def __init__(self, *args, **kwargs):
        super(LfsOstFullness, self).__init__(*args, **kwargs)
        self.subprocess_cmd = LFS_DF
        self.load()

    def __repr__(self):
        """Serialize object into an ASCII string

        Returns a string that resembles the input used to initialize this object:

            snx11025-OST0001_UUID 90767651352 63381521692 26424604184  71% /scratch1[OST:1]
        """
        repr_result = ""
        for target_name in sorted(self.keys()):
            obd_data = self[target_name]
            for obd_name in sorted(list(obd_data.keys()), key=lambda x: obd_data[x]['target_index']):
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

    def load_str(self, input_str):
        """Parse the output of `lfs df` to initialize self
        """
        degenerate_keys = 0
        for line in input_str.splitlines():
            match = _REX_LFS_DF.search(line)
            if match is not None:
                file_system, target_name = re.findall('[^-_]+', match.group(1))[0:2]

                if file_system not in self:
                    self[file_system] = {}

                # Duplicates can happen if a file system is doubly mounted
                if target_name in self[file_system]:
                    degenerate_keys += 1

                self[file_system][target_name] = {
                    'total_kib': int(match.group(2)),
                    'used_kib': int(match.group(3)),
                    'remaining_kib': int(match.group(4)),
                    'mount_pt': match.group(6),
                    'role': match.group(7).lower(),
                    'target_index': int(match.group(8)),
                }
