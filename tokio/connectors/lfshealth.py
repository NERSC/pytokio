#!/usr/bin/env python
"""
Connectors for the Lustre `lfs df` and `lctl dl -t` commands to determine the
health of Lustre file systems from the clients' perspective.
"""

import os
import sys
import gzip
import errno
import re
import warnings
import mimetypes
import subprocess

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

class SubprocessOutput(dict):
    """Generic class to support connectors that parse the output of a subprocess

    When deriving from this class, the child object will have to

    1. Define subprocess_cmd after initializing this parent object
    2. Define self.__repr__ (if necessary)
    2. Define its own self.load_str
    3. Define any introspective analysis methods
    """
    def __init__(self, cache_file=None, from_string=None, silent_errors=False):
        super(SubprocessOutput, self).__init__(self)
        self.cache_file = cache_file
        self.silent_errors = silent_errors
        self.from_string = from_string
        self.subprocess_cmd = []

    def load(self):
        """Load based on initialization state of object
        """

        if self.from_string is not None:
            self.load_str(self.from_string)
        elif self.cache_file:
            self.load_cache()
        else:
            self._load_subprocess()

    def _load_subprocess(self, *args):
        """Run a subprocess and pass its stdout to a self-initializing parser
        """

        cmd = [self.subprocess_cmd]
        if args:
            cmd += args

        try:
            if self.silent_errors:
                with open(os.devnull, 'w') as devnull:
                    output_str = subprocess.check_output(cmd, stderr=devnull)
            else:
                output_str = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as error:
            warnings.warn("%s returned nonzero exit code (%d)" % (cmd, error.returncode))
            output_str = error.output
        except OSError as error:
            if error[0] == errno.ENOENT:
                raise type(error)(error[0], "%s command not found" % self.subprocess_cmd[0])
            raise

        self.load_str(output_str)

    def load_cache(self):
        """Load subprocess output from a cached text file
        """
        _, encoding = mimetypes.guess_type(self.cache_file)
        if encoding == 'gzip':
            input_fp = gzip.open(self.cache_file, 'r')
        else:
            input_fp = open(self.cache_file, 'r')
        self.load_str(input_fp.read())
        input_fp.close()

    def load_str(self, input_str):
        """Load subprocess output from a string
        """
        self['_raw'] = input_str

    def save_cache(self, output_file=None):
        """Serialize subprocess output to a text file
        """
        if output_file is None:
            sys.stdout.write(str(self))
        else:
            with open(output_file, 'w') as output_fp:
                output_fp.write(str(self))


class LfsOstMap(SubprocessOutput):
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
            for obd_name in sorted(obd_data.keys(), key=lambda x: int(obd_data[x]['index'])):
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

        for file_system, ost_data in self.iteritems():
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
                        abnormal_ips[ip_addr] = [ost_name]

            resulting_data[file_system] = {
                'mode': mode,
                'abnormal_ips': abnormal_ips,
            }

        return resulting_data

class LfsOstFullness(SubprocessOutput):
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
                    'total_kib': long(match.group(2)),
                    'used_kib': long(match.group(3)),
                    'remaining_kib': long(match.group(4)),
                    'mount_pt': match.group(6),
                    'role': match.group(7).lower(),
                    'target_index': int(match.group(8)),
                }
