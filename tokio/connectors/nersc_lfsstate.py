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
import re

_REX_OST_MAP = re.compile('^\s*(\d+)\s+(\S+)\s+(\S+)\s+(snx\d+-\S+)\s+(\S+)\s+(\d+)\s+(\S+@\S+)\s*$')

class NERSCLFSOSTMap(dict):
    """
    Parser for ost-map.txt.  Generates a dict of form

        { timestamp(int) : { file_system: { ost_name : { keys: values } } } }

    This is a generally logical structure, although this map is always almost
    fed into a routine that tries to find multiple OSTs on the same OSS (i.e., a
    failover situation)

    """
    def __init__(self, cache_file=None):
        super(NERSCLFSOSTMap, self).__init__(self)
        self.cache_file = cache_file
        self.load_ost_map_file()

    def __repr__(self):
        """
        Serialize ost-map.txt back out into a format that can be re-loaded
        """
        repr_result = ""
        ### iterate over time steps ("BEGIN" lines)
        for timestamp in sorted(self.keys()):
            fs_data = self[timestamp]
            repr_result += "BEGIN %d\n" % timestamp
            ### iterate over file systems within each time step
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
        this_timestamp = None
        with open( self.cache_file, 'r' ) as fp:
            for line in fp:
                if line.startswith('BEGIN'):
                    this_timestamp = int(line.split()[1])
                    assert this_timestamp not in self
                    self.__setitem__(this_timestamp, {})
                else:
                    match = _REX_OST_MAP.search(line)
                    if match is not None:
                        file_system, target_name = (match.group(4).split('-')[0:2])
    
                        if file_system not in self[this_timestamp]:
                            self[this_timestamp][file_system] = {}
    
                        ### duplicates can happen if a file system is doubly mounted
                        if target_name in self[this_timestamp][file_system]:
                            raise KeyError("%s already found in timestamp %d" % (target_name, this_timestamp))
    
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
