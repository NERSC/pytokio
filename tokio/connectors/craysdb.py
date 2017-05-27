#!/usr/bin/env python
"""
Interface with the Cray XT/XC service database to determine information about a
node's configuration within the network fabric.
"""

import os
import sys
import subprocess

class CraySDBProc(dict):
    """
    Presents certain views of the Cray Service Database (SDB) as a dictionary.
    Lazy load data to prevent unnecessary touches to the actual service
    database.

    This may someday become a base class for table-specific classes.
    """
    def __init__(self, cache_file=None):
        dict.__init__(self)
        self.cache_file = cache_file
        self.dbdata = None

    def __getitem__(self, key):
        """
        Lazy load the data but otherwise behave as a dict
        """
        if self.dbdata is None:
            self.load_sdb_data()
        return dict.__getitem__(self, key)

    def load_cache(self):
        """
        Load an xtdb2proc output file for a system
        """
        if cache_file is None:
            print "Loading from xtdb2proc"
            try:
                sdb = subprocess.check_output(['xtdb2proc', '-f', '-'])
            except CalledProcessError as exception:
                sys.stderr.write(exception.output + "\n")
                raise
            else:
                self._load_cache(sdb)
        else:
            print "Loading from xtdb2proc"
            print "Loading from cache"
            with open(cache_file, 'r') as fp:
                self._load_cache(fp)

    def _load_cache(self, iterable):
        """
        Load a serialized SDB cache passed in as an iterable
        """
        for line in iterable:
            if line.startswith('#'):
                continue
            fields = line.split(',')
            record = {}
            for field in fields:
                key, val = field.split('=', 1)
                ### remove extra quotes
                val = val.strip().strip('\'"')
                ### replace "null" with Python None values
                if val == "null":
                    val = None
                else:
                    ### coerce ints into ints
                    try:
                        val = int(val)
                    except ValueError:
                        pass
                record[key] = val
            key = int(record['processor_id'])
            print "inserting key", key
            assert 'processor_id' in record
            assert key not in self
            self.__setitem__(key, record)

    def save_cache(self):
        """
        Serialize the object in a form compatible with the output of xtdb2proc
        """
        raise NotImplementedError
    
#   def load_xtprocadmin_file(xtprocadmin_file):
#       """
#       Load a cached xtprocadmin output file for a system
#       """
#       self.xtprocadmin = {}
#       with open(xtprocadmin_file, 'r') as fp:
#           for line in fp:
#               args = line.strip().split()
#               if args[0] == "NID":
#                   continue
#               self.xtprocadmin[args[0]] = {
#                   'nodename': args[2],
#                   'type': args[3],
#                   'status': args[4],
#                   'mode': args[5]
#               }
