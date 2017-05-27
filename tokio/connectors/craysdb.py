#!/usr/bin/env python
"""
Interface with the Cray XT/XC service database to determine information about a
node's configuration within the network fabric.
"""

import os
import sys
import collections
import subprocess

class CraySDBProc(dict):
    """
    Presents certain views of the Cray Service Database (SDB) as a dictionary.
    Lazy load data to prevent unnecessary touches to the actual service
    database.

    This may someday become a base class for table-specific classes.
    """
    def __init__(self, cache_file=None):
        super(CraySDBProc, self).__init__(self)
        self.cache_file = cache_file
        self.load_cache()

#   def __getitem__(self, key):
#       """
#       Lazy load the data but otherwise behave as a dict
#       """
#       try:
#           val = super(CraySDBProc, self).__getitem__(key)
#       except KeyError:
#           ### stubbing this out in case we want to lazy load keys later on
#           raise
#       else:
#           return val

    def load_cache(self):
        """
        Load an xtdb2proc output file for a system
        """
        if self.cache_file is None:
            sdb = subprocess.check_output(['xtdb2proc', '-f', '-'])
#           sdb = subprocess.Popen(['xtdb2proc', '-f', '-'], stdout=subprocess.PIPE).communicate()[0]
            self._load_cache(sdb.splitlines())
        else:
            with open(self.cache_file, 'r') as fp:
                self._load_cache(fp)

    def _load_cache(self, iterable):
        """
        Load a serialized SDB cache passed in as an iterable
        """
        for line in iterable:
            if line.startswith('#'):
                continue
            elif line.strip() == "":
                continue
            fields = line.split(',')
            record = {}
            for field in fields:
                try:
                    key, val = field.split('=', 1)
                except:
                    print line
                    print field
                    raise
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
