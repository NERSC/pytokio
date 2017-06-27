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
        self.key_order = []
        self.load_xtdb2proc_table()

    def __repr__(self):
        """
        Returns the object in the same format as the xtdb2proc output so that
        this object can be circularly serialized and deserialized
        """

        repr_result = ""
        for _, record in self.iteritems():
            line = []
            for key in self.key_order:
                try:
                    val = record[key]
                except KeyError:
                    sys.stderr.write("key does not appear in all records\n")
                    raise
                # We don't need to know the difference between basestring 
                # and other because when we parse it, we don't make the 
                # the difference
                # Need to know some examples of CrayDb and their values
                if val is None:
                    line.append("%s='%s'" % (key, val))
                elif isinstance(val, basestring):
                    line.append("%s=null" % key)
                else:
                    line.append("%s=%s" % (key, val))
            repr_result += ','.join(line) + "\n"
        return repr_result

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

    def load_xtdb2proc_table(self):
        """
        Load an xtdb2proc output file for a system
        """
        if self.cache_file is None:
            # Load directly from the Cray service database
            sdb = subprocess.check_output(['xtdb2proc', '-f', '-'])
            # sdb = subprocess.Popen(['xtdb2proc', '-f', '-'], stdout=subprocess.PIPE).communicate()[0]
            self._parse_xtdb2proc_table(sdb.splitlines())
        else:
            # Load a cached copy of the service database xtdb2proc table
            with open(self.cache_file, 'r') as fp:
                self._parse_xtdb2proc_table(fp)

    def _parse_xtdb2proc_table(self, iterable):
        """
        Load a serialized SDB cache passed in as an iterable
        """
        check_keys = True
        for line in iterable:
            if line.startswith('#') or line.strip() == "":
                continue
            fields = line.split(',')
            record = {}
            for field in fields:
                key, val = field.split('=', 1)
                # Remove extra quotes
                val = val.strip().strip('\'"')
                # Replace "null" with Python None values
                if val == "null":
                    val = None
                else:
                    # Coerce ints into ints
                    try:
                        val = int(val)
                    except ValueError:
                        pass
                record[key] = val
                if check_keys:
                    self.key_order.append(key)
            check_keys = False
            key = int(record['processor_id'])
            assert 'processor_id' in record
            assert key not in self
            self.__setitem__(key, record)


    def save_cache(self, output_file=None):
        """
        Serialize the object in a form compatible with the output of xtdb2proc
        """
        if output_file is None:
            self._save_cache(sys.stdout)
        else:
            with open(output_file, 'w') as fp:
                self._save_cache(fp)

    def _save_cache(self, output):
        output.write(str(self))

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
