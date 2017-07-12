#!/usr/bin/env python
"""
Interface with the Cray XT/XC service database to determine information about a
node's configuration within the network fabric.
"""

import os
import sys
import collections
import subprocess

class CraySdbProc(dict):
    """
    Presents certain views of the Cray Service Database (SDB) as a dictionary.
    Lazy load data to prevent unnecessary touches to the actual service
    database.

    This may someday become a base class for table-specific classes.
    
    """
    def __init__(self, cache_file=None):
        super(CraySdbProc, self).__init__(self)
        self.cache_file = cache_file
        # Keep the order in which we parse xtdb2proc output file  
        self.key_order = []
        self.load_xtdb2proc_table()

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
                #
                # Look at the type of each val and 
                # return the correponding string
                if val is None:
                    line.append("%s=null" % key)
                elif isinstance(val, basestring):
                    line.append("%s='%s'" % (key, val))
                else:
                    line.append("%s=%s" % (key, val))
                
            repr_result += ','.join(line) + "\n"
        return repr_result
    
    
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

    #========================================================#
        
    def _parse_xtdb2proc_table(self, iterable):
        """
        Load a serialized SDB cache passed in as an iterable

        """
        # Save the key order once
        check_keys = True
        for line in iterable:
            if line.startswith('#') or line.strip() == "":
                continue
            fields = line.split(',')
            record = {}
            for field in fields:
                key, val = field.split('=', 1)
                val = val.strip().strip('\'"')
                # Look at the type of each val
                if val == "null":
                    val = None
                else:
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
