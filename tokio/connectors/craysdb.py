#!/usr/bin/env python
"""
Interface with the Cray XT/XC service database to determine information about a
node's configuration within the network fabric.
"""

import os
import sys
import gzip
import errno
import collections
import subprocess
import mimetypes
from tokio.connectors.common import SubprocessOutputDict

class CraySdbProc(SubprocessOutputDict):
    """
    Presents certain views of the Cray Service Database (SDB) as a dictionary.
    Lazy load data to prevent unnecessary touches to the actual service
    database.

    This may someday become a base class for table-specific classes.
    
    """
    def __init__(self, *args, **kwargs):
        super(CraySdbProc, self).__init__(*args, **kwargs)
        self.subprocess_cmd = ['xtdb2proc', '-f', '-']
        self.key_order = [] # implement poor-man's OrderedDict
        self.load()

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
    
    def load_str(self, input_str):
        """
        Load a serialized SDB cache passed in as an iterable
        """
        # Save the key order once
        check_keys = True
        for line in input_str.splitlines():
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
