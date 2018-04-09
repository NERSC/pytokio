#!/usr/bin/env python
"""
This connection provides an interface to the Cray XT/XC service database (SDB).
It is intended to be used to determine information about a node's configuration
within the network fabric to provide topological information.
"""

import os
import sys
import gzip
import errno
import collections
import subprocess
import mimetypes

class CraySdbProc(dict):
    """Dictionary subclass that self-populates with Cray SDB data.

    Presents certain views of the Cray Service Database (SDB) as a
    dictionary-like object through the Cray SDB CLI.
    """
    def __init__(self, cache_file=None):
        """Load the processor configuration table from the SDB.

        Loads the Cray processor map table using either a cached, ASCII version
        of the table or by calling the ``xtdb2proc`` command.

        Args:
            cache_file (str, optional): Path to a cache file to load instead of
                issuing the ``xtdb2proc`` command.
        """
        super(CraySdbProc, self).__init__(self)
        self.cache_file = cache_file
        # Keep the order in which we parse xtdb2proc output file  
        self.key_order = []
        self.load_xtdb2proc_table()

    def load_xtdb2proc_table(self):
        """Load the xtdb2proc data for a Cray system.

        Either loads a cached version of the xtdb2proc output text, or calls
        ``xtdb2proc`` to collect that data from the Cray service database.
        Inserts keys/values into self.
        """
        if self.cache_file is None:
            # Load directly from the Cray service database
            try:
                sdb = subprocess.check_output(['xtdb2proc', '-f', '-'])
            except OSError as error:
                if error[0] == errno.ENOENT:
                    raise type(error)(error[0], "CraySDB CLI (xtdb2proc command) not found")
                raise
            # sdb = subprocess.Popen(['xtdb2proc', '-f', '-'], stdout=subprocess.PIPE).communicate()[0]
            self.parse_xtdb2proc_table(sdb.splitlines())
        else:
            # Load a cached copy of the service database xtdb2proc table
            _, encoding = mimetypes.guess_type(self.cache_file)
            if encoding == 'gzip':
                fp = gzip.open(self.cache_file, 'r')
            else:
                fp = open(self.cache_file, 'r')
            self.parse_xtdb2proc_table(fp)
            fp.close()

    def __repr__(self):
        """Serialize self in a format compatible with ``xtdb2proc``.

        Returns the object in the same format as the xtdb2proc output so that
        this object can be circularly serialized and deserialized.

        Returns:
            str: String representation of the processor mapping table in a
            format compatible with the output of ``xtdb2proc``.
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
        """Serialize self and write out to a cache file

        Serializes the object into a form compatible with the output of
        ``xtdb2proc`` and optionally stores that output to a file.

        Args:
            output_file (str, optional): Path to a file to which the serialized
                output should be written.  If None, print to stdout.
        """
        if output_file is None:
            self._save_cache(sys.stdout)
        else:
            with open(output_file, 'w') as fp:
                self._save_cache(fp)

    def _save_cache(self, output):
        """Serialize object into a form compatible with ``xtdb2proc``.
        """
        output.write(str(self))

    def parse_xtdb2proc_table(self, iterable):
        """Ingest a serialized SDB processor mapping.

        Convert an iterable representation of the Cray service database
        processor mapping into key-value pairs, then insert them into the
        object.

        Args:
            iterable: Any iterable that contains one service database record
                per element.  Typically a file-like object containing the output
                of ``xtdb2proc``.
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
