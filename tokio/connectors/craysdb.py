#!/usr/bin/env python
"""
This connection provides an interface to the Cray XT/XC service database (SDB).
It is intended to be used to determine information about a node's configuration
within the network fabric to provide topological information.
"""

import sys
from . import common
from tokio.common import isstr

class CraySdbProc(common.SubprocessOutputDict):
    """Dictionary subclass that self-populates with Cray SDB data.

    Presents certain views of the Cray Service Database (SDB) as a
    dictionary-like object through the Cray SDB CLI.
    """
    def __init__(self, *args, **kwargs):
        """Load the processor configuration table from the SDB.

        Args:
            *args: Passed to tokio.connectors.common.SubprocessOutputDict
            **kwargs: Passed to tokio.connectors.common.SubprocessOutputDict
        """
        super(CraySdbProc, self).__init__(*args, **kwargs)
        self.subprocess_cmd = ['xtdb2proc', '-f', '-']
        self.key_order = [] # implement poor-man's OrderedDict
        self.load()

    def __repr__(self):
        """Serialize self in a format compatible with ``xtdb2proc``.

        Returns the object in the same format as the xtdb2proc output so that
        this object can be circularly serialized and deserialized.

        Returns:
        str: String representation of the processor mapping table in a
        format compatible with the output of ``xtdb2proc``.
        """
        repr_result = ""
        for _, record in self.items():
            line = []
            for key in self.key_order:
                try:
                    val = record[key]
                except KeyError:
                    sys.stderr.write("key does not appear in all records\n")
                    raise

                # Look at the type of each val and return the correponding string
                if val is None:
                    line.append("%s=null" % key)
                elif isstr(val):
                    line.append("%s='%s'" % (key, val))
                else:
                    line.append("%s=%s" % (key, val))

            repr_result += ','.join(line) + "\n"
        return repr_result

    def load_str(self, input_str):
        """Load the xtdb2proc data for a Cray system.

        Parses the xtdb2proc output text and inserts keys/values into self.

        Args:
            input_str (str): stdout of the ``xtdb2proc`` command
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
