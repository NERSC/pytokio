#!/usr/bin/env python
"""
Connectors for the GPFS ``mmperfmon query usage`` and
``mmperfmon query gpfsNumberOperations``
"""

import re
import pandas
from tokio.connectors.common import SubprocessOutputDict

# Legend:
#  1:	ngfsv468.nersc.gov|CPU|cpu_user
#  2:	ngfsv468.nersc.gov|CPU|cpu_sys
#  3:	ngfsv468.nersc.gov|Memory|mem_total
#  4:	ngfsv468.nersc.gov|Memory|mem_free
#  5:	ngfsv468.nersc.gov|Network|lo|net_r
#  6:	ngfsv468.nersc.gov|Network|lo|net_s
#  
# Row           Timestamp cpu_user cpu_sys   mem_total    mem_free     net_r     net_s 
#   1 2019-01-11-10:00:00      0.2    0.56  31371.0 MB  18786.5 MB    1.7 kB    1.7 kB 
#   2 2019-01-11-10:01:00     0.22    0.57  31371.0 MB  18785.6 MB    1.7 kB    1.7 kB 
#   3 2019-01-11-10:02:00     0.14    0.55  31371.0 MB  18785.1 MB    1.7 kB    1.7 kB 

_REX_LEGEND = re.compile(r'^\s*(\d+):\s+([^|]+)|([^|]+)|(\S+)\s*$')
_REX_ROWHEAD = re.compile(r'^\s*Row\s+Timestamp')
_REX_ROW = re.compile(r'^\s*\d+\s+(\d{4}-\d\d-\d\d-\d\d:\d\d:\d\d)\s+')

MMPERFMON = 'mmperfmon'

class MmperfmonOutput(SubprocessOutputDict):
    """
    Representation for the mmperfmon query command.  Generates a dict of form

        { timestamp: { keys: values } }

    """
    def __init__(self, *args, **kwargs):
        super(LfsOstMap, self).__init__(*args, **kwargs)
        self.subprocess_cmd = MMPERFMON
        self.legend = {}
        self.col_offsets = []
        self.load()

    def __repr__(self):
        """Serialize object into an ASCII string

        Returns a string that resembles the input used to initialize this object
        """
        repr_result = ""

        # TODO

        return repr_result

    def load_str(self, input_str):
        """Parse the output of the subprocess output to initialize self
        """

        for line in input_str.splitlines():
            match = _REX_LEGEND.search(line)
            if match is not None:
                # extract values
                row, hostname, counter = (match.group(1), match.group(2), match.group(4))
                self.legend[int(row)] = {'host': hostname, 'counter': counter}
                self.legend[hostname] = {'row': int(row), 'counter': counter}
                continue

            match = _REX_ROWHEAD.search(line)
            if True:# find the row header to determine offsets
                substr = line
                self.col_offsets = []
                while True:
                    offset = substr.find(' ')
                    if offset == 1:
                        break
                    self.col_offsets.append(offset)
                    word = substr[:offset]
                    substr = substr[offset:].lstrip()

            match = _REX_ROW.search(line)
            if match is not None:
                # TODO - break up line based on self.col_offsets
                # strip each col
                # add to self

    def to_dataframe(self)
        """Convert to a pandas.DataFrame
        """
        # TODO
