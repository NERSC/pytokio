#!/usr/bin/env python
"""
Connectors for the GPFS ``mmperfmon query usage`` and
``mmperfmon query gpfsNumberOperations``.

The typical output of ``mmperfmon query usage`` may look something like::

    Legend:
     1:	xxxxxxxx.nersc.gov|CPU|cpu_user
     2:	xxxxxxxx.nersc.gov|CPU|cpu_sys
     3:	xxxxxxxx.nersc.gov|Memory|mem_total
     4:	xxxxxxxx.nersc.gov|Memory|mem_free
     5:	xxxxxxxx.nersc.gov|Network|lo|net_r
     6:	xxxxxxxx.nersc.gov|Network|lo|net_s

    Row           Timestamp cpu_user cpu_sys   mem_total    mem_free     net_r     net_s
      1 2019-01-11-10:00:00      0.2    0.56  31371.0 MB  18786.5 MB    1.7 kB    1.7 kB
      2 2019-01-11-10:01:00     0.22    0.57  31371.0 MB  18785.6 MB    1.7 kB    1.7 kB
      3 2019-01-11-10:02:00     0.14    0.55  31371.0 MB  18785.1 MB    1.7 kB    1.7 kB

"""

import re
import pandas
from tokio.connectors.common import SubprocessOutputDict

_REX_LEGEND = re.compile(r'^\s*(\d+):\s+([^|]+)\|([^|]+)\|(\S+)\s*$')
_REX_ROWHEAD = re.compile(r'^\s*Row\s+Timestamp')
_REX_ROW = re.compile(r'^\s*\d+\s+(\d{4}-\d\d-\d\d-\d\d:\d\d:\d\d)\s+')

MMPERFMON = 'mmperfmon'

class MmperfmonOutput(SubprocessOutputDict):
    """
    Representation for the mmperfmon query command.  Generates a dict of form

        {
            timestamp0: {
                    "something0.nersc.gov": {
                        "key0": value0,
                        "key1": value1,
                        ...
                    },
                    "something1.nersc.gov": {
                        ...
                    },
                    ...
            },
            timestamp1: {
                ...
            },
            ...
        }

    """
    def __init__(self, *args, **kwargs):
        super(MmperfmonOutput, self).__init__(*args, **kwargs)
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

    @classmethod
    def from_str(cls, input_str):
        """Instantiate from a string
        """
        return cls(from_string=input_str)

    def load_str(self, input_str):
        """Parse the output of the subprocess output to initialize self
        """

        for line in input_str.splitlines():
            # decode the legend
            match = _REX_LEGEND.search(line)
            if match is not None:
                # extract values
                row, hostname, counter = (match.group(1), match.group(2), match.group(4))
                self.legend[int(row)] = {'host': hostname, 'counter': counter}
                # the reverse map doesn't hurt to have
                # self.legend[hostname] = {'row': int(row), 'counter': counter}
                continue

            # find the row header to determine offsets
            match = _REX_ROWHEAD.search(line)
            if match is not None:
                self.col_offsets = get_col_pos(line, align='right')

            # divide data row into fixed-width fields based on row header format
            match = _REX_ROW.search(line)
            if match is not None and self.col_offsets:
                fields = [line[istart:istop] for istart, istop in self.col_offsets]
                # TODO: convert timestamp into an epoch seconds
                timestamp = fields[0].strip()
                for index, val in enumerate(fields[1:]):
                    # TODO: convert val from str into a native data type
                    counter = self.legend[index+1]['counter']
                    hostname = self.legend[index+1]['host']
                    if timestamp not in self:
                        self[timestamp] = {}
                    to_update = self[timestamp].get(hostname, {})
                    to_update[counter] = val.strip()
                    if hostname not in self[timestamp]:
                        self[timestamp][hostname] = to_update

    def to_dataframe(self):
        """Convert to a pandas.DataFrame
        """
        pass
        # TODO

def get_col_pos(line, align=None):
    """Return column offsets of a left-aligned text table

    For example, given the string::

        Row           Timestamp cpu_user cpu_sys   mem_total
        123456789x123456789x123456789x123456789x123456789x123456789x

    would return::

        [(0, 4), (15, 24), (25, 33), (34, 41), (44, 53)]

    for ``align=None``.

    Args:
        line (str): String from which offsets should be determined
        align (str or None): Expected column alignment; one of 'left', 'right',
            or None (to return the exact start and stop of just the non-space
            text)

    Returns:
        list: List of tuples of integer offsets denoting the start index
            (inclusive) and stop index (exclusive) for each column.
    """
    col_pos = []
    last_start = None
    for index, char in enumerate(line):
        if char == ' ' and last_start is not None:
                col_pos.append((last_start, index))
                last_start = None
        elif char != ' ' and last_start is None:
            last_start = index 
    if last_start:
        col_pos.append((last_start, None))

    if len(col_pos) > 1:
        if align == 'left':
            # TODO - create test to exercise this - it probably doesn't handle the final column correctly!
            old_col_pos = col_pos
            col_pos = []
            for index, (start, stop) in enumerate(old_col_pos[:-1]):
                col_pos.append((start, old_col_pos[index+1][0]))
        elif align == 'right':
            # TODO - create test to exercise this
            old_col_pos = col_pos
            col_pos = []
            for index, (start, stop) in enumerate(old_col_pos[1:]):
                col_pos.append((old_col_pos[index][1]+1, stop))

    return col_pos

def test_get_col_pos():
    """connectors.mmperfmon.get_col_pos()
    """
    input_strs = [
        "Row           Timestamp cpu_user cpu_sys   mem_total",
        "Row   Timestamp cpu_user cpu_sys mem_total",
        "   Row   Timestamp     cpu_user cpu_sys mem_total",
        "Row   Timestamp     cpu_user cpu_sys mem_total     ",
        "  Row   Timestamp     cpu_user    cpu_sys      mem_total     ",
    ]
    for input_str in input_strs:
        print("Evaluating [%s]" % input_str)
        tokens = input_str.strip().split()
        offsets = get_col_pos(input_str)
        print("Offsets are: " + str(offsets))
        assert offsets
        istart = 0
        num_tokens = 0
        for index, (istart, istop) in enumerate(offsets):
            token = input_str[istart:istop]
            print("    [%s] vs [%s]" % (token, tokens[index]))
            assert token == tokens[index]
            istart = istop
            num_tokens += 1
        assert num_tokens == len(tokens)
