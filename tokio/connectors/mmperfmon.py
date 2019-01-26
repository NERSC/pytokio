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

import os
import re
import tarfile
import datetime
import warnings

import pandas

from .common import SubprocessOutputDict, walk_file_collection
from ..common import to_epoch

_REX_LEGEND = re.compile(r'^\s*(\d+):\s+([^|]+)\|([^|]+)\|(\S+)\s*$')
_REX_ROWHEAD = re.compile(r'^\s*Row\s+Timestamp')
_REX_ROW = re.compile(r'^\s*\d+\s+(\d{4}-\d\d-\d\d-\d\d:\d\d:\d\d)\s+')

MMPERFMON_DATE_FMT = "%Y-%m-%d-%H:%M:%S"
MMPERFMON_UNITS_TO_BYTES = {
    "MB": 1048576,
    "kB": 1024,
}

class Mmperfmon(SubprocessOutputDict):
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
        super(Mmperfmon, self).__init__(*args, **kwargs)
        self.subprocess_cmd = None
        self.legend = {}
        self.col_offsets = []
        self.load()

    @classmethod
    def from_str(cls, input_str):
        """Instantiate from a string
        """
        return cls(from_string=input_str)

    @classmethod
    def from_file(cls, cache_file):
        """Instantiate from a cache file
        """
        return cls(cache_file=cache_file)

    def load(self):
        """Load either a tarfile, directory, or single mmperfmon output file

        Tries to load self.cache_file; if it is a directory or tarfile, it is
        handled by self.load_multiple; otherwise falls through to the load_str
        code path.
        """
        try:
            self.load_multiple(input_file=self.cache_file)
        except tarfile.ReadError:
            super(Mmperfmon, self).load()

    def load_multiple(self, input_file):
        """Load one or more input files from a directory or tarball

        Args:
            input_file (str): Path to either a directory or a tarfile containing
                multiple text files, each of which contains the output of a
                single mmperfmon invocation.
        """
        for (member_name, _, member_handle) in walk_file_collection(input_file):
            try:
                self.load_str(input_str=member_handle.read())
            except:
                warnings.warn("Parsing error in %s" % member_name)
                raise


    def load_str(self, input_str):
        """Parse the output of the subprocess output to initialize self
        """
        for line in input_str.splitlines():
            # decode the legend
            if not isinstance(line, str):
                line = line.decode()
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

    def to_dataframe_by_host(self, host):
        """Returns data from a specific host as a DataFrame

        Args:
            host (str): Hostname from which a DataFrame should be constructed

        Returns:
            pandas.DataFrame: All measurements from the given host.  Columns
                correspond to different metrics; indexed in time.
        """
        to_df = {}
        for timestamp, hosts in self.items():
            metrics = hosts.get(host)
            if metrics is not None:
                timestamp_o = datetime.datetime.strptime(timestamp, MMPERFMON_DATE_FMT)
                to_df[timestamp_o] = {}
                for key, value in metrics.items():
                    new_value = value_unit_to_bytes(value)
                    new_key = key + "_bytes" if new_value != value else key
                    to_df[timestamp_o][new_key] = new_value

        return pandas.DataFrame.from_dict(to_df, orient='index')
                

    def to_dataframe_by_metric(self, metric):
        """Returns data for a specific metric as a DataFrame

        Args:
            metric (str): Metric from which a DataFrame should be constructed

        Returns:
            pandas.DataFrame: All measurements of the given metric for all
                hosts.  Columns represent hosts; indexed in time.
        """
        to_df = {}
        for timestamp, hosts in self.items():
            for hostname, counters in hosts.items():
                value = counters.get(metric)
                if value is not None:
                    timestamp_o = datetime.datetime.strptime(timestamp, MMPERFMON_DATE_FMT)
                    if timestamp_o not in to_df:
                        to_df[timestamp_o] = {}
                    new_value = value_unit_to_bytes(value)
                    to_df[timestamp_o][hostname] = new_value

        return pandas.DataFrame.from_dict(to_df, orient='index')


    def to_dataframe(self, by_host=None, by_metric=None):
        """Convert to a pandas.DataFrame
        """
        if (by_host is None and by_metric is None) \
        or (by_host is not None and by_metric is not None):
            raise RuntimeError("must specify either by_host or by_metric")
        elif by_host is not None:
            return self.to_dataframe_by_host(host=by_host)

        return self.to_dataframe_by_metric(metric=by_metric)
            

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

def value_unit_to_bytes(value_unit):
    """Converts a value+unit string into bytes

    Converts a string containing both a numerical value and a unit of that value
    into a normalized value.  For example, "1 MB" will convert to 1048576.

    Args:
        value_unit (str): Of the format "float str" where float is the value and
            str is the unit by which value is expressed.

    Returns:
        int: Number of bytes represented by value_unit
    """
    try:
        val, unit = value_unit.split()
    except ValueError:
        return value_unit

    val = float(val)
    multiple = MMPERFMON_UNITS_TO_BYTES.get(unit)
    if multiple is not None:
        return val * multiple
    return value_unit

