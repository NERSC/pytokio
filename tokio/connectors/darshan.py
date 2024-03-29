#!/usr/bin/env python
"""Connect to Darshan logs.

This connector provides an interface into Darshan logs created by Darshan 3.0 or
higher and represents the counters and data contained therein as a Python
dictionary.  This dictionary has the following structure, where ``block``
denote literal key names.

* ``header`` which contains key-value pairs corresponding to each line in the
  header.  ``exe`` and ``metadata`` are lists; the other keys correspond to a
  single scalar value.

  * ``compression``, ``end_time``, ``end_time_string``, ``exe``, etc

* ``counters``

  * `modulename` which is ``posix``, ``lustre``, ``stdio``, etc

    * `recordname`, which is usually the full path to a file opened by the
      profiled application _or_ ``_perf`` (contains performance summary metrics)
      or ``_total`` (contains aggregate file statistics)

      * ranknum which is a string (``0``, ``1``, etc or ``-1``)

        * `counternames`, which depends on the Darshan module defined by
          `modulename` above

* ``mounts`` which is the mount table with keys of a path to a mount location
  and values of the file system type

The `counternames` are module-specific and have their module name prefix
stripped off.  The following counter names are examples of what a Darshan log
may expose through this connector for the ``posix`` module:

* ``BYTES_READ`` and ``BYTES_WRITTEN`` - number of bytes read/written to the file
* ``MAX_BYTE_WRITTEN`` and ``MAX_BYTE_READ`` - highest byte written/read; useful if an application re-reads or re-writes a lot of data
* ``WRITES`` and ``READS`` - number of write and read ops issued
* ``F_WRITE_TIME`` and ``F_READ_TIME`` - amount of time spent inside write and read calls (in seconds)
* ``F_META_TIME`` - amount of time spent in metadata (i.e., non-read/write) calls

Similarly the ``lustre`` module provides the following counter keys:

* ``MDTS`` - number of MDTs in the underlying file system
* ``OSTS`` - number of OSTs in the underlying file system
* ``OST_ID_0`` - the OBD index for the 0th OST over which the file is striped
* ``STRIPE_OFFSET`` - the setting used to define stripe offset when the file was created
* ``STRIPE_SIZE`` - the size, in bytes, of each stripe
* ``STRIPE_WIDTH`` - how many OSTs the file touches

Note:
    This connector presently relies on ``darshan-parser`` to convert the binary
    logs to ASCII, then convert the ASCII into Python objects.  In the future,
    we plan on using the Python API provided by darshan-utils to circumvent the
    ASCII translation.
"""

import os
import re
import json
import errno
import subprocess
import warnings
from .common import SubprocessOutputDict
from ..common import isstr

DARSHAN_PARSER_BIN = 'darshan-parser'

DARSHAN_FILENAME_REX = re.compile(r'([^_%s]+)_([^%s]*?)_id(\d+)_(\d+)-(\d+)-(\d+)-(\d+)_(\d+).darshan' % (os.path.sep, os.path.sep))

class Darshan(SubprocessOutputDict):
    def __init__(self, log_file=None, *args, **kwargs):
        """Initialize the object from either a Darshan log or a cache file.

        Configures the object's internal state to operate on a Darshan
        log file or a cached JSON representation of a previously processed
        Darshan log.

        Args:
            log_file (str, optional): Path to a Darshan log to be processed
            cache_file (str, optional): Path to a Darshan log's contents cached
            *args: Passed to tokio.connectors.common.SubprocessOutputDict
            *kwargs: Passed to tokio.connectors.common.SubprocessOutputDict

        Attributes:
            log_file (str): Path to the Darshan log file to load
        """
        super(Darshan, self).__init__(*args, **kwargs)
        self.log_file = log_file
        self._parser_mode = None
        self._only_modules = None
        self._only_counters = None
        self.subprocess_cmd = [DARSHAN_PARSER_BIN]
        self.filename_metadata = {}
        if log_file is None:
            self.load()
        else:
            self.filename_metadata = parse_filename_metadata(log_file)

    def __repr__(self):
        """Serialize self into JSON.

        Returns:
            str: JSON representation of the object
        """
        return json.dumps(list(self.values()))

    def load(self):
        if self.from_string is not None:
            self.load_str(self.from_string)
        elif self.cache_file:
            self.load_cache()
        elif self.log_file is None:
            raise Exception("parameters should be provided (at least log_file or cache_file)")

    def darshan_parser_base(self, modules=None, counters=None):
        """Populate data produced by ``darshan-parser --base``

        Runs the ``darshan-parser --base`` and convert all results into
        key-value pairs which are inserted into the object.

        Args:
            modules (list of str): If specified, only return data from the given
                Darshan modules
            counters (list of str): If specified, only return data for the
                given counters

        Returns:
            dict: Dictionary containing all key-value pairs generated by running
            ``darshan-parser --base``.  These values are also accessible via the
            `BASE` key in the object.
        """
        self._parser_mode = "BASE"
        self._only_modules = set(modules) if modules else None
        self._only_counters = set(counters) if counters else None
        return self._darshan_parser()

    def darshan_parser_total(self, modules=None, counters=None):
        """Populate data produced by ``darshan-parser --total``

        Runs the ``darshan-parser --total`` and convert all results into
        key-value pairs which are inserted into the object.

        Args:
            modules (list of str): If specified, only return data from the given
                Darshan modules
            counters (list of str): If specified, only return data for the
                given counters

        Returns:
            dict: Dictionary containing all key-value pairs generated by running
            ``darshan-parser --total``.  These values are also accessible via
            the `TOTAL` key in the object.
        """
        self._parser_mode = "TOTAL"
        self._only_modules = set() if not modules else set(modules)
        self._only_counters = set() if not counters else set(counters)
        return self._darshan_parser()

    def darshan_parser_perf(self, modules=None, counters=None):
        """Populate data produced by ``darshan-parser --perf``

        Runs the ``darshan-parser --perf`` and convert all results into
        key-value pairs which are inserted into the object.

        Args:
            modules (list of str): If specified, only return data from the given
                Darshan modules
            counters (list of str): If specified, only return data for the
                given counters

        Returns:
            dict: Dictionary containing all key-value pairs generated by running
            ``darshan-parser --perf``.  These values are also accessible via the
            `PERF` key in the object.
        """
        self._parser_mode = "PERF"
        self._only_modules = set() if not modules else set(modules)
        self._only_counters = set() if not counters else set(counters)
        return self._darshan_parser()

    def _darshan_parser(self):
        """Call darshan-parser to initialize values in self
        """

        if self.log_file is None:
            return self

        if self._parser_mode in ["BASE", "TOTAL", "PERF"]:
            darshan_flag = "--" + self._parser_mode.lower()
        else:
            self._parser_mode = "BASE"
            darshan_flag = ""

        args = [darshan_flag, self.log_file]

        # this loads the entire stdout into memory at once.  is a problem for
        # Darshan logs that expand to tens of gigabytes of ascii
        #
        # self._load_subprocess(*args)

        # this loads stdout line-by-line from a pipe and is memory-efficient
        # but may suffer from weird buffering effects on some platforms
        #
        self._load_subprocess_iter(*args)

        return self

    def _load_subprocess_iter(self, *args):
        """Run a subprocess and pass its stdout to a self-initializing parser
        """

        cmd = self.subprocess_cmd
        if args:
            cmd += args

        try:
            if self.silent_errors:
                dparser = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            else:
                dparser = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        except OSError as error:
            if error.errno == errno.ENOENT:
                raise type(error)(error.errno, "%s command not found" % self.subprocess_cmd[0])
            raise

        # Python 2 - subprocess.check_output returns a string
        self.load_str(iter(dparser.stdout.readline, ''))
        dparser.stdout.close()
        dparser.wait()

        retcode = dparser.returncode
        if retcode != 0:
            warnings.warn("%s returned nonzero exit code (%d)" % (cmd, retcode))

    def load_str(self, input_str):
        """Load from either a json cache or the output of darshan-parser

        Args:
            input_str: Either (1) stdout of the darshan-parser command as a
                string, (2) the json-encoded representation of a Darshan
                object that can be deserialized to initialize self, or (3)
                an iterator that produces the output of darshan-parser
                line-by-line
        """

        if isstr(input_str):
            loaded_data = None
            try:
                loaded_data = json.loads(input_str)
            except ValueError:
                pass

            if loaded_data:
                # if we could successfully load json, store it in self
                self.__setitem__(loaded_data)
            else:
                # otherwise, treat input_str as the raw stdout of darshan-parser
                self._parse_darshan_parser(input_str.splitlines())
        else:
            # treat input_str as an iterator that will produce lines of
            # darshan-parser
            self._parse_darshan_parser(input_str)

    def _parse_darshan_parser(self, lines):
        """Load values from output of darshan-parser

        Args:
            lines: Any iterable that produces lines of darshan-parser output
        """
        def is_valid_counter(counter):
            """
            if counter is not None, this line is valid (return True)
            if counter is None but we can identify a module section, return it
            if counter is None but we cannot identify a module section, return False
            """
            if counter is None:
                match = module_rex.search(line)
                if match is not None:
                    module_section = match.group(1)
                    module_section = module_section.replace('-', '') # because of "MPI-IO" and "MPIIO"
                    module_section = module_section.replace('/', '') # because of "BG/Q" and "BGQ"
                    return False, module_section
                return False, None
            return True, None

        def insert_record(section, module, file_name, rank, counter, value, counter_prefix=None):
            """
            Embed a counter=value pair deep within the darshan_data structure based
            on a bunch of nested keys.
            """
            # Force the local shadow of 'module' to lowercase
            module = module.lower()
            # Assert that the counter actually belongs to the current module
            if counter_prefix is not None:
                if counter.startswith(counter_prefix):
                    # Strip off the counter_prefix from the counter name
                    counter = counter[len(counter_prefix):]

            if (self._only_modules and module not in self._only_modules) \
            or (self._only_counters and counter not in self._only_counters):
                return # don't insert

            # Otherwise insert the record--this logic should be made more flexible
            if section not in self:
                self[section] = {}
            if module not in self[section]:
                self[section][module] = {}
            if file_name not in self[section][module]:
                self[section][module][file_name] = {}
            if rank is None:
                insert_base = self[section][module][file_name]
            else:
                if rank not in self[section][module][file_name]:
                    self[section][module][file_name][rank] = {}
                insert_base = self[section][module][file_name][rank]

            if counter in insert_base:
                raise Exception("Duplicate counter %s found in %s->%s->%s (rank=%s)" % (counter, section, module, file_name, rank))

            if '.' in value:
                value = float(value)
            else:
                value = int(value)
            insert_base[counter] = value

        section = None
        counter = None
        counter_prefix = None
        module_section = None
        # This regex must match every possible module name
        module_rex = re.compile(r'^# ([A-Z\-0-9/]+) module data\s*$')

        for line in lines:
            if not line:
                break
            if not isstr(line):
                # Python 3 - subprocess.check_output returns encoded bytes
                line = line.decode()

            # Is this the start of a new section?
            # Why do we look at section, refactorize failed
            if section is None and line.startswith("# darshan log version:"):
                section = "header"
                if section not in list(self.keys()):
                    self[section] = {}

            elif section == "header" and line.startswith("# mounted file systems"):
                section = "mounts"
                if section not in list(self.keys()):
                    self[section] = {}

            elif section == "mounts" and line.startswith("# **********************"):  # understand the utility of these stars
                section = "counters"
                if section not in list(self.keys()):
                    self[section] = {}

            # otherwise use the appropriate parser for this section
            if section == "header":
                key, val = parse_header(line)
                if key is None:
                    pass
                elif key == "metadata":
                    if key not in self[section]:
                        self[section][key] = []
                    self[section][key].append(val)
                else:
                    self[section][key] = val
            elif section == 'mounts':
                key, val = parse_mounts(line)
                if key is not None:
                    self[section][key] = val

            elif section == 'counters':
                if self._parser_mode == "BASE":
                    # module, rank, record_id, counter, value, file_name, mount_pt, fs_type = parse_base_counters(line)
                    _, rank, _, counter, value, file_name, _, _ = parse_base_counters(line)
                    if module_section is not None:
                        # If it is none, is_valid_counter check below will bail
                        counter_prefix = module_section + "_"
                elif self._parser_mode == "TOTAL":
                    counter, value = parse_total_counters(line)
                    file_name = '_total'
                    rank = None
                    if module_section is not None:
                        counter_prefix = 'total_%s_' % module_section
                elif self._parser_mode == "PERF":
                    counter, value = parse_perf_counters(line)
                    file_name = '_perf'
                    rank = None
                    counter_prefix = None
                else:
                    continue

            # If no valid counter found, is this the start of a new module?
            valid, new_module_section = is_valid_counter(counter)
            if new_module_section is not None:
                module_section = new_module_section
            if valid is False:
                continue
            # Reminder: section is already defined as the global parser state
            insert_record(section=section,
                          module=module_section,
                          file_name=file_name,
                          rank=rank,
                          counter=counter,
                          value=value,
                          counter_prefix=counter_prefix)
        return self


def parse_header(line):
    """Parse the header lines of ``darshan-parser``.

    Accepts a line that may or may not be a header line as printed by
    ``darshan-parser``.  Such header lines take the form::

    # darshan log version: 3.10
    # compression method: ZLIB
    # exe: /home/user/bin/myjob.exe --whatever
    # uid: 69615

    If it is a valid header line, return a key-value pair corresponding to
    its decoded contents.

    Args:
        line (str): A single line of output from ``darshan-parser``

    Returns:
        tuple: Returns a (key, value) corresponding to the key and value
        decoded from the header `line`, or ``(None, None)`` if the line does
        not appear to contain a known header field.
        """

    if line.startswith("# darshan log version:"):
        return 'version', line.split()[-1]
    elif line.startswith("# compression method:"):
        return "compression", line.split()[-1]
    elif line.startswith("# exe:"):
        return "exe", line.split()[2:]
    elif line.startswith("# uid:"):
        return "uid", int(line.split()[-1])
    elif line.startswith("# jobid:"):
        return 'jobid', line.split()[-1]
    elif line.startswith("# start_time:"):
        return 'start_time', int(line.split()[2])
    elif line.startswith("# start_time_asci:"):
        return 'start_time_string', line.split(None, 2)[-1].strip()
    elif line.startswith("# end_time:"):
        return 'end_time', int(line.split()[2])
    elif line.startswith("# end_time_asci:"):
        return 'end_time_string', line.split(None, 2)[-1].strip()
    elif line.startswith("# nprocs:"):
        return "nprocs", int(line.split()[-1])
    elif line.startswith("# run time:"):
        return "walltime", int(line.split()[-1])
    elif line.startswith("# metadata:"):
        return "metadata", line.split(None, 2)[-1].strip()
    return None, None


def parse_mounts(line):
    """Parse a mount table line from ``darshan-parser``.

    Accepts a line that may or may not be a mount table entry from
    ``darshan-parser``.  Such lines take the form::

    # mount entry:  /usr/lib64/libibverbs.so.1.0.0  dvs

    If `line` is a valid mount table entry, return a key-value
    representation of its contents.

    Args:
        line (str): A single line of output from ``darshan-parser``

    Returns:
        tuple: Returns a (key, value) corresponding to the mount table
        entry, or ``(None, None)`` if the line is not a valid mount
        table entry.
    """
    if line.startswith("# mount entry:"):
        key, val = line.split('\t')[1:3]
        return key, val.strip()
    return None, None



def parse_base_counters(line):
    """Parse a counter line from ``darshan-parser --base``.

    Parse the line containing an actual counter's data.  It is a tab-delimited
    line of the form

    module, rank, record_id, counter, value, file_name, mount_pt, fs_type

    Args:
        line (str): A single line of output from ``darshan-parser --base``

    Returns:
        tuple: Returns a tuple containing eight values.  If `line` is not a
        valid counter line, all values will be None.  The returned values are::

        0. module name
        1. MPI rank
        2. record id
        3. counter name
        4. counter value
        5. file name
        6. mount point
        7. file system type

    """
    if not line.startswith("#"):
        args = line.split('\t')
        if len(args) == 8:
            return tuple(args)
    return None, None, None, None, None, None, None, None

def parse_total_counters(line):
    """Parse a counter line from ``darshan-parser --total``.

    Parse a line containing counter data from ``darshan-parser --total``.
    Such lines are of the form:

        total_MPIIO_F_READ_END_TIMESTAMP: 0.000000

    Args:
        line (str): A single line of output from ``darshan-parser --total``

    Returns:
        tuple: Returns a single (key, value) pair corresponding to a
        counted metric and its total value.  If `line` is not a valid
        counter line, ``(None, None)`` are returned.
    """
    if not line.startswith("#"):
        args = line.split(':')
        if len(args) == 2:
            return args[0].strip(), args[1].strip()
    return None, None

def parse_perf_counters(line):
    """Parse a counter line from ``darshan-parser --perf``.

    Parse a line containing counter data from ``darshan-parser --perf``.
    Such lines look like::

        # total_bytes: 2199023259968
        # unique files: slowest_rank_io_time: 0.000000
        # shared files: time_by_cumul_io_only: 39.992327
        # agg_perf_by_slowest: 28670.996545

    Args:
        line (str): A single line of output from ``darshan-parser --perf``

    Returns:
        tuple: Returns a single (key, value) pair corresponding to the
        performance metric encoded in `line`.  If `line` is not a valid
        performance counter line, ``(None, None)`` is returned.
    """
    if line.startswith('# total_bytes:') or line.startswith('# agg_perf_by'):
        key, value = line[2:].split(':')
    elif line.startswith('# unique files:') or line.startswith('# shared files:'):
        key_suffix, key, value = line[2:].split(':')
        key += '_%s' % key_suffix.replace(' ', '_')
    else:
        return None, None

    return key.strip(), value.strip()

def parse_filename_metadata(filename):
    """Extracts metadata from a Darshan log's file name

    Args:
        filename (str): Name of a Darshan log file.  Can be basename or a full
            path.
    Returns:
        dict: key-value pairs describing the metadata extracted from the file
            name.
    """
    filename_metadata = {}
    # try to infer metadata from file name
    regex_match = DARSHAN_FILENAME_REX.search(filename)
    if regex_match:
        filename_metadata['username'] = regex_match.group(1)
        filename_metadata['exename'] = regex_match.group(2)
        filename_metadata['jobid'] = regex_match.group(3)
        filename_metadata['start_month'] = int(regex_match.group(4))
        filename_metadata['start_day'] = int(regex_match.group(5))
        filename_metadata['start_second_in_day'] = int(regex_match.group(6))
        filename_metadata['logmod'] = int(regex_match.group(7))
        filename_metadata['shutdown_secs'] = int(regex_match.group(8))

    return filename_metadata
