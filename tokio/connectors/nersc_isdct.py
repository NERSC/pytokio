#!/usr/bin/env python
"""Connect to NERSC's Intel Data Center Tool for SSDs outputs

Processes and aggregates the output of Intel Data Center Tool for SSDs outputs
in the format generated by NERSC's daily script.  The NERSC infrastructure runs
ISDCT in a verbose way on every burst buffer node, then collects all the
resulting output text files from a node into a directory bearing that node's nid
(e.g., ``nid01234/*.txt``).  There is also an optional timestamp file contained
in the toplevel directory.  Also processes .tar.gz versions of these collected
metrics.
"""

import os
import re
import tarfile
import mimetypes
import time
import datetime
import warnings
import numpy
import pandas
from tokio.common import isstr
from . import common

REX_SERIAL_NO = r'(Intel SSD|SMART Attributes|SMART and Health Information).*(CVF[^ ]+-\d+)'

class NerscIsdct(common.CacheableDict):
    """Dictionary subclass that self-populates with ISDCT output data
    """
    def __init__(self, input_file):
        """Load the output of a NERSC ISDCT dump.

        Args:
            input_file (str): Path to either a directory or a tar(/gzipped)
                directory containing the output of NERSC's ISDCT collection
                script.
        """
        super(NerscIsdct, self).__init__(self, input_file=input_file)

        # synthesize metrics independent of the input format
        self._synthesize_metrics()

    def load(self):
        """Wrapper around the filetype-specific loader.

        Infer the type of input being given, dispatch the correct loading
        function, and populate keys/values.
        """
        if not os.path.exists(self.input_file):
            raise Exception("Input file %s does not exist" % self.input_file)

        try:
            # try load_native first, because load_native understands that
            # self.input_file may actually be a directory
            self.load_native()
        except tarfile.ReadError:
            try:
                self.load_json()
            except ValueError as error:
                raise ValueError(str(error) + " (is this a valid json file?)")

    def load_native(self):
        """Load ISDCT output from a tar(.gz).

        Load a collection of ISDCT outputs as created by the NERSC ISDCT script.
        Assume that ISDCT output files each contain a single output from a
        single invocation of the isdct tool, and outputs are grouped into
        directories named according to their nid numbers (e.g.,
        nid00984/somefile.txt).
        """
        timestamp_str = None
        min_mtime = None
        parsed_counters_list = []
        for (member_name, mtime, member_handle) in walk_file_collection(self.input_file):
            # is this a magic timestamp file?
            if 'timestamp_' in member_name:
                timestamp_str = member_name.split('_')[-1]
                continue

            # independently, track the earliest mtime we can find.  In the
            # future, we may want to set an mtime for each individual file_obj,
            # but for now we treat the whole dump as a single timestamped entity
            if min_mtime is None:
                min_mtime = mtime
            else:
                min_mtime = min(min_mtime, mtime)

            # infer the node name from the member's path
            nodename = _decode_nersc_nid(member_name)

            # process the member's contents, then add it to self
            try:
                parsed_counters = parse_counters_fileobj(
                    member_handle,
                    nodename=nodename)
            except:
                warnings.warn("Parsing error in %s" % member_name)
                raise
            if parsed_counters is not None:
                parsed_counters_list.append(parsed_counters)

        # If no timestamp file was found, use the earliest mtime as a guess
        if timestamp_str is None:
            timestamp = datetime.datetime.fromtimestamp(min_mtime)
        # Otherwise, the timestamp file is the ground truth
        else:
            timestamp = datetime.datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")

        # Set the timestamp for each device serial number
        timestamp = int(time.mktime(timestamp.timetuple()))
        for parsed_counters in parsed_counters_list:
            if len(parsed_counters) > 1:
                warnings.warn("Multiple serial numbers detected in a single file_obj: " \
                              + str(list(parsed_counters.keys())))
            for counters in parsed_counters.values():
                counters['timestamp'] = timestamp

        merged_counters = _merge_parsed_counters(parsed_counters_list)
        for key, val in merged_counters.items():
            self.__setitem__(key, val)

    def to_dataframe(self, only_numeric=False):
        """Express self as a dataframe.

        Args:
            only_numeric (bool): Only output columns containing numeric data of
                True; otherwise, output all columns.

        Returns:
            pandas.DataFrame: Dataframe indexed by serial number and with ISDCT
            counters as columns
        """
        dataframe = pandas.DataFrame.from_dict(self, orient='index')
        dataframe.index.name = "serial_number"
        if only_numeric:
            numeric_cols = []
            for column in sorted(dataframe.keys()):
                ### don't print counters that are non-numeric
                if dataframe[column].dtype != numpy.int64 \
                and dataframe[column].dtype != numpy.float64:
                    continue

                ### don't print counters that are the same for all devices. dataframe.count > 1
                ### because we can't tell how many rows are unique if there's only one
                ### row
                if dataframe.shape[0] > 1 and len(dataframe[column].unique()) == 1:
                    continue

                numeric_cols.append(column)

            if 'node_name' in dataframe:
                numeric_cols = ['node_name'] + numeric_cols
            return dataframe[numeric_cols]
        else:
            return dataframe

    def diff(self, old_isdct, report_zeros=True):
        """Highlight differences between self and another NerscIsdct.

        Subtract each counter for each serial number in this object from its
        counterpart in ``old_isdct``.  Return the changes in each numeric counter
        and any serial numbers that have appeared or disappeared.

        Args:
            old_isdct (NerscIsdct): object with which we should be compared
            report_zeros (bool): If True, report all counters even if they
                showed no change.  Default is True.

        Returns:
            dict: Dictionary containing the following keys:
                * `added_devices` - device serial numbers which exist in self
                  but not old_isdct
                * `removed_devices` - device serial numbers which do not exist
                  in self but do in old_isdct
                * `devices` - dict keyed by device serial numbers and whose
                  values are dicts of keys whose values are the difference
                  between old_isdct and self
        """
        result = {
            'added_devices': [],
            'removed_devices': [],
            'devices': {},
        }
        existing_devices = set([])
        for serial_no, counters in self.items():
            existing_devices.add(serial_no)
            # new devices that are appearing for the first time
            if serial_no not in old_isdct:
                result['added_devices'].append(serial_no)
                continue

            # calculate the diff in each counter for this device
            diff_dict = {}
            for counter, value in counters.items():
                if counter not in old_isdct[serial_no]:
                    warnings.warn("Counter %s does not exist in old_isdct" % counter)

                # just highlight different strings, but ignore
                # endurance_analyzer (which can be numeric or string-like)
                elif isstr(value) and counter != "endurance_analyzer":
                    if old_isdct[serial_no][counter] != value:
                        diff_value = "+++%s ---%s" % (value, old_isdct[serial_no][counter])
                    else:
                        diff_value = ""
                    if report_zeros or diff_value != "":
                        diff_dict[counter] = diff_value

                # subtract numeric counters
                else:
                    try:
                        diff_value = value - old_isdct[serial_no][counter]
                    except TypeError:
                        ### endurance_analyzer can be either numeric (common
                        ### case) or an error string (if the drive is brand
                        ### new); just drop the counter in the non-numeric
                        ### case
                        if counter == 'endurance_analyzer':
                            continue
                        error = "incompatible numeric types for %s/%s: [%s] vs [%s]" % (
                            serial_no,
                            counter,
                            old_isdct[serial_no][counter],
                            value)
                        raise TypeError(error)
                    if report_zeros or diff_value != 0:
                        diff_dict[counter] = diff_value

            result['devices'][serial_no] = diff_dict

        # Look for serial numbers that used to exist but do not appear in self
        for serial_no in old_isdct:
            if serial_no not in existing_devices:
                result['removed_devices'].append(serial_no)

        return result

    def _synthesize_metrics(self):
        """Calculate additional metrics not presented by ISDCT.

        Calculates additional convenient metrics that are not directly presented
        by ISDCT, then adds the resulting key-value pairs to self.
        """

        for serial_no, counters in self.items():
            # native units are "kiloblocks," whatever that signifies
            for key in 'data_units_read', 'data_units_written':
                if key in counters:
                    self[serial_no][key + "_bytes"] = counters[key] * 1000 * 512

            # native units are in 32 MiB
            for key in 'smart_nand_bytes_written_raw', 'smart_host_bytes_written_raw':
                if key in counters:
                    self[serial_no][key.replace('_raw', '_bytes')] = \
                        counters[key] * 32 * 1024 * 1024

            # calculate WAF
            nand_writes = counters.get('smart_nand_bytes_written_raw')
            host_writes = counters.get('smart_host_bytes_written_raw')
            if nand_writes is not None and host_writes is not None and host_writes > 0:
                self[serial_no]['write_amplification_factor'] = \
                    float(nand_writes) / float(host_writes)

def _merge_parsed_counters(parsed_counters_list):
    """Merge ISDCT outputs into a single object.

    Aggregates counters from each record based on the NVMe device serial number,
    with redundant counters being overwritten.

    Args:
        parsed_counters_list (list): List of parsed ISDCT outputs as dicts.
            Each list element is a dict with a single key (a device serial
            number) and one or more values; each value is itself a dict of
            key-value pairs corresponding to ISDCT/SMART counters from that
            device.

    Returns:
        dict: Dict with keys given by all device serial numbers found in
            `parsed_counters_list` and whose values are a dict containing keys
            and values representing all unique keys across all elements of
            `parsed_counters_list`.
    """
    all_data = {}
    for parsed_counters in parsed_counters_list:
        if parsed_counters is None:
            continue
        elif len(parsed_counters) > 1:
            raise Exception("Received multiple serial numbers from parse_dct_counters_file")
        else:
            device_sn = next(iter(parsed_counters))

        ### merge file's counter dict with any previous counters we've parsed
        if device_sn not in all_data:
            all_data[device_sn] = parsed_counters[device_sn]
        else:
            all_data[device_sn].update(parsed_counters[device_sn])

    ### attempt to figure out the type of each counter
    new_counters = []
    for device_sn, counters in all_data.items():
        for counter, value in counters.items():
            ### first, handle counters that do not have an obvious way to cast
            if counter in ("temperature", "throttle_status", "endurance_analyzer"):
                tokens = value.split(None, 1)
                if len(tokens) == 2:
                    new_value, unit = tokens
                else:
                    new_value, unit = tokens[0], None
            else:
                new_value = value
                unit = None

            ### the order here is important, but hex that is not prefixed with
            ### 0x may be misinterpreted as integers.  if such counters ever
            ### surface, they must be explicitly cast above
            for cast in (int, float, lambda x: int(x, 16)):
                try:
                    new_value = cast(value)
                    break
                except ValueError:
                    pass
            if value == "True":
                new_value = True
            elif value == "False":
                new_value = False

            ### endurance_analyzer can be numeric or an error string
            if counter == 'endurance_analyzer':
                if isstr(new_value):
                    new_value = None
                    unit = None
                elif unit is None:
                    # Intel reports this counter multiple times using different print formats
                    unit = 'years'

            ### Insert the key-value pair and its unit of measurement (if available)
            all_data[device_sn][counter] = new_value
            if unit is not None:
                new_counters.append((device_sn, '%s_unit' % counter, unit))

    for (device_sn, counter, value) in new_counters:
        all_data[device_sn][counter] = value

    return all_data

def _decode_nersc_nid(path):
    """Convert path to ISDCT output into a nid.

    Given a path to some ISDCT output file, somehow figure out what the nid
    name for that node is.  This encoding is specific to the way NERSC collects
    and preserves ISDCT outputs.

    Args:
        path (str): path to an ISDCT output text file

    Returns:
        str: Node identifier (e.g., nid01234)
    """
    abs_path = os.path.abspath(path)
    nid_name = os.path.dirname(abs_path).split(os.sep)[-1]
    return nid_name

def _rekey_smart_buffer(smart_buffer):
    """Convert SMART values associated with one register into unique counters.

    Take a buffer containing smart values associated with one register and
    create unique counters.  Only necessary for older versions of ISDCT
    which did not output SMART registers in a standard "Key: value" text
    format.

    Args:
        smart_buffer (dict): SMART buffer as defined by parse_counters_fileobj()

    Returns:
        dict: unique key:value pairs whose key now includes distinguishing
            device-specific characteristics to avoid collision from other
            devices that generated SMART data
    """
    data = {}
    prefix = smart_buffer.get("description")
    if prefix is None:
        prefix = smart_buffer.get("_id")

    for key, val in smart_buffer.items():
        key = ("SMART_%s_%s" % (prefix, key.strip())).replace("-", "").replace(" ", "_")
        key = _normalize_key(key)
        data[key] = val.strip()
    return data

def _normalize_key(key):
    """Coerce all keys into a similar naming convention.

    Converts Intel's mix of camel-case and snake-case counters into all
    snake-case.  Contains some nasty acronym hacks that may require modification
    if/when Intel adds new funny acronyms that contain a mix of upper and lower
    case letters (e.g., SMBus and NVMe).

    Args:
        key (str): a string to normalize

    Returns:
        str: snake_case version of key
    """
    key = key.strip().replace(' ', '_').replace('-', '')
    key = key.replace("NVMe", "nvme").replace("SMBus", "smbus")
    last_letter = None
    new_key = ""
    letter_list = list(key)
    for index, letter in enumerate(letter_list):

        if index+1 < len(letter_list):
            next_letter = letter_list[index+1]
        else:
            next_letter = None

        # don't allow repeated underscores
        if letter == "_" and last_letter == "_":
            continue

        if last_letter is not None and last_letter != "_":
            # start of a camelcase word?
            if letter.isupper():
                # don't stick underscores in acronyms
                if not last_letter.isupper() and last_letter != "_":
                    new_key += "_"
                # unless this is the end of an acronym
                elif next_letter is not None and not next_letter.isupper() and next_letter != "_":
                    new_key += "_"
        new_key += letter
        last_letter = letter

    return new_key.lower()

def parse_counters_fileobj(fileobj, nodename=None):
    """Convert any output of ISDCT into key-value pairs.

    Reads the output of a file-like object which contains the output of a
    single isdct command.  Understands the output of the following options:

      * ``isdct show -smart`` (SMART attributes)
      * ``isdct show -sensor`` (device health sensors)
      * ``isdct show -performance`` (device performance metrics)
      * ``isdct show -a`` (drive info)

    Args:
        fileobj (file): file-like object containing the output of an ISDCT
            command
        nodename (str): name of node corresponding to `fileobj`, if known

    Returns:
        dict: dict of dicts keyed by the device serial number.
    """

    data = {}
    device_sn = None
    parse_mode = 0      # =0 for regular counters, 1 for SMART data
    smart_buffer = {}
    for line in fileobj.readlines():
        if not isstr(line):
            line = line.decode().strip()
        else:
            line = line.strip()

        if device_sn is None:
            rex_match = re.search(REX_SERIAL_NO, line)
            if rex_match is not None:
                device_sn = rex_match.group(2)
                if nodename is not None:
                    data['node_name'] = nodename
                if rex_match.group(1) == "Intel SSD" \
                or rex_match.group(1) == "SMART and Health Information":
                    parse_mode = 0
                elif rex_match.group(1) == "SMART Attributes":
                    parse_mode = 1
                else:
                    raise Exception("Unknown counter file format")
        elif parse_mode == 0 and ':' in line:
            key, val = line.split(':', 1)
            key = _normalize_key(key)
            data[key] = val.strip()
        elif parse_mode > 0 and ':' in line:
            key, val = line.split(':', 1)
            key = _normalize_key(key)
            smart_buffer[key] = val.strip()
        elif parse_mode > 0 and line.startswith('-') and line.endswith('-'):
            for key, val in _rekey_smart_buffer(smart_buffer).items():
                key = _normalize_key(key)
                data[key] = val
            smart_buffer = {'_id' : line.split()[1]}
    if parse_mode > 0: # flush the last SMART register
        for key, val in _rekey_smart_buffer(smart_buffer).items():
            key = _normalize_key(key)
            data[key] = val

    if device_sn is None:
        warnings.warn("Could not find device serial number in %s" % fileobj.name)
        return {}

    return {device_sn : data}

def walk_file_collection(input_source):
    """Walk all member files of an input source.

    Iterator that visits every member of an input source (either directory or
    tarfile) and yields its file name, last modify time, and a file handle to
    its contents.

    Args:
        input_source (str): A path to either a directory containing files or a
            tarfile containing files.

    Yields:
        tuple: Attributes for a member of `input_source` with the following
        data:

        * str: fully qualified path corresponding to its name
        * float: last modification time expressed as seconds since epoch
        * file: handle to access the member's contents
    """

    if os.path.isdir(input_source):
        for root, _, files in os.walk(input_source):
            for file_name in files:
                fq_file_name = os.path.join(root, file_name)
                yield (fq_file_name,
                       os.path.getmtime(fq_file_name),
                       open(fq_file_name, 'r'))
    else:
        _, encoding = mimetypes.guess_type(input_source)
        if encoding == 'gzip':
            file_obj = tarfile.open(input_source, 'r:gz')
        else:
            file_obj = tarfile.open(input_source, 'r')
        for member in file_obj.getmembers():
            if member.isfile():
                yield (member.name,
                       member.mtime,
                       file_obj.extractfile(member))
