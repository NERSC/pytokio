#!/usr/bin/env python

import os
import re
import sys
import json
import gzip
import pandas
import tarfile
import itertools
import mimetypes
import time
import datetime
import warnings

class NerscIsdct(dict):
    def __init__(self, input_file):
        super(NerscIsdct,self).__init__(self)
        self.input_file = input_file
        self.load()
        # synthesize metrics independent of the input format
        self._synthesize_metrics()

    def __repr__(self):
        return json.dumps(self)

    def load(self):
        """
        Infer the type of input we're receiving, dispatch the correct loading
        function, and populate keys/values
        """
        if not os.path.exists(self.input_file):
            raise Exception("Input file %s does not exist" % self.input_file)
        if os.path.isdir(self.input_file):
            self.load_directory()
        else:
            # encoding can be 'gzip' or None
            mime_type, encoding = mimetypes.guess_type(self.input_file)

            if mime_type == 'application/x-tar':
                self.load_tarfile(encoding)
            else:
                try:
                    self.load_json(encoding)
                except ValueError as error:
                    raise ValueError(str(error) + " (is this a valid json file?)")

    def load_tarfile(self, encoding):
        """
        Load a tarred (and optionally compressed) collection of ISDCT outputs.
        Assume that ISDCT output files each contain a single output from a
        single invocation of the isdct tool, and outputs are grouped into
        directories named according to their nid numbers (e.g.,
        nid00984/somefile.txt).

        TODO: this needs to be refactored with load_directory
        """
        parsed_counters_list = []
        if encoding == 'gzip':
            tar = tarfile.open(self.input_file, 'r:gz')
        else:
            tar = tarfile.open(self.input_file, 'r')

        timestamp_str = None
        min_mtime = None
        for member in tar.getmembers():
            # we only care about file members, not directories
            if not member.isfile():
                continue

            fq_file_name = member.name
            # is this a magic timestamp file?
            if 'timestamp_' in fq_file_name:
                timestamp_str = fq_file_name.split('_')[-1]
                continue

            # independently, track the earliest mtime we can find.  In the
            # future, we may want to set an mtime for each individual file_obj,
            # but for now we treat the whole dump as a single timestamped entity
            if min_mtime is None:
                min_mtime = member.mtime
            else:
                min_mtime = min(min_mtime, member.mtime)

            # extract the file member into a memory buffer
            file_obj = tar.extractfile(member)

            # infer the node name from the member's path
            nodename = _decode_nersc_nid(fq_file_name)

            # process the member's contents, then add it to self
            try:
                parsed_counters = self.parse_counters_fileobj(file_obj, nodename)
            except:
                warnings.warn("Parsing error in %s" % fq_file_name)
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
        timestamp = long(time.mktime(timestamp.timetuple()))
        for parsed_counters in parsed_counters_list:
            if len(parsed_counters.keys()) > 1:
                warnings.warn("Multiple serial numbers detected in a single file_obj: " + str(parsed_counters.keys()))
            for serialno, counters in parsed_counters.iteritems():
                counters['timestamp'] = timestamp

        merged_counters = self._merge_parsed_counters(parsed_counters_list)
        for key, val in merged_counters.iteritems():
            self.__setitem__(key, val)

    def load_directory(self):
        """
        Functionally identical to load_tarfile(), but operates on the untarred
        contents of the tarfile.
        """
        timestamp_str = None
        min_mtime = None
        parsed_counters_list = []
        for root, dirs, files in os.walk(self.input_file):
            for file_name in files:
                fq_file_name = os.path.join(root, file_name)
                # is this a magic timestamp file?
                if 'timestamp_' in fq_file_name:
                    timestamp_str = fq_file_name.split('_')[-1]
                    continue


                # independently, track the earliest mtime we can find.  In the
                # future, we may want to set an mtime for each individual file_obj,
                # but for now we treat the whole dump as a single timestamped entity
                if min_mtime is None:
                    min_mtime = os.path.getmtime(fq_file_name)
                else:
                    min_mtime = min(min_mtime, os.path.getmtime(fq_file_name))

                nodename = _decode_nersc_nid(fq_file_name)
                try:
                    parsed_counters = self.parse_counters_fileobj(
                        open(fq_file_name, 'r'),
                        nodename=nodename)
                except:
                    warnings.warn("Parsing error in %s" % fq_file_name)
                    raise
                parsed_counters_list.append(parsed_counters)

        # If no timestamp file was found, use the earliest mtime as a guess
        if timestamp_str is None:
            timestamp = datetime.datetime.fromtimestamp(min_mtime)
        # Otherwise, the timestamp file is the ground truth
        else:
            timestamp = datetime.datetime.strptime(timestamp_str, "%Y%m%d%H%M%S")

        # Set the timestamp for each device serial number
        timestamp = long(time.mktime(timestamp.timetuple()))
        for parsed_counters in parsed_counters_list:
            if len(parsed_counters.keys()) > 1:
                warnings.warn("Multiple serial numbers detected in a single file_obj: " + str(parsed_counters.keys()))
            for serialno, counters in parsed_counters.iteritems():
                counters['timestamp'] = timestamp

        merged_counters = self._merge_parsed_counters(parsed_counters_list)
        for key, val in merged_counters.iteritems():
            self.__setitem__(key, val)

    def load_json(self, encoding):
        """
        Load the serialized format of this object, encoded as a json dictionary
        where counters are keyed by the device serial number.  The converse of
        the save_cache() method below.
        """
        if encoding == 'gzip':
            open_func = gzip.open
        else:
            open_func = open

        for key, val in json.load(open_func(self.input_file, 'r')).iteritems():
            self.__setitem__(key, val)
 
    def save_cache(self, output_file=None):
        """
        Save the dictionary in a json file.  This output can be read back in using
        load_json() above.
        """
        if output_file is None:
            self._save_cache(sys.stdout)
        else:
            with open(output_file, 'w') as fp:
                self._save_cache(fp)

    def _save_cache(self, output):
        output.write(str(self))

    def to_dataframe(self, only_numeric=False):
        """
        Express the object as a dataframe
        """
        df = pandas.DataFrame.from_dict(self, orient='index')
        df.index.name = "serial_number"
        if only_numeric:
            numeric_df = df.apply(pandas.to_numeric, errors='coerce')
            numeric_keys = []
            for i in sorted(df.keys()):
                ### don't print counters that are non-numeric
                if df[i].dtype != np.int64 and df[i].dtype != np.float64:
                    continue

                ### don't print counters that are the same for all devices. df.count > 1
                ### because we can't tell how many rows are unique if there's only one
                ### row
                if df.shape[0] > 1 and len(df[i].unique()) == 1:
                    continue

                numeric_keys.append( i )

            if 'node_name' in df:
                numeric_keys = [ 'node_name' ] + numeric_keys
            return df[numeric_keys]
        else:
            return df

    def parse_counters_fileobj(self, fileobj, nodename=None):
        """
        Read the output of a file-like object which contains the output of a
        single isdct command.  Understands the output of the following options:
          isdct show -smart (SMART attributes)
          isdct show -sensor (device health sensors)
          isdct show -performance (device performance metrics)
          isdct show -a (drive info)
        Outputs a dict of dicts keyed by the device serial number.
        """
    
        data = {}
        device_sn = None
        parse_mode = 0      # =0 for regular counters, 1 for SMART data
        smart_buffer = {}
        for line in fileobj.readlines():
            line = line.strip()
            if device_sn is None:
                rex_match = re.search( '(Intel SSD|SMART Attributes|SMART and Health Information).*(CVF[^ ]+-\d+)', line )
                if rex_match is not None:
                    device_sn = rex_match.group(2)

                    if nodename is not None:
                        data['node_name'] = nodename
                    if rex_match.group(1) == "Intel SSD" or rex_match.group(1) == "SMART and Health Information":
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
                for key, val in _rekey_smart_buffer(smart_buffer).iteritems():
                    key = _normalize_key(key)
                    data[key] = val
                smart_buffer = { '_id' : line.split()[1] }
        if parse_mode > 0: # flush the last SMART register
            for key, val in _rekey_smart_buffer(smart_buffer).iteritems():
                key = _normalize_key(key)
                data[key] = val

        if device_sn is None:
            warnings.warn("Could not find device serial number in %s" % fileobj.name)
        else:
            return { device_sn : data }

    def diff(self, old_isdct, report_zeros=True):
        """
        Subtract each counter for each serial number in this object from its
        counterpart in old_isdct.  Return the changes in each numeric counter
        and any serial numbers that have appeared or disappeared.
        """
        result = {
            'added_devices': [],
            'removed_devices': [],
            'devices': {},
        }
        existing_devices = set([])
        for serial_no, counters in self.iteritems():
            existing_devices.add(serial_no)
            # new devices that are appearing for the first time
            if serial_no not in old_isdct:
                result['added_devices'].append(serial_no)
                continue

            # calculate the diff in each counter for this device
            diff_dict = {}
            for counter, value in counters.iteritems():
                if counter not in old_isdct[serial_no]:
                    warnings.warn("Counter %s does not exist in old_isdct")

                ### just highlight different strings
                elif isinstance(value, basestring):
                    if old_isdct[serial_no][counter] != value:
                        diff_value = "+++%s ---%s" % (value, old_isdct[serial_no][counter])
                    else:
                        diff_value = ""
                    if report_zeros or diff_value != "":
                        diff_dict[counter] = diff_value

                ### subtract numeric counters
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
        for serial_no in old_isdct.keys():
            if serial_no not in existing_devices:
                result['removed_devices'].append(serial_no)

        return result

    def _synthesize_metrics(self):
        """
        Calculate some additional convenient metrics that are not directly
        presented by ISDCT
        """

        for serial_no, counters in self.iteritems():
            # native units are "kiloblocks," whatever that signifies
            for key in 'data_units_read', 'data_units_written':
                if key in counters:
                    self[serial_no][key + "_bytes"] = counters[key] * 1000 * 512

            # native units are in 32 MiB
            for key in 'smart_nand_bytes_written_raw', 'smart_host_bytes_written_raw':
                if key in counters:
                    self[serial_no][key.replace('_raw', '_bytes')] = counters[key] * 32 * 1024 * 1024

            # calculate WAF
            nand_writes = counters.get('smart_nand_bytes_written_raw')
            host_writes = counters.get('smart_host_bytes_written_raw')
            if nand_writes is not None and host_writes is not None and host_writes > 0:
                self[serial_no]['write_amplification_factor'] = float(nand_writes) / float(host_writes)

    def _merge_parsed_counters(self, parsed_counters_list):
        """
        Receives a list of parsed ISDCT outputs as dicts.  The counters from
        each record are aggregated based on the NVMe device serial number, with
        redundant counters being overwritten.
        """
        all_data = {}
        for parsed_counters in parsed_counters_list:
            if parsed_counters is None:
                continue
            elif len(parsed_counters.keys()) > 1:
                raise Exception("Received multiple serial numbers from parse_dct_counters_file")
            else:
                device_sn = parsed_counters.keys()[0] 
    
            ### merge file's counter dict with any previous counters we've parsed
            if device_sn not in all_data:
                all_data[device_sn] = parsed_counters[device_sn]
            else:
                all_data[device_sn].update(parsed_counters[device_sn])
    
        ### attempt to figure out the type of each counter
        for device_sn, counters in all_data.iteritems():
            for counter, value in counters.iteritems():
                new_value = None
                ### first, handle counters that do not have an obvious way to cast
                if counter in ("Temperature", "Thermal_Throttle_Status_ThrottleStatus"):
                    value = value.split()[0]
    
                ### the order here is important, but hex that is not prefixed with
                ### 0x may be misinterpreted as integers.  if such counters ever
                ### surface, they must be explicitly cast above
                for cast in ( long, float, lambda x: long(x,16) ):
                    try:
                        new_value = cast(value)
                        break
                    except ValueError:
                        pass
                if value == "True":
                    new_value = True
                elif value == "False":
                    new_value = False
                if new_value is not None:
                    all_data[device_sn][counter] = new_value
    
        return all_data

def _decode_nersc_nid(path):
    """
    Given a path to some ISDCT output file, somehow figure out what the nid
    name for that node is.  This encoding is specific to the way NERSC collects
    and preserves ISDCT outputs.
    """
    abs_path = os.path.abspath(path)
    nid_name = os.path.dirname(abs_path).split(os.sep)[-1]
    return nid_name

def _rekey_smart_buffer(smart_buffer):
    """
    Take a buffer containing smart values associated with one register and
    create unique counters.  Only necessary for older versions of ISDCT
    which did not output SMART registers in a standard "Key: value" text
    format.
    """
    data = {}
    prefix = smart_buffer.get("description")
    if prefix is None:
        prefix = smart_buffer.get("_id")

    for key, val in smart_buffer.iteritems():
        key = ("SMART_%s_%s" % (prefix, key.strip())).replace("-","").replace(" ", "_")
        key = _normalize_key(key)
        data[key] = val.strip()
    return data

def _normalize_key(key):
    """
    Try to make the keys all follow similar formatting conventions.  Converts
    Intel's mix of camel-case and snake-case counters into all snake-case.
    Contains some nasty acronym hacks that may require modification if/when
    Intel adds new funny acronyms that contain a mix of upper and lower case
    letters (e.g., SMBus and NVMe)
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
            next_leter = None

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
