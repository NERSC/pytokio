#!/usr/bin/env python

import os
import re
import json
import gzip
import pandas
import tarfile
import itertools
import mimetypes

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
        if os.path.isdir(self.input_file):
            self.load_directory()
        else:
            # encoding can be 'gzip' or None
            mime_type, encoding = mimetypes.guess_type(self.input_file)

            if mime_type == 'application/x-tar':
                self.load_tarfile(encoding)
            elif mime_type == 'application/json':
                self.load_json(encoding)
            else:
                raise Exception("Unknown mime type %s" % mime_type)

    def load_tarfile(self, encoding):
        """
        Load a tarred (and optionally compressed) collection of ISDCT outputs.
        Assume that ISDCT output files each contain a single output from a
        single invocation of the isdct tool, and outputs are grouped into
        directories named according to their nid numbers (e.g.,
        nid00984/somefile.txt).
        """
        parsed_counters_list = []
        if encoding == 'gzip':
            tar = tarfile.open(self.input_file, 'r:gz')
        else:
            tar = tarfile.open(self.input_file, 'r')

        for member in tar.getmembers():
            # we only care about file members, not directories
            if not member.isfile():
                continue

            # extract the file member into a memory buffer
            file_obj = tar.extractfile(member)

            # infer the node name from the member's path
            nodename = _decode_nersc_nid(member.name)

            # process the member's contents, then add it to self
            parsed_counters_list.append(
                self.parse_counters_fileobj(file_obj, nodename))

        merged_counters = self._merge_parsed_counters(parsed_counters_list)
        for key, val in merged_counters.iteritems():
            self.__setitem__(key, val)

    def load_directory(self):
        """
        Functionally identical to load_tarfile(), but operates on the untarred
        contents of the tarfile.
        """
        parsed_counters_list = []
        for root, dirs, files in os.walk(self.input_file):
            for file_name in files:
                fq_file_name = os.path.join(root, file_name)
                nodename = _decode_nersc_nid(fq_file_name)
                parsed_counters = self.parse_counters_fileobj(
                    open(fq_file_name, 'r'),
                    nodename=nodename)
                parsed_counters_list.append(parsed_counters)

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

            if 'NodeName' in df:
                numeric_keys = [ 'NodeName' ] + numeric_keys
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
                        data['NodeName'] = nodename
                    if rex_match.group(1) == "Intel SSD" or rex_match.group(1) == "SMART and Health Information":
                        parse_mode = 0
                    elif rex_match.group(1) == "SMART Attributes":
                        parse_mode = 1
                    else:
                        raise Exception("Unknown counter file format")
            elif parse_mode == 0 and ':' in line:
                key, val = line.split(':')
                key = _normalize_key(key)
                data[key] = val.strip()
            elif parse_mode > 0 and ':' in line:
                key, val = line.split(':')
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
            warnings.warn("Couldn't find device sn in " + path)
        else:
            return { device_sn : data }

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
                warnings.warn("No valid counters found in " + f)
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
