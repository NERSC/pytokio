#!/usr/bin/env python
"""
Darned dirty way to convert the output of darshan-parser (version 3.0 or higher)
to a Python dict (or json).  Ideally this will eventually use a native Darshan
parsing library (like darshan-ruby does).
"""

import subprocess
import sys
import re
import json

class Darshan(dict):
    def __init__(self, log_file=None, cache_file=None):
        super(Darshan,self).__init__(self)
        self.cache_file = cache_file
        self.log_file = log_file
        self.load()
    
    def __repr__(self):
        return json.dumps(self.values())
        
    def load(self):
        if self.cache_file is None and self.log_file is None:
            raise Exception("parameters should be provided (at least log_file or cache_file)")
        if self.cache_file:
            self.__setitem__(json.load(cache_file))
    
    def save_cache(self, output_file=None):
        """
        Save the dictionnary in a json file
        """
        if output_file is None:
            self._save_cache(sys.stdout)
        else:
            with open(output_file, 'w') as fp:
                self.save_cache(fp)

    def _save_cache(self, output):
        output.write(json.dumps(self))
        
     #=================================================#
    
    def darshan_parser_base(self):
        return self._darshan_parser("BASE")["BASE"]

    def darshan_parser_total(self):
        return self._darshan_parser("TOTAL")["TOTAL"]

    def darshan_parser_perf(self):
        return self._darshan_parser("PERF")["PERF"]

    def _darshan_parser(self, counter_flag="BASE"):
        """
        Call darshan-parser --base on self.log_file and walk its output, identifying
        different sections and invoking the appropriate line parser function
        """

        def counter_parser(line, counter_flag):
            if counter_flag == "BASE":
                return self._parse_base_counters(line)
            if counter_flag == "PERF":
                return self._parse_perf_counters(line)
            if counter_flag == "TOTAL":
                return self._parse_total_counters(line)
            return None

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
                    module_section = module_section.replace('-','') # because of "MPI-IO" and "MPIIO"
                    module_section = module_section.replace('/','') # because of "BG/Q" and "BGQ"
                    return False, module_section
                else:
                    return False, None
            else:
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
                else:
                    raise Exception("counter %s does not start with prefix %s" % (counter, counter_prefix))
                    
            # Otherwise insert the record--this logic should be made more flexible
            if section not in darshan_data:
                darshan_data[section] = {}
            if module not in darshan_data[section]:
                darshan_data[section][module] = {}
            if file_name not in darshan_data[section][module]:
                darshan_data[section][module][file_name] = {}
            if rank is None:
                insert_base = darshan_data[section][module][file_name]
            else:
                if rank not in darshan_data[section][module][file_name]:
                    darshan_data[section][module][file_name][rank] = {}
                insert_base = darshan_data[section][module][file_name][rank]

            if counter in insert_base:
                raise Exception("Duplicate counter %s found in %s->%s->%s (rank=%s)" % (counter, section, module, file_name, rank))
            else:
                if '.' in value:
                    value = float(value)
                else:
                    value = long(value)
                insert_base[counter] = value
        
        if self.log_file is None:
            return self

        darshan_data = {}
        section = None
        counter = None
        module_section = None
        # This regex must match every possible module name
        module_rex = re.compile('^# ([A-Z\-0-9/]+) module data\s*$')
        if counter_flag in ["BASE", "TOTAL", "PERF"]:
            darshan_flag = "--" + counter_flag.lower()
        else:
            darshan_flag = ""
        p = subprocess.Popen(['darshan-parser', darshan_flag, self.log_file], stdout=subprocess.PIPE)
            

        for line in p.stdout:
            # Is this the start of a new section?
            # Why do we look at section, refactorize failed 
            if section is None and line.startswith("# darshan log version:"):
                section = "header"
                if section not in darshan_data:
                    darshan_data[section] = {}
                else:
                    raise Exception("duplicate %s sections found" % section)
                    
            elif section == "header" and line.startswith("# mounted file systems"):
                section = "mounts"
                if section not in darshan_data:
                    darshan_data[section] = {}
                else:
                    raise Exception("duplicate %s sections found" % section)
                    
            elif section == "mounts" and line.startswith("# **********************"):  # understand the utility of these stars
                section = "counters"
                if section not in darshan_data:
                    darshan_data[section] = {}
                else:
                    raise Exception("duplicate %s sections found" % section)
                            

            # otherwise use the appropriate parser for this section
            if section == "header":
                key, val = self._parse_header(line)
                if key is None:
                    pass
                elif key == "metadata":
                    if key not in darshan_data[section]:
                        darshan_data[section][key] = []
                    darshan_data[section][key].append(val)
                else:
                    darshan_data[section][key] = val
            elif section == 'mounts':
                key, val = self._parse_mounts(line)
                if key is not None:
                    darshan_data[section][key] = val

            elif section == 'counters':
                if counter_flag == "BASE":
                    module, rank, record_id, counter, value, file_name, mount_pt, fs_type = counter_parser(line, counter_flag)
                    if module_section is not None:
                        # If it is none, is_valid_counter check below will bail
                        counter_prefix = module_section + "_"
                elif counter_flag == "TOTAL":
                    counter, value = counter_parser(line, counter_flag)
                    file_name = '_total'
                    rank = None
                    if module_section is not None:
                        counter_prefix = 'total_%s_' % module_section
                elif counter_flag == "PERF":
                    counter, value = counter_parser(line, counter_flag)
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
        self.__setitem__(counter_flag, darshan_data)
        return self

    def _parse_header(self, line):
        """
        Parse the header lines which look something like
        
        # darshan log version: 3.10
        # compression method: ZLIB
        # exe: /home/user/bin/myjob.exe --whatever
        # uid: 69615
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
            return 'start_time', long(line.split()[2])
        elif line.startswith("# start_time_asci:"):
            return 'start_time_string', line.split(None, 2)[-1].strip()
        elif line.startswith("# end_time:"):
            return 'end_time', long(line.split()[2])
        elif line.startswith("# end_time_asci:"):
            return 'end_time_string', line.split(None, 2)[-1].strip()
        elif line.startswith("# nprocs:"):
            return "nprocs", int(line.split()[-1])
        elif line.startswith("# run time:"):
            return "walltime", int(line.split()[-1])
        elif line.startswith("# metadata:"):
            return "metadata", line.split(None, 2)[-1].strip()
        return None,None
        

    def _parse_mounts(self, line):
        """
        Parse the mount table lines which should look something like

        # mount entry:  /usr/lib64/libibverbs.so.1.0.0  dvs

        """
        if line.startswith("# mount entry:\t"):
            key, val = line.split('\t')[1:3]
            return key, val.strip()
        return None, None

    def _parse_base_counters(self, line):
        """
        Parse the line containing an actual counter's data.  It is a tab-delimited
        line of the form
        
        module, rank, record_id, counter, value, file_name, mount_pt, fs_type = parse_counters(line)
        """
        if not line.startswith("#"):
            args = line.split('\t')
            if len(args) == 8:
                return tuple(args)
        return None, None, None, None, None, None, None, None

    def _parse_total_counters(self, line):
        """
        Parse the line containing an actual counter's data as output from
    darshan-parser --total.  It should be a line of the form
    
        total_MPIIO_F_READ_END_TIMESTAMP: 0.000000

        """
        if not line.startswith("#"):
            args = line.split(':')
            if len(args) == 2:
                return args[0].strip(), args[1].strip()
        return None, None

    def _parse_perf_counters(self, line):
        """
        Parse the line containing a counter's data as an output from darshan-parser
        --perf.  These lines can be of several forms:

        - # total_bytes: 2199023259968
        - # unique files: slowest_rank_io_time: 0.000000
        - # shared files: time_by_cumul_io_only: 39.992327
        - # agg_perf_by_slowest: 28670.996545

        """        
        if line.startswith('# total_bytes:') or line.startswith('# agg_perf_by'):
            key, value = line[2:].split(':')
        elif line.startswith('# unique files:') or line.startswith('# shared files:'):
            key_suffix, key, value = line[2:].split(':')
            key += '_%s' % key_suffix.replace(' ', '_')
        else:
            return None, None

        return key.strip(), value.strip()


        
