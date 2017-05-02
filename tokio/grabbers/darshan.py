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

def parse_header(line):
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
    elif line.endswith("# end_time:"):
        return 'end_time', long(line.split()[2])
    elif line.endswith("# end_time_asci:"):
        return 'end_time_string', line.split(None, 2)[-1].strip()
    elif line.startswith("# nprocs:"):
        return "nprocs", int(line.split()[-1])
    elif line.startswith("# run time:"):
        return "walltime", int(line.split()[-1])
    elif line.startswith("# metadata:"):
        return "metadata", line.split(None, 2)[-1].strip()

    return None, None


def parse_mounts(line):
    if line.startswith("# mount entry:\t"):
        key, val = line.split('\t')[1:3]
        return key, val.strip()
    return None, None

def parse_counters(line):
    # module, rank, record_id, counter, value, file_name, mount_pt, fs_type = parse_counters(line)
    if not line.startswith("#"):
        args = line.split('\t')
        if len(args) == 8:
            return tuple(args)
    return None, None, None, None, None, None, None, None
 
def process_darshan_log( log_file ):
    darshan_data = {}
    section = None
    module_section = None

    ### this regex must match every possible module name
    module_rex = re.compile('^# ([A-Z\-0-9/]+) module data\s*$')

    p = subprocess.Popen(['darshan-parser', log_file], stdout=subprocess.PIPE)
    for line in p.stdout:
        ### is this the start of a new section?
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
        elif section == "mounts" and line.startswith("# description of columns:"):
            section = "counters"
            if section not in darshan_data:
                darshan_data[section] = {}
            else:
                raise Exception("duplicate %s sections found" % section)

        ### otherwise use the appropriate parser for this section
        if section == "header":
            key, val = parse_header(line)
            if key is None:
                pass
            elif key == "metadata":
                if key not in darshan_data[section]:
                    darshan_data[section][key] = []
                darshan_data[section][key].append(val)
            else:
                darshan_data[section][key] = val
        elif section == 'mounts':
            key, val = parse_mounts(line)
            if key is not None:
                darshan_data[section][key] = val
        elif section == 'counters':
            module, rank, record_id, counter, value, file_name, mount_pt, fs_type = parse_counters(line)

            ### try to identify new module section
            if module is None:
                match = module_rex.search(line)
                if match is not None:
                    module_section = match.group(1)
                    module_section = module_section.replace('-','') # because of "MPI-IO" and "MPIIO"
                    module_section = module_section.replace('/','') # because of "BG/Q" and "BGQ"
                continue

            if counter.startswith('%s_' % module_section):
                module = module_section.lower()
                counter = counter[len(module)+1:]
            else:
                raise Exception("found counter %s in module section %s" % (counter, module_section))

            ### otherwise insert the record -- this logic should be made more flexible
            if module not in darshan_data[section]:
                darshan_data[section][module] = {}
            if file_name not in darshan_data[section][module]:
                darshan_data[section][module][file_name] = {}
            if rank not in darshan_data[section][module][file_name]:
                darshan_data[section][module][file_name][rank] = {}

            if counter in darshan_data[section][module][file_name][rank]:
                raise Exception("Duplicate counter %s found in %s->%s->%s->%s" % (counter, section, module, file_name, rank))
            else:
                if '.' in value:
                    value = float(value)
                else:
                    value = long(value)
                darshan_data[section][module][file_name][rank][counter] = value

    return darshan_data

if __name__ == "__main__":
    darshan_data = process_darshan_log(sys.argv[1])
    print json.dumps(darshan_data,indent=4,sort_keys=True)
