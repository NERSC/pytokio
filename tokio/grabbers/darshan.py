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

def _parse_header(line):
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

    return None, None


def _parse_mounts(line):
    """
    Parse the mount table lines which should look something like

        # mount entry:  /.shared/base/default/etc/dat.conf      dvs
        # mount entry:  /usr/lib64/libibverbs.so.1.0.0  dvs
        # mount entry:  /usr/lib64/libibumad.so.3.0.2   dvs
        # mount entry:  /usr/lib64/librdmacm.so.1.0.0   dvs
    """
    if line.startswith("# mount entry:\t"):
        key, val = line.split('\t')[1:3]
        return key, val.strip()
    return None, None

def _parse_base_counters(line):
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

def _parse_total_counters(line):
    """
    Parse the line containing an actual counter's data as output from
    darshan-parser --total.  It should be a line of the form

        total_MPIIO_F_READ_END_TIMESTAMP: 0.000000
        total_MPIIO_F_WRITE_END_TIMESTAMP: 98.847931
        total_MPIIO_F_CLOSE_TIMESTAMP: 99.596245
    """
    if not line.startswith("#"):
        args = line.split(':')
        if len(args) == 2:
            return args[0].strip(), args[1].strip()
    return None, None

def _parse_perf_counters(line):
    """
    Parse the line containing a counter's data as an output from darshan-parser
    --perf.  These lines can be of several forms:

    # performance
    # -----------
    # total_bytes: 2199023259968
    #
    # I/O timing for unique files (seconds):
    # ...........................
    # unique files: slowest_rank_io_time: 0.000000
    # unique files: slowest_rank_meta_only_time: 0.000000
    # unique files: slowest rank: 0
    #
    # I/O timing for shared files (seconds):
    # (multiple estimates shown; time_by_slowest is generally the most accurate)
    # ...........................
    # shared files: time_by_cumul_io_only: 39.992327
    # shared files: time_by_cumul_meta_only: 0.016496
    # shared files: time_by_open: 95.743623
    # shared files: time_by_open_lastio: 94.995309
    # shared files: time_by_slowest: 73.145417
    #
    # Aggregate performance, including both shared and unique files (MiB/s):
    # (multiple estimates shown; agg_perf_by_slowest is generally the most accurate)
    # ...........................
    # agg_perf_by_cumul: 52438.859493
    # agg_perf_by_open: 21903.829658
    # agg_perf_by_open_lastio: 22076.374336
    # agg_perf_by_slowest: 28670.996545
    """

    if line.startswith('# total_bytes:') or line.startswith('# agg_perf_by'):
        key, value = line[2:].split(':')
    elif line.startswith('# unique files:') or line.startswith('# shared files:'):
        key_suffix, key, value = line[2:].split(':')
        key += '_%s' % key_suffix.replace(' ', '_')
    else:
        return None, None

    return key.strip(), value.strip()

def _darshan_parser( log_file, counter_parser=_parse_base_counters):
    """
    Call darshan-parser --base on log_file and walk its output, identifying
    different sections and invoking the appropriate line parser function
    """

    def is_valid_counter( counter ):
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
        ### force the local shadow of 'module' to lowercase
        module = module.lower()

        ### assert that the counter actually belongs to the current module
        if counter_prefix is not None:
            if counter.startswith(counter_prefix):
                ### strip off the counter_prefix from the counter name
                counter = counter[len(counter_prefix):]
            else:
                raise Exception("counter %s does not start with prefix %s" % (counter, counter_prefix))

        ### otherwise insert the record--this logic should be made more flexible
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

    darshan_data = {}
    section = None
    module_section = None

    ### this regex must match every possible module name
    module_rex = re.compile('^# ([A-Z\-0-9/]+) module data\s*$')

    if counter_parser == _parse_base_counters:
        counter_flag = '--base'
    elif counter_parser == _parse_total_counters:
        counter_flag = '--total'
    elif counter_parser == _parse_perf_counters:
        counter_flag = '--perf'
    else:
        counter_flag = None

    if counter_flag is None:
        p = subprocess.Popen(['darshan-parser', log_file], stdout=subprocess.PIPE)
    else:
        p = subprocess.Popen(['darshan-parser', counter_flag, log_file], stdout=subprocess.PIPE)
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
        elif section == "mounts" and line.startswith("# **********************"):
            section = "counters"
            if section not in darshan_data:
                darshan_data[section] = {}
            else:
                raise Exception("duplicate %s sections found" % section)

        ### otherwise use the appropriate parser for this section
        if section == "header":
            key, val = _parse_header(line)
            if key is None:
                pass
            elif key == "metadata":
                if key not in darshan_data[section]:
                    darshan_data[section][key] = []
                darshan_data[section][key].append(val)
            else:
                darshan_data[section][key] = val
        elif section == 'mounts':
            key, val = _parse_mounts(line)
            if key is not None:
                darshan_data[section][key] = val

        elif section == 'counters':
            if counter_parser == _parse_base_counters:
                module, rank, record_id, counter, value, file_name, mount_pt, fs_type = counter_parser(line)
                if module_section is not None:
                    ### if it is none, is_valid_counter check below will bail
                    counter_prefix = module_section + "_"
            elif counter_parser == _parse_total_counters:
                counter, value = counter_parser(line)
                file_name = '_total'
                rank = None
                if module_section is not None:
                    counter_prefix = 'total_%s_' % module_section
            elif counter_parser == _parse_perf_counters:
                counter, value = counter_parser(line)
                file_name = '_perf'
                rank = None
                counter_prefix = None
            else:
                continue

            ### if no valid counter found, is this the start of a new module?
            valid, new_module_section = is_valid_counter(counter)
            if new_module_section is not None:
                module_section = new_module_section
            if valid is False:
                continue

            ### reminder: section is already defined as the global parser state
            insert_record(section=section,
                          module=module_section,
                          file_name=file_name,
                          rank=rank,
                          counter=counter,
                          value=value,
                          counter_prefix=counter_prefix)

    return darshan_data

def darshan_parser_base( log_file ):
    return _darshan_parser(log_file, _parse_base_counters)

def darshan_parser_total( log_file ):
    return _darshan_parser(log_file, _parse_total_counters)

def darshan_parser_perf( log_file ):
    return _darshan_parser(log_file, _parse_perf_counters)
