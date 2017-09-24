#!/usr/bin/env python

import sys
import copy
import json
import errno
import pandas
import datetime
import StringIO
import subprocess

def expand_nodelist(node_string):
    """
    Wraps `scontrol show hostname nid0[5032-5159]` to expand a Slurm nodelist
    string into a list of nodes
    """
    node_names = set([])
    try:
        output_str = subprocess.check_output(['scontrol', 'show', 'hostname', node_string])
    except OSError as error:
        if error[0] == errno.ENOENT:
            raise type(error)(error[0], "Slurm CLI (sacct command) not found")
        raise

    for line in output_str.splitlines():
        node_name = line.strip()
        if len(node_name) > 0:
            node_names.add(node_name)
    return node_names

def compact_nodelist(node_string):
    """
    Wraps `scontrol show hostlist nid05032,nid05033,...` to compact a list of
    nodes to a Slurm nodelist string.  The inverse of expand_nodelist()
    """
    if not isinstance(node_string, basestring):
        node_string = ','.join(list(node_string))

    try:
        node_string = subprocess.check_output(['scontrol', 'show', 'hostlist', node_string]).strip()
    except OSError as error:
        if error[0] == errno.ENOENT:
            # "No such file or directory" from subprocess.check_output
            pass
    return node_string

# keyed by Slurm field keys; value[0] is the function to cast to Python;
#                            value[1] is the function to cast back to a str
_RECAST_KEY_MAP = {
    'start':    (lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S"), lambda x: x.strftime("%Y-%m-%dT%H:%M:%S")),
    'end':      (lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S"), lambda x: x.strftime("%Y-%m-%dT%H:%M:%S")),
    'nodelist': (lambda x: expand_nodelist(x), lambda x: compact_nodelist(x)),
}

class SlurmEncoder(json.JSONEncoder):
    """
    Necessary because sets are not JSON-serializable
    """
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%dT%H:%M:%S")
        return json.JSONEncoder.default(self, obj)

class Slurm(dict):
    """
    A dictionary that is keyed as
    {
        taskid: {
            slurmfield1: value1
            slurmfield2: value2
            ...
        }
    }
    where taskid can be any of
        jobid
        jobid.<step>
        jobid.batch
    """
    def __init__(self, jobid=None, cache_file=None):
        super(Slurm, self).__init__(self)
        self.cache_file = cache_file
        if jobid is not None:
            self.jobid = str(jobid)
        else:
            self.jobid = jobid
        self._load()
        
    def __repr__(self):
        """
        Returns the object in a similar format as the sacct output so that this
        object can be circularly serialized and deserialized.
        """
        output_str = ""
        key_order = [ 'jobidraw' ]
        for jobidraw, counters in self.iteritems():
            # print the column headers on the first pass
            if output_str is "":
                for key in counters:
                    if key not in key_order:
                        key_order.append(key)
                output_str = '|'.join(key_order)

            print_values = []
            for key in key_order:
                value = counters[key]
                # convert specific keys back into strings
                if key in _RECAST_KEY_MAP:
                    value = _RECAST_KEY_MAP[key][1](value)
                print_values.append(value)
            output_str += "\n" + '|'.join(print_values)
        return output_str
        
    def _load(self):
        """
        Routine to initialize own values, either from cache or sacct
        """
        if self.cache_file is not None:
            self._load_cache()
        elif self.jobid is not None:
            self.load_keys('jobidraw', 'start', 'end', 'nodelist')
        else:
            raise Exception("Either jobid or cache_file must be specified on init")

    def _load_cache(self):
        """
        Load a Slurm job from a JSON-encoded cache file
        """
        if self.cache_file is None:
            raise Exception("load_cache with None as cache_file")
        self.from_json(open(self.cache_file, 'r').read())

    def load_keys(self, *keys):
        """
        Retrieve a list of keys from sacct and populate self.  This always
        invokes sacct and can be used to overwrite the contents of a cache file.
        """
        if self.jobid is None:
            raise Exception("Slurm.jobid is None")

        try:
            sacct_str = subprocess.check_output([
                'sacct',
                '--jobs', self.jobid,
                '--format=%s' % ','.join(keys),
                '--parsable2'])
        except OSError as error:
            if error[0] == errno.ENOENT:
                raise type(error)(error[0], "Slurm CLI (sacct command) not found")
            raise 
        except subprocess.CalledProcessError:
            raise
        self.update(parse_sacct(sacct_str))
        self._recast_keys()

    def _recast_keys(self, *target_keys):
        """
        Scan self and convert special keys into native Python objects where
        appropriate.  If no keys are given, scan everything.  Do NOT attempt
        to recast anything that is not a string--this is to avoid relying on
        expand_nodelist if a key is already recast since expand_nodelist does
        not function outside of an environment containing Slurm.
        """
        scan_keys = len(target_keys)
        for taskid, counters in self.iteritems():
            # if specific keys were passed, only look for those keys
            if scan_keys > 0:
                for key, value in target_keys.iteritems():
                    if key in _RECAST_KEY_MAP and isinstance(value, basestring):
                        counters[key] = _RECAST_KEY_MAP[key][0](value)
            # otherwise, attempt to recast every key
            else:
                for key, value in counters.iteritems():
                    if key in _RECAST_KEY_MAP and isinstance(value, basestring):
                        counters[key] = _RECAST_KEY_MAP[key][0](value)

    def save_cache(self, output_file=None):
        """
        Save the dictionary in a json file
        """
        if output_file is None:
            self._save_cache(sys.stdout)
        else:
            with open(output_file, 'w') as fp:
                self._save_cache(fp)

    def _save_cache(self, output):
        output.write(self.to_json())
   
#   def get_task_startend(self, taskid=self.jobid):
#       """
#       Convert raw Slurm keys into datetime objects and return them
#       """
#       # initialize self if needed
#       if len(self) == 0 or taskid not in self.keys():
#           self.load_keys('jobidraw', 'start', 'end')
#
#       # make sure the requested taskid exists
#       if taskid not in self.keys():
#           raise Exception("Invalid task id '%s' for job '%s'" % (str(taskid), str(self.jobid)))
#
#       # if somehow got partially loaded
#       if 'start' not in self[taskid] or 'end' not in self[taskid]:
#           raise Exception("No start/end information for task id %s" % taskid)
#        
#       return self[taskid]['start'], self[taskid]['end']
    
    def get_job_nodes(self):
        """
        Return a list of all job nodes used across all tasks.  Useful if the
        object contains only a subset of tasks executed by the Slurm job.
        """
        nodelist = set([])

        for taskid, counters in self.iteritems():
            for jobnode in counters['nodelist']:
                nodelist.add(jobnode)
 
        return nodelist

    def get_job_startend(self):
        """
        For an entire job and all its tasks, find the absolute earliest start
        time and absolute latest end time
        """
        min_start = None
        max_end = None
        for rawjobid, counters in self.iteritems():
            if min_start is None or min_start > counters['start']:
                min_start = counters['start']
            if max_end is None or max_end < counters['end']:
                max_end = counters['end']

        return min_start, max_end

    def get_job_ids(self):
        """
        Return the top-level jobid(s) (not taskids) contained in object
        """
        jobids = []
        for rawjobid, counters in self.iteritems():
            if '.' not in rawjobid:
                jobids.append(rawjobid)
        return jobids

    def to_json(self, **kwargs):
        """
        Return a json-encoded string representation of self.  Can't just
        json.dumps(slurm.Slurm) because of the effects of _RECAST_KEY_MAP.
        """
        return json.dumps(self, cls=SlurmEncoder, **kwargs)

    def from_json(self, json_string):
        """
        Take a json-encoded string and use it to initialize self
        """
        decoded_dict = json.loads(json_string)
        for key, value in decoded_dict.iteritems():
            self[key] = value
        self._recast_keys()
        
    def to_dataframe(self):
        """
        Return a pandas DataFrame representation of this object.  It's not an
        unreasonable fit since the raw output from sacct is essentially a CSV.
        """
        buf = StringIO.StringIO(str(self))
        return pandas.read_csv(buf, sep='|', parse_dates=['start', 'end'])

def parse_sacct(sacct_str):
    """
    Parse the output of sacct -p and return a dictionary with the full (raw)
    contents
    """
    result = {}
    cols = []
    for lineno, line in enumerate(sacct_str.splitlines()):
        fields = line.split('|')
        if lineno == 0:
            cols = [ x.lower() for x in fields ]
        else:
            record = {}
            jobidraw = fields[0]
            if jobidraw in result:
                warnings.warn("Duplicate raw jobid '%s' found" % jobidraw)
            for col, key in enumerate(cols):
                record[key] = fields[col]
            result[jobidraw] = record
    return result
