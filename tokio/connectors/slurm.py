#!/usr/bin/env python
"""Connect to Slurm via Slurm CLI outputs.

This connector provides Python bindings to retrieve information made available
through the standard Slurm saccount and scontrol CLI commands.
"""

import json
import errno
import warnings
import datetime
try:
    import StringIO as io
except ImportError:
    import io
import subprocess
import pandas
from tokio.common import isstr
from tokio.connectors.common import SubprocessOutputDict

SACCT = 'sacct'
SCONTROL = 'scontrol'

def expand_nodelist(node_string):
    """Expand Slurm compact nodelist into a set of nodes.

    Wraps ``scontrol show hostname nid0[5032-5159]`` to expand a Slurm nodelist
    string into a list of nodes.

    Args:
        node_string (str): Node list in Slurm's compact notation (e.g.,
            ``nid0[5032-5159]``)

    Returns:
        set: Set of strings which encode the fully expanded node names contained
        in `node_string`.
    """
    node_names = set([])
    try:
        output_str = subprocess.check_output([SCONTROL, 'show', 'hostname', node_string])
    except OSError as error:
        if error.errno == errno.ENOENT:
            raise type(error)(error.errno, "Slurm CLI (%s command) not found" % SCONTROL)
        raise

    if not isstr(output_str):
        output_str = output_str.decode() # for python3

    for line in output_str.splitlines():
        node_name = line.strip()
        if node_name:
            node_names.add(node_name)

    return node_names

def compact_nodelist(node_string):
    """Convert a string of nodes into compact representation.

    Wraps ``scontrol show hostlist nid05032,nid05033,...`` to compress a list of
    nodes to a Slurm nodelist string.  This is effectively the reverse of
    ``expand_nodelist()``

    Args:
        node_string (str): Comma-separated list of node names (e.g.,
            ``nid05032,nid05033,...``)
    Returns:
        str: The compact representation of `node_string` (e.g.,
        ``nid0[5032-5159]``)
    """
    if not isstr(node_string):
        node_string = ','.join(list(node_string))

    try:
        node_string = subprocess.check_output([SCONTROL, 'show', 'hostlist', node_string]).strip()
    except OSError as error:
        if error.errno == errno.ENOENT:
            # "No such file or directory" from subprocess.check_output
            pass

    if isstr(node_string):
        return node_string
    else:
        return node_string.decode()

_RECAST_KEY_MAP = {
    'start':    (
        lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S"),
        lambda x: x.strftime("%Y-%m-%dT%H:%M:%S")
    ),
    'end':      (
        lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S"),
        lambda x: x.strftime("%Y-%m-%dT%H:%M:%S")
    ),
    'nodelist': (
        expand_nodelist,
        compact_nodelist
    ),
}
"""dict: Methods to convert Slurm string outputs into Python objects

This table provides the methods to apply to various Slurm output keys to convert
them from strings (the default Slurm output type) into more useful Python
objects such as datetimes or lists.

* ``value[0]`` is the function to cast to Python
* ``value[1]`` is the function to cast back to a string
"""

class SlurmEncoder(json.JSONEncoder):
    """Encode sets as lists and datetimes as ISO 8601.
    """
    def default(self, o): # pylint: disable=E0202
        if isinstance(o, set):
            return list(o)
        elif isinstance(o, datetime.datetime):
            return o.strftime("%Y-%m-%dT%H:%M:%S")
        return json.JSONEncoder.default(self, o)

class Slurm(SubprocessOutputDict):
    """Dictionary subclass that self-populates with Slurm output data

    Presents a schema that is keyed as::

        {
            taskid: {
                slurmfield1: value1
                slurmfield2: value2
                ...
            }
        }

    where taskid can be any of

        * jobid
        * jobid.<step>
        * jobid.batch
    """
    def __init__(self, jobid=None, *args, **kwargs):
        """Load basic information from Slurm

        Args:
            jobid (str): Slurm Job ID associated with data this object contains

        Attributes:
            jobid (str): Slurm Job ID associated with data contained in this
                object
        """
        super(Slurm, self).__init__(*args, **kwargs)
        self.subprocess_cmd = [SACCT]
        if jobid is not None:
            self.jobid = str(jobid)
        else:
            self.jobid = jobid
        self.load()

    def __repr__(self):
        """Serialize object in the same format as sacct.

        Returns:
            str: Serialized version of self in a similar format as the ``sacct``
            output so that this object can be circularly serialized and
            deserialized.
        """
        output_str = ""
        key_order = ['jobidraw']
        for counters in self.values():
            # print the column headers on the first pass
            if output_str == "":
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

    def load(self):
        """Initialize values either from cache or sacct
        """
        if self.from_string is not None:
            self.load_str(self.from_string)
        elif self.cache_file:
            self.load_cache()
        elif self.jobid is None:
            raise Exception("either jobid or cache_file must be specified on init")
        else:
            self.load_keys('jobidraw', 'start', 'end', 'nodelist')

    def load_keys(self, *keys):
        """Retrieve a list of keys from sacct and insert them into self.

        This always invokes sacct and can be used to overwrite the contents of a
        cache file.

        Args:
            *keys (list): Slurm attributes to include; names should be
                valid input to `sacct --format` CLI utility.
        """
        if self.jobid is None:
            raise Exception("Slurm.jobid is None")

        args = ['--jobs', self.jobid,
                '--format=%s' % ','.join(keys),
                '--parsable2']

        self._load_subprocess(*args)

        return self

    def load_str(self, input_str):
        """Load from either a json cache or the output of sacct
        """
        loaded_data = None
        try:
            loaded_data = self.from_json(input_str)
        except ValueError:
            pass

        if loaded_data is None:
            loaded_data = parse_sacct(input_str)

        self.update(loaded_data)

        self._recast_keys()

    def _recast_keys(self, *target_keys):
        """Convert own keys into native Python objects.

        Scan self and convert special keys into native Python objects where
        appropriate.  If no keys are given, scan everything.  Do NOT attempt
        to recast anything that is not a string--this is to avoid relying on
        expand_nodelist if a key is already recast since expand_nodelist does
        not function outside of an environment containing Slurm.

        Args:
            *target_keys (list, optional): Only convert these keys into native
                Python object types.  If omitted, convert all keys.
        """
        scan_keys = len(target_keys)
        for counters in self.values():
            # if specific keys were passed, only look for those keys
            if scan_keys > 0:
                for key in target_keys:
                    value = counters[key]
                    if key in _RECAST_KEY_MAP and isstr(value):
                        counters[key] = _RECAST_KEY_MAP[key][0](value)
            # otherwise, attempt to recast every key
            else:
                for key, value in counters.items():
                    if key in _RECAST_KEY_MAP and isstr(value):
                        counters[key] = _RECAST_KEY_MAP[key][0](value)

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
        """Return a list of all job nodes used.

        Creates a list of all nodes used across all tasks for the self.jobid.
        Useful if the object contains only a subset of tasks executed by the
        Slurm job.

        Returns:
            set: Set of node names used by the job described by this object
        """
        nodelist = set([])

        for counters in self.values():
            for jobnode in counters['nodelist']:
                nodelist.add(jobnode)

        return nodelist

    def get_job_startend(self):
        """Find earliest start and latest end time for a job.

        For an entire job and all its tasks, find the absolute earliest start
        time and absolute latest end time.

        Returns:
            tuple: Two-item tuple of (earliest start time, latest end time) in
                whatever type ``self['start']`` and ``self['end']`` are stored
        """
        min_start = None
        max_end = None
        for counters in self.values():
            if min_start is None or min_start > counters['start']:
                min_start = counters['start']
            if max_end is None or max_end < counters['end']:
                max_end = counters['end']

        return min_start, max_end

    def get_job_ids(self):
        """Return the top-level jobid(s) contained in object.

        Retrieve the jobid(s) contained in self without any accompanying taskid
        information.

        Returns:
            list of str: list of jobid(s) contained in self.
        """
        jobids = []
        for rawjobid in self:
            if '.' not in rawjobid:
                jobids.append(rawjobid)
        return jobids

    def to_json(self, **kwargs):
        """Return a json-encoded string representation of self.

        Serializes self to json using ``_RECAST_KEY_MAP`` to convert Python
        types back into JSON-compatible types.

        Returns:
            str: JSON representation of self
        """
        return json.dumps(self, cls=SlurmEncoder, **kwargs)

    def from_json(self, json_string):
        """Initialize self from a JSON-encoded string.

        Args:
            json_string (str): JSON representation of self
        """
        decoded_dict = json.loads(json_string)
        for key, value in decoded_dict.items():
            self[key] = value
        self._recast_keys()

    def to_dataframe(self):
        """Convert self into a Pandas DataFrame.

        Returns a Pandas DataFrame representation of this object.

        Returns:
            pandas.DataFrame: DataFrame representation of the same schema as
            the Slurm ``sacct`` command.
        """
        buf = io.StringIO(str(self))
        return pandas.read_csv(buf, sep='|', parse_dates=['start', 'end'])

def parse_sacct(sacct_str):
    """Convert output of ``sacct -p`` into a dictionary.

    Parses the output of ``sacct -p`` and return a dictionary with the full (raw)
    contents.

    Args:
        sacct_str (str): stdout of an invocation of ``sacct -p``

    Returns:
        dict: Keyed by Slurm Job ID and whose values are dicts containing
        key-value pairs corresponding to the Slurm quantities returned
        by ``sacct -p``.
    """
    result = {}
    cols = []
    for lineno, line in enumerate(sacct_str.splitlines()):
        fields = line.split('|')
        if lineno == 0:
            cols = [x.lower() for x in fields]
        else:
            record = {}
            jobidraw = fields[0]
            if jobidraw in result:
                warnings.warn("Duplicate raw jobid '%s' found" % jobidraw)
            for col, key in enumerate(cols):
                record[key] = fields[col]
            result[jobidraw] = record
    return result
