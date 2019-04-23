"""
Common methods and classes used by connectors
"""

import os
import sys
import gzip
import json
import errno
import tarfile
import warnings
import mimetypes
import subprocess
from tokio.common import isstr

class CacheableDict(dict):
    """Generic class to support connectors that are dicts that can be cached as JSON

    When deriving from this class, the child object will have to define its own
    ``load_native()`` method to be invoked when ``input_file`` is not JSON.

    """
    def __init__(self, input_file=None):
        """Either initialize as empty or load from cache

        Args:
            input_file (str): Path to either a JSON file representing the dict
                or a native file that will be parsed into a JSON-compatible
                format
        """
        super(CacheableDict, self).__init__()
        self.input_file = input_file
        self.load()

    def load(self, input_file=None):
        """Wrapper around the filetype-specific loader.

        Infer the type of input being given, dispatch the correct loading
        function, and populate keys/values.

        Args:
            input_file (str or None): The input file to load.  If not specified,
                uses whatever self.input_file is
        """
        if input_file:
            self.input_file = input_file

        if self.input_file is None:
            return

        if not os.path.exists(self.input_file):
            raise OSError("Input file %s does not exist" % self.input_file)

        try:
            self.load_json()
        except ValueError:
            self.load_native()

    def load_native(self, input_file=None):
        """Parse an uncached, native object

        This is a stub that should be overloaded on derived classes.

        Args:
            input_file (str or None): The input file to load.  If not specified,
                uses whatever self.input_file is
        """
        pass

    def load_json(self, input_file=None):
        """Loads input from serialized JSON

        Load the serialized format of this object, encoded as a json dictionary.
        This is the converse of the save_cache() method.

        Args:
            input_file (str or None): The input file to load.  If not specified,
                uses whatever self.input_file is
        """
        if input_file:
            self.input_file = input_file

        _, encoding = mimetypes.guess_type(self.input_file)

        if encoding == 'gzip':
            open_func = gzip.open
        else:
            open_func = open

        for key, val in json.load(open_func(self.input_file, 'r')).items():
            self.__setitem__(key, val)

    def save_cache(self, output_file=None, **kwargs):
        """Serializes self into a JSON output.

        Save the dictionary in a JSON file.  This output can be read back in using
        load_json().

        Args:
            output_file (str or None): Path to file to which json should be
                written.  If None, write to stdout.  Default is None.
            kwargs (dict): Additional arguments to be passed to json.dumps()
        """
        if output_file is None:
            self._save_cache(sys.stdout, **kwargs)
        else:
            with open(output_file, 'w') as output:
                self._save_cache(output, **kwargs)

    def _save_cache(self, output, **kwargs):
        """Generates serialized representation of self

        Args:
            output: Object with a ``.write()`` method into which the serialized
                form of self will be passed
        """
        output.write(json.dumps(self, **kwargs))

class SubprocessOutputDict(dict):
    """Generic class to support connectors that parse the output of a subprocess

    When deriving from this class, the child object will have to

        1. Define subprocess_cmd after initializing this parent object
        2. Define self.__repr__ (if necessary)
        3. Define its own self.load_str
        4. Define any introspective analysis methods

    """
    def __init__(self, cache_file=None, from_string=None, silent_errors=False):
        super(SubprocessOutputDict, self).__init__(self)
        self.cache_file = cache_file
        self.silent_errors = silent_errors
        self.from_string = from_string
        self.subprocess_cmd = []

    def load(self, cache_file=None):
        """Load based on initialization state of object

        Args:
            cache_file (str or None): The cached input file to load.  If not
                specified, uses whatever self.cache_file is
        """
        if cache_file:
            self.cache_file = cache_file

        if self.from_string is not None:
            self.load_str(self.from_string)
        elif self.cache_file:
            self.load_cache()
        elif self.subprocess_cmd:
            self._load_subprocess()

    def _load_subprocess(self, *args):
        """Run a subprocess and pass its stdout to a self-initializing parser
        """

        cmd = self.subprocess_cmd
        if args:
            cmd += args

        try:
            if self.silent_errors:
                with open(os.devnull, 'w') as devnull:
                    output_str = subprocess.check_output(cmd, stderr=devnull)
            else:
                output_str = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as error:
            warnings.warn("%s returned nonzero exit code (%d)" % (cmd, error.returncode))
            output_str = error.output
        except OSError as error:
            if error.errno == errno.ENOENT:
                raise type(error)(error.errno, "%s command not found" % self.subprocess_cmd[0])
            raise

        if isstr(output_str):
            # Python 2 - subprocess.check_output returns a string
            self.load_str(output_str)
        else:
            # Python 3 - subprocess.check_output returns encoded bytes
            self.load_str(output_str.decode())

    def load_cache(self, cache_file=None):
        """Load subprocess output from a cached text file

        Args:
            cache_file (str or None): The cached input file to load.  If not
                specified, uses whatever self.cache_file is
        """
        if cache_file:
            self.cache_file = cache_file

        _, encoding = mimetypes.guess_type(self.cache_file)
        if encoding == 'gzip':
            input_fp = gzip.open(self.cache_file, 'rt')
        else:
            input_fp = open(self.cache_file, 'r')
        self.load_str(input_fp.read())
        input_fp.close()

    def load_str(self, input_str):
        """Load subprocess output from a string

        Args:
            input_str (str): The text that came from the subprocess's stdout and
                should be parsed by this method.
        """
        self['_raw'] = input_str

    def save_cache(self, output_file=None):
        """Serialize subprocess output to a text file

        Args:
            output_file (str): Path to a file to which the output cache should
                be written.  If None, write to stdout.
        """
        if output_file is None:
            sys.stdout.write(str(self))
        else:
            with open(output_file, 'w') as output_fp:
                output_fp.write(str(self))

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
