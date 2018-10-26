"""
Common methods and classes used by connectors
"""

import os
import sys
import gzip
import errno
import warnings
import mimetypes
import subprocess
from tokio.common import isstr

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

    def load(self):
        """Load based on initialization state of object
        """
        if self.from_string is not None:
            self.load_str(self.from_string)
        elif self.cache_file:
            self.load_cache()
        else:
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

    def load_cache(self):
        """Load subprocess output from a cached text file
        """
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
