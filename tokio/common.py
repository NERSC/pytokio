"""Common convenience routines used throughout pytokio
"""

import time
import json
import datetime
import numpy

class ConfigError(RuntimeError):
    pass

def isstr(obj):
    """Determine if an object is a string or string-derivative

    Provided for Python2/3 compatibility

    Args:
        obj: object to be tested for stringiness

    Returns:
        bool: is it string-like?
    """
    try:
        # basestring is only in Python 2
        return isinstance(obj, basestring)
    except NameError:
        # Python 3
        return isinstance(obj, str)

class JSONEncoder(json.JSONEncoder):
    """Convert common pytokio data types into serializable formats
    """
    def default(self, obj): # pylint: disable=E0202
        if isinstance(obj, datetime.datetime):
            return int(time.mktime(obj.timetuple()))
        elif isinstance(obj, numpy.int64):
            return int(obj)
        return json.JSONEncoder.default(self, obj)
