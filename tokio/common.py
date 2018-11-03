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

def humanize_units(byte_count, divisor=1024):
    """Convert raw byte count into human-readable units

    Args:
        byte_count (int): Number of bytes
        divisor (int): Either 1024 or 1000 for base-2 units or base-10 units

    Returns:
        tuple of (float, str): First argument is a number between 1.0 and
        divisor, and string is the units in which the first argument is
        expressed.
    """
    units_2 = ["bytes", "KiB", "MiB", "GiB", "TiB"]
    units_10 = ["bytes", "KB", "MB", "GB", "TB"]

    if divisor == 1024:
        units = units_2
    elif divisor == 1000:
        units = units_10
    else:
        raise RuntimeError("divisor must be 1024 or 1000")

    result = byte_count
    index = 0
    while index < len(units) - 1:
        new_result = result / divisor
        if new_result < 1.0:
            break
        else:
            index += 1
            result = new_result

    return result, units[index]

class JSONEncoder(json.JSONEncoder):
    """Convert common pytokio data types into serializable formats
    """
    def default(self, obj): # pylint: disable=E0202
        if isinstance(obj, datetime.datetime):
            return int(time.mktime(obj.timetuple()))
        elif isinstance(obj, numpy.int64):
            return int(obj)
        return json.JSONEncoder.default(self, obj)
