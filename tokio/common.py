"""Common convenience routines used throughout pytokio
"""

import time
import json
import datetime
import numpy

UNITS = [
    (2**80, "YiB"),
    (2**70, "ZiB"),
    (2**60, "EiB"),
    (2**50, "PiB"),
    (2**40, "TiB"),
    (2**30, "GiB"),
    (2**20, "MiB"),
    (2**10, "KiB")
]
UNITS_10 = [
    (10**24, "YiB"),
    (10**21, "ZiB"),
    (10**18, "EiB"),
    (10**15, "PiB"),
    (10**12, "TiB"),
    (10**9, "GiB"),
    (10**6, "MiB"),
    (10**3, "KiB")
]

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

def humanize_bytes(bytect, base10=False, fmt="%.1f %s"):
    """Converts bytes into human-readable units

    Args:
        bytect (int): Number of bytes
        base10 (bool): Convert to base-10 units (MB, GB, etc) if True
        fmt (str or None): Format of string to return; must contain %f/%d and %s
            for the quantity and units, respectively.

    Returns:
        str: Quantity and units expressed in a human-readable quantity
    """

    if base10:
        units = UNITS_10
    else:
        units = UNITS

    for unit in units:
        if bytect >= unit[0]:
            return fmt % (bytect / unit[0], unit[1])

    return fmt % (bytect, "bytes" if bytect != 1 else "byte")

def to_epoch(datetime_obj):
    """Convert datetime.datetime into epoch seconds

    Currently assumes input datetime is expressed in localtime.  Does not handle
    timezones very well.

    Args:
        datetime_obj (datetime.datetime): Datetime to convert to seconds-since-epoch

    Returns:
        int: Seconds since epoch
    """

    return int(time.mktime(datetime_obj.timetuple()))

def recast_string(value):
    """Converts a string to some type of number or True/False if possible

    Args:
        value (str): A string that may represent an int or float

    Returns:
        int, float, bool, or str: The most precise numerical or boolean
        representation of ``value`` if ``value`` is a valid string-encoded
        version of that type.  Returns the unchanged string otherwise.
    """

    ### the order here is important, but hex that is not prefixed with
    ### 0x may be misinterpreted as integers.
    new_value = value
    for cast in (int, float, lambda x: int(x, 16)):
        try:
            new_value = cast(value)
            break
        except ValueError:
            pass

    if value == "True":
        new_value = True
    elif value == "False":
        new_value = False

    return new_value
