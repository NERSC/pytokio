#!/usr/bin/env python
"""
Load the pytokio configuration file, which is encoded as json and contains
various site-specific constants, paths, and defaults.
"""

import os
import sys
import json

PYTOKIO_CONFIG = ""
"""Path to configuration file to load
"""

CONFIG = {}
"""Global variable for the parsed configuration
"""

def init_config():
    global PYTOKIO_CONFIG
    global CONFIG

    # Load a pytokio config from a special location
    PYTOKIO_CONFIG = os.environ.get(
        'PYTOKIO_CONFIG',
        os.path.join(os.path.abspath(os.path.dirname(__file__)), 'site.json'))

    # Load pytokio config file and convert its keys into a set of constants
    for _key, _value in json.load(open(PYTOKIO_CONFIG, 'r')).iteritems():
        # config keys beginning with an underscore get skipped
        if _key.startswith('_'):
            pass

        # if setting this key will overwrite something already in the tokio.config
        # namespace, only overwrite if the existing object is something we probably
        # loaded from a json
        _old_attribute = CONFIG.get(_key.lower())
        if _old_attribute is None \
        or isinstance(_old_attribute, basestring) \
        or isinstance(_old_attribute, dict) \
        or isinstance(_old_attribute, list):
            CONFIG[_key.lower()] = _value

    # Check for magic environment variables to override the contents of the config
    # file at runtime
    for _magic_variable in ['HDF5_FILES', 'ISDCT_FILES', 'LFSSTATUS_FULLNESS_FILES', 'LFSSTATUS_MAP_FILES']:
        _magic_value = os.environ.get("PYTOKIO_" + _magic_variable)
        if _magic_value is not None:
            try:
                _magic_value_decoded = json.loads(_magic_value)
            except ValueError:
                CONFIG[_magic_variable.lower()] = _magic_value
            else:
                CONFIG[_magic_variable.lower()] = _magic_value_decoded

init_config()
