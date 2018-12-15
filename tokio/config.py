#!/usr/bin/env python
"""
Load the pytokio configuration file, which is encoded as json and contains
various site-specific constants, paths, and defaults.
"""

import os
import json
from tokio.common import isstr

#: Global variable containing the configuration
CONFIG = {} 

#: Path to configuration file to load
PYTOKIO_CONFIG_FILE = "" 

#: Path of default site configuration file
DEFAULT_CONFIG_FILE = ""

def init_config():
    """Loads the global configuration.

    Loads the site-wide configuration file, then inspects relevant environment
    variables for overrides.
    """
    global CONFIG
    global PYTOKIO_CONFIG_FILE
    global DEFAULT_CONFIG_FILE

    # Escape hatch for cases where we want to load the module without initalizing
    if os.environ.get('PYTOKIO_SKIP_CONFIG') is not None:
        return

    # Set the default config path - set here rather than in the global namespace
    # so site-specific paths don't get baked into the autodoc documentation
    DEFAULT_CONFIG_FILE = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'site.json')

    # Load a pytokio config from a special location
    PYTOKIO_CONFIG_FILE = os.environ.get('PYTOKIO_CONFIG', DEFAULT_CONFIG_FILE)

    try:
        with open(PYTOKIO_CONFIG_FILE, 'r') as config_file:
            loaded_config = json.load(config_file)
    except (OSError, IOError):
        loaded_config = {}

    # Load pytokio config file and convert its keys into a set of constants
    for _key, _value in loaded_config.items():
        # config keys beginning with an underscore get skipped
        if _key.startswith('_'):
            pass

        # if setting this key will overwrite something already in the tokio.config
        # namespace, only overwrite if the existing object is something we probably
        # loaded from a json
        _old_attribute = CONFIG.get(_key.lower())
        if _old_attribute is None \
        or isstr(_old_attribute) \
        or isinstance(_old_attribute, (dict, list)):
            CONFIG[_key.lower()] = _value

    # Check for magic environment variables to override the contents of the config
    # file at runtime
    for _magic_variable in ['HDF5_FILES', 'ISDCT_FILES', 'LFSSTATUS_FULLNESS_FILES', 'LFSSTATUS_MAP_FILES', 'DARSHAN_LOG_DIRS', 'ESNET_SNMP_URI']:
        _magic_value = os.environ.get("PYTOKIO_" + _magic_variable)
        if _magic_value is not None:
            try:
                _magic_value_decoded = json.loads(_magic_value)
            except ValueError:
                CONFIG[_magic_variable.lower()] = _magic_value
            else:
                CONFIG[_magic_variable.lower()] = _magic_value_decoded

init_config()
