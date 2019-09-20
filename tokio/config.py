#!/usr/bin/env python
"""Loads the pytokio configuration file.

The pytokio configuration file can be either a JSON or YAML file that contains
various site-specific constants, paths, and defaults.  pytokio should import
correctly and all connectors should be functional in the absence of a
configuration file.

If the PyYAML package is available, the configuration file may reference
environment variables which get correctly resolved during import.  This requires
that the configuration file only reference $VARIABLES in unquoted strings;
quoted strings (such as those when loading JSON-formatted YAML) will not be
expanded.

A subset of configuration parameters can be overridden by environment variables
prefixed with PYTOKIO_.
"""

import os
import re
import json
from tokio.common import isstr
HAVE_YAML = True
try:
    import yaml
except ImportError:
    HAVE_YAML = False

#: Global variable containing the configuration
CONFIG = {} 

#: Path to configuration file to load
PYTOKIO_CONFIG_FILE = "" 

#: Path of default site configuration file
DEFAULT_CONFIG_FILE = ""

#: Config parameters that can be overridden using PYTOKIO_* environment variable
MAGIC_VARIABLES = [
    'HDF5_FILES',
    'ISDCT_FILES',
    'LFSSTATUS_FULLNESS_FILES',
    'LFSSTATUS_MAP_FILES',
    'DARSHAN_LOG_DIRS',
    'ESNET_SNMP_URI'
]

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
        with open(PYTOKIO_CONFIG_FILE, 'rt') as config_file:
            if HAVE_YAML:
                loaded_config = load_and_expand_yaml(config_file)
            else:
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
    for _magic_variable in MAGIC_VARIABLES:
        _magic_value = os.environ.get("PYTOKIO_" + _magic_variable)
        if _magic_value is not None:
            try:
                _magic_value_decoded = json.loads(_magic_value)
            except ValueError:
                CONFIG[_magic_variable.lower()] = _magic_value
            else:
                CONFIG[_magic_variable.lower()] = _magic_value_decoded

def load_and_expand_yaml(file_handle):
    """Loads YAML config file and expands all environment variables contained
    therein

    Args:
        file_handle (file): File containing YAML to load
    """
    path_matcher = re.compile(r'.*\$\{([^}^{]+)\}.*')

    def path_constructor(loader, node):
        return os.path.expandvars(node.value)

    class EnvVarLoader(yaml.SafeLoader):
        pass

    EnvVarLoader.add_implicit_resolver('!path', path_matcher, None)
    EnvVarLoader.add_constructor('!path', path_constructor)

    return yaml.load(file_handle, Loader=EnvVarLoader)

init_config()
