#!/usr/bin/env python
"""
Load the pytokio configuration file, which is encoded as json and contains
various site-specific constants, paths, and defaults.
"""

import os
import sys
import json

# Load a pytokio config from a special location
PYTOKIO_CONFIG = os.environ.get(
    'PYTOKIO_CONFIG',
    os.path.join(os.path.abspath(os.path.dirname(__file__)), 'site.json'))

# Load pytokio config file and convert its keys into a set of constants
_CONFIG = json.load(open(PYTOKIO_CONFIG, 'r'))
for _key, _value in _CONFIG.iteritems():
    # config keys beginning with an underscore get skipped
    if _key.startswith('_'):
        pass

    # if setting this key will overwrite something already in the tokio.config
    # namespace, only overwrite if the existing object is something we probably
    # loaded from a json
    _old_attribute = getattr(sys.modules[__name__], _key.upper(), None)
    if _old_attribute is None \
    or isinstance(_old_attribute, basestring) \
    or isinstance(_old_attribute, dict) \
    or isinstance(_old_attribute, list):
        setattr(sys.modules[__name__], _key.upper(), _value)

# Check for magic environment variables to override the contents of the config
# file at runtime
for _magic_variable in ['H5LMT_BASE_DIR', 'LFSSTATUS_BASE_DIR', 'LFSSTATUS_FULLNESS_FILE', 'LFSSTATUS_MAP_FILE']:
    _magic_value = os.environ.get("PYTOKIO_" + _magic_variable)
    if _magic_value is not None:
        setattr(sys.modules[__name__], _magic_variable, _magic_value)
