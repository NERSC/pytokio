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

H5LMT_BASE_DIR = os.environ.get('PYTOKIO_H5LMT_BASE')
LFSSTATUS_BASE_DIR = os.environ.get('PYTOKIO_LFSSTATUS_BASE')

_CONFIG = json.load(open(PYTOKIO_CONFIG, 'r'))
# Convert config json into a set of top-level constants
for _key, _value in _CONFIG.iteritems():
    if not getattr(sys.modules[__name__], _key.upper(), False):
        setattr(sys.modules[__name__], _key.upper(), _value)
