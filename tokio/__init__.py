#!/usr/bin/env python
"""
TODO: add support for external configuration
"""

import tokio.timeseries

try:
    from .connectors.hdf5 import HDF5
except ImportError:
    pass
try:
    from .connectors.lmtdb import LMTDB
except ImportError:
    pass

from .config import *
from .debug import *
