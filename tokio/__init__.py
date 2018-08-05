#!/usr/bin/env python
"""
The Total Knowledge of I/O (TOKIO) reference implementation, pytokio.
"""

import tokio.timeseries as timeseries

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
