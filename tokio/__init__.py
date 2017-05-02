#!/usr/bin/env python
"""
TODO: add support for external configuration
"""

import sys

DEBUG = False
LMT_TIMESTEP = 5.0

def _debug_print( string ):
    """
    Print debug messages if the module's global debug flag is enabled.
    """
    if DEBUG:
        sys.stderr.write( string + "\n" )
    return

def error( string ):
    """
    Handle errors generated within TOKIO.  Currently just a passthrough to
    stderr; should probably provide exceptions later on.
    """
    sys.stderr.write( string + "\n" )

def warning( string ):
    """
    Handle warnings generated within TOKIO.  Currently just a passthrough to
    stderr; should probably provide a more rigorous logging/reporting
    interface later on.
    """
    sys.stderr.write( string + "\n" )

### LMTDB requires MySQLdb
try:
    from connectors.lmt import LMTDB
except ImportError:
    pass

### HDF5 requires h5py
try:
    from connectors.hdf5 import HDF5
except ImportError:
    pass
