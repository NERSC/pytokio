#!/usr/bin/env python

import sys
from lmt import LMTDB

DEBUG = False
LMT_TIMESTEP = 5

def _debug_print( string ):
    if DEBUG:
        sys.stderr.write( string + "\n" )
    return
