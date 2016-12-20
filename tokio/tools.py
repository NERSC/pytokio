#!/usr/bin/env python
"""
Miscellaneous tools that are functionally handy for dealing with files and data
sources used by the TOKIO framework.  This particular package is not intended to
have a stable API; as different functions prove themselves here, they will be
incorporated into the formal TOKIO package API.
"""

import os
import tokio
import time
import datetime
import h5py
import numpy as np

if os.environ.get("H5LMT_BASE"):
    H5LMT_BASE = os.environ.get("H5LMT_BASE")
else:
    ### default location at NERSC
    H5LMT_BASE = "/global/project/projectdirs/pma/www/daily"

def enumerate_h5lmts( file_name, datetime_start, datetime_end=None ):

    if datetime_end is None:
        datetime_end_local = datetime_start
    else:
        datetime_end_local = datetime_end

    today = datetime_start
    h5lmt_files = []
    while today <= datetime_end_local:
        h5lmt_file = os.path.join( H5LMT_BASE, today.strftime("%Y-%m-%d"), file_name )
        if os.path.isfile( h5lmt_file ):
            h5lmt_files.append( h5lmt_file )

        today += datetime.timedelta(days=1)

    if datetime_end is None:
        if len(h5lmt_files) == 0:
            return None
        else:
            return h5lmt_files[0]
    else:
        return h5lmt_files

if __name__ == '__main__':
    pass
