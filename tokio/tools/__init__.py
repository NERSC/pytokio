#!/usr/bin/env python
"""
Miscellaneous tools that are functionally handy for dealing with files and data
sources used by the TOKIO framework.  This particular package is not intended to
have a stable API; as different functions prove themselves here, they will be
incorporated into the formal TOKIO package API.
"""

### Subpackages to include in the tokio.tools.* namespace
try:
    import hdf5
except ImportError:
    pass

try:
    import topology
except ImportError:
    pass

try:
    import lfsstatus
except ImportError:
    pass

try:
    import umami
except ImportError:
    pass
