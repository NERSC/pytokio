#!/usr/bin/env python
"""
A higher-level interface that wraps various connectors and site-dependent
configuration to provide convenient abstractions upon which analysis tools can
be portably built.
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
    import tokio.analysis.umami as umami
except ImportError:
    pass
