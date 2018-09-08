#!/usr/bin/env python
"""
A higher-level interface that wraps various connectors and site-dependent
configuration to provide convenient abstractions upon which analysis tools can
be portably built.
"""

### Subpackages to include in the tokio.tools.* namespace
try:
    import tokio.tools.darshan
except ImportError:
    pass

try:
    import tokio.tools.hdf5
except ImportError:
    pass

try:
    import tokio.tools.topology
except ImportError:
    pass

try:
    import tokio.tools.lfsstatus
except ImportError:
    pass

### For backwards compatibility
try:
    import tokio.analysis.umami as umami
except ImportError:
    pass
