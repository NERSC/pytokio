#!/usr/bin/env python
"""
Various functions that may be of use in analyzing TOKIO data.  These are
provided as a convenience rather than a set of core functionality.
"""

### Subpackages to include in the tokio.analysis.* namespace
try:
    import tokio.analysis.umami
except ImportError:
    pass
