#!/usr/bin/env python
"""
Connector interfaces for pytokio.  Each connector provides a Python interface
into one component-level monitoring tool.
"""

### The following modules are used internally and do not need to clutter up the namespace
# cachingdb
# common

### The following modules are specific to NERSC
# nersc_jobsdb
# collectd_es
# nersc_isdct
# nersc_lfsstate

try:
    import tokio.connectors.craysdb
except ImportError:
    pass

try:
    import tokio.connectors.darshan
except ImportError:
    pass

try:
    import tokio.connectors.hdf5
except ImportError:
    pass

try:
    import tokio.connectors.hpss
except ImportError:
    pass

try:
    import tokio.connectors.lfshealth
except ImportError:
    pass

try:
    import tokio.connectors.lmtdb
except ImportError:
    pass

try:
    import tokio.connectors.slurm
except ImportError:
    pass

try:
    import tokio.connectors.esnet_snmp
except ImportError:
    pass

try:
    import tokio.connectors.mmperfmon
except ImportError:
    pass
