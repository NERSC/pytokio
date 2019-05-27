.. pytokio documentation master file, created by
   sphinx-quickstart on Wed Oct 10 16:47:48 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

pytokio |version|
================================================================================

pytokio is a Python library that provides the APIs necessary to develop analysis
routines that combine data from different I/O monitoring tools that may be
available in your HPC data center.  The design and capabilities of pytokio have
been documented in the `pytokio architecture paper`_ presented at the
2018 Cray User Group.

.. include:: quickstart_install.inc

.. toctree::
    :maxdepth: 2
    :caption: Contents:

.. toctree::
    :caption: User Guide
    :maxdepth: 2

    user/install
    user/architecture
    user/command-line
    user/timeseries

.. toctree::
    :caption: API Documentation
    :maxdepth: 2

    api/tokio
    api/tokio.analysis
    api/tokio.cli
    api/tokio.connectors
    api/tokio.tools

.. toctree::
    :maxdepth: 1
    :caption: Developer Documentation

    developer/release



.. _pytokio architecture paper: https://escholarship.org/uc/item/8j14j182

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

Copyright
================================================================================

Total Knowledge of I/O Copyright (c) 2017, The Regents of the University of
California, through Lawrence Berkeley National Laboratory (subject to receipt
of any required approvals from the U.S. Dept. of Energy).  All rights reserved.

If you have questions about your rights to use or distribute this software,
please contact Berkeley Lab's Innovation & Partnerships Office at IPO@lbl.gov.

NOTICE.  This Software was developed under funding from the U.S. Department of
Energy and the U.S. Government consequently retains certain rights. As such,
the U.S. Government has been granted for itself and others acting on its behalf
a paid-up, nonexclusive, irrevocable, worldwide license in the Software to
reproduce, distribute copies to the public, prepare derivative works, and
perform publicly and display publicly, and to permit other to do so.

