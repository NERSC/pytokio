TOKIO - Total Knowledge of I/O
================================================================================

This a reference implementation of the TOKIO framework composed as a Python
package.  The full documentation is located on the [pytokio GitHub wiki][].

Installation
--------------------------------------------------------------------------------

pytokio has a single site-specific configuration file:

    tokio/site.json

which you may wish to edit and configure to match your site's file system names
and the naming conventions.  Most of these parameters are only required for the
higher-level convenience tools, so editing this is not essential to getting
started.

Then simply

    $ pip install .
    $ pip install -r requirements.txt

or
    $ python setup.py install --prefix=/path/to/installdir

To create an egg file,

    $ python -c "import setuptools; execfile('setup.py')" bdist_egg

Quick Start
--------------------------------------------------------------------------------

pytokio is a python library that does not do anything by itself; it provides the
APIs necessary to develop analysis routines.  However several simple utilities
are included to demonstrate how pytokio can be used.

- `bin/` directory contains useful tools implemented on top of pytokio
- `examples/` contains standalone Jupyter notebooks and scripts that illustrate
  different aspects of the pytokio API that do useful things.  They are designed
  to run on NERSC systems via https://jupyter.nersc.gov/.
- `tests/` contains unit and integration tests for the pytokio library and
   the scripts bundled in `/bin`
- `tokio/` is the importable Python package itself

Copyright and License
--------------------------------------------------------------------------------

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

For license terms, please see `LICENSE.md` included in this repository.

[![Build Status](https://travis-ci.org/NERSC/pytokio.svg?branch=master)](https://travis-ci.org/NERSC/pytokio)

[pytokio GitHub wiki]: https://github.com/NERSC/pytokio/wiki
