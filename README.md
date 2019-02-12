TOKIO - Total Knowledge of I/O
================================================================================

pytokio is a Python implementation of the TOKIO framework.
The full documentation can be found at https://pytokio.readthedocs.io/en/latest/

Installation
--------------------------------------------------------------------------------

pytokio has a single site-specific configuration file:

    tokio/site.json

which you may wish to edit and configure to match your site's file system names
and the naming conventions.  Most of these parameters are only required for the
higher-level convenience tools, so editing this is not essential to getting
started.

Once you've edited `tokio/site.json` to your liking, simply do

    $ pip install .

or

    $ python setup.py install --prefix=/path/to/installdir

Alternatively, pytokio does not technically require a proper installation and it
is sufficient to clone the git repo, add it to `PYTHONPATH`, and `import tokio`
from there.  If you wish to use the pytokio CLI tools without properly installing
pytokio, also add the git repo's `bin/` subdirectory to `PATH`.

pytokio supports both Python 2.7 and 3.6 and, at minimum, requires h5py, numpy,
and pandas.  The full requirements are listed in `requirements.txt`.

Quick Start
--------------------------------------------------------------------------------

pytokio is a Python library that provides the APIs necessary to develop analysis
routines that combine data from different I/O monitoring tools that may be
available in your data center.  However several simple utilities are included to
demonstrate how pytokio can be used in the `bin/` directory.

Additionally, the pytokio git repository contains several other examples and
tests to demonstrate the ways in which pytokio can be used.

- `examples/` contains standalone Jupyter notebooks and scripts that illustrate
  different aspects of the pytokio API that do useful things.  They are designed
  to run on NERSC systems via https://jupyter.nersc.gov/.
- `tests/` contains unit and integration tests for the pytokio library and
   the scripts bundled in `/bin`

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
[![Documentation Status](https://readthedocs.org/projects/pytokio/badge/?version=latest)](https://pytokio.readthedocs.io/en/latest/?badge=latest)
[![Coverage Status](https://coveralls.io/repos/github/NERSC/pytokio/badge.svg?branch=master)](https://coveralls.io/github/NERSC/pytokio?branch=master)
