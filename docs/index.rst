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

Quick Start
--------------------------------------------------------------------------------

**Step 1. Download pytokio**: Download the latest pytokio from the
`pytokio release page`_ and unpack it somewhere::

    $ wget https://github.com/NERSC/pytokio/releases/download/v0.10.1/pytokio-0.10.1.tar.gz
    $ tar -zxf pytokio-0.10.1.tar.gz

**Step 2. (Optional): Configure `site.json`**: pytokio ships with a ``site.json``
configuration file that's located in the tarball's ``tokio/`` subdirectory.  You
can edit this to reflect the location of various data sources and configurations
on your system::

    $ vi pytokio-0.10.1/tokio/site.json
    ...

However it is also perfectly fine to not worry about this now, as this file is
only used for higher-level interfaces.

**Step 3. Install pytokio**: Install the pytokio package using your favorite
package installation mechanism::

    $ ls
    pytokio-0.10.1        pytokio-0.10.1.tar.gz

    $ pip install pytokio-0.10.1/

or::

    $ cd pytokio-0.10.1/
    $ python setup.py install --prefix=/path/to/installdir

or::

    $ cd pytokio-0.10.1/
    $ pip install --user .

Alternatively, pytokio does not technically require a proper installation and it
is sufficient to clone the git repo, add it to ``PYTHONPATH``, and
``import tokio`` from there::

    $ cd pytokio-0.10.1/
    $ export PYTHONPATH=$PYTHONPATH:`pwd`

Then verify that pytokio can be imported::

    $ python
    >>> import tokio
    >>> tokio.__version__
    '0.10.1'

pytokio supports both Python 2.7 and 3.6 and, at minimum, requires h5py, numpy,
and pandas.  The full requirements are listed in ``requirements.txt``.

**Step 4. (Optional) Test pytokio CLI tools**: pytokio includes some basic CLI
wrappers around many of its interfaces which are installed in your Python
package install directory's ``bin/`` directory::

    $ export PATH=$PATH:/path/to/installdir/bin
    $ cache_darshanlogs.py --perf /path/to/a/darshanlog.darshan
    {
        "counters": {
            "mpiio": {
                ...

Because pytokio is a *framework* for tying together different data sources,
exactly which CLI tools will work on your system is dependent on what data
sources are available to you.  Darshan is perhaps the most widely deployed
source of data.  If you have Darshan logs collected in a central location on
your system, you can try using pytokio's ``summarize_darshanlogs.py`` tool to
create an index of all logs generated on a single day::

    $ summarize_darshanlogs.py /global/darshanlogs/2018/10/8/fbench_*.darshan
    {
    "/global/darshanlogs/2018/10/8/fbench_IOR_CORI2_id15540806_10-8-6559-7673881787757600104_1.darshan": {
        "/global/project": {
            "read_bytes": 0, 
            "write_bytes": 206144000000
        }
    }, 
    ...

All pytokio CLI tools' options can be displayed by running them with the ``-h``
option.

Finally, if you have downloaded the entire pytokio repository, there are some
sample Darshan logs (and other files) in the ``tests/inputs`` directory which
you can also use to verify basic functionality.


.. toctree::
    :maxdepth: 2
    :caption: Contents:

.. toctree::
    :caption: User Guide
    :maxdepth: 2

    install

.. toctree::
    :caption: API Documentation
    :maxdepth: 2

    tokio

.. toctree::
    :maxdepth: 1
    :caption: Developer Documentation

    developer/release

.. _pytokio architecture paper: https://escholarship.org/uc/item/8j14j182
.. _pytokio release page: https://github.com/NERSC/pytokio/releases

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

