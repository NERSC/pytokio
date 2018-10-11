Installation
================================================================================

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

    $ pwd
    .../pytokio-0.10.1

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
    $ python

Then verify that pytokio can be imported::

    $ python
    >>> import tokio
    >>> tokio.__version__
    '0.10.1'

pytokio supports both Python 2.7 and 3.6 and, at minimum, requires h5py, numpy,
and pandas.  The full requirements are listed in ``requirements.txt``.

.. _pytokio release page: https://github.com/NERSC/pytokio/releases

Code Distributions
--------------------------------------------------------------------------------

There are two ways to get pytokio:

1. The source distribution, which contains everything needed to install pytokio
   and begin developing new applications with it

2. The full source repository, which includes tests, example notebooks, and this
   documentation

Site Configuration (site.json)
--------------------------------------------------------------------------------

The ``site.json`` file, located in the ``tokio/`` directory, contains optional
parameters that allow pytokio to automatically discover the location of specific
monitoring.
