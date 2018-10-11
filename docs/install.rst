Installation
================================================================================

Quick Start
--------------------------------------------------------------------------------
pytokio has a single site-specific configuration file::

    tokio/site.json

which you may wish to edit and configure to match your site's file system names
and the naming conventions.  Most of these parameters are only required for the
higher-level convenience tools, so editing this is not essential to getting
started.

Once you've edited ``tokio/site.json`` to your liking, simply do::

    $ pip install .

or::

    $ python setup.py install --prefix=/path/to/installdir

Alternatively, pytokio does not technically require a proper installation and it
is sufficient to clone the git repo, add it to ``PYTHONPATH``, and
``import tokio`` from there.

pytokio supports both Python 2.7 and 3.6 and, at minimum, requires h5py, numpy,
and pandas.  The full requirements are listed in ``requirements.txt``.

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
