Installation
================================================================================

Downloading pytokio
--------------------------------------------------------------------------------

There are two ways to get pytokio:

1. The source distribution, which contains everything needed to install pytokio,
   use its bundled CLI tools, and begin developing new applications with it.
   This tarball is available on the `pytokio release page`_.

2. The full repository, which includes tests, example notebooks, and this
   documentation.  This is most easily obtained via git
   (``git clone https://github.com/nersc/pytokio``).

If you are just kicking the tires on pytokio, download #1.  If you want to
create your own connectors or tools, contribute to development, or run into any
issues that you would like to debug, install #2.

Editing the Site Configuration
--------------------------------------------------------------------------------

The ``site.json`` file, located in the ``tokio/`` directory, contains optional
parameters that allow various pytokio tools to automatically discover the
location of specific monitoring data and expose a more fully integrated feel
through its APIs.

The file is set up as JSON containing key-value pairs.  No one key has to be
specified (or set to a valid value), as each key is only consulted when a
specific tool requests it.  If you simply never use a tool, its configuration
keys will never be examined.

Configuration Options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As of pytokio 0.10, the following keys can be defined:

- lmt_timestep
    Number of seconds between successive measurements contained in
    the LMT database.  Only used by ``summarize_job`` tool to
    establish padding to account for cache flushes.
- mount_to_fsname
    Dictionary that maps mount points (expressed as regular expressions) to
    logical file system names.  Used by several CLI tools to made output more
    digestible for humans.
- fsname_to_backend_name
    Dictionary that maps logical file system names to backend file system names.
    Needed for cases where the name of a file system as described to users
    (e.g., "the scratch file system") has a different backend name ("snx11168")
    that monitoring tools may use.  Allows users to access data from file
    systems without knowing names used only by system admins.
- hdf5_files
    *Time-indexed file path template* describing where TOKIO Time Series HDF5
    files are stored, and where in the file path their timestamp is encoded.
- isdct_files
    *Time-indexed file path template* describing where NERSC-style ISDCT tar
    files files are stored, and where in the file path their timestamp is
    encoded.
- lfsstatus_fullness_files
    *Time-indexed file path template* describing where NERSC-style Lustre file
    system fullness logs are stored, and where in the file path their timestamp
    is encoded.
- lfsstatus_map_files
    *Time-indexed file path template* describing where NERSC-style Lustre file
    system OSS-OST mapping logs are stored, and where in the file path their
    timestamp is encoded.
- hpss_report_files
    *Time-indexed file path template* describing where HPSS daily report logs
    are stored, and where in the file path their timestamp is encoded.
- jobinfo_jobid_providers
    *Provider list* to inform which TOKIO connectors should be used to find job
    info through the :mod:`tokio.tools.jobinfo` API
- lfsstatus_fullness_providers
    *Provider list* to inform which TOKIO connectors should be used to find file
    system fullness data through the :mod:`tokio.tools.lfsstatus` API


Special Configuration Values
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are two special types of value described above:

**Time-indexed file path templates** are strings that describe a file path
that is passed through ``strftime`` with a user-specified time to resolve
where pytokio can find a specific file containing data relevant to that
time. Consider the following example: 

.. code-block:: none

        "isdct_files": "/global/project/projectdirs/pma/www/daily/%Y-%m-%d/Intel_DCT_%Y%m%d.tgz",

If pytokio is asked to find the ISDCT log file generated for January 14, 2017, it
will use this template string and try to extract the requested data from the
following file:

    /global/project/projectdirs/pma/www/daily/2017-01-14/Intel_DCT_20170114.tgz

Time-indexed file path templates need not only be strings; they can be lists or
dicts as well with the following behavior:

- str: search for files matching this template
- list of str: search for files matching each template
- dict: use the key to determine the element in the dictionary to use as the
  template.  That value is treated as a new template and is processed
  recursively.

This is documented in more detail in :meth:`tokio.tools.common.enumerate_dated_files`.

**Provider lists** are used by tools that can extract the same piece of
information from multiple data sources.  For example, :mod:`tokio.tools.jobinfo`
provides an API to convert a job id into a start and end time, and it can do this
by either consulting Slurm's `sacct` command or a site-specific jobs database.
The provider list for this tool would look like

.. code-block:: none

    "jobinfo_jobid_providers": [
        "slurm",
        "nersc_jobsdb"
    ],

where ``slurm`` and ``nersc_jobsdb`` are magic strings recognized by the
:meth:`tokio.tools.jobinfo.get_job_startend` function.

Installing pytokio
--------------------------------------------------------------------------------

pytokio can be used either as an installed Python package or as just an
unraveled tarball.  It has no components that require compilation and its only
path-dependent component is ``site.json`` which can be overridden using the
``PYTOKIO_CONFIG`` environment variable.

As described above, installing the Python package is accomplished by any one of
the following::

    $ pip install /path/to/pytokio-0.10.1/
    $ pip install --user /path/to/pytokio-0.10.1/
    $ cd /path/to/pytokio-0.10.1/ && python setup.py install --prefix=/path/to/installdir

You may also wish to install a single packaged blob.  In these cases though,
you will not be able to edit the default ``site.json`` and will have to create
an external ``site.json`` and define its path in the ``PYTOKIO_CONFIG``
environment variable::

    $ pip install pytokio
    $ pip install /path/to/pytokio-0.10.1.tar.gz
    $ vi ~/pytokio-config.json
    ...
    $ export PYTOKIO_CONFIG=$HOME/pytokio-config.json

For this reason, pytokio is not distributed as wheels or eggs.  While they
should work without problems when ``PYTOKIO_CONFIG`` is defined (or you never
use any features that require looking up configuration values), installing
such bdists is not officially supported.

Testing the Installation
--------------------------------------------------------------------------------

The `pytokio git repository`_ contains a comprehensive, self-contained test
suite in its tests/ subdirectory that can be run after installation if `nose`_
is installed::

    $ pip install /path/to/pytokio-0.10.1
    ...
    $ git clone https://github.com/nersc/pytokio
    $ cd pytokio/tests
    $ ./run_tests.sh
    ........

This test suite also contains a number of small sample inputs in the
tests/inputs/ subdirectory that may be helpful for basic testing.

.. _pytokio release page: https://github.com/NERSC/pytokio/releases
.. _pytokio git repository: https://github.com/NERSC/pytokio
.. _nose: https://nose.readthedocs.io
