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

1. Downloading pytokio
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

2. Editing the Site Configuration
--------------------------------------------------------------------------------

The ``site.json`` file, located in the ``tokio/`` directory, contains optional
parameters that allow various pytokio tools to automatically discover the
location of specific monitoring data and expose a more fully integrated feel
through its APIs.

The file is set up as JSON containing key-value pairs.  No one key has to be
specified (or set to a valid value), as each key is only consulted when a
specific tool requests it.  If you simply never use a tool, its configuration
keys will never be examined.

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

2. Installing pytokio
--------------------------------------------------------------------------------

* bdists are not currently supported

3. Testing the Installation
--------------------------------------------------------------------------------

* download the test suite

.. _pytokio release page: https://github.com/NERSC/pytokio/releases
