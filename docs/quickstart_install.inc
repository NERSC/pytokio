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

.. _pytokio release page: https://github.com/NERSC/pytokio/releases
