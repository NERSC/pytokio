pytokio Release Process
================================================================================

Releasing pytokio
--------------------------------------------------------------------------------

First ensure that the ``tokio.__version__`` (in ``tokio/__init__.py``) is
correctly set.

Edit ``setup.py`` and set ``RELEASE = True``.

Build the source distribution::

    python setup.py sdist

The resulting build should be in the ``dist/`` subdirectory.  Test this using
Docker as below.


Testing on Docker
--------------------------------------------------------------------------------

Start a Docker image::

    host$ docker run -it ubuntu bash

Use the Ubuntu docker image::

    root@082cdfb246a1$ apt-get update
    root@082cdfb246a1$ apt-get install git wget tzdata python-tk python-nose python-pip

Then download and install the release candidate's sdist tarball::

    host$ docker ps
    ...

    host$ docker cp dist/pytokio-0.10.1b2.tar.gz 082cdfb246a1:root/

    root@082cdfb246a1$ pip install pytokio-0.10.1b2.tar.gz
    
Then download the git repo and remove the package contents from it (we only want
the tests)::

    root@082cdfb246a1$ git clone -b rc https://github.com/nersc/pytokio
    root@082cdfb246a1$ cd pytokio
    root@082cdfb246a1$ rm -rf tokio

Finally, run the tests to ensure that the install contained everything needed to
pass the tests::

    cd tests
    ./run_tests.sh

Travis should be doing most of this already; the main thing Travis does *not* do
is delete the ``tokio`` library subdirectory to ensure that its contents are not
being relied upon by any tests.


Packaging pytokio
--------------------------------------------------------------------------------

Create ``$HOME``/.pypirc with permissions ``0600x`` and contents::

    [pypi]
    username = <username>
    password = <password>

Then do a standard ``sdist build``::

    python setup.py sdist

and upload it to pypi::

    twine upload -r testpypi dist/pytokio-0.10.1b2.tar.gz
    
and ensure that ``testpypi`` is defined in .pypirc::

    [testpypi]
    repository = https://test.pypi.org/legacy/
    username = <username>
    password = <password>


More Info
--------------------------------------------------------------------------------

See https://packaging.python.org/guides/distributing-packages-using-setuptools/
