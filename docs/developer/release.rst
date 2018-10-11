pytokio Release Process
================================================================================

Testing on Docker
--------------------------------------------------------------------------------

Start a Docker image::

    docker run -it ubuntu bash

Use the Ubuntu docker image::

    apt-get update
    apt-get install git wget tzdata python-tk python-nose python-pip

Then download and install the release candidate's sdist tarball::

    wget https://test-files.pythonhosted.org/packages/.../pytokio-0.10.1b2.tar.gz
    pip install pytokio-0.10.1b2.tar.gz
    
Then download the git repo and remove the package contents from it (we only want
the tests)::

    git clone -b rc https://github.com/nersc/pytokio
    cd pytokio
    rm -rf tokio

Finally, run the tests to ensure that the install contained everything needed to
pass the tests::

    cd tests
    ./run_tests.sh

Travis should be doing most of this already; the main thing Travis does *not* do
is delete the ``tokio`` library subdirectory to ensure that its contents are not
being relied upon by any tests.

In the future, these tests (and tokiobin) should be merged into the tokio
library itself so that tests, test data, and CLI interfaces are all bundled as
part of the installation.


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
