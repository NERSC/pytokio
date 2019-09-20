pytokio Release Process
================================================================================

Branching process
--------------------------------------------------------------------------------

General branching model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

What are the principal development and release branches?

- ``master`` contains complete features but is not necessarily bug-free
- ``rc`` contains stable code
- Version branches (e.g., ``0.12``) contain code that is on track for release

Where to commit code?

- All features should land in ``master`` once they are complete and pass tests
- ``rc`` should only receive merge or cherry-pick from ``master``, no other
  branches
- Version branches should only receive merge or cherry-pick from ``rc``, no
  other branches

How should commits flow between branches?

- ``rc`` should _never_ be merged back into master
- Version branches should _never_ be merged into rc
- Hotfixes that cannot land in master (e.g., because a feature they fix no
  longer exists) should go directly to the ``rc`` branch (if appropriate) and/or
  version branch.

General versioning guidelines
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The authoritative version of pytokio is contained in ``tokio/__init__.py`` and
nowhere else.
 
1. The ``master`` branch should always be at least one minor number above
   ``rc`` 
2. Both ``master`` and ``rc`` branches should have versions suffixed with
   ``.devX`` where ``X`` is an arbitrary integer
3. Only version branches (``0.11``, ``0.12``) should have versions that end in
   ``b1``, ``b2``, etc
4. Only version branches should have release versions (``0.11.0``)

Generally, the tip of a version branch should be one beta release ahead of what
has actually been released so that subsequent patches automatically have a
version reflecting a higher number than the last release.

Feature freezing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is done by 

1. Merge master into rc
2. In master, update version in ``tokio/__init__.py`` from ``0.N.0.devX`` to
   ``0.(N+1).0.dev1``
3. Commit to master

Cutting a first beta release
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Create a branch from ``rc`` called ``0.N``
2. In that branch, update the version from ``0.N.0.devX`` to ``0.N.0b1``
3. Commit to ``0.N``
4. Tag/release ``v0.N.0b1`` from GitHub's UI from the ``0.N`` branch
5. Update the version in ``0.N`` from ``0.N.0b1`` to ``0.N.0b2`` to prepare for
   a hypothetical next release
6. Commit to ``0.N``

Applying fixes to a beta release
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Merge changes into ``master`` if the fix still applies there.  Commit changes
   to ``rc`` if the fix still applies there, or commit to the version branch
   otherwise.
2. Cherry-pick the changes into downstream branches (``rc`` if committed to
   ``master``, version branch from ``rc``)

Cutting a second beta release
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Tag the version (``git tag v0.N.0-beta2``) on the ``0.N`` branch
2. ``git push --tags`` to send the new tag up to GitHub
3. **Make sure the tag passes all tests in Travis**
4. Build the source tarball using the release process described in the `Releasing pytokio`_
   section
5. Release ``v0.N.0-beta2`` from GitHub's UI and upload the tarball from the
   previous step
6. Update the version in ``0.N`` from ``0.N.0b2`` to ``0.N.0b3`` (or ``b4``, etc)
   to prepare for a hypothetical next release
7. Commit to ``0.N``

Releasing pytokio
--------------------------------------------------------------------------------

Ensure that the ``tokio.__version__`` (in ``tokio/__init__.py``) is correctly
set in the version branch from which you would like to cut a release.

Then edit ``setup.py`` and set ``RELEASE = True``.

Build the source distribution::

    python setup.py sdist

The resulting build should be in the ``dist/`` subdirectory.

It is recommended that you do this all from within a minimal Docker environment
for cleanliness.


Testing on Docker
--------------------------------------------------------------------------------

pytokio now includes a dockerfile which will can be used to build and test
pytokio in a clean environment.

To test pytokio directly from GitHub, you can::

    docker build -t pytokio_test https://github.com/nersc/pytokio.git

Or if you have the repository checked out locally,::

    docker build -t pytokio_test .

Once the image is built, simply::

    docker run -it pytokio_test

to execute the full test suite.

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
