"""pytokio is a framework that enables holistic analysis of telemetry from HPC systems

pytokio is a reference implementation of the Total Knowledge of I/O (TOKIO)
framework designed to simplify the process of holistically analyzing the
performance of parallel storage systems commonly deployed in high-performance
computing.  It provides connectors to interface with many common monitoring
tools, analysis routines to derive insight from multiple tools, and example
applications that demonstrate simple but powerful performance analyses enabled
by this holistic approach.

pytokio is made available under a modified BSD license.  See
https://pytokio.readthedocs.io/en/latest/ for the full documentation.
"""

#
#  To install: python setup.py install
#              python setup.py install --prefix=/path/to/prefix
#
#  To package: python setup.py sdist
#                              bdist
#                              bdist_egg
#
#  Making bdists is not supported because site.json cannot be correctly
#  resolved.  If you choose to use a bdist, you must explicitly set
#  PYTOKIO_CONFIG to point to a valid site.json.
#
import os
import re
import glob
import subprocess

RELEASE = False # set to True when building a release distribution

REQUIREMENTS = [
    "h5py>=2.7",
    "matplotlib>=2.0.0",
    "numpy>=1.13",
    "pandas>=0.20",
    "scipy>=0.19",
]

DOCLINES = (__doc__ or '').split("\n")
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

if os.path.exists('MANIFEST'):
    os.remove('MANIFEST')

def setup_package():
    import setuptools

    include_scripts = [os.path.relpath(x, BASE_DIR) for x in glob.glob(os.path.join(BASE_DIR, 'bin', '*')) if '__init__' not in x]
    print("Including scripts:")
    print('\n  '.join(include_scripts))

    METADATA = dict(
        name='pytokio',
        version=find_version(),
        author='Glenn K. Lockwood et al.',
        author_email='glock@lbl.gov',
        description=DOCLINES[0],
        long_description="\n".join(DOCLINES[2:]),
        url="http://www.nersc.gov/research-and-development/tokio/",
        download_url="https://www.github.com/nersc/pytokio",
        license='BSD',
#       packages=['tokio', 'tokio.connectors', 'tokio.tools', 'tokio.analysis'],
        packages=setuptools.find_packages(exclude=['bin']),
        scripts=include_scripts, # TODO: convert to console_scripts
        # If we want to keep site.json at the top-level and copy it in during
        # install time.  This would force users to correctly install pytokio
        # before it could be used though, which is not strictly necessary for
        # any other purpose.
        # data_files=[('tokio', ['site.json'])],
        data_files=[('tokio', ['tokio/site.json'])],
        platforms=["Linux", "MacOS-X"],
        install_requires=REQUIREMENTS,
        extras_require={
            'collectdes': ['elasticsearch>=5.4'],
        },
        python_requires=">=2.7",
        classifiers=[
            'Intended Audience :: Science/Research',
            'Programming Language :: Python',
            'Programming Language :: Python :: 2',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3',
            'Topic :: Software Development',
            'Topic :: Scientific/Engineering',
            'Operating System :: POSIX',
            'Operating System :: Unix',
            'Operating System :: MacOS',
        ],
        keywords='I/O performance monitoring'
    )

    setuptools.setup(**METADATA)

def find_version():
    """Extract the package version number from __init__.py

    See https://packaging.python.org/guides/single-sourcing-package-version
    """
    init_file = os.path.join(BASE_DIR, "tokio", "__init__.py")
    if os.path.isfile(init_file):
        match = re.search(r"^__version__\s*=\s*['\"]([^'\"]+?)['\"]",
                          open(init_file, 'r').read(),
                          re.M)
        if match:
            version = match.group(1)
            if not RELEASE:
                revision = git_version()
                version = "%s.dev0+%s" % (version, revision[:7])
            return version
        else:
            raise RuntimeError("Unable to find version string")
    else:
        raise RuntimeError("Unable to find version string")


# Return the git revision as a string
def git_version():
    def _minimal_ext_cmd(cmd):
        # construct minimal environment
        env = {}
        for k in ['SYSTEMROOT', 'PATH', 'HOME']:
            v = os.environ.get(k)
            if v is not None:
                env[k] = v
        # LANGUAGE is used on win32
        env['LANGUAGE'] = 'C'
        env['LANG'] = 'C'
        env['LC_ALL'] = 'C'
        out = subprocess.Popen(cmd, stdout=subprocess.PIPE, env=env).communicate()[0]
        return out

    try:
        out = _minimal_ext_cmd(['git', 'rev-parse', 'HEAD'])
        revision = out.strip().decode('ascii')
    except OSError:
        revision = "unknown"

    return revision

if __name__ == "__main__":
    setup_package()
