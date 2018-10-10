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
#  To create an egg: python setup.py bdist_egg
#
import os
import re
import glob

DOCLINES = (__doc__ or '').split("\n")

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

if os.path.exists('MANIFEST'):
    os.remove('MANIFEST')

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
            return match.group(1)
        else:
            raise RuntimeError("Unable to find version string")
    else:
        raise RuntimeError("Unable to find version string")

def setup_package():
    from setuptools import setup

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
        packages=['tokio', 'tokio.connectors', 'tokio.tools', 'tokio.analysis'],
        scripts=include_scripts, # TODO: convert to console_scripts
        platforms=["Linux", "MacOS-X"],
        install_requires=open(os.path.join(BASE_DIR, 'requirements.txt'), 'r').readlines(),
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

    setup(**METADATA)

if __name__ == "__main__":
    setup_package()
