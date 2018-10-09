#
#  To install:
#
#    python setup.py install
#
#  or
#
#    python setup.py install --prefix=/path/to/prefix
#
#  To create an egg:
#
#    python -c "import setuptools; execfile('setup.py')" bdist_egg
#
from distutils.core import setup

setup(
    name='pytokio',
    description= 'Total Knowledge of I/O',
    version='0.10',
    author='Glenn K. Lockwood',
    author_email='glock@lbl.gov',
    license='BSD',
    packages=['tokio', 'tokio.connectors', 'tokio.tools', 'tokio.analysis'],
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
    ],
    package_data={'tokio': ['site.json']},
    keywords='I/O performance monitoring'
)
