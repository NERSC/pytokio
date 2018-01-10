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
    name='tokio',
    description= 'Total Knowledge of I/O',
    version='0.9',
    author='Glenn K. Lockwood',
    author_email = 'glock@lbl.gov',
    license='None',
    packages=['tokio', 'tokio.connectors', 'tokio.tools'],
    classifiers=[
        'Programming Language :: Python :: 2.7',
    ],
    package_data={'tokio': ['site.json']},
    keywords='I/O NERSC performance'
)
