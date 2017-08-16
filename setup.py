from distutils.core import setup

setup(
    name='tokio',
    description= 'Total Knowledge of I/O',
    version='0.0.1',
    author='Glenn K. Lockwood',
    author_email = 'glock@nersc.gov',
    license='None',
    packages=['tokio', 'tokio.connectors', 'tokio.tools'],
    classifiers=[
        'Programming Language :: Python :: 2.7',
    ],
    keywords='I/O NERSC performance'
)
