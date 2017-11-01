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
    include_package_data=True,
    keywords='I/O NERSC performance'
)
