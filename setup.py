from setuptools import setup, find_packages

setup(
    name='tokio',
    description= 'Total Knowledge of I/O',
    version='0.0.1',
    author='Glenn K. Lockwood',
    author_email = 'glock@nersc.gov', 
    license='None',
    packages=find_packages(exclude=['abcutil', 'examples', 'tests']),
    classifiers=[
        'Programming Language :: Python :: 2.7',
    ],
    keywords='I/O NERSC performance'
)
