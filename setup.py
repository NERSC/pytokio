from setuptools import setup, find_packages

setup(
    name='tokio',
    description= 'Total Knowledge of I/O',
    version='0.0.1',
    author='Glenn K. Lockwood',
    author_email = 'glock@nersc.gov',
    install_requires=['matplotlib==2.0.0',
                      'pandas==0.18.1',
                      'numpy==1.11.1',
                      'scipy==0.17.1',                     
                  ],
    license='None',
    packages=find_packages(exclude=['bin', 'examples', 'tests']),
    classifiers=[
        'Programming Language :: Python :: 2.7',
    ],
    keywords='I/O NERSC performance'
)
