TOKIO - Total Knowledge of I/O
================================================================================

This a reference implementation of the TOKIO framework composed as a Python
package.  The full documentation is located on the [pytokio GitHub wiki][].

Contents
--------------------------------------------------------------------------------

- `bin/` directory contains useful tools implemented on top of pytokio. For instance 
   summary_job is a script on which TOKIO-ABC relies on to retrieve data from the
   pytokio package.
- `examples/` contains standalone Jupyter notebooks and scripts that illustrate
  different aspects of the pytokio API that do useful things.  These
  demonstrate common use patterns and common ways multiple pytokio components
  can be strung together.
- `tests/` contains unit tests, integration tests, and code that is in the
  process of being converted to such unit/integration tests.
- `pytokio/` is the Python package itself.  Import this to access the pytokio
  API

License
--------------------------------------------------------------------------------
This software is currently not licensed for public distribution.  As such it can
only be downloaded and used by employees of the Lawrence Berkeley National
Laboratory.

[pytokio GitHub wiki]: https://github.com/NERSC/pytokio/wiki
