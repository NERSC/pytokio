#!/usr/bin/env python
"""
Test the functionality of the pytokio REST API.
"""

# Currently not implemented.  Test by hand by first launching the REST API:
#
#   PYTOKIO_H5LMT_BASE_DIR=$(greadlink -f ../tests/inputs)/%Y-%m-%d ./pytokio.py
#
# Then accessing the API using the test data:
#
#   http://localhost:18880/hdf5/scratch2/mdscpu?start=1490012544&end=1490016603
