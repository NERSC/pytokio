#!/usr/bin/env bash

# For non-GNU systems
READLINK=${READLINK-readlink}

# Always run tests in the test directory since site.json contains relative paths
TEST_DIR="$(dirname $($READLINK -f ${BASH_SOURCE[0]}))"
cd "$TEST_DIR"

# This is required to work around a known bug in pytokio
export TZ=America/Los_Angeles

# nosetests cannot reliably pass environment variables between tests
export NERSC_HOST="edison"
export PYTOKIO_CONFIG="${TEST_DIR}/site.json"
echo "Test environment will load configuration from $PYTOKIO_CONFIG"

nosetests --cover-package=tokio,tokiobin $@
