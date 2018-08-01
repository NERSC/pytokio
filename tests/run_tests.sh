#!/usr/bin/env bash

# Always run tests in the test directory since site.json contains relative paths
TEST_DIR="$(dirname $(readlink -f ${BASH_SOURCE[0]}))"
cd "$TEST_DIR"

# This is required to work around a known bug in pytokio
export TZ=America/Los_Angeles

# nosetests cannot reliably pass environment variables between tests
export PYTOKIO_CONFIG="${TEST_DIR}/site.json"

nosetests --cover-package=tokio,tokiobin $@
