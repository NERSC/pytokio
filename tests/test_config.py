#!/usr/bin/env python
"""
Ensure that the tokio site config loads correctly
"""

import os
import json
import nose
import tokiotest
import tokio.config

# This should be defined in the sample config included in this test suite, but
# NOT in the config that ships with pytokio
DEADBEEF_KEY = "debug_dummy"
DEADBEEF_VALUE = 0xDEADBEEF

### tokio.config settings that should be settable via environment variables
MAGIC_VARIABLES = {
    'H5LMT_BASE_DIR': os.path.join('abc', 'def'),
    'LFSSTATUS_BASE_DIR': os.path.join('ghi', 'klmno', 'p'),
}

def magic_variable(variable, value):
    """
    Set an environment variable, load tokio.config, and ensure that the
    environment variable was correctly picked up.
    """
    os.environ["PYTOKIO_" + variable] = value
    reload(tokio.config)
    del os.environ["PYTOKIO_" + variable]

    print "%s: Supposed to be [%s], actual runtime value is [%s]" % (
        variable,
        value,
        getattr(tokio.config, variable))
    assert getattr(tokio.config, variable) == value

def compare_config_to_runtime(config_file):
    """
    Given the path to a config.json, ensure that its contents are what were
    loaded into the tokio.config namespace
    """
    # Verify that the config file tokio.config loaded is a real file
    assert tokio.config.PYTOKIO_CONFIG
    assert os.path.isfile(tokio.config.PYTOKIO_CONFIG)
    # Verify that the loaded config wasn't empty
    assert len(tokio.config._CONFIG) > 0
    # Load the reference file and compare its contents to the tokio.config namespace
    config_contents = json.load(open(config_file, 'r'))
    for key, value in config_contents.iteritems():
        runtime_value = getattr(tokio.config, key.upper())
        print "Verifying tokio.config.%s=%s(%s)" % (key.upper(), value, type(value))
        print "Loaded value: %s(%s)" % (runtime_value, type(runtime_value))
        assert tokio.config._CONFIG[key] == value
        assert getattr(tokio.config, key.upper()) == value

def test_default_config():
    """
    Load config file from default location
    """
    reload(tokio.config)
    compare_config_to_runtime(tokio.config.PYTOKIO_CONFIG)

def test_configfile_env():
    """
    Load config file from PYTOKIO_CONFIG
    """
    config_path = os.path.join(tokiotest.INPUT_DIR, 'sample_config.json')

    os.environ["PYTOKIO_CONFIG"] = config_path
    print "Set PYTOKIO_CONFIG to %s" % os.environ["PYTOKIO_CONFIG"]
    reload(tokio.config)
    print "tokio.config.PYTOKIO_CONFIG = %s" % tokio.config.PYTOKIO_CONFIG
    del os.environ["PYTOKIO_CONFIG"]

    assert tokio.config.PYTOKIO_CONFIG == config_path
    compare_config_to_runtime(config_path)
    assert getattr(tokio.config, DEADBEEF_KEY.upper()) == DEADBEEF_VALUE

def test_config_magic_variable():
    """
    Use of magic overriding variable
    """
    for variable, value in MAGIC_VARIABLES.iteritems():
        yield magic_variable, variable, value

def test_config_post_load():
    """
    Change tokio configuration after it is loaded
    """
    pass
