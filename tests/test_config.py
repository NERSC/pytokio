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

def flush_env():
    """
    Ensure that the runtime environment isn't tainted by magic variables before
    running a test.
    """
    for variable in os.environ.keys():
        if variable.startswith('PYTOKIO_'):
            print "Purging %s from runtime environment" % variable
            del os.environ[variable]

def magic_variable(variable, set_value):
    """
    Set an environment variable, load tokio.config, and ensure that the
    environment variable was correctly picked up.
    """
    os.environ["PYTOKIO_" + variable] = set_value
    reload(tokio.config)

    print "%s: Supposed to be [%s], actual runtime value is [%s]" % (
        variable,
        set_value,
        getattr(tokio.config, variable))
    assert getattr(tokio.config, variable) == set_value

def compare_config_to_runtime(config_file):
    """
    Given the path to a config.json, ensure that its contents are what were
    loaded into the tokio.config namespace
    """

    # Verify that the config file tokio.config loaded is a real file
    assert tokio.config.PYTOKIO_CONFIG
    assert os.path.isfile(tokio.config.PYTOKIO_CONFIG)

    # Verify that the loaded config wasn't empty
    assert len(tokio.config._CONFIG) > 0    #pylint: disable=protected-access

    # Load the reference file and compare its contents to the tokio.config namespace
    print "Comparing runtime config to %s" % config_file
    config_contents = json.load(open(config_file, 'r'))
    for key, expected_value in config_contents.iteritems():
        runtime_value = getattr(tokio.config, key.upper())
        print "Verifying tokio.config.%s:\n  %s == %s" % (key.upper(),
                                                          str(expected_value)[0:30],
                                                          str(runtime_value)[0:30])
        # Loaded correctly into the _CONFIG structure
        assert tokio.config._CONFIG[key] == expected_value    #pylint: disable=protected-access
        # Loaded correctly as a module attribute
        assert getattr(tokio.config, key.upper()) == expected_value

@nose.tools.with_setup(flush_env)
def test_default_config():
    """
    Load config file from default location
    """
    # Reload the module to force reinitialization from config
    reload(tokio.config)

    # Verify the loaded attributes are what was in the config file
    compare_config_to_runtime(tokio.config.PYTOKIO_CONFIG)

@nose.tools.with_setup(flush_env)
def test_configfile_env():
    """
    Load config file from PYTOKIO_CONFIG
    """
    config_path = os.path.join(tokiotest.INPUT_DIR, 'sample_config.json')

    os.environ["PYTOKIO_CONFIG"] = config_path
    print "Set PYTOKIO_CONFIG to %s" % os.environ["PYTOKIO_CONFIG"]
    reload(tokio.config)
    print "tokio.config.PYTOKIO_CONFIG = %s" % tokio.config.PYTOKIO_CONFIG

    assert tokio.config.PYTOKIO_CONFIG == config_path
    compare_config_to_runtime(config_path)
    assert getattr(tokio.config, DEADBEEF_KEY.upper()) == DEADBEEF_VALUE

@nose.tools.with_setup(flush_env)
def test_config_magic_variable():
    """
    Use of magic overriding variable
    """
    # Ensure that each magic environment variable is picked up correctly
    for variable, value in MAGIC_VARIABLES.iteritems():
        yield magic_variable, variable, value

@nose.tools.with_setup(flush_env)
def test_config_post_load():
    """
    Change tokio configuration after it is loaded
    """
    # Ensure that the runtime environment isn't tainted

    pass
