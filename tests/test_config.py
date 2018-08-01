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
    'HDF5_FILES': os.path.join('abc', 'def'),
    'LFSSTATUS_FULLNESS_FILES': os.path.join('ghi', 'klmno', 'p'),
    'LFSSTATUS_MAP_FILES': os.path.join('y', 'z', ''),
    'ISDCT_FILES': os.path.join('qrs', 'tuv', 'wx'),
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
    # this test is pointless if we are overriding the default config with the
    # same value as its default because it's impossible to verify that this
    # value was taken from the environment variable
    if getattr(tokio.config, variable) == set_value:
        raise Exception("test is broken; attempting to set a magic variable to its default value?")

    os.environ["PYTOKIO_" + variable] = set_value
    reload(tokio.config)

    print "%s: Supposed to be [%s], actual runtime value is [%s]" % (
        variable,
        set_value,
        getattr(tokio.config, variable))
    runtime_value = getattr(tokio.config, variable)
    assert type(runtime_value) == type(set_value) #pylint: disable=unidiomatic-typecheck
    assert runtime_value == set_value

def compare_config_to_runtime(config_file):
    """
    Given the path to a config.json, ensure that its contents are what were
    loaded into the tokio.config namespace
    """

    # Verify that the config file tokio.config loaded is a real file
    assert tokio.config.PYTOKIO_CONFIG
    assert os.path.isfile(tokio.config.PYTOKIO_CONFIG)

    # Verify that the loaded config wasn't empty
    assert len(tokio.config._CONFIG) > 0 #pylint: disable=protected-access

    # Load the reference file and compare its contents to the tokio.config namespace
    print "Comparing runtime config to %s" % config_file
    config_contents = json.load(open(config_file, 'r'))
    for key, expected_value in config_contents.iteritems():
        runtime_value = getattr(tokio.config, key.upper())
        print "Verifying tokio.config.%s:\n  [%s] == [%s]" % (
            key.upper(),
            str(expected_value),
            str(runtime_value))
        assert type(runtime_value) == type(expected_value) #pylint: disable=unidiomatic-typecheck
        assert runtime_value == expected_value

@nose.tools.with_setup(flush_env)
def test_default_config():
    """
    tokio.config: Load config file from default location
    """
    # Reload the module to force reinitialization from config
    reload(tokio.config)

    # Verify the loaded attributes are what was in the config file
    compare_config_to_runtime(tokio.config.PYTOKIO_CONFIG)

@nose.tools.with_setup(flush_env)
def test_configfile_env():
    """
    tokio.config: Load config file from PYTOKIO_CONFIG
    """
    config_file = os.path.join(tokiotest.INPUT_DIR, 'sample_config.json')

    os.environ["PYTOKIO_CONFIG"] = config_file
    print "Set PYTOKIO_CONFIG to %s" % os.environ["PYTOKIO_CONFIG"]
    reload(tokio.config)
    print "tokio.config.PYTOKIO_CONFIG = %s" % tokio.config.PYTOKIO_CONFIG

    assert tokio.config.PYTOKIO_CONFIG == config_file
    compare_config_to_runtime(config_file)
    assert getattr(tokio.config, DEADBEEF_KEY.upper()) == DEADBEEF_VALUE

@nose.tools.with_setup(flush_env)
def test_config_magic_variable():
    """
    tokio.config: Use of magic overriding variable
    """
    # Ensure that each magic environment variable is picked up correctly
    for variable, value in MAGIC_VARIABLES.iteritems():
        yield magic_variable, variable, value


@nose.tools.with_setup(flush_env)
def test_no_env_effects_post_load():
    """
    tokio.config: Magic variables don't affect runtime post-load
    """
    # First load and verify the default config
    test_default_config()

    # Then set a magic environment variable and assert that it is *not*
    # automatically picked up
    for variable, set_value in MAGIC_VARIABLES.iteritems():
        orig_value = getattr(tokio.config, variable)
        if orig_value == set_value:
            raise Exception("test is broken; trying to set a magic variable to its default value?")
        os.environ[variable] = set_value
        assert getattr(tokio.config, variable) == orig_value


@nose.tools.with_setup(flush_env)
def test_config_post_load_by_env():
    """
    tokio.config: Change tokio configuration after it is loaded (scalars)
    """
    # First load and verify the default config
    test_default_config()

    # Then manually set the values of magic variables at runtime
    for variable, set_value in MAGIC_VARIABLES.iteritems():
        orig_value = getattr(tokio.config, variable)
        setattr(tokio.config, variable, set_value)
        assert getattr(tokio.config, variable) != orig_value
        assert getattr(tokio.config, variable) == set_value

@nose.tools.with_setup(flush_env)
def test_config_post_load_from_file():
    """
    tokio.config: Change tokio configuration after it is loaded (scalars+dicts)
    """
    # First load and verify the default config
    test_default_config()

    # Then load the config file into buffer
    config_file = os.path.join(tokiotest.INPUT_DIR, 'sample_config.json')
    # and manually set each loaded variable as a tokio.config attribute
    config_contents = json.load(open(config_file, 'r'))
    for key, set_value in config_contents.iteritems():
        setattr(tokio.config, key.upper(), set_value)

    # Then verify that all the runtime values have now changed
    compare_config_to_runtime(config_file)
