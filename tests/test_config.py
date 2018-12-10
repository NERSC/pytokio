#!/usr/bin/env python
"""
Ensure that the tokio site config loads correctly
"""

import os
import json
import nose
import tokiotest
import tokio.config
try:
    from imp import reload
except ImportError:
    # Python 2
    pass

# This should be defined in the sample config included in this test suite, but
# NOT in the config that ships with pytokio
DEADBEEF_KEY = "debug_dummy"
DEADBEEF_VALUE = 0xDEADBEEF

TMP_ENV_PREFIX = "_PYTOKIO_TEST_"

### tokio.config settings that should be settable via environment variables
MAGIC_VARIABLES = {
    'hdf5_files': os.path.join('abc', 'def'),
    'lfsstatus_fullness_files': os.path.join('ghi', 'klmno', 'p'),
    'lfsstatus_map_files': os.path.join('y', 'z', ''),
    'isdct_files': os.path.join('qrs', 'tuv', 'wx'),
    'darshan_log_dirs': os.path.join('hello', 'world'),
}

def delete_pytokio_vars(backup=True):
    """Remove any environment variables that begin with PYTOKIO_

    Args:
        backup (bool): create a backup of the value of each variable purged
    """
    del_keys = []
    for var_name in [x for x in os.environ if x.startswith("PYTOKIO_")]:
        if backup:
            backup_name = TMP_ENV_PREFIX + var_name
            print("\033[93mBacking up %s to %s in runtime environment\033[0m" % (var_name, backup_name))
            os.environ[backup_name] = os.environ[var_name]
        del os.environ[var_name]

def flush_env():
    """
    Ensure that the runtime environment isn't tainted by magic variables before
    running a test.
    """
    print("\033[94mEntering flush_env\033[0m")
    delete_pytokio_vars()

    tokio.config.init_config()

def restore_env():
    """Restore PYTOKIO_ environment variables purged by flush_env()
    """
    print("\033[94mEntering restore_env\033[0m")

    delete_pytokio_vars(backup=False)

    # Now swap back in PYTOKIO_ variables
    del_keys = []
    for backup_name in [x for x in os.environ if x.startswith(TMP_ENV_PREFIX)]:
        var_name = backup_name[len(TMP_ENV_PREFIX):]
        print("\033[92mRestoring %s to runtime environment during restoration\033[0m" % var_name)
        os.environ[var_name] = os.environ[backup_name]
        del os.environ[backup_name]

    tokio.config.init_config()

def magic_variable(variable, set_value):
    """
    Set an environment variable, load tokio.config, and ensure that the
    environment variable was correctly picked up.
    """
    # this test is pointless if we are overriding the default config with the
    # same value as its default because it's impossible to verify that this
    # value was taken from the environment variable
    if tokio.config.CONFIG[variable] == set_value:
        raise Exception("test is broken; attempting to set a magic variable to its default value?")

    os.environ["PYTOKIO_" + variable.upper()] = set_value
    reload(tokio.config)

    print("%s: Supposed to be [%s], actual runtime value is [%s]" % (
        variable,
        set_value,
        tokio.config.CONFIG[variable]))
    runtime_value = tokio.config.CONFIG[variable]
    assert type(runtime_value) == type(set_value)
    assert runtime_value == set_value

def compare_config_to_runtime(config_file):
    """
    Given the path to a config.json, ensure that its contents are what were
    loaded into the tokio.config namespace
    """

    # Verify that the config file tokio.config loaded is a real file
    assert tokio.config.PYTOKIO_CONFIG_FILE
    assert os.path.isfile(tokio.config.PYTOKIO_CONFIG_FILE)

    # Verify that the loaded config wasn't empty
    assert len(tokio.config.CONFIG) > 0

    # Load the reference file and compare its contents to the tokio.config namespace
    print("Comparing runtime config to %s" % config_file)
    config_contents = json.load(open(config_file, 'r'))
    for key, expected_value in config_contents.items():
        runtime_value = tokio.config.CONFIG[key]
        print("Verifying tokio.config.%s:\n  [%s] == [%s]" % (
            key.upper(),
            str(expected_value),
            str(runtime_value)))
        assert type(runtime_value) == type(expected_value)
        assert runtime_value == expected_value

@nose.tools.with_setup(setup=flush_env, teardown=restore_env)
def test_default_config():
    """
    tokio.config: Load config file from default location
    """
    # Reload the module to force reinitialization from config
    reload(tokio.config)

    # Verify the loaded attributes are what was in the config file
    compare_config_to_runtime(tokio.config.PYTOKIO_CONFIG_FILE)

@nose.tools.with_setup(setup=flush_env, teardown=restore_env)
def test_configfile_env():
    """
    tokio.config: Load config file from PYTOKIO_CONFIG
    """
    config_file = os.path.join(tokiotest.INPUT_DIR, 'sample_config.json')

    os.environ["PYTOKIO_CONFIG"] = config_file
    print("Set PYTOKIO_CONFIG to %s" % os.environ["PYTOKIO_CONFIG"])
    reload(tokio.config)
    print("tokio.config.PYTOKIO_CONFIG = %s" % tokio.config.PYTOKIO_CONFIG_FILE)

    assert tokio.config.PYTOKIO_CONFIG_FILE == config_file
    compare_config_to_runtime(config_file)
    assert tokio.config.CONFIG[DEADBEEF_KEY] == DEADBEEF_VALUE

@nose.tools.with_setup(setup=flush_env, teardown=restore_env)
def test_config_magic_variable():
    """
    tokio.config: Use of magic overriding variable
    """
    # Ensure that each magic environment variable is picked up correctly
    for variable, value in MAGIC_VARIABLES.items():
        yield magic_variable, variable, value


@nose.tools.with_setup(setup=flush_env, teardown=restore_env)
def test_no_env_effects_post_load():
    """
    tokio.config: Magic variables don't affect runtime post-load
    """
    # First load and verify the default config
    test_default_config()

    # Then set a magic environment variable and assert that it is *not*
    # automatically picked up
    for variable, set_value in MAGIC_VARIABLES.items():
        orig_value = tokio.config.CONFIG[variable]
        if orig_value == set_value:
            raise Exception("test is broken; trying to set a magic variable to its default value?")
        os.environ[variable] = set_value
        assert tokio.config.CONFIG[variable] == orig_value


@nose.tools.with_setup(setup=flush_env, teardown=restore_env)
def test_config_post_load_by_env():
    """
    tokio.config: Change tokio configuration after it is loaded (scalars)
    """
    # First load and verify the default config
    test_default_config()

    # Then manually set the values of magic variables at runtime
    for variable, set_value in MAGIC_VARIABLES.items():
        orig_value = tokio.config.CONFIG[variable]
        tokio.config.CONFIG[variable] = set_value
        assert tokio.config.CONFIG[variable] != orig_value
        assert tokio.config.CONFIG[variable] == set_value

@nose.tools.with_setup(setup=flush_env, teardown=restore_env)
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
    for key, set_value in config_contents.items():
        tokio.config.CONFIG[key] = set_value

    # Then verify that all the runtime values have now changed
    compare_config_to_runtime(config_file)
