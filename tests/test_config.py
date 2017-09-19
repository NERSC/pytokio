#!/usr/bin/env python
"""
Ensure that the tokio site config loads correctly
"""

import os
import nose

# TODO

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
    import tokio.config
    reload(tokio.config)
    print variable, getattr(tokio.config, variable), value
    assert getattr(tokio.config, variable) == value

def test_default_config():
    """
    Load config file from default location
    """
    pass

def test_configfile_env():
    """
    Load config file from PYTOKIO_CONFIG
    """
    pass

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
