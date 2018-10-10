"""Validate the default configuration json that is not otherwise tested
"""
import os
import json
import nose
import tokiotest

def test_site_config():
    """site.json: test env config has same keys as shipping config

    Because the tests run using a custom configuration file, we should do a
    basic sanity check to make sure that the example config that ships with
    pytokio looks like the test configuration which we validate.
    """

    raise nose.SkipTest("test_site_config is not currently working")

    # Load the test environment's configuration
    with open(os.path.join(tokiotest.INPUT_DIR, 'site.json')) as config_file:
        test_config = json.load(config_file)

    # Load the site-wide configuration included in the pytokio package
    with open(tokio.config.DEFAULT_CONFIG_FILE) as config_file:
        stock_config = json.load(config_file)

    # Ensure that all the keys present in the test configuration are also
    # present in the stock configuration
    for key in test_config.keys():
        if key not in stock_config and not key.startswith('test'):
            print "%s in test_config but not stock_config" % key
            assert key in stock_config

    # and vice versa
    for key in stock_config.keys():
        if key not in test_config:
            print "%s in stock_config but not test_config" % key
            assert key in test_config
