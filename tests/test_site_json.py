"""Validate the default configuration json that is not otherwise tested
"""
import os
import json

def test_site_config():
    """site.json: test env config has same keys as shipping config

    Because the tests run using a custom configuration file, we should do a
    basic sanity check to make sure that the example config that ships with
    pytokio looks like the test configuration which we validate.
    """

    # Load the test environment's configuration
    with open('site.json') as config_file:
        test_config = json.load(config_file)

    # Load the stock configuration that ships with the pytokio package
    with open(os.path.join('..', 'tokio', 'site.json')) as config_file:
        stock_config = json.load(config_file)

    # Ensure that all the keys present in the test configuration are also
    # present in the stock configuration
    for key in test_config:
        if key not in stock_config and not key.startswith('test'):
            print("%s in test_config but not stock_config" % key)
            assert key in stock_config

    # and vice versa
    for key in stock_config:
        if key not in test_config:
            print("%s in stock_config but not test_config" % key)
            assert key in test_config
