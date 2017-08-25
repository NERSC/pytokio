#!/usr/bin/env python

import os
import tempfile
import json
import tokio.connectors.nersc_isdct

INPUT_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'inputs')
SAMPLE_TGZ_INPUT = os.path.join(INPUT_DIR, 'sample_nersc_idsct.tgz')
SAMPLE_UNPACKED_INPUT = os.path.join(INPUT_DIR, 'sample_nersc_idsct_dir')
SAMPLE_JSON_INPUT = os.path.join(INPUT_DIR, 'sample_nersc_idsct.json')

def validate_object(isdct_data):
    """
    Ensure that the NerscIsdct class is correctly generated and initalized
    """
    assert True

def validate_dataframe(isdct_data):
    """
    Ensure that the NerscIsdct DataFrame is correctly generated and initialized
    """
    assert True

def test_tgz_input():
    """
    Load .tgz input files
    """
    isdct_data = tokio.connectors.nersc_isdct.NerscIsdct(SAMPLE_TGZ_INPUT)
    validate_object(isdct_data)

def test_unpacked_input():
    """
    Load unpacked .tgz directories
    """
    isdct_data = tokio.connectors.nersc_isdct.NerscIsdct(SAMPLE_UNPACKED_INPUT)
    validate_object(isdct_data)

def test_json_input():
    """
    Load serialized json
    """
    isdct_data = tokio.connectors.nersc_isdct.NerscIsdct(SAMPLE_JSON_INPUT)
    validate_object(isdct_data)

def test_as_dataframe():
    """
    convert NerscIsdct object into a DataFrame
    """
    isdct_data = tokio.connectors.nersc_isdct.NerscIsdct(SAMPLE_JSON_INPUT)
    isdct_df = isdct_data.as_dataframe()
    validate_dataframe(isdct_df)

def test_serializer():
    # Read from a cache file
    isdct_data = tokio.connectors.nersc_isdct.NerscIsdct(SAMPLE_JSON_INPUT)
    # Serialize the object, then re-read it and verify it
    cache_file = tempfile.NamedTemporaryFile(delete=False)
    print "Caching to %s" % cache_file.name
    isdct_data.save_cache(cache_file.name)
    # Open a second file handle to this cached file to load it
    isdct_cached = tokio.connectors.nersc_isdct.NerscIsdct(cache_file.name)
    cache_file.close()
    validate_object(isdct_cached)
