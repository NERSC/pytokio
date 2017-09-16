#!/usr/bin/env python

import os
import gzip
import tarfile
import tempfile
import json
import shutil
import tokio.connectors.nersc_isdct

INPUT_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'inputs')
SAMPLE_TGZ_INPUT = os.path.join(INPUT_DIR, 'sample_nersc_isdct.tgz')
SAMPLE_TIMESTAMPED_INPUT = os.path.join(INPUT_DIR, 'sample_nersc_isdct_timestamped.tgz')
SAMPLE_TAR_INPUT = os.path.join(INPUT_DIR, 'sample_nersc_isdct.tar')
SAMPLE_UNPACKED_INPUT = os.path.join(INPUT_DIR, 'sample_nersc_isdct_dir')
SAMPLE_JSON_INPUT = os.path.join(INPUT_DIR, 'sample_nersc_isdct.json')
SAMPLE_JSON_GZ_INPUT = os.path.join(INPUT_DIR, 'sample_nersc_isdct.json.gz')
DEFAULT_INPUT = SAMPLE_TGZ_INPUT

def validate_object(isdct_data):
    """
    Ensure that the NerscIsdct class is correctly generated and initalized
    """
    assert isdct_data is not None
    assert len(isdct_data) > 0
    for serial_no, counters in isdct_data.iteritems():
        assert len(serial_no) > 1
        for counter in counters:
            # a counter didn't get parsed correctly
            assert not counter.startswith("None") 
        # ensure that synthesized metrics are being calculated
        assert 'write_amplification_factor' in counters
        # ensure that timestamp is set
        print json.dumps(counters,indent=4,sort_keys=True)
        assert 'timestamp' in counters

def validate_dataframe(isdct_data):
    """
    Ensure that the NerscIsdct DataFrame is correctly generated and initialized
    """
    assert len(isdct_data) > 0

def test_tgz_input():
    """
    Load NerscIsdct from .tgz input files
    """
    isdct_data = tokio.connectors.nersc_isdct.NerscIsdct(SAMPLE_TGZ_INPUT)
    validate_object(isdct_data)

def test_timestamped_input():
    """
    Load NerscIsdct containing a timestamp file
    """
    isdct_data = tokio.connectors.nersc_isdct.NerscIsdct(SAMPLE_TIMESTAMPED_INPUT)
    validate_object(isdct_data)

def test_tar_input():
    """
    Load NerscIsdct from .tar input files (no compression)
    """
    gunzip(SAMPLE_TGZ_INPUT, SAMPLE_TAR_INPUT)
    isdct_data = tokio.connectors.nersc_isdct.NerscIsdct(SAMPLE_TAR_INPUT)
    try_unlink(SAMPLE_TAR_INPUT)
    validate_object(isdct_data)

def test_unpacked_input():
    """
    Load NerscIsdct from unpacked .tgz directories
    """
    untar(SAMPLE_TGZ_INPUT)
    isdct_data = tokio.connectors.nersc_isdct.NerscIsdct(SAMPLE_UNPACKED_INPUT)
    cleanup_untar(SAMPLE_TGZ_INPUT) 
    validate_object(isdct_data)

def test_json_gz_input():
    """
    Load NerscIsdct from compressed, serialized json
    """
    isdct_data = tokio.connectors.nersc_isdct.NerscIsdct(SAMPLE_JSON_GZ_INPUT)
    validate_object(isdct_data)

def test_json_input():
    """
    Load NerscIsdct from uncompressed serialized json
    """
    gunzip(SAMPLE_JSON_GZ_INPUT, SAMPLE_JSON_INPUT)
    isdct_data = tokio.connectors.nersc_isdct.NerscIsdct(SAMPLE_JSON_INPUT)
    try_unlink(SAMPLE_JSON_INPUT)
    validate_object(isdct_data)

def test_to_dataframe():
    """
    Convert NerscIsdct object into a DataFrame
    """
    isdct_data = tokio.connectors.nersc_isdct.NerscIsdct(DEFAULT_INPUT)
    isdct_df = isdct_data.to_dataframe()
    validate_dataframe(isdct_df)

def test_serializer():
    """
    NerscIsdct can deserialize its serialization
    """
    # Read from a cache file
    isdct_data = tokio.connectors.nersc_isdct.NerscIsdct(DEFAULT_INPUT)
    # Serialize the object, then re-read it and verify it
    cache_file = tempfile.NamedTemporaryFile(delete=False, suffix='.json')
    print "Caching to %s" % cache_file.name
    isdct_data.save_cache(cache_file.name)
    # Open a second file handle to this cached file to load it
    isdct_cached = tokio.connectors.nersc_isdct.NerscIsdct(cache_file.name)
    os.unlink(cache_file.name)
    cache_file.close()
    validate_object(isdct_cached)

################################################################################
#  Helper functions
################################################################################
def untar(input_file):
    cleanup_untar(input_file)
    tar = tarfile.open(input_file)
    tar.extractall(path=INPUT_DIR)
    tar.close()

def cleanup_untar(input_file):
    tar = tarfile.open(input_file)
    for member in tar.getmembers():
        fq_name = os.path.join(INPUT_DIR, member.name)
        if os.path.exists(fq_name) and fq_name.startswith(INPUT_DIR): # one final backstop
            print "Removing", fq_name
            if member.isdir():
                shutil.rmtree(fq_name)
            else:
                os.unlink(fq_name)

def gunzip(input_file, output_file):
    """
    To check support for both compressed and uncompressed data streams, create
    an uncompressed version of an input file on the fly
    """
    try_unlink(output_file)
    with gzip.open(input_file, 'rb') as f:
        file_content = f.read()
    with open(output_file, 'w+b') as f:
        print "Creating %s" % output_file
        f.write(file_content)

def try_unlink(output_file):
    """
    Destroy a temporarily decompressed input file
    """
    if os.path.exists(output_file):
        print "Destroying %s" % output_file
        os.unlink(output_file)


