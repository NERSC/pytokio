#!/usr/bin/env python
"""
Test the NERSC Lustre health data connector
"""

import os
import tempfile
import nose
import tokiotest
import tokio.connectors.lfshealth

def verify_ost(fs_data, input_type):
    """
    Verify the basic structure of lfsstate objects
    """

    if input_type == 'ostmap':
        tmp_os = 'osc'
    elif input_type == 'ostfullness':
        tmp_os = 'ost'

    assert fs_data
    print("Found %d file systems" % len(fs_data))
    for target_name, obd_data in fs_data.items():
        assert obd_data
        print("Found %d OBD IDs" % len(obd_data))
        obd_data = fs_data[target_name]
        found_roles = set()
        for obd_name, keyvalues in obd_data.items():
            if input_type == 'ostmap':
                verify_ostmap(obd_name, keyvalues, target_name)
            elif input_type == 'ostfullness':
                verify_ostfullness(keyvalues)

            found_roles.add(str(keyvalues['role']))
        # Every Lustre file system should have at least one OSC
        assert tmp_os in found_roles

def verify_ostmap(obd_name, keyvalues, target_name):
    """
    Basic bounds checking and other validation of data from an ostmap
    """
    # Indices should never be negative
    assert keyvalues['index'] >= 0
    # Make sure that the role_id is consistent with parsed values
    assert keyvalues['role_id'].startswith(target_name)
    assert obd_name in keyvalues['role_id']

def verify_ostfullness(keyvalues):
    """
    Basic bounds checking and other validation of data from an ostfullness
    """
    assert keyvalues['target_index'] >= 0
    assert keyvalues['total_kib'] > 0
    assert keyvalues['total_kib'] >= (keyvalues['remaining_kib'] + keyvalues['used_kib'])

@tokiotest.needs_lustre_cli
def test_ostmap():
    """lfshealth.LfsOstMap: lctl subprocess
    """
    tokiotest.check_lustre_cli()
    ostmap = tokio.connectors.lfshealth.LfsOstMap()
    verify_ost(ostmap, input_type='ostmap')

@tokiotest.needs_lustre_cli
def test_ostfullness():
    """lfshealth.LfsOstFullness: lfs subprocess
    """
    tokiotest.check_lustre_cli()
    ostfullness = tokio.connectors.lfshealth.LfsOstFullness()
    verify_ost(ostfullness, input_type='ostfullness')

def test_ostmap_from_cache():
    """lfshealth.LfsOstMap: read from compressed cache file
    """
    ostmap = tokio.connectors.lfshealth.LfsOstMap(cache_file=tokiotest.SAMPLE_LCTL_DL_T_FILE)
    verify_ost(ostmap, input_type='ostmap')

def test_ostfullness_from_cache_gz():
    """lfshealth.LfsOstFullness: read from compressed cache file
    """
    ostfullness = tokio.connectors.lfshealth.LfsOstFullness(cache_file=tokiotest.SAMPLE_LFS_DF_FILE)
    verify_ost(ostfullness, input_type='ostfullness')

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_ostmap_from_cache_gz():
    """lfshealth.LfsOstMap: read from cache file
    """
    tokiotest.TEMP_FILE.close()
    tokiotest.gunzip(tokiotest.SAMPLE_LCTL_DL_T_FILE, tokiotest.TEMP_FILE.name)
    ostmap = tokio.connectors.lfshealth.LfsOstMap(cache_file=tokiotest.TEMP_FILE.name)
    verify_ost(ostmap, input_type='ostmap')

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_ostfullness_from_cache_gz():
    """lfshealth.LfsOstFullness: read from cache file
    """
    tokiotest.TEMP_FILE.close()
    tokiotest.gunzip(tokiotest.SAMPLE_LFS_DF_FILE, tokiotest.TEMP_FILE.name)
    ostfullness = tokio.connectors.lfshealth.LfsOstFullness(cache_file=tokiotest.TEMP_FILE.name)
    verify_ost(ostfullness, input_type='ostfullness')

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_ostmap_serializer():
    """lfshealth.LfsOstMap: can serialize and deserialize circularly
    """
    # Read from a cache file
    ostmap = tokio.connectors.lfshealth.LfsOstMap(cache_file=tokiotest.SAMPLE_LCTL_DL_T_FILE)
    # Serialize the object, then re-read it and verify it
    print("Caching to %s" % tokiotest.TEMP_FILE.name)
    ostmap.save_cache(tokiotest.TEMP_FILE.name)
    # Open a second file handle to this cached file to load it
    ostmap = tokio.connectors.lfshealth.LfsOstMap(cache_file=tokiotest.TEMP_FILE.name)
    tokiotest.TEMP_FILE.close()
    verify_ost(ostmap, input_type='ostmap')

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_ostfullness_serializer():
    """lfshealth.LfsOstFullness: can serialize and deserialize circularly
    """
    # Read from a cache file
    ostfullness = tokio.connectors.lfshealth.LfsOstFullness(cache_file=tokiotest.SAMPLE_LFS_DF_FILE)
    print(ostfullness)
    # Serialize the object, then re-read it and verify it
    print("Caching to %s" % tokiotest.TEMP_FILE.name)
    ostfullness.save_cache(tokiotest.TEMP_FILE.name)
    print("Cache file has size %s" % os.path.getsize(tokiotest.TEMP_FILE.name))
    # Open a second file handle to this cached file to load it
    ostfullness = tokio.connectors.lfshealth.LfsOstFullness(cache_file=tokiotest.TEMP_FILE.name)
    print(ostfullness)
    tokiotest.TEMP_FILE.close()
    verify_ost(ostfullness, input_type='ostfullness')
