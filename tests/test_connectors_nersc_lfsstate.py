#!/usr/bin/env python
"""
Test the NERSC Lustre health data connector
"""

import os
import nose
import tokiotest
import tokio.connectors.nersc_lfsstate as nersc_lfsstate

INPUTS = os.path.join(os.getcwd(), 'inputs')

def verify_ost(ost, input_type):
    """
    Verify the basic structure of lfsstate objects
    """
    assert ost
    print "Found %d time stamps" % len(ost)

    if input_type == 'ostmap':
        tmp_os = 'osc'
    elif input_type == 'ostfullness':
        tmp_os = 'ost'

    for _, fs_data in ost.iteritems():
        assert fs_data
        print "Found %d file systems" % len(fs_data)
        for target_name, obd_data in fs_data.iteritems():
            assert obd_data
            print "Found %d OBD IDs" % len(obd_data)
            obd_data = fs_data[target_name]
            found_roles = set()
            for obd_name, keyvalues in obd_data.iteritems():
                if input_type == 'ostmap':
                    verify_ostmap(obd_name, keyvalues, target_name)
                elif input_type == 'ostfullness':
                    verify_ostfullness(keyvalues)

                found_roles.add(unicode(keyvalues['role']))
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

def test_ostmap_from_cache():
    """
    Read OST map from a cache file
    """
    ostmap = nersc_lfsstate.NerscLfsOstMap(tokiotest.SAMPLE_OSTMAP_FILE)
    verify_ost(ostmap, input_type='ostmap')

def test_ostfullness_from_cache():
    """
    Read OST fullness from a cache file
    """
    ostfullness = nersc_lfsstate.NerscLfsOstFullness(tokiotest.SAMPLE_OSTFULLNESS_FILE)
    verify_ost(ostfullness, input_type='ostfullness')

def test_ostmap_from_cache_gz():
    """
    Read OST map from a compressed cache file
    """
    ostmap = nersc_lfsstate.NerscLfsOstMap(tokiotest.SAMPLE_OSTMAP_FILE_GZ)
    verify_ost(ostmap, input_type='ostmap')

def test_ostfullness_from_cache_gz():
    """
    Read OST fullness from a compressed cache file
    """
    ostfullness = nersc_lfsstate.NerscLfsOstFullness(tokiotest.SAMPLE_OSTFULLNESS_FILE_GZ)
    verify_ost(ostfullness, input_type='ostfullness')

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_ostmap_serializer():
    """
    OST map can deserialize its serialization
    """
    # Read from a cache file
    ostmap = nersc_lfsstate.NerscLfsOstMap(tokiotest.SAMPLE_OSTMAP_FILE)
    # Serialize the object, then re-read it and verify it
    print "Caching to %s" % tokiotest.TEMP_FILE.name
    ostmap.save_cache(tokiotest.TEMP_FILE.name)
    # Open a second file handle to this cached file to load it
    ostmap = nersc_lfsstate.NerscLfsOstMap(tokiotest.TEMP_FILE.name)
    tokiotest.TEMP_FILE.close()
    verify_ost(ostmap, input_type='ostmap')

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_ostfullness_serializer():
    """
    OST fullness can deserialize its serialization
    """
    # Read from a cache file
    ostfullness = nersc_lfsstate.NerscLfsOstFullness(tokiotest.SAMPLE_OSTFULLNESS_FILE)
    # Serialize the object, then re-read it and verify it
    print "Caching to %s" % tokiotest.TEMP_FILE.name
    ostfullness.save_cache(tokiotest.TEMP_FILE.name)
    # Open a second file handle to this cached file to load it
    ostfullness = nersc_lfsstate.NerscLfsOstFullness(tokiotest.TEMP_FILE.name)
    tokiotest.TEMP_FILE.close()
    verify_ost(ostfullness, input_type='ostfullness')
