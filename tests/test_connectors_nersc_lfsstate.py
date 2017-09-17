#!/usr/bin/env python

import os
import json
import nose
import tokiotest
import tokio.connectors.nersc_lfsstate

INPUTS = os.path.join(os.getcwd(), 'inputs')
SAMPLE_OSTMAP_FILE = os.path.join(INPUTS, 'sample_ost-map.txt')
SAMPLE_OSTFULLNESS_FILE = os.path.join(INPUTS, 'sample_ost-fullness.txt')

def verify_ost(ost,type):
    assert ost
    print "Found %d time stamps" % len(ost)
    
    if type == 'ostmap': 
        tmp_os = 'osc'
    elif type == 'ostfullness':
        tmp_os = 'ost'
        
    for timestamp, fs_data in ost.iteritems():
        assert fs_data 
        print "Found %d file systems" % len(fs_data)
        for target_name, obd_data in fs_data.iteritems():
            assert obd_data
            print "Found %d OBD IDs" % len(obd_data)
            obd_data = fs_data[target_name]
            found_roles = set()
            for obd_name, keyvalues in obd_data.iteritems():
                if type == 'ostmap': 
                    verify_ostmap(obd_name, keyvalues, target_name)
                elif type == 'ostfullness':
                    verify_ostfullness(keyvalues)

                found_roles.add(unicode(keyvalues['role']))
        # Every Lustre file system should have at least one OSC
        assert tmp_os in found_roles

def verify_ostmap(obd_name, keyvalues, target_name):
    # Indices should never be negative
    assert keyvalues['index'] >= 0
    # Make sure that the role_id is consistent with parsed values
    assert keyvalues['role_id'].startswith(target_name)
    assert obd_name in keyvalues['role_id']
    
 
def verify_ostfullness(keyvalues):
    """
    snx11035-OST0000_UUID 90767651352 66209262076 23598372720  74% /scratch2[OST:0]
    """
    assert keyvalues['target_index'] >= 0
    assert keyvalues['total_kib'] > 0
    assert keyvalues['total_kib'] >= (keyvalues['remaining_kib'] + keyvalues['used_kib'])
             

def test_ostmap_from_cache():
    """
    Read OST map from a cache file
    """
    ostmap = tokio.connectors.nersc_lfsstate.NerscLfsOstMap(SAMPLE_OSTMAP_FILE)
    verify_ost(ostmap, type='ostmap')

def test_ostfullness_from_cache():
    """
    Read OST fullness from a cache file
    """
    ostfullness = tokio.connectors.nersc_lfsstate.NerscLfsOstFullness(SAMPLE_OSTFULLNESS_FILE)
    verify_ost(ostfullness, type='ostfullness')

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_ostmap_serializer():
    """
    OST map can deserialize its serialization
    """
    # Read from a cache file
    ostmap = tokio.connectors.nersc_lfsstate.NerscLfsOstMap(SAMPLE_OSTMAP_FILE)
    # Serialize the object, then re-read it and verify it
    print "Caching to %s" % tokiotest.TEMP_FILE.name
    ostmap.save_cache(tokiotest.TEMP_FILE.name)
    # Open a second file handle to this cached file to load it
    ostmap = tokio.connectors.nersc_lfsstate.NerscLfsOstMap(tokiotest.TEMP_FILE.name)
    tokiotest.TEMP_FILE.close()
    verify_ost(ostmap, type='ostmap')

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_ostfullness_serializer():
    """
    OST fullness can deserialize its serialization
    """
    # Read from a cache file
    ostfullness = tokio.connectors.nersc_lfsstate.NerscLfsOstFullness(SAMPLE_OSTFULLNESS_FILE)
    # Serialize the object, then re-read it and verify it
    print "Caching to %s" % tokiotest.TEMP_FILE.name
    ostfullness.save_cache(tokiotest.TEMP_FILE.name)
    # Open a second file handle to this cached file to load it
    ostfullness = tokio.connectors.nersc_lfsstate.NerscLfsOstFullness(tokiotest.TEMP_FILE.name)
    tokiotest.TEMP_FILE.close()
    verify_ost(ostfullness, type='ostfullness')
