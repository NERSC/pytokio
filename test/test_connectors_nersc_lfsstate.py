#!/usr/bin/env python

import os
import json
import tempfile
import tokio.connectors.nersc_lfsstate

SAMPLE_OSTMAP_FILE = os.path.join(os.getcwd(), 'inputs', 'sample_ost-map.txt')
SAMPLE_OSTFULLNESS_FILE = os.path.join(os.getcwd(), 'inputs', 'sample_ost-fullness.txt')

def verify_ostmap(ostmap):
    assert len(ostmap) > 0
    print "Found %d time stamps" % len(ostmap)
    for timestamp, fs_data in ostmap.iteritems():
        assert len(fs_data) > 0
        print "Found %d file systems" % len(fs_data)
        for target_name, obd_data in fs_data.iteritems():
            assert len(obd_data) > 0
            print "Found %d OBD IDs" % len(obd_data)
            obd_data = fs_data[target_name]
            found_roles = set([])
            for obd_name, keyvalues in obd_data.iteritems():
                ### indices should never be negative
                assert keyvalues['index'] >= 0
                ### make sure that the role_id is consistent with parsed values
                assert keyvalues['role_id'].startswith(target_name)
                assert obd_name in keyvalues['role_id']
                found_roles.add(keyvalues['role'])
            ### every Lustre file system should have at least one OSC
            assert 'osc' in found_roles

def test_ostmap_from_cache():
    ### read from a cache file
    ostmap = tokio.connectors.nersc_lfsstate.NERSCLFSOSTMap( SAMPLE_OSTMAP_FILE )

    ### verify that the results are sensible
    verify_ostmap(ostmap)

def test_ostmap_serializer():
    ### read from a cache file
    ostmap = tokio.connectors.nersc_lfsstate.NERSCLFSOSTMap( SAMPLE_OSTMAP_FILE )

    ### serialize the object, then re-read it and verify it
    cache_file = tempfile.NamedTemporaryFile(delete=False)
    print "Caching to %s" % cache_file.name
    ostmap.save_cache(cache_file.name)

    ### open a second file handle to this cached file to load it
    ostmap = tokio.connectors.nersc_lfsstate.NERSCLFSOSTMap( cache_file.name )

    ### destroy the cache file
    cache_file.close()

    ### verify the integrity of what we serialized
    verify_ostmap(ostmap)

def test_ostfullness_from_cache():
    ### read from a cache file
    ostfullness = tokio.connectors.nersc_lfsstate.NERSCLFSOSTFullness( SAMPLE_OSTFULLNESS_FILE )

    ### verify that the results are sensible
#   verify_ostfullness(ostfullness)

def test_ostfullness_serializer():
    ### read from a cache file
    ostfullness = tokio.connectors.nersc_lfsstate.NERSCLFSOSTFullness( SAMPLE_OSTFULLNESS_FILE )

    ### serialize the object, then re-read it and verify it
    cache_file = tempfile.NamedTemporaryFile(delete=False)
    print "Caching to %s" % cache_file.name
    ostfullness.save_cache(cache_file.name)

    ### open a second file handle to this cached file to load it
    ostfullness = tokio.connectors.nersc_lfsstate.NERSCLFSOSTFullness( cache_file.name )

    ### destroy the cache file
    cache_file.close()

    ### verify the integrity of what we serialized
#   verify_ostfullness(ostmap)

def verify_ostfullness(ostfullness):
    """
    snx11035-OST0000_UUID 90767651352 66209262076 23598372720  74% /scratch2[OST:0]
    """
    assert len(ostfullness) > 0
    print "Found %d time stamps" % len(ostfullness)
    for timestamp, fs_data in ostfullness.iteritems():
        assert len(fs_data) > 0
        print "Found %d file systems" % len(fs_data)
        for target_name, obd_data in fs_data.iteritems():
            assert len(obd_data) > 0
            print "Found %d OBD IDs" % len(obd_data)
            obd_data = fs_data[target_name]
            found_roles = set([])
            for obd_name, keyvalues in obd_data.iteritems():
                ### indices should never be negative
                assert keyvalues['target_index'] >= 0
                ### OBDs should never have no capacity
                assert keyvalues['total_kib'] > 0
                ### the sum of available and used should never exceed total
                assert keyvalues['total_kib'] >= (keyvalues['remaining_kib'] + keyvalues['used_kib'])
                found_roles.add(keyvalues['role'])
            ### every Lustre file system should have at least one OSC
            assert 'ost' in found_roles
