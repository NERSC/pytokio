#!/usr/bin/env python

import os
import json
import tempfile
import tokio.connectors.nersc_lfsstate

SAMPLE_OSTMAP_FILE = os.path.join(os.getcwd(), 'inputs', 'sample_ost-map.txt')

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
