#!/usr/bin/env python
"""
Test the Slurm connector
"""

import json
import random
import datetime
import nose
import tokiotest
import tokio.connectors.slurm

# Just to verify that the basic sacct interface works.  Hopefully jobid=1000
# exists on the system where this test is run.
SAMPLE_JOBID = 1000

# keep it deterministic
random.seed(0)

def verify_slurm(slurm):
    """
    Verify contents of a Slurm object
    """
    assert len(slurm) > 0
    for counters in slurm.itervalues():
        assert len(counters) > 0

def verify_slurm_json(slurm_json):
    """
    Verify correctness of a json-serialized Slurm object
    """
    assert len(slurm_json) > 0
    for counters in slurm_json.itervalues():
        assert len(counters) > 0
        for key in tokiotest.SAMPLE_SLURM_CACHE_KEYS:
            assert key in counters.keys()

def test_load_slurm_cache():
    """
    Initialize Slurm from cache file
    """
    slurm_data = tokio.connectors.slurm.Slurm(cache_file=tokiotest.SAMPLE_SLURM_CACHE_FILE)
    verify_slurm(slurm_data)

@tokiotest.needs_slurm
def test_load_slurm_sacct():
    """
    Initialize Slurm from sacct command
    """
    tokiotest.check_slurm()
    try:
        _ = tokio.connectors.slurm.Slurm(SAMPLE_JOBID)
    except OSError as error:
        # sacct is not available
        raise nose.SkipTest(error)

    ### Not really fair to validate the data structure since we have no idea
    ### what sacct is going to give us on whatever machine these tests are run
#   verify_slurm(slurm_data)

def test_slurm_to_json():
    """
    Slurm.to_json() baseline functionality
    """
    slurm_data = tokio.connectors.slurm.Slurm(cache_file=tokiotest.SAMPLE_SLURM_CACHE_FILE)
    json_str = slurm_data.to_json()
    slurm_json = json.loads(json_str)
    verify_slurm_json(slurm_json)

def test_slurm_to_json_kwargs():
    """
    Slurm.to_json() functionality with json.dumps kwargs
    """
    slurm_data = tokio.connectors.slurm.Slurm(cache_file=tokiotest.SAMPLE_SLURM_CACHE_FILE)
    json_str_sm = slurm_data.to_json(indent=2, sort_keys=True)
    json_str_lg = slurm_data.to_json(indent=4, sort_keys=True)
    # one made with two spaces instead of four should be shorter
    assert len(json_str_lg) > len(json_str_sm)
    slurm_json = json.loads(json_str_lg)
    verify_slurm_json(slurm_json)

def test_slurm_to_dataframe():
    """
    Slurm.to_dataframe() functionality and correctness
    """
    slurm_data = tokio.connectors.slurm.Slurm(cache_file=tokiotest.SAMPLE_SLURM_CACHE_FILE)

    dataframe = slurm_data.to_dataframe()

    assert len(dataframe) > 0

    # Ensure that a few critical keys are included
    for key in tokiotest.SAMPLE_SLURM_CACHE_KEYS:
        assert key in dataframe.columns
        assert len(dataframe[key]) > 0

    # Ensure that datetime fields were correctly converted
    for datetime_field in 'start', 'end':
        # ensure that the absolute time is sensible
        assert dataframe[datetime_field][0] > datetime.datetime(1980, 1, 1)

        # verify that basic arithmetic works
        day_ago = dataframe[datetime_field][0] - datetime.timedelta(days=1)
        assert (dataframe[datetime_field][0] - day_ago).total_seconds() == 86400

def expand_nodelist(min_nid, max_nid):
    """
    Create a known nodelist and ensure that it expands correctly
    """
    # + 1 below because node list is inclusive
    num_nodes = max_nid - min_nid + 1
    nid_str = "nid[%05d-%05d]" % (min_nid, max_nid)
    try:
        node_list = tokio.connectors.slurm.expand_nodelist(nid_str)
    except OSError as error:
        # scontrol is not available
        raise nose.SkipTest(error)

    print "node range is", min_nid, max_nid
    print "length of node list is", len(node_list)
    assert len(node_list) == num_nodes

def test_expand_nodelist():
    """
    expand_nodelist functionality
    """
    min_node = random.randint(1, 6000)
    max_node = min_node + random.randint(1, 10000)
    expand_nodelist(min_node, max_node)

def test_compact_nodelist():
    """
    compact_nodelist functionality (from set and string)
    """
    min_node = random.randint(1, 6000)
    max_node = min_node + random.randint(1, 10000)
    nodelist = set(["nid%05d" % i for i in range(min_node, max_node + 1)])
    try:
        nodelist_str_from_set = tokio.connectors.slurm.compact_nodelist(nodelist)
        nodelist_str_from_str = tokio.connectors.slurm.compact_nodelist(','.join(list(nodelist)))
        assert len(nodelist_str_from_set) > 0
        assert len(nodelist_str_from_str) > 0
        assert nodelist_str_from_set == nodelist_str_from_str
    except OSError as error:
        # scontrol is not available
        raise nose.SkipTest(error)


@tokiotest.needs_slurm
@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_slurm_serializer():
    """
    Serialize and deserialize connectors.Slurm
    """
    tokiotest.check_slurm()
    # Read from a cache file
    slurm_data = tokio.connectors.slurm.Slurm(cache_file=tokiotest.SAMPLE_SLURM_CACHE_FILE)
    # Serialize the object, then re-read it and verify it
    print "Caching to %s" % tokiotest.TEMP_FILE.name
    slurm_data.save_cache(tokiotest.TEMP_FILE.name)
    # Open a second file handle to this cached file to load it
    slurm_cached = tokio.connectors.slurm.Slurm(cache_file=tokiotest.TEMP_FILE.name)
    tokiotest.TEMP_FILE.close()
    verify_slurm(slurm_cached)

def test_get_job_ids():
    """
    Slurm.get_job_ids() functionality
    """
    slurm_data = tokio.connectors.slurm.Slurm(cache_file=tokiotest.SAMPLE_SLURM_CACHE_FILE)
    assert len(slurm_data.get_job_ids()) == tokiotest.SAMPLE_SLURM_CACHE_JOBCT

def test_get_job_nodes():
    """
    Slurm.get_job_nodes() functionality
    """
    slurm_data = tokio.connectors.slurm.Slurm(cache_file=tokiotest.SAMPLE_SLURM_CACHE_FILE)
    assert len(slurm_data.get_job_nodes()) == tokiotest.SAMPLE_SLURM_CACHE_NODECT

def test_get_job_startend():
    """
    Slurm.get_job_startend() functionality
    """
    slurm_data = tokio.connectors.slurm.Slurm(cache_file=tokiotest.SAMPLE_SLURM_CACHE_FILE)
    start, end = slurm_data.get_job_startend()
    assert (end - start).total_seconds() < tokiotest.SAMPLE_SLURM_CACHE_MAX_WALLSECS
