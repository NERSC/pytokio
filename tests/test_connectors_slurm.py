#!/usr/bin/env python

import os
import json
import random
import tempfile
import datetime
import tokio.connectors.slurm

# Just to verify that the basic sacct interface works.  Hopefully jobid=1000
# exists on the system where this test is run.
SAMPLE_JOBID = 1000

# If you change the contents of the SAMPLE_INPUT file, you must update the
# expected input values as well or the validation tests may fail
SAMPLE_INPUT = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'inputs', 'sample.slurm')
SAMPLE_INPUT_KEYS = ['start', 'end', 'jobidraw']
SAMPLE_INPUT_JOBCT = 1
SAMPLE_INPUT_NODECT = 128
SAMPLE_INPUT_MAX_WALLSECS = 3600

# keep it deterministic
random.seed(0)

def verify_slurm(slurm):
    """
    Verify contents of a Slurm object
    """
    assert len(slurm) > 0
    for rawjobid, counters in slurm.iteritems():
        assert len(counters) > 0

def verify_slurm_json(slurm_json):
    """
    Verify correctness of a json-serialized Slurm object
    """
    assert len(slurm_json) > 0
    for rawjobid, counters in slurm_json.iteritems():
        assert len(counters) > 0
        for key in SAMPLE_INPUT_KEYS:
            assert key in counters.keys()

def test_load_slurm_cache():
    """
    Initialize Slurm from cache file
    """
    slurm_data = tokio.connectors.slurm.Slurm(cache_file=SAMPLE_INPUT)
    verify_slurm(slurm_data)

def test_load_slurm_sacct():
    """
    Initialize Slurm from sacct command
    """
    try:
        slurm_data = tokio.connectors.slurm.Slurm(SAMPLE_JOBID)
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
    slurm_data = tokio.connectors.slurm.Slurm(cache_file=SAMPLE_INPUT)
    json_str = slurm_data.to_json()
    slurm_json = json.loads(json_str)
    verify_slurm_json(slurm_json)

def test_slurm_to_json():
    """
    Slurm.to_json() functionality with json.dumps kwargs
    """
    slurm_data = tokio.connectors.slurm.Slurm(cache_file=SAMPLE_INPUT)
    json_str = slurm_data.to_json(indent=4, sort_keys=True)
    slurm_json = json.loads(json_str)
    verify_slurm_json(slurm_json)

def test_slurm_to_dataframe():
    """
    Slurm.to_dataframe() functionality and correctness
    """
    slurm_data = tokio.connectors.slurm.Slurm(cache_file=SAMPLE_INPUT)

    df = slurm_data.to_dataframe()

    assert len(df) > 0

    # Ensure that a few critical keys are included
    for key in SAMPLE_INPUT_KEYS:
        assert key in df.columns
        assert len(df[key]) > 0

    # Ensure that datetime fields were correctly converted
    for datetime_field in 'start', 'end':
        # ensure that the absolute time is sensible
        assert df[datetime_field][0] > datetime.datetime(1980, 1, 1)

        # verify that basic arithmetic works
        day_ago = df[datetime_field][0] - datetime.timedelta(days=1)
        assert (df[datetime_field][0] - day_ago).total_seconds() == 86400

def expand_nodelist(min_nid, max_nid):
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
    compact_nodelist functionality
    """
    min_node = random.randint(1, 6000)
    max_node = min_node + random.randint(1, 10000)
    nodelist = set([ "nid%05d" % i for i in range(min_node, max_node + 1) ])
    nodelist_str_from_set = tokio.connectors.slurm.compact_nodelist(nodelist)
    nodelist_str_from_str = tokio.connectors.slurm.compact_nodelist(','.join(list(nodelist)))
    assert len(nodelist_str_from_set) > 0
    assert len(nodelist_str_from_str) > 0
    assert nodelist_str_from_set == nodelist_str_from_str

def test_slurm_serializer():
    """
    Serialize and deserialize connectors.Slurm
    """
    # Read from a cache file
    slurm_data = tokio.connectors.slurm.Slurm(cache_file=SAMPLE_INPUT)
    # Serialize the object, then re-read it and verify it
    cache_file = tempfile.NamedTemporaryFile(delete=False)
    print "Caching to %s" % cache_file.name
    slurm_data.save_cache(cache_file.name)
    # Open a second file handle to this cached file to load it
    slurm_cached = tokio.connectors.slurm.Slurm(cache_file=cache_file.name)
    cache_file.close()
    verify_slurm(slurm_cached)

def test_get_job_ids():
    """
    Slurm.get_job_ids() functionality
    """
    slurm_data = tokio.connectors.slurm.Slurm(cache_file=SAMPLE_INPUT)
    assert len(slurm_data.get_job_ids()) == SAMPLE_INPUT_JOBCT

def test_get_job_nodes():
    """
    Slurm.get_job_nodes() functionality
    """
    slurm_data = tokio.connectors.slurm.Slurm(cache_file=SAMPLE_INPUT)
    assert len(slurm_data.get_job_nodes()) == SAMPLE_INPUT_NODECT

def test_get_job_startend():
    """
    Slurm.get_job_startend() functionality
    """
    slurm_data = tokio.connectors.slurm.Slurm(cache_file=SAMPLE_INPUT)
    start, end = slurm_data.get_job_startend()
    assert (end - start).total_seconds() < SAMPLE_INPUT_MAX_WALLSECS
