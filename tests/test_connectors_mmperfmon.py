"""Test the mmperfmon connector
"""
import os
import gzip
import json
import nose

import tokio.connectors.mmperfmon
import tokiotest

def validate_iterable(obj):
    """Ensure that a obj is a valid data structure of some sort
    """
    assert obj is not None
    print("object has length %d" % len(obj))
    assert len(obj) > 0

def validate_object(obj):
    """Ensure that a obj is a valid Mmperfmon object
    """
    validate_iterable(obj)

    # ensure that all top-level keys are timestamps
    for timestamp in obj:
        print("key [%s] is type [%s]" % (timestamp, type(timestamp)))
        assert isinstance(timestamp, int)

def test_get_col_pos():
    """connectors.mmperfmon.get_col_pos()
    """
    input_strs = [
        "Row           Timestamp cpu_user cpu_sys   mem_total",
        "Row   Timestamp cpu_user cpu_sys mem_total",
        "   Row   Timestamp     cpu_user cpu_sys mem_total",
        "Row   Timestamp     cpu_user cpu_sys mem_total     ",
        "  Row   Timestamp     cpu_user    cpu_sys      mem_total     ",
    ]
    for input_str in input_strs:
        print("Evaluating [%s]" % input_str)
        tokens = input_str.strip().split()
        offsets = tokio.connectors.mmperfmon.get_col_pos(input_str)
        print("Offsets are: " + str(offsets))
        assert offsets
        istart = 0
        num_tokens = 0
        for index, (istart, istop) in enumerate(offsets):
            token = input_str[istart:istop]
            print("    [%s] vs [%s]" % (token, tokens[index]))
            assert token == tokens[index]
            istart = istop
            num_tokens += 1
        assert num_tokens == len(tokens)

def test_to_df():
    """connectors.mmperfmon.Mmperfmon.to_dataframe()
    """
    mmpout = tokio.connectors.mmperfmon.Mmperfmon.from_file(tokiotest.SAMPLE_MMPERFMON_USAGE_INPUT)
    validate_object(mmpout)

    for sample_host in tokiotest.SAMPLE_MMPERFMON_HOSTS:
        print("\nRetrieving dataframe for host [%s]" % sample_host)
        dataframe = mmpout.to_dataframe(by_host=sample_host)
        print(dataframe)
        validate_iterable(dataframe)

    for sample_metric in tokiotest.SAMPLE_MMPERFMON_METRICS:
        print("\nRetrieving dataframe for metric [%s]" % sample_metric)
        dataframe = mmpout.to_dataframe(by_metric=sample_metric)
        print(dataframe)
        validate_iterable(dataframe)

def test_load_single_single():
    """connectors.mmperfmon.Mmperfmon, load single, load diff single
    """
    mmpout = tokio.connectors.mmperfmon.Mmperfmon(tokiotest.SAMPLE_MMPERFMON_NUMOPS_INPUT)
    mmpout.load_str(gzip.open(tokiotest.SAMPLE_MMPERFMON_USAGE_INPUT).read())
    print(json.dumps(mmpout, indent=4, sort_keys=True))
    validate_object(mmpout)

    for sample_host in tokiotest.SAMPLE_MMPERFMON_HOSTS:
        print("\nRetrieving dataframe for host [%s]" % sample_host)
        dataframe = mmpout.to_dataframe(by_host=sample_host)
        print(dataframe)
        validate_iterable(dataframe)

def test_load_multi_single_idempotent():
    """connectors.mmperfmon.Mmperfmon, load multiple, load single
    """
    print("Loading from %s" % tokiotest.SAMPLE_MMPERFMON_TGZ_INPUT)
    mmpout = tokio.connectors.mmperfmon.Mmperfmon(tokiotest.SAMPLE_MMPERFMON_TGZ_INPUT)
    validate_object(mmpout)

    mmpout_orig = json.dumps(mmpout, sort_keys=True)

    # load a subset of the original load
    print("Reloading from %s" % tokiotest.SAMPLE_MMPERFMON_USAGE_INPUT)
    input_str = gzip.open(tokiotest.SAMPLE_MMPERFMON_USAGE_INPUT, 'r').read()
    mmpout.load_str(input_str)
    validate_object(mmpout)

    assert json.dumps(mmpout, sort_keys=True) == mmpout_orig

def test_tgz_input():
    """connectors.mmperfmon.Mmperfmon, .tgz input file
    """
    print("Loading from %s" % tokiotest.SAMPLE_MMPERFMON_TGZ_INPUT)
    mmp_data = tokio.connectors.mmperfmon.Mmperfmon(tokiotest.SAMPLE_MMPERFMON_TGZ_INPUT)
    validate_object(mmp_data)

def test_tar_input():
    """connectors.mmperfmon.Mmperfmon, .tar input file
    """
    tokiotest.gunzip(tokiotest.SAMPLE_MMPERFMON_TGZ_INPUT, tokiotest.SAMPLE_MMPERFMON_TAR_INPUT)
    print("Loading from %s" % tokiotest.SAMPLE_MMPERFMON_TAR_INPUT)
    mmp_data = tokio.connectors.mmperfmon.Mmperfmon(tokiotest.SAMPLE_MMPERFMON_TAR_INPUT)
    tokiotest.try_unlink(tokiotest.SAMPLE_MMPERFMON_TAR_INPUT)
    validate_object(mmp_data)

def test_unpacked_input():
    """connectors.mmperfmon.Mmperfmon, directory input
    """
    tokiotest.untar(tokiotest.SAMPLE_MMPERFMON_TGZ_INPUT)
    print("Loading from %s" % tokiotest.SAMPLE_MMPERFMON_UNPACKED_INPUT)
    mmp_data = tokio.connectors.mmperfmon.Mmperfmon(tokiotest.SAMPLE_MMPERFMON_UNPACKED_INPUT)
    tokiotest.cleanup_untar(tokiotest.SAMPLE_MMPERFMON_TGZ_INPUT)
    validate_object(mmp_data)

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_mmperfmon_serializer():
    """connectors.mmperfmon.Mmperfmon: can serialize and deserialize circularly
    """
    # Read from a cache file
    mmp_data = tokio.connectors.mmperfmon.Mmperfmon(cache_file=tokiotest.SAMPLE_MMPERFMON_USAGE_INPUT)

    # Serialize the object, then re-read it and verify it
    print("Caching to %s" % tokiotest.TEMP_FILE.name)
    mmp_data.save_cache(tokiotest.TEMP_FILE.name)

    # Open a second file handle to this cached file to load it
    mmp_data = tokio.connectors.mmperfmon.Mmperfmon(cache_file=tokiotest.TEMP_FILE.name)
    tokiotest.TEMP_FILE.close()
    validate_object(mmp_data)
