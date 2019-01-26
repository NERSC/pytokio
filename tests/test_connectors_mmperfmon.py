"""Test the mmperfmon connector
"""
import os
import gzip
import json

import tokio.connectors.mmperfmon
import tokiotest

SAMPLE_USAGE_INPUT = os.path.join(tokiotest.INPUT_DIR, 'mmperfmon-usage.txt.gz')
SAMPLE_NUMOPS_INPUT = os.path.join(tokiotest.INPUT_DIR, 'mmperfmon-gpfsNumberOperations.txt.gz')
SAMPLE_TGZ_INPUT = os.path.join(tokiotest.INPUT_DIR, 'mmperfmon.tgz')
SAMPLE_TAR_INPUT = os.path.join(tokiotest.INPUT_DIR, 'mmperfmon.tar')
SAMPLE_UNPACKED_INPUT = os.path.join(tokiotest.INPUT_DIR, 'mmperfmon_dir')
SAMPLE_METRICS = ['cpu_user', 'cpu_sys', 'mem_free', 'mem_total']
SAMPLE_HOSTS = ['ngfsv468.nersc.gov']

def validate_object(obj):
    """Ensure that a obj is a valid Mmperfmon object
    """
    assert obj is not None
    print("object has length %d" % len(obj))
    assert len(obj) > 0

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
    mmpout = tokio.connectors.mmperfmon.Mmperfmon.from_file(SAMPLE_USAGE_INPUT)
    validate_object(mmpout)

    for sample_host in SAMPLE_HOSTS:
        print("\nRetrieving dataframe for host [%s]" % sample_host)
        dataframe = mmpout.to_dataframe(by_host=sample_host)
        print(dataframe)
        validate_object(dataframe)

    for sample_metric in SAMPLE_METRICS:
        print("\nRetrieving dataframe for metric [%s]" % sample_metric)
        dataframe = mmpout.to_dataframe(by_metric=sample_metric)
        print(dataframe)
        validate_object(dataframe)

def test_load_single_single():
    """connectors.mmperfmon.Mmperfmon, load single, load diff single
    """
    mmpout = tokio.connectors.mmperfmon.Mmperfmon(SAMPLE_NUMOPS_INPUT)
    mmpout.load_str(gzip.open(SAMPLE_USAGE_INPUT).read())
    print(json.dumps(mmpout, indent=4, sort_keys=True))
    validate_object(mmpout)

    for sample_host in SAMPLE_HOSTS:
        print("\nRetrieving dataframe for host [%s]" % sample_host)
        dataframe = mmpout.to_dataframe(by_host=sample_host)
        print(dataframe)
        validate_object(dataframe)

def test_load_multi_single_idempotent():
    """connectors.mmperfmon.Mmperfmon, load multiple, load single
    """
    print("Loading from %s" % SAMPLE_TGZ_INPUT)
    mmpout = tokio.connectors.mmperfmon.Mmperfmon(SAMPLE_TGZ_INPUT)
    validate_object(mmpout)

    mmpout_orig = json.dumps(mmpout, sort_keys=True)

    # load a subset of the original load
    print("Reloading from %s" % SAMPLE_USAGE_INPUT)
    input_str = gzip.open(SAMPLE_USAGE_INPUT, 'r').read()
    mmpout.load_str(input_str)
    validate_object(mmpout)

    assert json.dumps(mmpout, sort_keys=True) == mmpout_orig

def test_tgz_input():
    """connectors.mmperfmon.Mmperfmon, .tgz input file
    """
    print("Loading from %s" % SAMPLE_TGZ_INPUT)
    mmp_data = tokio.connectors.mmperfmon.Mmperfmon(SAMPLE_TGZ_INPUT)
    validate_object(mmp_data)

def test_tar_input():
    """connectors.mmperfmon.Mmperfmon, .tar input file
    """
    tokiotest.gunzip(SAMPLE_TGZ_INPUT, SAMPLE_TAR_INPUT)
    print("Loading from %s" % SAMPLE_TAR_INPUT)
    mmp_data = tokio.connectors.mmperfmon.Mmperfmon(SAMPLE_TAR_INPUT)
    tokiotest.try_unlink(SAMPLE_TAR_INPUT)
    validate_object(mmp_data)

def test_unpacked_input():
    """connectors.mmperfmon.Mmperfmon, directory input
    """
    tokiotest.untar(SAMPLE_TGZ_INPUT)
    print("Loading from %s" % SAMPLE_UNPACKED_INPUT)
    mmp_data = tokio.connectors.mmperfmon.Mmperfmon(SAMPLE_UNPACKED_INPUT)
    tokiotest.cleanup_untar(SAMPLE_TGZ_INPUT)
    validate_object(mmp_data)
