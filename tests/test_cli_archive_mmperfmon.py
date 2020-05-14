#!/usr/bin/env python
"""
Test the archive_mmperfmon.py tool
"""

import os
import time
import nose
import datetime
import h5py
import tokio
import tokio.connectors.hdf5
import tokio.cli.archive_mmperfmon
import tokiotest

def generate_tts(output_file,
                 input_file=tokiotest.SAMPLE_MMPERFMON_MULTI,
                 init_start=tokiotest.SAMPLE_MMPERFMON_MULTI_START,
                 init_end=tokiotest.SAMPLE_MMPERFMON_MULTI_END,
                 query_start=tokiotest.SAMPLE_MMPERFMON_MINI_START,
                 query_end=tokiotest.SAMPLE_MMPERFMON_MINI_END):
    """Create a TokioTimeSeries output file
    """
    argv = [
        '--init-start', init_start,
        '--init-end', init_end,
        '--timestep', str(tokiotest.SAMPLE_MMPERFMON_TIMESTEP),
        '--output', output_file,
        '--filesystem', 'testfs-mini',
        query_start,
        query_end 
    ]
    print("Running [%s]" % ' '.join(argv))
    tokio.cli.archive_mmperfmon.main(argv)
    print("Created %s" % output_file)

def update_tts(output_file):
    """
    Append to an existing tts file
    """
    assert os.path.isfile(output_file) # must update an existing file

    argv = [
        '--output', output_file,
        '--filesystem', 'testfs-micro',
        tokiotest.SAMPLE_MMPERFMON_MICRO_START,
        tokiotest.SAMPLE_MMPERFMON_MICRO_END
    ]

    print("Running [%s]" % ' '.join(argv))
    tokio.cli.archive_mmperfmon.main(argv)
    print("Updated %s" % output_file)

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_bin_archive_mmperfmon_basic():
    """cli.archive_mmperfmon: basic functionality

    """
    tokiotest.TEMP_FILE.close()

    # initialize a new TimeSeries, populate it, and write it out as HDF5
    generate_tts(tokiotest.TEMP_FILE.name)
    hdf5 = tokio.connectors.hdf5.Hdf5(tokiotest.TEMP_FILE.name, 'r')

    for dataset_name in tokiotest.SAMPLE_MMPERFMON_DATASETS:
        print("Was %s created?" % (dataset_name))
        assert dataset_name in hdf5
        print("Was %s nonzero?" % (dataset_name))
        assert hdf5[dataset_name][...].sum()

def test_bin_archive_mmperfmon_server_type():
    """cli.archive_mmperfmon.Archiver.server_type()"""
    archive = tokio.cli.archive_mmperfmon.Archiver(
        init_start=datetime.datetime.strptime(tokiotest.SAMPLE_MMPERFMON_MULTI_START, tokio.cli.archive_mmperfmon.DATE_FMT),
        init_end=datetime.datetime.strptime(tokiotest.SAMPLE_MMPERFMON_MULTI_END, tokio.cli.archive_mmperfmon.DATE_FMT),
        timestep=tokiotest.SAMPLE_MMPERFMON_TIMESTEP,
        num_luns=None,
        num_servers=None)

    lun_type = archive.lun_type(tokiotest.SAMPLE_MMPERFMON_MDT)
    print("LUN type for %s is %s" % (tokiotest.SAMPLE_MMPERFMON_MDT, lun_type))
    assert lun_type[0].lower() == "m"

    lun_type = archive.lun_type(tokiotest.SAMPLE_MMPERFMON_OST)
    print("LUN type for %s is %s" % (tokiotest.SAMPLE_MMPERFMON_OST, lun_type))
    assert lun_type[0].lower() == "d"

    server_type = archive.server_type(tokiotest.SAMPLE_MMPERFMON_MDS)
    print("Server type for %s is %s" % (tokiotest.SAMPLE_MMPERFMON_MDS, server_type))
    assert server_type[0].lower() == "m"

    server_type = archive.server_type(tokiotest.SAMPLE_MMPERFMON_OSS)
    print("Server type for %s is %s" % (tokiotest.SAMPLE_MMPERFMON_OSS, server_type))
    assert server_type[0].lower() == "d"

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_bin_archive_mmperfmon_overlaps():
    """cli.archive_mmperfmon: write + overwrite correctness

    1. initialize a new HDF5 and pull down a large window
    2. pull down a complete subset of that window to this newly minted HDF5
    3. ensure that the hdf5 between #1 and #2 doesn't change
    """
    tokiotest.TEMP_FILE.close()

    # initialize a new TimeSeries, populate it, and write it out as HDF5
    generate_tts(tokiotest.TEMP_FILE.name)
    h5_file = h5py.File(tokiotest.TEMP_FILE.name, 'r')
    summary0 = tokiotest.summarize_hdf5(h5_file)
    h5_file.close()

    time.sleep(1.5)

    # append an overlapping subset of data to the same HDF5
    update_tts(tokiotest.TEMP_FILE.name)
    h5_file = h5py.File(tokiotest.TEMP_FILE.name, 'r')
    summary1 = tokiotest.summarize_hdf5(h5_file)
    h5_file.close()

    tokiotest.identical_datasets(summary0, summary1)

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_bin_archive_mmperfmon_edges():
    """cli.archive_mmperfmon: test boundary correctness
    """
    tokiotest.TEMP_FILE.close()

    # initialize a new TimeSeries, populate it, and write it out as HDF5
    generate_tts(
        tokiotest.TEMP_FILE.name,
        query_start=tokiotest.SAMPLE_MMPERFMON_MINI_START,
        query_end=tokiotest.SAMPLE_MMPERFMON_MINI_END)

    query_start_dt = datetime.datetime.strptime(tokiotest.SAMPLE_MMPERFMON_MINI_START, "%Y-%m-%dT%H:%M:%S")
    query_end_dt = datetime.datetime.strptime(tokiotest.SAMPLE_MMPERFMON_MINI_END, "%Y-%m-%dT%H:%M:%S")

    h5_file = tokio.connectors.hdf5.Hdf5(tokiotest.TEMP_FILE.name, 'r')
    df = h5_file.to_dataframe('datatargets/readbytes')
    h5_file.close()

    # confirm that query_start is included in resulting dataset
    print(df.loc[query_start_dt].sum())
    assert not df.loc[query_start_dt].isna().all()

    # confirm that query_end is excluded from resulting dataset
    print(df.loc[query_end_dt].sum())
    assert df.loc[query_end_dt].isna().all()

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_bin_archive_mmperfmon_endofday():
    """cli.archive_mmperfmon: test boundary correctness, end of day

    Exactly fills an HDF5 file.  It should have exactly zero unfilled rows, and
    in combination with test_bin_archive_mmperfmon_edges(), confirms that the
    time ranges aren't pulling in or leaving out the first or last row.
    """
    tokiotest.TEMP_FILE.close()

    query_start = tokiotest.SAMPLE_MMPERFMON_MINI_START
    query_end = tokiotest.SAMPLE_MMPERFMON_MINI_END
    query_start_dt = datetime.datetime.strptime(query_start, "%Y-%m-%dT%H:%M:%S")
    query_end_dt = datetime.datetime.strptime(query_end, "%Y-%m-%dT%H:%M:%S")

    generate_tts(
        tokiotest.TEMP_FILE.name,
        init_start=query_start,
        init_end=query_end,
        query_start=query_start,
        query_end=query_end)

    h5_file = tokio.connectors.hdf5.Hdf5(tokiotest.TEMP_FILE.name, 'r')
    df = h5_file.to_dataframe('datatargets/readbytes')
    h5_file.close()

    print(df.isna().all(axis=1).isna().any())
    assert not df.isna().all(axis=1).isna().any()
