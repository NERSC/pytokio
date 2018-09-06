#!/usr/bin/env python
"""
Test the HDF5 connector
"""

import datetime
import random
import nose
import numpy
import tokiotest
import tokio.connectors

DATASETS_1D = [
    'FSStepsGroup/FSStepsDataSet',
    'MDSCPUGroup/MDSCPUDataSet'
]
DATASETS_2D = [
    'FSMissingGroup/FSMissingDataSet',
    'MDSOpsGroup/MDSOpsDataSet',
    'OSSCPUGroup/OSSCPUDataSet',
    'OSTReadGroup/OSTBulkReadDataSet',
    'OSTWriteGroup/OSTBulkWriteDataSet',
]
POSITIVE_2D = [
    'MDSOpsGroup/MDSOpsDataSet', 'OSSCPUGroup/OSSCPUDataSet',
    'OSTReadGroup/OSTBulkReadDataSet', 'OSTWriteGroup/OSTBulkWriteDataSet'
]
GET_SET_TRUE_VERSIONS = {
    '/': 'global',
    '/a/': 'group',
    '/a/b': 'dataset_b',
    '/a/c': 'dataset_c',
    '/a/d': 'global',
}

def test_h5lmt():
    """connectors.hdf5.Hdf5() h5lmt support
    """
    hdf5_file = tokio.connectors.hdf5.Hdf5(tokiotest.SAMPLE_H5LMT_FILE)

    for dataset in DATASETS_1D:
        print("Testing %s" % dataset)
        assert dataset in hdf5_file
        assert len(hdf5_file[dataset].shape) == 1
        assert hdf5_file[dataset][:].sum() > 0
        if dataset != "FSStepsGroup/FSStepsDataSet":
            # TOKIO HDF5 has no direct support for FSStepsGroup since timestamps
            # aren't considered a dataset
            assert len(hdf5_file.to_dataframe(dataset).index)

    for dataset in DATASETS_2D:
        print("Testing %s" % dataset)
        assert dataset in hdf5_file
        assert len(hdf5_file[dataset].shape) == 2
        assert hdf5_file[dataset][:, :].sum() > 0
        assert len(hdf5_file.to_dataframe(dataset).columns)

        # Test dataset-dependent correctness
        #
        # Last timstamp greater than the first timestamp
        assert hdf5_file['FSStepsGroup/FSStepsDataSet'][0] \
               < hdf5_file['FSStepsGroup/FSStepsDataSet'][-1]

        # No negative loads
        assert numpy.greater_equal(hdf5_file['MDSCPUGroup/MDSCPUDataSet'][:], 0.0).all()

        # Only 0 or 1 allowed
        assert numpy.logical_or(
            numpy.equal(hdf5_file['FSMissingGroup/FSMissingDataSet'][:, :], 0),
            numpy.equal(hdf5_file['FSMissingGroup/FSMissingDataSet'][:, :], 1)).all()

    # No negative rates
    for dataset in POSITIVE_2D:
        assert numpy.greater_equal(hdf5_file[dataset][:], 0.0).all()

def test_h5lmt_compat():
    """connectors.hdf5.Hdf5() h5lmt support via compatibility
    """
    hdf5_file = tokio.connectors.hdf5.Hdf5(tokiotest.SAMPLE_H5LMT_FILE)
    for dataset_name in tokio.connectors.hdf5.SCHEMA_DATASET_PROVIDERS[None]:
        print("Checking %s" % dataset_name)
        assert hdf5_file[dataset_name] is not None
        assert hdf5_file[dataset_name][0, 0] is not None # make sure we can index 2d
        # check using new names to get dataframes from old files
        dataframed = hdf5_file.to_dataframe(dataset_name)
        assert len(dataframed) > 0
        assert len(dataframed.columns) > 0

@nose.tools.raises(KeyError)
def test_h5lmt_invalid_dataset():
    """connectors.hdf5.Hdf5() h5lmt support, non-existent dataset
    """
    hdf5_file = tokio.connectors.hdf5.Hdf5(tokiotest.SAMPLE_H5LMT_FILE)
    dataset_name = '/abc/def'
    print("Checking %s" % dataset_name)
    dataset = hdf5_file[dataset_name]

def test_h5lmt_invalid_dataset_get():
    """connectors.hdf5.Hdf5() h5lmt support, .get(non-existent dataset)
    """
    hdf5_file = tokio.connectors.hdf5.Hdf5(tokiotest.SAMPLE_H5LMT_FILE)
    dataset_name = '/abc/def'
    print("Checking %s" % dataset_name)
    assert hdf5_file.get(dataset_name) is None

def _test_to_dataframe(hdf5_file, dataset_name):
    """Exercise to_dataframe() and check basic correctness
    """
    dataframe = hdf5_file.to_dataframe(dataset_name)
    assert len(dataframe.columns) > 0
    assert len(dataframe) > 0

def test_to_dataframe():
    """connectors.hdf5.Hdf5.to_dataframe
    """
    hdf5_file = tokio.connectors.hdf5.Hdf5(tokiotest.SAMPLE_LMTDB_TTS_HDF5)
    for dataset_name in hdf5_file.schema:
        if hdf5_file.get(dataset_name) is None:
            continue

        func = _test_to_dataframe
        func.description = "connectors.hdf5.Hdf5.to_dataframe(%s)" % dataset_name
        yield func, hdf5_file, dataset_name

def test_tts():
    """
    connectors.hdf5.Hdf5() TOKIO Time Series support
    """
    assert tokio.connectors.hdf5.Hdf5(tokiotest.SAMPLE_COLLECTDES_HDF5)

def test_invalid_dataset_get():
    """
    connectors.hdf5.get(non-existent dataset)
    """
    hdf5_file = tokio.connectors.hdf5.Hdf5(tokiotest.SAMPLE_COLLECTDES_HDF5)
    dataset_name = '/abc/def'
    print("Checking %s" % dataset_name)
    assert hdf5_file.get(dataset_name) is None

@nose.tools.raises(KeyError)
def test_invalid_dataset():
    """
    connectors.hdf5, non-existent dataset
    """
    hdf5_file = tokio.connectors.hdf5.Hdf5(tokiotest.SAMPLE_COLLECTDES_HDF5)
    dataset_name = '/abc/def'
    print("Checking %s" % dataset_name)
    dataset = hdf5_file[dataset_name]

def test_mapped_dataset():
    """
    connectors.hdf5 mapped dataset correctness
    """
    for input_type, input_file in tokiotest.SAMPLE_TIMESERIES_FILES.items():
        func = _test_mapped_dataset
        func.description = "connectors.hdf5 mapped dataset correctness (%s)" % input_type
        yield func, input_file

    func = _test_transpose_mapping
    func.description = "connectors.hdf5 transpose mapping"
    yield func, tokiotest.SAMPLE_H5LMT_FILE

def _test_mapped_dataset(input_file):
    """
    Load two views of the same data set (rates and bytes) and ensure that they
    are being correctly calculated.
    """
    numpy.set_printoptions(formatter={'float': '{: 0.1f}'.format},
                           edgeitems=5,
                           linewidth=100)
    print("Testing %s" % input_file)
    hdf5_file = tokio.connectors.hdf5.Hdf5(input_file)
    readbytes = hdf5_file['datatargets/readbytes']
    readrates = hdf5_file['datatargets/readrates']
    timestamps = hdf5_file.get_timestamps('datatargets/readbytes')[0:2]
    timestep = timestamps[1] - timestamps[0]

    print("Timestep appears to be %s" % timestep)
    print("readbytes is")
    print(readbytes[:, :])
    print()
    print("readrates is")
    print(readrates[:, :] * timestep)
    print()
    print("Are readrates a factor of %.2f away from readbytes?" % timestep)

    equivalency = numpy.isclose(readrates[:, :] * timestep, readbytes[:, :])
    print((readrates[:, :] * timestep) - readbytes[:, :])
    assert equivalency.all()

def _test_transpose_mapping(input_file):
    """
    Load the same dataset using two different interfaces
    """
    numpy.set_printoptions(formatter={'float': '{: 0.1f}'.format},
                           edgeitems=5,
                           linewidth=100)
    print("Testing %s" % input_file)
    hdf5_file = tokio.connectors.hdf5.Hdf5(input_file)
    interpreted = hdf5_file['datatargets/readrates'][:, :]
    raw = hdf5_file['OSTReadGroup/OSTBulkReadDataSet'][:, :].T

    equivalency = numpy.isclose(interpreted, raw)
    assert equivalency.all()

def test_get_index():
    """
    connectors.hdf5.Hdf5.get_index()
    """
    for input_type, input_file in tokiotest.SAMPLE_TIMESERIES_FILES.items():
        func = _test_get_index
        func.description = "connectors.hdf5.Hdf5.get_index() with %s" % input_type
        yield func, input_file

def _test_get_index(input_file):
    """
    Ensure that get_index() returns valid results
    """
    hdf5_file = tokio.connectors.hdf5.Hdf5(input_file)
    for dataset_name in tokiotest.SAMPLE_TIMESERIES_DATASETS:
        dataset = hdf5_file.get(dataset_name)
        assert dataset is not None

        timestamps = hdf5_file.get_timestamps(dataset_name)
        timestep = timestamps[1] - timestamps[0]
        num_stamps = len(timestamps)
        target_indices = [0, num_stamps//4, num_stamps//2, 3*num_stamps//4, num_stamps-1]

        for target_index in target_indices:
            for fuzz in range(timestep):
                target_datetime = datetime.datetime.fromtimestamp(timestamps[target_index]) \
                                  + datetime.timedelta(seconds=fuzz)
                new_index = hdf5_file.get_index(dataset_name, target_datetime)
                print("%d(%s) == %d(%s)? %s" % (target_index, type(target_index), new_index, type(new_index), target_index == new_index))
                assert target_index == new_index
                assert (dataset[new_index] == dataset[target_index]).all()

def test_get_columns():
    """
    connectors.hdf5.Hdf5.get_columns()
    """
    for input_type, input_file in tokiotest.SAMPLE_TIMESERIES_FILES.items():
        func = _test_get_columns
        func.description = "connectors.hdf5.Hdf5.get_columns() with %s" % input_type
        yield func, input_file

def _test_get_columns(input_file):
    """
    Ensure that get_columns() returns valid results
    """
    print("Testing %s" % input_file)
    hdf5_file = tokio.connectors.hdf5.Hdf5(input_file)
    for dataset_name in tokiotest.SAMPLE_TIMESERIES_DATASETS:
        print("Getting %s from %s" % (dataset_name, hdf5_file.filename))
        result = hdf5_file.get(dataset_name)
        print('result: %s' % result)
        assert result is not None
        column_names = hdf5_file.get_columns(dataset_name)
        assert len(column_names) > 0

def test_get_timestamps():
    """
    connectors.hdf5.Hdf5.get_timestamps()
    """
    for input_type, input_file in tokiotest.SAMPLE_TIMESERIES_FILES.items():
        func = _test_get_columns
        func.description = "connectors.hdf5.Hdf5.get_timestamps() with %s" % input_type
        yield func, input_file

def _test_get_timestamps(input_file):
    """
    Ensure that get_timestamps() returns valid results
    """
    print("Testing %s" % input_file)
    hdf5_file = tokio.connectors.hdf5.Hdf5(input_file)
    for dataset_name in tokiotest.SAMPLE_TIMESERIES_DATASETS:
        assert hdf5_file.get(dataset_name) is not None
        timestamps = hdf5_file.get_timestamps(dataset_name)
        assert len(timestamps[:]) > 0

        # ensure that every timestamp is equidistant
        prev_delta = None
        for index in range(1, len(timestamps[:])):
            delta = timestamps[index] - timestamps[index - 1]
            if prev_delta is not None:
                assert prev_delta == delta
            prev_delta = delta

def test_missing_values():
    """
    connectors.hdf5.missing_values()
    """
    num_cols = 40
    num_rows = 800
    num_missing = num_cols * num_rows // 4

    random.seed(0)
    dataset = numpy.random.random(size=(num_rows, num_cols)) + 0.1
    inverse = numpy.full((num_rows, num_cols), False)

    remove_list = set([])
    for _ in range(num_missing):
        irow = numpy.random.randint(0, num_rows)
        icol = numpy.random.randint(0, num_cols)
        remove_list.add((irow, icol))

    for irow, icol in remove_list:
        dataset[irow, icol] = -0.0
        inverse[irow, icol] = True

    missing_matrix = tokio.connectors.hdf5.missing_values(dataset)

    print("Added %d missing data; missing_matrix contains %d" % (len(remove_list),
                                                                 missing_matrix.sum()))
    assert len(remove_list) == missing_matrix.sum()
    assert ((missing_matrix == 0.0) | inverse).all()

def test_get_versions(hdf5_filename=tokiotest.SAMPLE_VERSIONS_HDF5):
    """connectors.hdf5.get_version()
    """
    hdf5 = tokio.connectors.hdf5.Hdf5(hdf5_filename, 'r', ignore_version=True)
    for groupname, true_version in GET_SET_TRUE_VERSIONS.items():
        version = hdf5.get_version(groupname)
        print("Version from HDF5: %s(%s) vs. truth %s(%s)" % (
            version, type(version),
            true_version, type(true_version)))
        assert version == true_version

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_set_versions():
    """connectors.hdf5.set_version()
    """
    hdf5 = tokio.connectors.hdf5.Hdf5(tokiotest.TEMP_FILE.name, 'w', ignore_version=True)
    for groupname, true_version in GET_SET_TRUE_VERSIONS.items():
        if not groupname.endswith('/'):
            hdf5.create_dataset(groupname, (10,))
            print("Created group %s" % groupname)
        elif groupname not in hdf5:
            hdf5.create_group(groupname)
            print("Created dataset %s" % groupname)
        hdf5.set_version(version=true_version, dataset_name=groupname)

    hdf5.close()
    test_get_versions(hdf5_filename=tokiotest.TEMP_FILE.name)
