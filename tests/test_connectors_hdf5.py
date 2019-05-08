#!/usr/bin/env python
"""
Test the HDF5 connector
"""

import datetime
import random
import warnings
import shutil
import nose
import numpy
import tokiotest
import tokio.connectors.hdf5

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

INVALID_DATASET = '/abc/def'

def generate_light_timeseries(file_name=tokiotest.SAMPLE_COLLECTDES_HDF5):
    """
    Return a TimeSeries object that's initialized against the tokiotest sample
    input.  Uses light=True when attaching to prevent loading the entire dataset
    into memory.
    """
    output_hdf5 = tokio.connectors.hdf5.Hdf5(file_name, 'r')
    timeseries = tokio.timeseries.TimeSeries()

    output_hdf5.to_timeseries(dataset_name=tokiotest.SAMPLE_COLLECTDES_DSET, light=True)
#   timeseries.attach(output_hdf5, dataset_name=tokiotest.SAMPLE_COLLECTDES_DSET, light=True)
    return timeseries



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

def test_h5lmt_invalid_dataset_get():
    """connectors.hdf5.Hdf5() h5lmt support, get(non-existent dataset)
    """
    hdf5_file = tokio.connectors.hdf5.Hdf5(tokiotest.SAMPLE_H5LMT_FILE)
    print("Checking %s" % INVALID_DATASET)
    assert hdf5_file.get(INVALID_DATASET) is None

@nose.tools.raises(KeyError)
def test_h5lmt_invalid_dataset():
    """connectors.hdf5.Hdf5() h5lmt support, __getitem__(non-existent dataset)
    """
    hdf5_file = tokio.connectors.hdf5.Hdf5(tokiotest.SAMPLE_H5LMT_FILE)
    print("Checking %s" % INVALID_DATASET)
    dataset = hdf5_file[INVALID_DATASET]

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
    print("Checking %s" % INVALID_DATASET)
    assert hdf5_file.get(INVALID_DATASET) is None

@nose.tools.raises(KeyError)
def test_invalid_dataset():
    """
    connectors.hdf5.__getitem__(non-existent dataset)
    """
    hdf5_file = tokio.connectors.hdf5.Hdf5(tokiotest.SAMPLE_COLLECTDES_HDF5)
    print("Checking %s" % INVALID_DATASET)
    dataset = hdf5_file[INVALID_DATASET]

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

def _test_mapped_dataset(input_file, datasets=None):
    """
    Load two views of the same data set (rates and bytes) and ensure that they
    are being correctly calculated.
    """
    if datasets is None:
        datasets = [
            {
                'rate': 'datatargets/readrates',
                'tot': 'datatargets/readbytes', 
            },
            {
                'rate': 'mdtargets/openrates',
                'tot': 'mdtargets/opens', 
            },
        ]
    numpy.set_printoptions(formatter={'float': '{: 0.1f}'.format},
                           edgeitems=5,
                           linewidth=100)
    print("Testing %s" % input_file)
    hdf5_file = tokio.connectors.hdf5.Hdf5(input_file)
    for dataset_pair in datasets:
        totals = hdf5_file[dataset_pair['tot']]
        rates = hdf5_file[dataset_pair['rate']]
        timestamps = hdf5_file.get_timestamps(dataset_pair['rate'])[0:2]
        timestep = timestamps[1] - timestamps[0]

        print("Timestep appears to be %s" % timestep)
        print(dataset_pair['tot'] + " is")
        print(totals[:, :])
        print()
        print(dataset_pair['rate'] + " is")
        print(rates[:, :] * timestep)
        print()
        print("Are rates a factor of %.2f away from totals?" % timestep)

        equivalency = numpy.isclose(rates[:, :] * timestep, totals[:, :])
        print(((rates[:, :] * timestep) - totals[:, :]).sum())
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

        timestamps = hdf5_file.get_timestamps(dataset_name)[...]
        timestep = timestamps[1] - timestamps[0]
        num_stamps = len(timestamps)
        target_indices = [0, num_stamps//4, num_stamps//2, 3*num_stamps//4, num_stamps-1]

        for target_index in target_indices:
            for fuzz in range(timestep):
                target_datetime = datetime.datetime.fromtimestamp(timestamps[target_index]) \
                                  + datetime.timedelta(seconds=fuzz)
                new_index = hdf5_file.get_index(dataset_name, target_datetime)
                print("%d(%s) == %d(%s)? %s" % (
                    target_index,
                    type(target_index),
                    new_index,
                    type(new_index),
                    target_index == new_index))
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
        func = _test_get_columns_missing
        func.description = "connectors.hdf5.Hdf5.get_columns() with %s, /missing suffix" % input_type
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

def _test_get_columns_missing(input_file):
    """
    Ensure that get_columns() works with /missing suffix
    """
    print("Testing %s" % input_file)
    hdf5_file = tokio.connectors.hdf5.Hdf5(input_file)
    for dataset_name in tokiotest.SAMPLE_TIMESERIES_DATASETS:
        dataset_name += '/missing'
        print("Getting %s from %s" % (dataset_name, hdf5_file.filename))
        result = hdf5_file.get(dataset_name)
        print('result: %s' % result)
        assert result is not None
        column_names = hdf5_file.get_columns(dataset_name)
        assert len(column_names) > 0

@nose.tools.raises(KeyError)
def test_get_columns_invalid():
    """
    connectors.hdf5.Hdf5.get_columns(invalid dataset)
    """
    hdf5_file = tokio.connectors.hdf5.Hdf5(tokiotest.SAMPLE_COLLECTDES_HDF5)
    hdf5_file.get_columns(INVALID_DATASET)

def test_get_timestamps():
    """
    connectors.hdf5.Hdf5.get_timestamps()
    """
    for input_type, input_file in tokiotest.SAMPLE_TIMESERIES_FILES.items():
        func = _test_get_columns
        func.description = "connectors.hdf5.Hdf5.get_timestamps() with %s" % input_type
        yield func, input_file
        func = _test_get_columns
        func.description = "connectors.hdf5.Hdf5.get_timestamps() with %s, /missing suffix" % input_type
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

def _test_get_timestamps_missing(input_file):
    """
    Ensure that get_timestamps() returns valid results with /missing suffix
    """
    print("Testing %s" % input_file)
    hdf5_file = tokio.connectors.hdf5.Hdf5(input_file)
    for dataset_name in tokiotest.SAMPLE_TIMESERIES_DATASETS:
        dataset_name += '/missing'
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

@nose.tools.raises(KeyError)
def test_get_timestamps_invalid():
    """
    connectors.hdf5.Hdf5.get_timestamps(invalid dataset)
    """
    hdf5_file = tokio.connectors.hdf5.Hdf5(tokiotest.SAMPLE_COLLECTDES_HDF5)
    hdf5_file.get_timestamps(INVALID_DATASET)

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

def test_missing_getitem():
    """
    connectors.hdf5 missing values via __getitem__ API
    """
    hdf5 = tokio.connectors.hdf5.Hdf5(tokiotest.SAMPLE_TOKIOTS_FILE, 'r')
    for dset_name in ['/datatargets/readbytes', '/datatargets/writebytes']:
        missing_dset_name = dset_name + "/missing"
        values = hdf5[missing_dset_name][...]

        assert ((values == 1) | (values == 0)).all()
        assert (values == hdf5.get_missing(dset_name)).all()

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

@nose.tools.raises(KeyError)
def test_get_versions_invalid():
    """
    connectors.hdf5.Hdf5.get_version(invalid dataset)
    """
    hdf5_file = tokio.connectors.hdf5.Hdf5(tokiotest.SAMPLE_COLLECTDES_HDF5)
    hdf5_file.get_version(INVALID_DATASET)

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

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_commit_timeseries():
    """connectors.hdf5.Hdf5.commit_timeseries()
    """
    tokiotest.TEMP_FILE.close()

    # Connect to the sample input file
    timeseries1 = tokiotest.generate_timeseries()

    # Create a new HDF5 file into which timeseries1 will be copied
    hdf5_file = tokio.connectors.hdf5.Hdf5(tokiotest.TEMP_FILE.name, 'w')

    # Write the output out as a new HDF5 file
    hdf5_file.commit_timeseries(timeseries1)
    hdf5_file.close()

    # Read that newly generated HDF5 file back in
    timeseries2 = tokiotest.generate_timeseries(file_name=tokiotest.TEMP_FILE.name)

    # Compare the original to the reprocessed
    print("Comparing before/after read/write/read")
    tokiotest.compare_timeseries(timeseries2, timeseries1, verbose=True)

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_commit_timeseries_bad_bounds():
    """connectors.hdf5.Hdf5.commit_timeseries() with out-of-bounds
    """
    tokiotest.TEMP_FILE.close()

    # Connect to the sample input file
    timeseries1 = tokiotest.generate_timeseries(dataset_name=tokiotest.SAMPLE_COLLECTDES_DSET)
    timeseries2 = tokiotest.generate_timeseries(dataset_name=tokiotest.SAMPLE_COLLECTDES_DSET2)

    # Write the output as a new HDF5 file (should work like normal)
    with tokio.connectors.hdf5.Hdf5(tokiotest.TEMP_FILE.name, 'w') as hdf5_file:
        hdf5_file.commit_timeseries(timeseries1)

    # Now trim down the dataset so it no longer spans the same range as the
    # existing HDF5 file - this should work, because the HDF5 will simply retain
    # its start/end since the new data being committed is a complete subset
    print("Attempting trimmed down dataset")
    timeseries1.trim_rows(3)
    with tokio.connectors.hdf5.Hdf5(tokiotest.TEMP_FILE.name) as hdf5_file:
        hdf5_file.commit_timeseries(timeseries1)

    # Add back the rows we took off, and then some - this should NOT work
    # because the data now goes beyond the original maximum timestamp for the
    # file
    print("Attempting bloated existing dataset")
    timeseries1.add_rows(12)
    with tokio.connectors.hdf5.Hdf5(tokiotest.TEMP_FILE.name) as hdf5_file:
        print('Global start: %s' % hdf5_file.attrs.get('start'))
        print('Global end:   %s' % hdf5_file.attrs.get('end'))
        caught = False
        try:
            hdf5_file.commit_timeseries(timeseries1)
        except IndexError:
            caught = True
        assert caught

    # Now commit a completely new dataset that doesn't fit - this should throw
    # a warning (or should we make it throw an exception?)
    print("Attempting bloated non-existent dataset")
    timeseries2.add_rows(12)
    timeseries2.dataset_name = '/blah/blah'
    timeseries2.timestamp_key = '/blah/timestamps'
    with tokio.connectors.hdf5.Hdf5(tokiotest.TEMP_FILE.name) as hdf5_file:
        print('Global start: %s' % hdf5_file.attrs.get('start'))
        print('Global end:   %s' % hdf5_file.attrs.get('end'))
        caught = False
        try:
            hdf5_file.commit_timeseries(timeseries2)
        except IndexError:
            caught = True
#       with warnings.catch_warnings(record=True) as warn:
#           caught = True
#           # cause all warnings to always be triggered
#           warnings.simplefilter("always")
#           hdf5_file.commit_timeseries(timeseries2)
#           print len(warn)
#           assert len(warn) == 1
#           print warn[-1].category, type(warn[-1].category)
#           assert issubclass(warn[-1].category, UserWarning)
#           assert "some warning message" in str(warn[-1].message)
        assert caught

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_uneven_columns():
    """connectors.hdf5.Hdf5.to_timeseries(with uneven columns)
    """

    # We expect to trigger some warnings here
    warnings.filterwarnings('ignore')

    tokiotest.TEMP_FILE.close()
    # copy the input file into a new temporary file with which we can tinker
    with open(tokiotest.SAMPLE_COLLECTDES_HDF5, 'rb') as input_file:
        with open(tokiotest.TEMP_FILE.name, 'wb') as output_file:
            shutil.copyfileobj(input_file, output_file)

    print("Ensure that the input dataset has even column lengths before we make them uneven")
    h5_file = tokio.connectors.hdf5.Hdf5(tokiotest.TEMP_FILE.name, 'r+')
    dataset = h5_file[tokiotest.SAMPLE_COLLECTDES_DSET]
    orig_col_names = list(h5_file.get_columns(dataset.name))
    result = len(orig_col_names) == dataset.shape[1]
    print(orig_col_names)
    print(dataset.attrs['columns'])
    print("%-3d == %3d? %s" % (len(orig_col_names), dataset.shape[1], result))
    assert result

    print("Test case where there are more column names than shape of dataset")
    extra_columns = orig_col_names + ['argle', 'bargle', 'fffff']
    timeseries = tokiotest.generate_timeseries(file_name=tokiotest.TEMP_FILE.name)
    timeseries.set_columns(extra_columns)
    result = len(timeseries.columns) == timeseries.dataset.shape[1]
    print("%-3d == %3d? %s" % (len(timeseries.columns), timeseries.dataset.shape[1], result))
    assert result
    for icol in range(timeseries.dataset.shape[1]):
        print("%-15s(%-15s) == %15s(%-15s)? %s" % (
            extra_columns[icol],
            type(extra_columns[icol]),
            timeseries.columns[icol],
            type(timeseries.columns[icol]),
            extra_columns[icol] == timeseries.columns[icol]))
        assert extra_columns[icol] == timeseries.columns[icol]

    print("Test cases where column names are incomplete compared to shape of dataset")
    fewer_columns = orig_col_names[0:-(3*len(orig_col_names)//4)]
    timeseries = tokiotest.generate_timeseries(file_name=tokiotest.TEMP_FILE.name)
    timeseries.set_columns(fewer_columns)
    result = len(timeseries.columns) < timeseries.dataset.shape[1]
    print("%-3d < %3d? %s" % (len(timeseries.columns), timeseries.dataset.shape[1], result))
    assert result
    for icol, fewer_col in enumerate(fewer_columns):
        print("%-15s == %15s? %s" % (fewer_col, timeseries.columns[icol],
                                     fewer_col == timeseries.columns[icol]))
        assert fewer_col == timeseries.columns[icol]

def test_light_attach():
    """connectors.hdf5.Hdf5.attach_dataset(light=True)
    """
    tokiotest.TEMP_FILE.close()

    full = tokiotest.generate_timeseries()
    light = generate_light_timeseries()
    tokiotest.compare_timeseries(light, full, verbose=True)

def test_get_insert_pos():
    """connectors.hdf5.get_insert_pos()
    """
    # TODO
    # 1. ensure correctness of turning timestamp into an index
    # 2. ensure that create_col=False works
    # 3. ensure that create_col=True works
    raise nose.SkipTest("test not implemented")
