#!/usr/bin/env python
"""
Test the HDF5 connector
"""

import numpy
import tokiotest
import tokio.connectors

DATASETS_1D = ['FSStepsGroup/FSStepsDataSet', 'MDSCPUGroup/MDSCPUDataSet']
DATASETS_2D = [
    'FSMissingGroup/FSMissingDataSet', 'MDSOpsGroup/MDSOpsDataSet',
    'OSSCPUGroup/OSSCPUDataSet', 'OSTReadGroup/OSTBulkReadDataSet',
    'OSTWriteGroup/OSTBulkWriteDataSet'
]
POSITIVE_2D = [
    'MDSOpsGroup/MDSOpsDataSet', 'OSSCPUGroup/OSSCPUDataSet',
    'OSTReadGroup/OSTBulkReadDataSet', 'OSTWriteGroup/OSTBulkWriteDataSet'
]

def test_h5lmt():
    """
    connectors.hdf5.Hdf5() h5lmt support
    """
    hdf5_file = tokio.connectors.Hdf5(tokiotest.SAMPLE_H5LMT_FILE)

    # Make sure group_name=None works
    assert len(hdf5_file.to_dataframe().index)

    for dataset in DATASETS_1D:
        assert dataset in hdf5_file
        assert len(hdf5_file[dataset].shape) == 1
        assert hdf5_file[dataset][:].sum() > 0
        assert len(hdf5_file.to_dataframe(dataset).index)

    for dataset in DATASETS_2D:
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

def test_tts():
    """
    connectors.hdf5.Hdf5() TOKIO Time Series support
    """
    hdf5_file = tokio.connectors.Hdf5(tokiotest.SAMPLE_COLLECTDES_HDF5)

def test_mapped_dataset():
    """
    connectors.hdf5 mapped dataset
    """
    hdf5_file = tokio.connectors.Hdf5(tokiotest.SAMPLE_COLLECTDES_HDF5)
    _ = hdf5_file['datatargets/readbytes']
    _ = hdf5_file['datatargets/readrates']
