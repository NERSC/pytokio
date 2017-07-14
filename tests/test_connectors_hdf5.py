#!/usr/bin/env python

import os
import numpy as np
import tokio.connectors

SAMPLE_INPUT = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'inputs', 'sample.h5lmt')
DATASETS_1D = ['FSStepsGroup/FSStepsDataSet', 'MDSCPUGroup/MDSCPUDataSet']
DATASETS_2D = [
    'FSMissingGroup/FSMissingDataSet', 'MDSOpsGroup/MDSOpsDataSet',
    'OSSCPUGroup/OSSCPUDataSet', 'OSTReadGroup/OSTBulkReadDataSet',
    'OSTWriteGroup/OSTBulkWriteDataSet'
]
POSITIVE_2D = [
    'MDSOpsGroup/MDSOpsDataSet','OSSCPUGroup/OSSCPUDataSet',
    'OSTReadGroup/OSTBulkReadDataSet','OSTWriteGroup/OSTBulkWriteDataSet'
]

def test_connectors_hdf5():
    file = tokio.connectors.Hdf5(SAMPLE_INPUT)

    # Make sure group_name=None works
    assert len(file.to_dataframe().index)
    
    for dataset in DATASETS_1D:
        assert dataset in file
        assert len(file[dataset].shape) == 1
        assert file[dataset][:].sum() > 0
        assert len(file.to_dataframe(dataset).index)
    
    for dataset in DATASETS_2D:
        assert dataset in file
        assert len(file[dataset].shape) == 2
        assert file[dataset][:,:].sum() > 0
        assert len(file.to_dataframe(dataset).columns)

        # Test dataset-dependent correctness
        #
        # Last timstamp greater than the first timestamp
        assert file['FSStepsGroup/FSStepsDataSet'][0] < file['FSStepsGroup/FSStepsDataSet'][-1]

        # No negative loads
        assert np.greater_equal(file['MDSCPUGroup/MDSCPUDataSet'][:], 0.0).all()
        
        # Only 0 or 1 allowed
        assert np.logical_or(np.equal(file['FSMissingGroup/FSMissingDataSet'][:,:], 0),
                             np.equal(file['FSMissingGroup/FSMissingDataSet'][:,:], 1)).all()
        
        # No negative rates
        for dataset in POSITIVE_2D:
            assert np.greater_equal(file[dataset][:], 0.0).all()
