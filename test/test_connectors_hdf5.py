#!/usr/bin/env python

import os
import numpy as np

import tokio.connectors

SAMPLE_INPUT = os.path.join(os.getcwd(), 'inputs', 'sample.hdf5')

DATASETS_1D = [ 'FSStepsGroup/FSStepsDataSet', 'MDSCPUGroup/MDSCPUDataSet' ]
DATASETS_2D = [ 'FSMissingGroup/FSMissingDataSet',
                'MDSOpsGroup/MDSOpsDataSet',
                'OSSCPUGroup/OSSCPUDataSet',
                'OSTReadGroup/OSTBulkReadDataSet',
                'OSTWriteGroup/OSTBulkWriteDataSet' ]
POSITIVE_2D = [ 'MDSOpsGroup/MDSOpsDataSet',
                'OSSCPUGroup/OSSCPUDataSet',
                'OSTReadGroup/OSTBulkReadDataSet',
                'OSTWriteGroup/OSTBulkWriteDataSet' ]

f = tokio.connectors.HDF5(SAMPLE_INPUT)

### make sure group_name=None works
assert len(f.to_dataframe().index) > 0

for dataset in DATASETS_1D:
    assert dataset in f
    assert len(f[dataset].shape) == 1
    assert f[dataset][:].sum() > 0
    assert len(f.to_dataframe(dataset).index) > 0

for dataset in DATASETS_2D:
    assert dataset in f
    assert len(f[dataset].shape) == 2
    assert f[dataset][:,:].sum() > 0
    assert len(f.to_dataframe(dataset).columns) > 0

### test dataset-dependent correctness

### last timstamp greater than the first timestamp
assert f['FSStepsGroup/FSStepsDataSet'][0] < f['FSStepsGroup/FSStepsDataSet'][-1]

### no negative loads
assert np.greater_equal(f['MDSCPUGroup/MDSCPUDataSet'][:], 0.0).all()

### only 0 or 1 allowed
assert np.logical_or(np.equal(f['FSMissingGroup/FSMissingDataSet'][:,:], 0),
                     np.equal(f['FSMissingGroup/FSMissingDataSet'][:,:], 1)).all()

### no negative rates
for dataset in POSITIVE_2D:
    assert np.greater_equal(f[dataset][:], 0.0).all()
