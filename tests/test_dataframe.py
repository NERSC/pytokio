#!/usr/bin/env python

import os
import tokio.connectors.dataframe
import nose.tools

SAMPLE_INPUT = os.path.join(os.getcwd(), 'inputs', 'sample.hdf5')

def check_dataframe_associativity():
    df = tokio.dataframe.DataFrame().from_hdf5(SAMPLE_INPUT, 'OSTReadGroup/OSTBulkReadDataSet')
    sum1 = df.sum().sum()
    sum2 = df.sum(1).sum()
    nose.tools.assert_almost_equal(sum2/1e8, sum1/1e8, 7)

def test_dataframe():
    df = tokio.connectors.dataframe.DataFrame()
    df = df.from_hdf5(SAMPLE_INPUT, 'OSTReadGroup/OSTBulkReadDataSet')
    
