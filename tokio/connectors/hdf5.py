#!/usr/bin/env python

import time
import datetime
import h5py
import numpy as np
import pandas as pd
from ..debug import debug_print as _debug_print
from ..config import LMT_TIMESTEP

_NATIVE_VERSION = 1

class Hdf5(h5py.File):
    """
    Create a parsed Hdf5 file class 
    """
    def __init__(self, *args, **kwargs):
        super(Hdf5,self).__init__(*args, **kwargs)

        # Timestanp that will help to sort hdf5 files  
        if 'FSStepsGroup/FSStepsDataSet' in self:
            self.first_timestamp = datetime.datetime.fromtimestamp(self['FSStepsGroup/FSStepsDataSet'][0])
            self.last_timestamp = datetime.datetime.fromtimestamp(self['FSStepsGroup/FSStepsDataSet'][-1])
        else:
            self.first_timestamp = None
            self.last_timestamp = None

        # Timestep saving 
        self.timestep = self['/'].attrs.get('timestep')
        if self.timestep is None:
            self.timestep = LMT_TIMESTEP
        
        # Define the version format of the object
        if self['/'].attrs.get('version') is None:
            self.version = _NATIVE_VERSION
        else:
            self.version = self.attrs.get('version')
    
    def get_index(self, t):
        """
        Turn a datetime object into an integer that can be used to reference
        specific times in datasets.
        
        """
        # Initialize our timestep if we don't already have this
        if self.timestep is None:
            if 'timestep' in self.attrs: 
                self.timestep = self.attrs['timestep']
            elif 'FSStepsGroup/FSStepsDataSet' in self and len(self['FSStepsGroup/FSStepsDataSet']) > 1:
                self.timestep = self['FSStepsGroup/FSStepsDataSet'][1] - self['FSStepsGroup/FSStepsDataSet'][0]
            else:
                self.timestep = LMT_TIMESTEP
        
        if 'first_timestamp' in self.attrs: 
            t0 = datetime.datetime.fromtimestamp(self.attrs['first_timestamp'])
        else:
            t0 = datetime.datetime.fromtimestamp(self['FSStepsGroup/FSStepsDataSet'][0])

        return int((t - t0).total_seconds()) / int(self.timestep)

    def to_dataframe(self, dataset_name=None):
        """
        Convert the hdf5 class in a pandas dataframe 
        """
        # Convenience:may put in lower case
        _INDEX_DATASET_NAME = '/FSStepsGroup/FSStepsDataSet'
        if dataset_name is None:
            dataset_name = _INDEX_DATASET_NAME
        # Normalize to absolute path
        if not dataset_name.startswith('/'):
            dataset_name = '/' + dataset_name

        if dataset_name in ('/OSTReadGroup/OSTBulkReadDataSet',
                          '/OSTWriteGroup/OSTBulkWriteDataSet'):
            col_header_key = 'OSTNames'
        elif dataset_name == '/MDSOpsGroup/MDSOpsDataSet':
            col_header_key = 'OpNames'
        elif dataset_name == '/OSSCPUGroup/OSSCPUDataSet':
            col_header_key = 'OSSNames'
        else:
            col_header_key = None
        
        # Get column header from col_header_key
        if col_header_key is not None:
            col_header = self[dataset_name].attrs[col_header_key]
        elif dataset_name == '/FSMissingGroup/FSMissingDataSet' \
        and '/OSSCPUGroup/OSSCPUDataSet' in self:
            # Because FSMissingDataSet lacks the appropriate metadata in v1...
            col_header = self['/OSSCPUGroup/OSSCPUDataSet'].attrs['OSSNames']
        else:
            col_header = None

        # Retrieve timestamp indexes
        index = self[_INDEX_DATASET_NAME][:]

        # Retrieve hdf5 values
        if dataset_name == _INDEX_DATASET_NAME:
            values = None
        else:
            num_dims = len(self[dataset_name].shape)
            if num_dims == 1:
                values = self[dataset_name][:]
            elif num_dims == 2:
                values = self[dataset_name][:,:].T
            elif num_dims > 2:
                raise Exception("Can only convert 1d or 2d datasets to dataframe")

        return pd.DataFrame(data=values,
                            index=[datetime.datetime.fromtimestamp(tstamp) for tstamp in index],
                            columns=col_header)
