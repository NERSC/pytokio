#!/usr/bin/env python

import tokio
import time
import datetime
import h5py
import numpy as np

def connect( *args, **kwargs ):
    """
    Generate a special tokio.HDF5 object which is derived from h5py's File
    object
    """
    ret = h5py.File( *args, **kwargs )
    ret.__class__ = HDF5
    return ret


class HDF5(h5py.File):
    def __init__( self ):
        pass

    def init_datasets( self, num_osts, num_timesteps ):
        self._init_datasets_v1( num_osts, num_timesteps )
        self._init_datasets_v2( num_osts, num_timesteps )

    def _init_datasets_v1( self, num_osts, num_timesteps ):
        if 'OSTReadGroup/OSTBulkReadDataSet' not in self:
            self.create_dataset('OSTReadGroup/OSTBulkReadDataSet', (num_osts, num_timesteps), dtype='f8')
        if 'OSTWriteGroup/OSTBulkWriteDataSet' not in self:
            self.create_dataset('OSTWriteGroup/OSTBulkWriteDataSet', (num_osts, num_timesteps), dtype='f8')
        if 'FSStepsGroup/FSStepsDataSet' not in self:
            self.create_dataset('FSStepsGroup/FSStepsDataSet', (num_timesteps,), dtype='i8')

    def _init_datasets_v2( self, num_osts, num_timesteps ):
        if 'ost_bytes_read' not in self:
            self.create_dataset('ost_bytes_read', (num_osts, num_timesteps), dtype='f4')
        if 'ost_bytes_written' not in self:
            self.create_dataset('ost_bytes_written', (num_osts, num_timesteps), dtype='f4')

def get_index_from_time( t ):
    return (t.hour * 3600 + t.minute * 60 + t.second) / int(tokio.LMT_TIMESTEP)

if __name__ == '__main__':
    pass
