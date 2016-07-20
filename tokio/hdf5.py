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

    def init_datasets( self, num_osts, num_timesteps, **kwargs ):
        self._init_datasets_v1( num_osts, num_timesteps, **kwargs )
        self._init_datasets_v2( num_osts, num_timesteps, **kwargs )

    def _init_datasets_v1( self, num_osts, num_timesteps, **kwargs ):
        if 'OSTReadGroup/OSTBulkReadDataSet' not in self:
            self.create_dataset('OSTReadGroup/OSTBulkReadDataSet', (num_osts, num_timesteps), dtype='f8')
        if 'OSTWriteGroup/OSTBulkWriteDataSet' not in self:
            self.create_dataset('OSTWriteGroup/OSTBulkWriteDataSet', (num_osts, num_timesteps), dtype='f8')
        if 'FSStepsGroup/FSStepsDataSet' not in self:
            self.create_dataset('FSStepsGroup/FSStepsDataSet', (num_timesteps,), dtype='i8')

    def _init_datasets_v2( self, num_osts, num_timesteps, **kwargs ):
        for x in [ 'ost_bytes_read', 'ost_bytes_written' ]:
            if x not in self:
                self.create_dataset(x, (num_osts, num_timesteps), dtype='f8')

    def get_ost_data( self, t_start, t_stop ):
        """
        """
        ### Find the lower and upper indices corresponding to t_start/t_stop.
        ### We do not assume the timestamps are equidistant (although they
        ### should be), but we DO assume that they increase monotonically.
        int_start = time.mktime( t_start.timetuple() )
        int_stop  = time.mktime( t_stop.timetuple() )
        assert( int_stop > int_start )
        i_0 = -1
        i_f = -1
        for idx, val in enumerate(self['FSStepsGroup/FSStepsDataSet'][:]):
            if i_0 == -1 and val >= int_start:
                i_0 = idx
            elif i_f == -1 and val > int_stop:
                i_f = idx
                break

        if i_f == -1:
            i_f = self['FSStepsGroup/FSStepsDataSet'].shape[0]

        print i_0, '->', i_f
        assert( i_0 >= 0 and i_f > i_0 )

        ### Load the entire output slice into memory to avoid doing a lot of
        ### tiny I/Os.  May have to optimize this for memory later on down the
        ### road.
        ts_array = self['FSStepsGroup/FSStepsDataSet'][i_0:i_f]
        r_array = self['OSTReadGroup/OSTBulkReadDataSet'][:, i_0:i_f]
        w_array = self['OSTWriteGroup/OSTBulkWriteDataSet'][:, i_0:i_f]

        assert( r_array.shape[0] == w_array.shape[0] )

        for t_idx in range( 0, i_f-i_0 ):
            for ost_idx in range( r_array.shape[0] ):
                yield ( 
                    ts_array[t_idx], 
                    r_array[ost_idx,t_idx]*tokio.LMT_TIMESTEP, 
                    w_array[ost_idx,t_idx]*tokio.LMT_TIMESTEP )


def get_index_from_time( t ):
    return (t.hour * 3600 + t.minute * 60 + t.second) / int(tokio.LMT_TIMESTEP)

if __name__ == '__main__':
    pass
