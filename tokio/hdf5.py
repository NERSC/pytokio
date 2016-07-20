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
    return HDF5( *args, **kwargs )


class HDF5(h5py.File):
    def __init__( self, *args, **kwargs ):
        self.first_timestamp = None
        self.timestep = None
        super(HDF5,self).__init__( *args, **kwargs )

    def init_datasets( self, num_osts, num_timesteps, host='unknown', filesystem='unknown' ):
        """
        Create datasets if they do not exist, and set the appropriate attributes
        """
        ### note that we do square chunking here because there are times when we
        ### want to both iterate over time (rows), such as when collecting
        ### aggregate file system metrics, as well as iterate over STs, such as
        ### when trying to subselect STs participating in a specific job.

        ### version 1 format
        for x in [ 'OSTReadGroup/OSTBulkReadDataSet', 'OSTWriteGroup/OSTBulkWriteDataSet' ]:
            if x not in self:
                self.create_dataset(
                    name=x,
                    shape=(num_osts, num_timesteps),
                    chunks=True,
                    compression="gzip",
                    dtype='f8'
                )
        for x in [ 'FSStepsGroup/FSStepsDataSet' ]:
            if x not in self:
                self.create_dataset(
                    name=x,
                    shape=(num_timesteps,),
                    chunks=True,
                    compression="gzip",
                    dtype='i8'
                )

        self['FSStepsGroup/FSStepsDataSet'].attrs['fs'] = filesystem
        self['FSStepsGroup/FSStepsDataSet'].attrs['host'] = host

        ### version 2 format
        for x in [ 'ost_bytes_read', 'ost_bytes_written' ]:
            if x not in self:
                self.create_dataset(
                    name=x,
                    shape=(num_timesteps, num_osts),
                    chunks=True,
                    compression="gzip",
                    dtype='f8'
                )
        self.attrs['host'] = host
        self.attrs['filesystem'] = filesystem

    def init_timestamps( self, t_start, t_stop, timestep=tokio.LMT_TIMESTEP, fs=None, host=None ):
        """
        Initialize timestamps for the whole HDF5 file.  Version 1 populates an
        entire dataset with equally spaced epoch timestamps, while version 2
        only sets a global timestamp for the first row of each dataset and a
        timestep thereafter.
        """
        t0_day = t_start.replace(hour=0,minute=0,second=0,microsecond=0)
        t0_epoch = time.mktime( t0_day.timetuple() ) # truncate t_start

        ts_ct = int( (t_stop - t_start).total_seconds() / timestep )

        ### version 1 format - create a full dataset of timestamps
        ts_map = np.empty( shape=(ts_ct,), dtype='i8' )
        for t in range( ts_ct ):
            ts_map[t] = t0_epoch + t * timestep
        self['FSStepsGroup/FSStepsDataSet'][:] = ts_map[:]
        del ts_map ### free the array from memory

        self['FSStepsGroup/FSStepsDataSet'].attrs['day'] = t_start.strftime("%Y-%m-%d")
        self['FSStepsGroup/FSStepsDataSet'].attrs['nextday'] = (t0_day + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        ### version 2 format - just store the first timestamp and the timestep
        self.first_timestamp = int(t0_epoch)
        self.attrs['first_timestamp'] = self.first_timestamp
        self.timestep = timestep
        self.attrs['timestep'] = self.timestep


    def get_ost_data( self, t_start, t_stop ):
        """
        Return a generator that produces tuples of (timestamp, read_bytes,
        write_bytes) between t_start (inclusive) and t_stop (exclusive)
        """
        idx0 = self.get_index( t_start )
        idxf = self.get_index( t_stop )

        assert( idx0 <= idxf )

        ### Load the entire output slice into memory to avoid doing a lot of
        ### tiny I/Os.  May have to optimize this for memory later on down the
        ### road.
        ts_array = self['FSStepsGroup/FSStepsDataSet'][idx0:idxf]
        r_array = self['OSTReadGroup/OSTBulkReadDataSet'][:, idx0:idxf]
        w_array = self['OSTWriteGroup/OSTBulkWriteDataSet'][:, idx0:idxf]

        assert( r_array.shape[0] == w_array.shape[0] )

        for t_idx in range( 0, idxf - idx0 ):
            for ost_idx in range( r_array.shape[0] ):
                yield ( 
                    ts_array[t_idx], 
                    r_array[ost_idx,t_idx] * self.timestep, 
                    w_array[ost_idx,t_idx] * self.timestep)

    def get_index( self, t, safe=False ):
        """
        Turn a datetime object into an integer that can be used to reference
        specific times in datasets.
        """
        ### initialize our timestep if we don't already have this
        if self.timestep is None:
            if 'timestep' in self.attrs: 
                self.timestep = self.attrs['timestep']
            elif 'FSStepsGroup/FSStepsDataSet' in self and len(self['FSStepsGroup/FSStepsDataSet']) > 1:
                self.timestep = self['FSStepsGroup/FSStepsDataSet'][1] - self['FSStepsGroup/FSStepsDataSet'][0]
            else:
                self.timestep = tokio.LMT_TIMESTEP
            
        if 'first_timestamp' in self.attrs: 
            t0 = datetime.datetime.fromtimestamp( self.attrs['first_timestamp'] )
        else:
            t0 = datetime.datetime.fromtimestamp( self['FSStepsGroup/FSStepsDataSet'][0] )

        return int((t - t0).total_seconds()) / self.timestep

if __name__ == '__main__':
    pass
