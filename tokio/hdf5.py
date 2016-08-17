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

    def init_datasets( self, oss_names, ost_names, mds_op_names, num_timesteps, host='unknown', filesystem='unknown' ):
        """
        Create datasets if they do not exist, and set the appropriate attributes
        """
        ### Notes:
        ### 1. We do square chunking here because there are times when we
        ###    want to both iterate over time (rows), such as when collecting
        ###    aggregate file system metrics, as well as iterate over STs,
        ###    such as when trying to subselect STs participating in a
        ###    specific job.
        ### 2. v1 has num_timesteps+1 for reasons unknown.  Strict v1 leaves
        ###    the first timestep zero and includes the first timestep from
        ###    the following day as the (num_timesteps+1)th row, but we
        ###    simply leave the (num_timesteps+1)th zero and correctly populate
        ###    the first datapoints of the day instead.
        num_osses = len(oss_names)
        num_osts = len(ost_names)
        num_mds_ops = len(mds_op_names)
        _V1_SCHEMA = {
            '/FSMissingGroup/FSMissingDataSet' : {
                'shape': (num_osses, num_timesteps+1),
                'chunks': True,
                'compression': 'gzip',
                'dtype': 'i4',
            },
            '/FSStepsGroup/FSStepsDataSet' : {
                'shape': (num_timesteps+1,),
                'chunks': True,
                'compression': 'gzip',
                'dtype': 'i4',
            },
            '/MDSCPUGroup/MDSCPUDataSet' : {
                'shape': (num_timesteps+1,),
                'chunks': True,
                'compression': 'gzip',
                'dtype': 'f8',
            },
            '/MDSOpsGroup/MDSOpsDataSet' : {
                'shape': (num_mds_ops,num_timesteps+1),
                'chunks': True,
                'compression': 'gzip',
                'dtype': 'f8',
            },
            '/OSSCPUGroup/OSSCPUDataSet' : {
                'shape': (num_osses, num_timesteps+1),
                'chunks': True,
                'compression': 'gzip',
                'dtype': 'f8',
            },
            '/OSTReadGroup/OSTBulkReadDataSet' : {
                'shape': (num_osts, num_timesteps+1),
                'chunks': True,
                'compression': 'gzip',
                'dtype': 'f8',
            },
            '/OSTWriteGroup/OSTBulkWriteDataSet' : {
                'shape': (num_osts, num_timesteps+1),
                'chunks': True,
                'compression': 'gzip',
                'dtype': 'f8',
            },
        }

        ### version 1 format - datasets
        for dset_name, dset_params in _V1_SCHEMA.iteritems():
            if dset_name not in self:
                hard_name = '/version1/' + dset_name
                self.create_dataset(
                    name=hard_name,
                    **dset_params)
                self[dset_name] = h5py.SoftLink(hard_name)
                tokio._debug_print( "creating softlink %s -> %s" % ( dset_name, hard_name ) )

        ### version 1 format - metadata
        self['/FSStepsGroup/FSStepsDataSet'].attrs['fs'] = filesystem
        self['/FSStepsGroup/FSStepsDataSet'].attrs['host'] = host
        self['/FSStepsGroup/FSStepsDataSet'].attrs['day'] = ""
        self['/FSStepsGroup/FSStepsDataSet'].attrs['nextday'] = ""
        self['/MDSOpsGroup/MDSOpsDataSet'].attrs['OpNames'] = mds_op_names
        self['/OSSCPUGroup/OSSCPUDataSet'].attrs['OSSNames'] = oss_names
        self['/OSTReadGroup/OSTBulkReadDataSet'].attrs['OSTNames'] = ost_names
        self['/OSTWriteGroup/OSTBulkWriteDataSet'].attrs['OSTNames'] = ost_names

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
        ts_map = np.empty( shape=(ts_ct+1,), dtype='i8' )
        for t in range( ts_ct ):
            ts_map[t] = t0_epoch + t * timestep
        ts_map[ts_ct] = t0_epoch + ts_ct * timestep # for the final extraneous v1 point...
        self['/FSStepsGroup/FSStepsDataSet'][:] = ts_map[:]
        del ts_map ### free the array from memory

        self['/FSStepsGroup/FSStepsDataSet'].attrs['day'] = t_start.strftime("%Y-%m-%d")
        self['/FSStepsGroup/FSStepsDataSet'].attrs['nextday'] = (t0_day + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

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
