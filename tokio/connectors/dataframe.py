#!/usr/bin/env python

import hdf5
import pandas

class DataFrame(pandas.DataFrame):
    def __init__( self, *args, **kwargs ):
        super(DataFrame,self).__init__( *args, **kwargs )

    def from_hdf5(self, hdf5_file, group_name):
        """
        Understand pyTOKIO's special flavor of HDF5 for timeseries data
        """
        return hdf5.HDF5(hdf5_file).to_dataframe(group_name)
