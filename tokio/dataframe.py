#!/usr/bin/env python

import pandas

class DataFrame(pandas.DataFrame):
    def __init__( self, *args, **kwargs ):
        super(DataFrame,self).__init__( *args, **kwargs )

    def from_hdf5(self, hdf5_file):
        """
        Understand pyTOKIO's special flavor of HDF5 for timeseries data
        """
        pass
