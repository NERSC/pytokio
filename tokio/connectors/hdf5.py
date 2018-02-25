#!/usr/bin/env python
"""
Provide a TOKIO-aware HDF5 class that knows how to interpret schema versions
encoded in a TOKIO HDF5 file and translate a universal schema into file-specific
schemas.  Also supports dynamically mapping static HDF5 datasets into new
derived datasets dynamically.
"""

import math
import datetime
import h5py
import numpy
import pandas
from tokio.connectors._hdf5 import convert_counts_rates, map_dataset, demux_column

SCHEMA = {
    None: {},
    "1": {
        "datatargets/readbytes": "/datatargets/readbytes",
        "datatargets/writebytes": "/datatargets/writebytes",
        "datatargets/readrates": "/datatargets/readrates",
        "datatargets/writerates": "/datatargets/writerates",
        "datatargets/readops": "/datatargets/readops",
        "datatargets/writeops": "/datatargets/writeops",
        "datatargets/readoprates": "/datatargets/readoprates",
        "datatargets/writeoprates": "/datatargets/writeoprates",
        "mdtargets/opens": "/mdtargets/opens",
        "mdtargets/openrates": "/mdtargets/openrates",
        "mdtargets/closes": "/mdtargets/closes",
        "mdtargets/closerates": "/mdtargets/closerates",
        "mdtargets/mknods": "/mdtargets/mknods",
        "mdtargets/mknodrates": "/mdtargets/mknodrates",
        "mdtargets/links": "/mdtargets/links",
        "mdtargets/linkrates": "/mdtargets/linkrates",
        "mdtargets/unlinks": "/mdtargets/unlinks",
        "mdtargets/unlinkrates": "/mdtargets/unlinkrates",
        "mdtargets/mkdirs": "/mdtargets/mkdirs",
        "mdtargets/mkdirrates": "/mdtargets/mkdirrates",
        "mdtargets/rmdirs": "/mdtargets/rmdirs",
        "mdtargets/rmdirrates": "/mdtargets/rmdirrates",
        "mdtargets/renames": "/mdtargets/renames",
        "mdtargets/renamerates": "/mdtargets/renamerates",
        "mdtargets/getxattrs": "/mdtargets/getxattrs",
        "mdtargets/getxattrrates": "/mdtargets/getxattrrates",
        "mdtargets/statfss": "/mdtargets/statfss",
        "mdtargets/statfsrates": "/mdtargets/statfsrates",
        "mdtargets/setattrs": "/mdtargets/setattrs",
        "mdtargets/setattrrates": "/mdtargets/setattrrates",
        "mdtargets/getattrs": "/mdtargets/getattrs",
        "mdtargets/getattrrates": "/mdtargets/getattrrates",
        "mdservers/cpuuser": "/mdservers/cpuuser",
        "mdservers/cpusys": "/mdservers/cpusys",
        "mdservers/cpuidle": "/mdservers/cpuidle",
        "mdservers/cpuload": "/mdservers/cpuload",
        "mdservers/memfree": "/mdservers/memfree",
        "mdservers/memused": "/mdservers/memused",
        "mdservers/memcached": "/mdservers/memcached",
        "mdservers/memslab": "/mdservers/memslab",
        "mdservers/memslab_unrecl": "/mdservers/memslab_unrecl",
        "mdservers/memtotal": "/mdservers/memtotal",
        "dataservers/cpuuser": "/dataservers/cpuuser",
        "dataservers/cpusys": "/dataservers/cpusys",
        "dataservers/cpuidle": "/dataservers/cpuidle",
        "dataservers/cpuload": "/dataservers/cpuload",
        "dataservers/memfree": "/dataservers/memfree",
        "dataservers/memused": "/dataservers/memused",
        "dataservers/memcached": "/dataservers/memcached",
        "dataservers/memslab": "/dataservers/memslab",
        "dataservers/memslab_unrecl": "/dataservers/memslab_unrecl",
        "dataservers/memtotal": "/dataservers/memtotal",
        "fullness/bytes": "/fullness/bytes",
        "fullness/bytestotal": "/fullness/bytestotal",
        "fullness/inodes": "/fullness/inodes",
        "fullness/inodestotal": "/fullness/inodestotal",
        "failover/datatargets": "/failover/datatargets",
        "failover/mdtargets": "/failover/mdtargets",
    },
}

# Map keys which don't exist as datasets in the underlying HDF5 but can be
# calculated from datasets that _do_ exist to the functions that do these
# conversions.  This table is only consulted when a dataset is not found
# directly in the underlying HDF5 _and_ a mapping from the SCHEMA table
# above does not return a match, so this table contains what appear to be
# some circular references (e.g., both datatargets/readbytes and
# datatargets/readrates).  This allows any valid HDF5 file to contain either
# bytes or rates but have them all present the same datasets to the downstream
# application.
SCHEMA_DATASET_PROVIDERS = {
    None: {
        "datatargets/readbytes": {
            'func': convert_counts_rates,
            'args': {
                'from_key': 'OSTReadGroup/OSTBulkReadDataSet',
                'to_rates': False,
                'transpose': True,
            },
        },
        "datatargets/writebytes": {
            'func': convert_counts_rates,
            'args': {
                'from_key': 'OSTWriteGroup/OSTBulkWriteDataSet',
                'to_rates': False,
                'transpose': True,
            },
        },
        "datatargets/readrates": {
            'func': map_dataset,
            'args': {
                'from_key': "/OSTReadGroup/OSTBulkReadDataSet",
                'transpose': True,
            },
        },
        "datatargets/writerates": {
            'func': map_dataset,
            'args': {
                'from_key': "/OSTWriteGroup/OSTBulkWriteDataSet",
                'transpose': True,
            },
        },
        "dataservers/cpuload": {
            'func': map_dataset,
            'args': {
                'from_key': "/OSSCPUGroup/OSSCPUDataSet",
                'transpose': True,
            },
        },
        "mdservers/cpuload": {
            'func': map_dataset,
            'args': {
                'from_key': "/MDSCPUGroup/MDSCPUDataSet",
                'transpose': True,
                'force2d': True,
            },
        },

        ### MDSOpsGroup, as counts per timestep
        "mdtargets/opens": {
            'func': demux_column,
            'args': {
                'from_key': "/MDSOpsGroup/MDSOpsDataSet",
                'column': 'open',
                'apply_timestep_func': lambda x, timestep: x * timestep,
                'transpose': True
            },
        },
        "mdtargets/closes": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'close',
                'apply_timestep_func': lambda x, timestep: x * timestep,
                'transpose': True,
            },
        },
        "mdtargets/mknods": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'mknod',
                'apply_timestep_func': lambda x, timestep: x * timestep,
                'transpose': True,
            },
        },
        "mdtargets/links": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'link',
                'apply_timestep_func': lambda x, timestep: x * timestep,
                'transpose': True,
            },
        },
        "mdtargets/unlinks": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'unlink',
                'apply_timestep_func': lambda x, timestep: x * timestep,
                'transpose': True,
            },
        },
        "mdtargets/mkdirs": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'mkdir',
                'apply_timestep_func': lambda x, timestep: x * timestep,
                'transpose': True,
            },
        },
        "mdtargets/rmdirs": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'rmdir',
                'apply_timestep_func': lambda x, timestep: x * timestep,
                'transpose': True,
            },
        },
        "mdtargets/renames": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'rename',
                'apply_timestep_func': lambda x, timestep: x * timestep,
                'transpose': True,
            },
        },
        "mdtargets/getxattrs": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'getxattr',
                'apply_timestep_func': lambda x, timestep: x * timestep,
                'transpose': True,
            },
        },
        "mdtargets/statfss": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'statfs',
                'apply_timestep_func': lambda x, timestep: x * timestep,
                'transpose': True,
            },
        },
        "mdtargets/setattrs": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'setattr',
                'apply_timestep_func': lambda x, timestep: x * timestep,
                'transpose': True,
            },
        },
        "mdtargets/getattrs": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'getattr',
                'apply_timestep_func': lambda x, timestep: x * timestep,
                'transpose': True,
            },
        },
        ### MDSOpsGroup, as counts per second
        "mdtargets/openrates": {
            'func': demux_column,
            'args': {
                'from_key': "/MDSOpsGroup/MDSOpsDataSet",
                'column': 'open',
                'transpose': True
            },
        },
        "mdtargets/closerates": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'close',
                'transpose': True,
            },
        },
        "mdtargets/mknodrates": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'mknod',
                'transpose': True,
            },
        },
        "mdtargets/linkrates": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'link',
                'transpose': True,
            },
        },
        "mdtargets/unlinkrates": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'unlink',
                'transpose': True,
            },
        },
        "mdtargets/mkdirrates": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'mkdir',
                'transpose': True,
            },
        },
        "mdtargets/rmdirrates": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'rmdir',
                'transpose': True,
            },
        },
        "mdtargets/renamerates": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'rename',
                'transpose': True,
            },
        },
        "mdtargets/getxattrrates": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'getxattr',
                'transpose': True,
            },
        },
        "mdtargets/statfsrates": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'statfs',
                'transpose': True,
            },
        },
        "mdtargets/setattrrates": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'setattr',
                'transpose': True,
            },
        },
        "mdtargets/getattrrates": {
            'func': demux_column,
            'args': {
                'from_key': '/MDSOpsGroup/MDSOpsDataSet',
                'column': 'getattr',
                'transpose': True,
            },
        },
    },
    "1": {
        "datatargets/readbytes": {
            'func': convert_counts_rates,
            'args': {
                'from_key': 'datatargets/readrates',
                'to_rates': False,
            },
        },
        "datatargets/writebytes": {
            'func': convert_counts_rates,
            'args': {
                'from_key': 'datatargets/writerates',
                'to_rates': False,
            },
        },
        "datatargets/readrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': 'datatargets/readbytes',
                'to_rates': True,
            },
        },
        "datatargets/writerates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': 'datatargets/writebytes',
                'to_rates': True,
            },
        },
        "mdtargets/opens": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/openrates",
                'to_rates': True,
            },
        },
        "mdtargets/closes": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/closerates",
                'to_rates': True,
            },
        },
        "mdtargets/mknods": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/mknodrates",
                'to_rates': True,
            },
        },
        "mdtargets/links": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/linkrates",
                'to_rates': True,
            },
        },
        "mdtargets/unlinks": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/unlinkrates",
                'to_rates': True,
            },
        },
        "mdtargets/mkdirs": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/mkdirrates",
                'to_rates': True,
            },
        },
        "mdtargets/rmdirs": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/rmdirrates",
                'to_rates': True,
            },
        },
        "mdtargets/renames": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/renamerates",
                'to_rates': True,
            },
        },
        "mdtargets/getxattrs": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/getxattrrates",
                'to_rates': True,
            },
        },
        "mdtargets/statfss": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/statfsrates",
                'to_rates': True,
            },
        },
        "mdtargets/setattrs": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/setattrrates",
                'to_rates': True,
            },
        },
        "mdtargets/getattrs": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/getattrrates",
                'to_rates': True,
            },
        },
        "mdtargets/openrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/opens",
                'to_rates': False,
            },
        },
        "mdtargets/closerates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/closes",
                'to_rates': False,
            },
        },
        "mdtargets/mknodrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/mknods",
                'to_rates': False,
            },
        },
        "mdtargets/linkrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/links",
                'to_rates': False,
            },
        },
        "mdtargets/unlinkrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/unlinks",
                'to_rates': False,
            },
        },
        "mdtargets/mkdirrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/mkdirs",
                'to_rates': False,
            },
        },
        "mdtargets/rmdirrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/rmdirs",
                'to_rates': False,
            },
        },
        "mdtargets/renamerates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/renames",
                'to_rates': False,
            },
        },
        "mdtargets/getxattrrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/getxattrs",
                'to_rates': False,
            },
        },
        "mdtargets/statfsrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/statfss",
                'to_rates': False,
            },
        },
        "mdtargets/setattrrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/setattrs",
                'to_rates': False,
            },
        },
        "mdtargets/getattrrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/getattrs",
                'to_rates': False,
            },
        },
    },
}

H5LMT_COLUMN_ATTRS = {
    'MDSOpsGroup/MDSOpsDataSet': 'OpNames',
    'OSTReadGroup/OSTBulkReadDataSet': 'OSTNames',
    'OSTWriteGroup/OSTBulkWriteDataSet': 'OSTNames',
    'OSSCPUGroup/OSSCPUDataSet': 'OSSNames',
}

TIMESTAMP_KEY = 'timestamps'
DEFAULT_TIMESTAMP_DATASET = 'timestamps' # this CANNOT be an absolute location
COLUMN_NAME_KEY = 'columns'

class Hdf5(h5py.File):
    """
    Create a parsed Hdf5 file class
    """
    def __init__(self, *args, **kwargs):
        """
        This is just an HDF5 file object; the magic is in the additional methods
        and indexing that are provided by the TOKIO Time Series-specific HDF5
        object.
        """
        super(Hdf5, self).__init__(*args, **kwargs)

        self.version = self.attrs.get('version')
        self._timesteps = {}

        # Connect the schema map to this object
        if self.version in SCHEMA:
            self.schema = SCHEMA[self.version]
        elif self.version is None:
            self.schema = {}
        else:
            raise KeyError("Unknown schema version %s" % self.version)

        # Connect the schema dataset providers to this object
        if self.version in SCHEMA_DATASET_PROVIDERS:
            self.dataset_providers = SCHEMA_DATASET_PROVIDERS[self.version]
        else:
            self.dataset_providers = {}

    def __getitem__(self, key):
        """
        Return the h5py.Dataset if key is a literal dataset name
                   h5py.Dataset if key maps directly to a literal dataset name
                                given the file schema version
                   numpy.ndarray if key maps to a provider function that can
                                 calculate the requested data
        """
        resolved_key, provider = self._resolve_schema_key(key)
        if resolved_key:
            return super(Hdf5, self).__getitem__(resolved_key)
        elif provider:
            provider_func = provider.get('func')
            provider_args = provider.get('args', {})
            if provider_func is None:
                errmsg = "No provider function for %s" % key
                raise KeyError(errmsg)
            else:
                return provider_func(self, **provider_args)
        else:
            # this should never be hit based on the possible outputs of _resolve_schema_key
            errmsg = "_resolve_schema_key: undefined output from %s" % key
            raise KeyError(errmsg)

    def _resolve_schema_key(self, key):
        """
        Given a key, either return a key that can be used to index self
        directly, or return a provider function and arguments to generate the
        dataset dynamically
        """
        try:
            # If the dataset exists in the underlying HDF5 file, just return it
            super(Hdf5, self).__getitem__(key)
            return key, None

        except KeyError:
            # Straight mapping between the key and a dataset
            key = key.lstrip('/') if isinstance(key, basestring) else key
            if key in self.schema:
                hdf5_key = self.schema.get(key)
                if super(Hdf5, self).__contains__(hdf5_key):
                    return hdf5_key, None

            # Key maps to a transformation
            if key in self.dataset_providers:
                return None, self.dataset_providers[key]

            errmsg = "Unknown key %s in %s" % (key, self.filename)
            raise KeyError(errmsg)

    def get_columns(self, dataset_name):
        """
        Get the column names of a dataset
        """
        # retrieve the dataset to resolve the schema key or get MappedDataset
        dataset = self.__getitem__(dataset_name)

        if self.version is None:
            dataset_name = dataset.name.lstrip('/')
            if dataset_name in H5LMT_COLUMN_ATTRS:
                return dataset.attrs[H5LMT_COLUMN_ATTRS[dataset_name]]
            return []
        else:
            return self.__getitem__(dataset_name).attrs[COLUMN_NAME_KEY]

    def get_timestep(self, dataset_name, timestamps=None):
        """
        Cache or calculate the timestep for a dataset
        """
        if dataset_name not in self._timesteps:
            if timestamps is None:
                timestamps = self.get_timestamps(dataset_name)[0:2]
            self._timesteps[dataset_name] = timestamps[1] - timestamps[0]
        return self._timesteps[dataset_name]

    def get_index(self, dataset_name, target_datetime):
        """
        Turn a datetime object into an integer that can be used to reference
        specific times in datasets.
        """
        timestamps = self.get_timestamps(dataset_name)[0:2]
        timestep = self.get_timestep(dataset_name, timestamps)
        t_start = datetime.datetime.fromtimestamp(timestamps[0])
        return long((target_datetime - t_start).total_seconds() / timestep)

    def get_timestamps(self, dataset_name):
        """
        Return timestamps dataset corresponding to given dataset name
        """
        return get_timestamps(self, dataset_name)

    def get_missing(self, dataset_name, inverse=False):
        """Convert a dataset into a binary matrix highlighting missing values
        """
        return missing_values(self[dataset_name][:], inverse)

    def to_dataframe(self, dataset_name):
        """Convert a dataset into a dataframe

        Args:
            dataset_name (str): dataset name to conver to DataFrame

        Returns:
            Pandas DataFrame indexed by datetime objects corresponding to
            timestamps, columns labeled appropriately, and values from the
            dataset
        """
        if self.version is None:
            return self._to_dataframe_h5lmt(dataset_name)
        return self._to_dataframe(dataset_name)

    def _to_dataframe(self, dataset_name):
        """Convert a dataset into a dataframe via TOKIO HDF5 schema
        """
        values = self[dataset_name][:]
        columns = self.get_columns(dataset_name)
        timestamps = self.get_timestamps(dataset_name)
        dataframe = pandas.DataFrame(data=values,
                                     index=[datetime.datetime.fromtimestamp(t) for t in timestamps],
                                     columns=columns)
        return dataframe

    def _to_dataframe_h5lmt(self, dataset_name):
        """Convert a dataset into a dataframe via H5LMT native schema
        """
        normed_name = dataset_name.lstrip('/')
        col_header_key = H5LMT_COLUMN_ATTRS.get(normed_name)

        # Hack around datasets that lack column headers to retrieve column names
        if col_header_key is not None:
            columns = self[dataset_name].attrs[col_header_key]
        elif normed_name == 'FSMissingGroup/FSMissingDataSet':
            columns = self['/OSSCPUGroup/OSSCPUDataSet'].attrs['OSSNames']
        elif normed_name == 'MDSCPUGroup/MDSCPUDataSet':
            columns = ['unknown_mds']
        else:
            columns = None

        # Get timestamps through regular API
        timestamps = self.get_timestamps(dataset_name)

        # Retrieve and transform data using H5LMT schema directly
        if normed_name == 'FSStepsGroup/FSStepsDataSet':
            values = None
        else:
            num_dims = len(self[dataset_name].shape)
            if num_dims == 1:
                values = self[dataset_name][:]
            elif num_dims == 2:
                values = self[dataset_name][:].T
            elif num_dims > 2:
                raise Exception("Can only convert 1d or 2d datasets to dataframe")

        return pandas.DataFrame(data=values,
                                index=[datetime.datetime.fromtimestamp(t) for t in timestamps],
                                columns=columns)

def get_timestamps_key(hdf5_file, dataset_name):
    """
    Read into an HDF5 file and extract the name of the dataset containing the
    timestamps correspond to the given dataset_name
    """
    # Get dataset out of HDF5 file
    hdf5_dataset = hdf5_file.get(dataset_name)
    if hdf5_dataset is None:
        return None

    if hdf5_file.attrs.get('version') is None and '/FSStepsGroup/FSStepsDataSet' in hdf5_file:
        return '/FSStepsGroup/FSStepsDataSet'

    # Identify the dataset containing timestamps for this dataset
    if TIMESTAMP_KEY in hdf5_dataset.attrs:
        timestamp_key = hdf5_dataset.attrs[TIMESTAMP_KEY]
    else:
        timestamp_key = hdf5_dataset.parent.name + '/' + DEFAULT_TIMESTAMP_DATASET

    # Load timestamps dataset into memory
    if timestamp_key not in hdf5_file:
        raise KeyError("timestamp_key %s does not exist" % timestamp_key)

    return timestamp_key

def get_timestamps(hdf5_file, dataset_name):
    """
    Return the timestamps dataset for a given dataset name
    """
    return hdf5_file[get_timestamps_key(hdf5_file, dataset_name)]

def missing_values(dataset, inverse=False):
    """Identify matrix values that are missing

    Because we initialize datasets with -0.0, we can scan the sign bit of every
    element of an array to determine how many data were never populated.  This
    converts negative zeros to ones and all other data into zeros then count up
    the number of missing elements in the array.

    Args: dataset
    """
    if inverse:
        converter = numpy.vectorize(lambda x:
                                    0 if (x == 0.0 and math.copysign(1, x) < 0.0) else 1)
    else:
        converter = numpy.vectorize(lambda x:
                                    1 if (x == 0.0 and math.copysign(1, x) < 0.0) else 0)
    return converter(dataset)
