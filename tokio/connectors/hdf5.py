#!/usr/bin/env python
"""
Provide a TOKIO-aware HDF5 class that knows how to interpret schema versions
encoded in a TOKIO HDF5 file and translate a universal schema into file-specific
schemas.  Also supports dynamically mapping static HDF5 datasets into new
derived datasets dynamically.
"""

import math
import time
import datetime
import warnings
import h5py
import numpy
import pandas
import tokio.common
from tokio.connectors._hdf5 import (convert_counts_rates, #pylint: disable=unused-import
                                    map_dataset,
                                    demux_column,
                                    get_timestamps,
                                    get_timestamps_key,
                                    DEFAULT_TIMESTAMP_DATASET,
                                    TIMESTAMP_KEY,
                                    COLUMN_NAME_KEY)

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
        "mdservers/membuffered": "/mdservers/membuffered",
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
        "dataservers/membuffered": "/dataservers/membuffered",
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
                'to_rates': False,
            },
        },
        "mdtargets/closes": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/closerates",
                'to_rates': False,
            },
        },
        "mdtargets/mknods": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/mknodrates",
                'to_rates': False,
            },
        },
        "mdtargets/links": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/linkrates",
                'to_rates': False,
            },
        },
        "mdtargets/unlinks": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/unlinkrates",
                'to_rates': False,
            },
        },
        "mdtargets/mkdirs": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/mkdirrates",
                'to_rates': False,
            },
        },
        "mdtargets/rmdirs": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/rmdirrates",
                'to_rates': False,
            },
        },
        "mdtargets/renames": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/renamerates",
                'to_rates': False,
            },
        },
        "mdtargets/getxattrs": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/getxattrrates",
                'to_rates': False,
            },
        },
        "mdtargets/statfss": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/statfsrates",
                'to_rates': False,
            },
        },
        "mdtargets/setattrs": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/setattrrates",
                'to_rates': False,
            },
        },
        "mdtargets/getattrs": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/getattrrates",
                'to_rates': False,
            },
        },
        "mdtargets/openrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/opens",
                'to_rates': True,
            },
        },
        "mdtargets/closerates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/closes",
                'to_rates': True,
            },
        },
        "mdtargets/mknodrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/mknods",
                'to_rates': True
            },
        },
        "mdtargets/linkrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/links",
                'to_rates': True,
            },
        },
        "mdtargets/unlinkrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/unlinks",
                'to_rates': True,
            },
        },
        "mdtargets/mkdirrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/mkdirs",
                'to_rates': True,
            },
        },
        "mdtargets/rmdirrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/rmdirs",
                'to_rates': True,
            },
        },
        "mdtargets/renamerates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/renames",
                'to_rates': True,
            },
        },
        "mdtargets/getxattrrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/getxattrs",
                'to_rates': True,
            },
        },
        "mdtargets/statfsrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/statfss",
                'to_rates': True,
            },
        },
        "mdtargets/setattrrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/setattrs",
                'to_rates': True,
            },
        },
        "mdtargets/getattrrates": {
            'func': convert_counts_rates,
            'args': {
                'from_key': "mdtargets/getattrs",
                'to_rates': True,
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

class Hdf5(h5py.File):
    """Hdf5 file class with extra hooks to parse different schemas

    Provides an h5py.File-like class with added methods to provide a generic
    API that can decode different schemata used to store file system load
    data.

    Attributes:
        always_translate (bool): If True, looking up datasets by keys will
            always attempt to map that key to a new dataset according to the
            schema even if the key matches the name of an existing dataset.
        dataset_providers (dict): Map of logical dataset names (keys) to dicts
            that describe the functions used to convert underlying literal
            dataset data into the format expected when dereferencing the logical
            dataset name.
        schema (dict): Map of logical dataset names (keys) to the literal
            dataset names in the underlying file (values)
        _version (str): Defined and used at initialization time to determine
            what schema to apply to map the HDF5 connector API to the underlying
            HDF5 file.
        _timesteps (dict): Keyed by dataset name (str) and has values
            corresponding to the timestep (in seconds) between each sampled
            datum in that dataset.
    """
    def __init__(self, *args, **kwargs):
        """Initialize an HDF5 file

        This is just an HDF5 file object; the magic is in the additional methods
        and indexing that are provided by the TOKIO Time Series-specific HDF5
        object.

        Args:
            ignore_version (bool): If true, do not throw KeyError if the HDF5
                file does not contain a valid version.
        """
        ignore_version = kwargs.pop('ignore_version', False)

        super(Hdf5, self).__init__(*args, **kwargs)

        # If True, always translate __getitem__ requests according to the
        # schema, even if __getitem__ requests a dataset that exists
        self.always_translate = False

        self._version = self.attrs.get('version')
        if isinstance(self._version, bytes):
            self._version = self._version.decode()
        self._timesteps = {}

        # Connect the schema map to this object
        if self._version in SCHEMA:
            self.schema = SCHEMA[self._version]
        elif self._version is None:
            self.schema = {}
        elif not ignore_version:
            raise KeyError("Unknown schema version %s" % self._version)

        # Connect the schema dataset providers to this object
        if self._version in SCHEMA_DATASET_PROVIDERS:
            self.dataset_providers = SCHEMA_DATASET_PROVIDERS[self._version]
        else:
            self.dataset_providers = {}

    def __getitem__(self, key):
        """Resolve dataset names into actual data

        Provides a single interface through which standard keys can be
        dereferenced and a semantically consistent view of data is returned
        regardless of the schema of the underlying HDF5 file.

        Passes through the underlying h5py.Dataset via direct access or a 1:1
        mapping between standardized key and an underlying dataset name, or a
        numpy array if an underlying h5py.Dataset must be transformed to match the
        structure and semantics of the data requested.

        Can also suffix datasets with special meta-dataset names
        (e.g., "/missing") to access data that is related to the root
        dataset.

        Args:
            key (str): The standard name of a dataset to be accessed.

        Returns:
            h5py.Dataset or numpy.ndarray:

              * h5py.Dataset if key is a literal dataset name
              * h5py.Dataset if key maps directly to a literal dataset name
                given the file schema version
              * numpy.ndarray if key maps to a provider function that can
                calculate the requested data
        """
        if not self.always_translate and super(Hdf5, self).__contains__(key):
            # If the dataset exists in the underlying HDF5 file, just return it
            return super(Hdf5, self).__getitem__(key)

        # Quirky way to access the missing data of a dataset through the
        # __getitem__ API via recursion
        base_key, modifier = reduce_dataset_name(key)
        if modifier and modifier == 'missing':
            return self.get_missing(base_key)

        resolved_key, provider = self._resolve_schema_key(key)

        if resolved_key:
            # Otherwise, attempt to map the logical key to a literal key
            return super(Hdf5, self).__getitem__(resolved_key)
        elif provider:
            # Or run the value through a key provider
            provider_func = provider.get('func')
            provider_args = provider.get('args', {})
            if provider_func is None:
                errmsg = "No provider function for %s" % key
                raise KeyError(errmsg)
            else:
                return provider_func(self, **provider_args)
        else:
            # This should never be hit based on the possible outputs of _resolve_schema_key
            errmsg = "_resolve_schema_key: undefined output from %s" % key
            raise KeyError(errmsg)

    def _resolve_schema_key(self, key):
        """
        Given a key, either return a key that can be used to index self
        directly, or return a provider function and arguments to generate the
        dataset dynamically
        """
        if super(Hdf5, self).__contains__(key):
            # If the dataset exists in the underlying HDF5 file, just return it
            return key, None

        # Straight mapping between the key and a dataset
        key = key.lstrip('/') if tokio.common.isstr(key) else key
        if key in self.schema:
            hdf5_key = self.schema.get(key)
            if super(Hdf5, self).__contains__(hdf5_key):
                return hdf5_key, None

        # Key maps to a transformation
        if key in self.dataset_providers:
            return None, self.dataset_providers[key]

        errmsg = "Unknown key %s in %s" % (key, self.filename)
        raise KeyError(errmsg)

    def get_version(self, dataset_name=None):
        """Get the version attribute from an HDF5 file dataset

        Args:
            dataset_name (str): Name of dataset to retrieve version.  If None,
                return the global file's version.
        Returns:
            str: The version string for the specified dataset
        """
        if dataset_name is None:
            return self._version
        else:
            # resolve dataset name
            dataset = self.__getitem__(dataset_name)
            try:
                # dataset can be either an HDF5 dataset or numpy.ndarray
                version = dataset.attrs.get("version")
            except AttributeError:
                version = None
            if version is None:
                version = self._version
            if isinstance(version, bytes):
                return version.decode() # for python3
            return version

    def set_version(self, version, dataset_name=None):
        """Set the version attribute from an HDF5 file dataset

        Provide a portable way to set the global schema version or the version
        of a specific dataset.

        Args:
            version (str): The new version to be set
            dataset_name (str): Name of dataset to set version.  If None,
                set the global file's version.
        """
        if dataset_name is None:
            self._version = version
            return self._version

        # resolve dataset name
        dataset = self.__getitem__(dataset_name)
        if dataset is None:
            raise KeyError("Dataset %s does not exist" % dataset_name)
        dataset.attrs["version"] = version
        return version

    def get_columns(self, dataset_name):
        """Get the column names of a dataset

        Args:
            dataset_name (str): name of dataset whose columns will be retrieved

        Returns:
            numpy.ndarray: Array of column names, or empty if no columns defined
        """
        # Look for special 'missing' dataset hack
        if len(dataset_name.strip('/').split('/')) == 3:
            dataset_name = dataset_name.rsplit('/', 1)[0]

        if self.get_version(dataset_name=dataset_name) is None:
            return self._get_columns_h5lmt(dataset_name)

        return self.__getitem__(dataset_name).attrs.get(COLUMN_NAME_KEY).astype('U')

    def _get_columns_h5lmt(self, dataset_name):
        """Get the column names of an h5lmt dataset
        """
        dataset = self.__getitem__(dataset_name)
        orig_dataset_name = dataset_name.lstrip('/')
        dataset_name = dataset.name.lstrip('/')
        if dataset_name == 'MDSOpsGroup/MDSOpsDataSet' and orig_dataset_name != dataset_name:
            return numpy.array([SCHEMA_DATASET_PROVIDERS[None][orig_dataset_name]['args']['column']])
        elif dataset_name in H5LMT_COLUMN_ATTRS:
            return dataset.attrs[H5LMT_COLUMN_ATTRS[dataset_name]].astype('U')
        elif dataset_name == 'MDSCPUGroup/MDSCPUDataSet':
            return numpy.array(['_unknown'])
        elif dataset_name == 'FSMissingGroup/FSMissingDataSet':
            return numpy.array(['_unknown%04d' % i for i in range(dataset.shape[1])])
        else:
            raise KeyError('Unknown h5lmt dataset %s' % dataset_name)

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
        return int((target_datetime - t_start).total_seconds() / timestep)

    def get_timestamps(self, dataset_name):
        """Return timestamps dataset corresponding to given dataset name

        This method returns a dataset, not a numpy array, so you can face severe
        performance penalties trying to iterate directly on the return value!
        To iterate over timestamps, it is almost always better to dereference
        the dataset to get a numpy array and iterate over that in memory.

        Args:
            dataset_name (str): Logical name of dataset whose timestamps should
                be retrieved

        Returns:
            h5py.Dataset: The dataset containing the timestamps corresponding to
            dataset_name.
        """
        return get_timestamps(self, dataset_name)

    def get_missing(self, dataset_name, inverse=False):
        """Convert a dataset into a matrix indicating the abscence of data

        Args:
            dataset_name (str): name of dataset to access
            inverse (bool): return 0 for missing and 1 for present if True

        Returns:
            numpy.ndarray: Array of numpy.int8 of 1 and 0 to indicate the
            presence or absence of specific elements
        """
        if self.get_version(dataset_name=dataset_name) is None:
            return self._get_missing_h5lmt(dataset_name, inverse=inverse)
        return missing_values(self[dataset_name][:], inverse)

    def _get_missing_h5lmt(self, dataset_name, inverse=False):
        """Return the FSMissingGroup dataset from an H5LMT file

        Encodes a hot mess of hacks to return something that looks like what
        `get_missing()` would return for a real dataset.

        Args:
            dataset_name (str): name of dataset to access
            inverse (bool): return 0 for missing and 1 for present if True

        Returns:
            numpy.ndarray: Array of numpy.int8 of 1 and 0 to indicate the
            presence or absence of specific elements
        """
        dataset = self.__getitem__(dataset_name)
        missing_dataset = self.get('/FSMissingGroup/FSMissingDataSet')
        if len(dataset.shape) == 1:
            result = numpy.zeros((dataset.shape[0], 1), dtype=numpy.int8)
        elif dataset.shape == missing_dataset.shape:
            result = missing_dataset[:, :].astype('i8').T
        else:
            result = numpy.zeros(dataset[:, :].shape, dtype=numpy.int8).T

        if inverse:
            return (~result.astype(bool)).astype('i8')
        return result

    def to_dataframe(self, dataset_name):
        """Convert a dataset into a dataframe

        Args:
            dataset_name (str): dataset name to convert to DataFrame

        Returns:
            pandas.DataFrame: DataFrame indexed by datetime objects
            corresponding to timestamps, columns labeled appropriately, and
            values from the dataset
        """
        if self.get_version(dataset_name=dataset_name) is None:
            return self._to_dataframe_h5lmt(dataset_name)
        return self._to_dataframe(dataset_name)

    def _to_dataframe(self, dataset_name):
        """Convert a dataset into a dataframe via TOKIO HDF5 schema
        """
        values = self[dataset_name][:]
        columns = self.get_columns(dataset_name)
        timestamps = self.get_timestamps(dataset_name)[...]
        if len(columns) < values.shape[1]:
            columns.resize(values.shape[1])

        # transform missing data into NaNs
        mask = missing_values(values) != 0
        try:
            values[mask] = numpy.nan
        except ValueError: # ValueError: cannot convert float NaN to integer
            # don't bother converting non-float arrays' -0.0 into NaNs
            pass

        dataframe = pandas.DataFrame(data=values,
                                     index=[datetime.datetime.fromtimestamp(t) for t in timestamps],
                                     columns=columns)
        return dataframe

    def _to_dataframe_h5lmt(self, dataset_name):
        """Convert a dataset into a dataframe via H5LMT native schema
        """
        normed_name, modifier = reduce_dataset_name(dataset_name)
        if not modifier:
            normed_name = dataset_name.lstrip('/')
        else:
            normed_name = normed_name.lstrip('/')

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
        timestamps = self.get_timestamps(normed_name)[...]

        # Retrieve and transform data using H5LMT schema directly
        if normed_name == 'FSStepsGroup/FSStepsDataSet':
            values = None
        else:
            num_dims = len(self[dataset_name].shape)
            if num_dims == 1:
                values = self[dataset_name][:]
                num_indices_expected = self[dataset_name].shape[0]
            elif num_dims == 2:
                # only transpose if dataset_name refers to a native type
                if normed_name in SCHEMA_DATASET_PROVIDERS[None]:
                    values = self[dataset_name][:]
                    columns = self.get_columns(normed_name)
                else:
                    values = self[dataset_name][:].T
                num_indices_expected = values.shape[0]
            elif num_dims > 2:
                raise Exception("Can only convert 1d or 2d datasets to dataframe")

        indices = [datetime.datetime.fromtimestamp(t) for t in timestamps]
        num_indices = len(indices)

        # if an HDF5 file was initialized but not fully populated, the number of
        # timestamps (indices) might be less than the size of the matrix being
        # stored.  this is OK as long as there are no valid values in the part
        # of the matrix that don't have corresponding indices.
        if num_indices_expected != num_indices:
            warning_msg = "dataset size and timestamps are inconsistent (%d vs %d values; missing sum is %f)" \
                % (num_indices_expected, num_indices, values[num_indices:, :].sum())
            missings = self.get_missing(normed_name)
#           print("shape of defined range: (%d, %d)" % missings[:num_indices, :].shape)
#           print("shape of undefined range: (%d, %d)" % missings[num_indices:, :].shape)
#           print("missing elements in defined range:", missings[:num_indices, :].sum())
#           print("missing elements in undefined range:", missings[num_indices:, :].sum())

            # if we have more indices than values, this is unrecoverable because
            # we don't know where in `values` data was written twice
            if num_indices > num_indices_expected:
                raise IndexError(warning_msg)

            # h5lmt format doesn't support distinguishing uninitialized elements
            # from initialized-but-zero, so we assume that a zero indicates
            # missing here
            if (self.get_version() is None and values[num_indices:, :].sum() != 0.0) \
            or (self.get_version() is not None and ~(missings[num_indices:, :].astype(bool)).sum() != 0):
                raise IndexError(warning_msg)
            if num_dims == 1:
                values = values[:num_indices]
            else:
                values = values[:num_indices, :]
            warnings.warn(warning_msg)

        return pandas.DataFrame(data=values,
                                index=indices,
                                columns=columns)

    def to_timeseries(self, dataset_name, light=False):
        """Creates a TimeSeries representation of a dataset

        Create a TimeSeries dataset object with the data from an existing HDF5
        dataset.

        Responsible for setting timeseries.dataset_name, timeseries.columns, timeseries.dataset,
        timeseries.dataset_metadata, timeseries.group_metadata, timeseries.timestamp_key

        Args:
            dataset_name (str): Name of existing dataset in self to convert into
                a TimeSeries object
            light (bool): If True, don't actually load datasets into memory;
                reference them directly into the HDF5 file

        Returns:
            tokio.timeseries.TimeSeries: The in-memory representation of the
            given dataset.
        """
        timeseries = tokio.timeseries.TimeSeries()
        timeseries.dataset_name = dataset_name

        try:
            dataset = self[dataset_name]
        except KeyError:
            # can't attach because dataset doesn't exist; pass this back to caller so it can init
            return None

        timeseries.dataset = dataset if light else dataset[:, :]

        # load and decode version of dataset and file schema
        timeseries.global_version = self['/'].attrs.get('version')
        timeseries.version = self.get_version(dataset_name)
        if isinstance(timeseries.version, bytes):
            timeseries.version = timeseries.version.decode()

        # copy columns into memory
        columns = self.get_columns(dataset_name)
        timeseries.set_columns(columns)

        # copy metadata into memory
        for key, value in dataset.attrs.items():
            if isinstance(value, bytes):
                timeseries.dataset_metadata[key] = value.decode()
            else:
                timeseries.dataset_metadata[key] = value
        for key, value in dataset.parent.attrs.items():
            if isinstance(value, bytes):
                timeseries.group_metadata[key] = value.decode()
            else:
                timeseries.group_metadata[key] = value

        timeseries.timestamp_key = get_timestamps_key(self, dataset_name)
        timeseries.timestamps = self[timeseries.timestamp_key]
        timeseries.timestamps = timeseries.timestamps if light else timeseries.timestamps[:]

        timeseries.timestep = timeseries.timestamps[1] - timeseries.timestamps[0]
        return timeseries

    def commit_timeseries(self, timeseries, **kwargs):
        """Writes contents of a TimeSeries object into a group

        Args:
            timeseries (tokio.timeseries.TimeSeries): the time series to save
                as a dataset within self
            kwargs (dict): Extra arguments to pass to self.create_dataset()
        """
        extra_dataset_args = {
            'dtype': 'f8',
            'chunks': True,
            'compression': 'gzip',
        }
        extra_dataset_args.update(kwargs)

        # Create the dataset in the HDF5 file (if necessary)
        if timeseries.dataset_name in self:
            dataset_hdf5 = self[timeseries.dataset_name]
        else:
            dataset_hdf5 = self.create_dataset(name=timeseries.dataset_name,
                                               shape=timeseries.dataset.shape,
                                               **extra_dataset_args)

        # when timestamp_key has been left empty, use the default
        timestamp_key = timeseries.timestamp_key
        if timestamp_key is None:
            timestamp_key = '/'.join(timeseries.dataset_name.split('/')[0:-1] \
                                 + [DEFAULT_TIMESTAMP_DATASET])

        # Create the timestamps in the HDF5 file (if necessary) and calculate
        # where to insert our data into the HDF5's dataset
        if timestamp_key not in self:
            timestamps_hdf5 = self.create_dataset(name=timestamp_key,
                                                  shape=timeseries.timestamps.shape,
                                                  dtype='i8')
            # Copy the in-memory timestamp dataset into the HDF5 file
            timestamps_hdf5[:] = timeseries.timestamps[:]
            t_start = 0
            t_end = timeseries.timestamps.shape[0]
            start_timestamp = timeseries.timestamps[0]
            end_timestamp = timeseries.timestamps[-1] + timeseries.timestep
        else:
            existing_timestamps = self.get_timestamps(timeseries.dataset_name)
            t_start, t_end = get_insert_indices(timeseries.timestamps, existing_timestamps)

            if t_start < 0 \
            or t_start > (len(existing_timestamps) - 2) \
            or t_end < 1 \
            or t_end > len(existing_timestamps):
                raise IndexError("cannot commit dataset that is not a subset of existing data")

            start_timestamp = existing_timestamps[0]
            end_timestamp = existing_timestamps[-1] + timeseries.timestep

        # Make sure that the start/end timestamps are consistent with the HDF5
        # file's global time range
        if 'start' not in self.attrs:
            self.attrs['start'] = start_timestamp
            self.attrs['end'] = end_timestamp
        else:
            if self.attrs['start'] != start_timestamp \
            or self.attrs['end'] != end_timestamp:
#               warnings.warn(
                raise IndexError("Mismatched start or end values:  %d != %d or %d != %d" % (
                    start_timestamp, self.attrs['start'],
                    end_timestamp, self.attrs['end']))

        # If we're updating an existing dataset, use its column names and ordering.
        # Otherwise sort the columns before committing them.
        if COLUMN_NAME_KEY in dataset_hdf5.attrs:
            timeseries.rearrange_columns(timeseries.columns)
        else:
            timeseries.sort_columns()

        # Copy the in-memory dataset into the HDF5 file
        dataset_hdf5[t_start:t_end, :] = timeseries.dataset[:, :]

        # Copy column names into metadata before committing metadata
        timeseries.dataset_metadata[COLUMN_NAME_KEY] = timeseries.columns
        timeseries.dataset_metadata['updated'] = int(time.mktime(datetime.datetime.now().timetuple()))

        # If timeseries.version was never set, don't set a dataset-level version in the HDF5
        if timeseries.version is not None:
            self.set_version(timeseries.version, dataset_name=timeseries.dataset_name)

        # Set the file's global version to indicate its schema
        if timeseries.global_version is not None:
            self['/'].attrs['version'] = timeseries.global_version

        # Insert/update dataset metadata
        for key, value in timeseries.dataset_metadata.items():
            # special hack for column names
            if key == COLUMN_NAME_KEY:
                # note: the behavior of numpy.string_(x) where
                # type(x) == numpy.array is _different_ in python2 vs. python3.
                # Python3 happily converts each element to a numpy.string_,
                # while Python2 first calls a.__repr__ to turn it into a single
                # string, then converts that to numpy.string_.
                dataset_hdf5.attrs[key] = numpy.array([numpy.string_(x) for x in value])
            elif tokio.common.isstr(value):
                dataset_hdf5.attrs[key] = numpy.string_(value)
            elif value is None:
                warnings.warn("Skipping attribute %s (null value) for %s" % (key, timeseries.dataset_name))
            else:
                dataset_hdf5.attrs[key] = value

        # Insert/update group metadata
        for key, value in timeseries.group_metadata.items():
            if tokio.common.isstr(value):
                dataset_hdf5.parent.attrs[key] = numpy.string_(value)
            else:
                dataset_hdf5.parent.attrs[key] = value

def missing_values(dataset, inverse=False):
    """Identify matrix values that are missing

    Because we initialize datasets with -0.0, we can scan the sign bit of every
    element of an array to determine how many data were never populated.  This
    converts negative zeros to ones and all other data into zeros then count up
    the number of missing elements in the array.

    Args:
        dataset: dataset to access
        inverse (bool): return 0 for missing and 1 for present if True

    Returns:
        numpy.ndarray: Array of numpy.int8 of 1 and 0 to indicate the presence
        or absence of specific elements
    """
    zero = numpy.int8(0)
    one = numpy.int8(1)
    if inverse:
        converter = numpy.vectorize(lambda x:
                                    zero if (x == 0.0 and math.copysign(1, x) < 0.0) else one)
    else:
        converter = numpy.vectorize(lambda x:
                                    one if (x == 0.0 and math.copysign(1, x) < 0.0) else zero)
    return converter(dataset)


def reduce_dataset_name(key):
    """Divide a dataset name into is base and modifier
    Args:
        dataset_name (str): Key to reference a dataset that may or may not have
            a modifier suffix
    Returns:
        tuple of (str, str or None): First string is the base key, the second
            string is the modifier.
    """
    if key.endswith('/missing'):
        return tuple(key.rsplit('/', 1))
    return key, None

def get_insert_indices(my_timestamps, existing_timestamps):
    """
    Given new timestamps and an existing series of timestamps, find the indices
    overlap so that new data can be inserted into the middle of an existing
    dataset
    """
    existing_timestep = existing_timestamps[1] - existing_timestamps[0]
    my_timestep = my_timestamps[1] - my_timestamps[0]

    # make sure the time delta is ok
    if existing_timestep != my_timestep:
        raise Exception("Existing dataset has different timestep (mine=%d, existing=%d)"
                        % (my_timestep, existing_timestep))

    my_offset = (my_timestamps[0] - existing_timestamps[0]) // existing_timestep
    my_end = my_offset + len(my_timestamps)

    return my_offset, my_end
