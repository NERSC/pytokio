"""Helper classes and functions used by the HDF5 connector

This contains some of the black magic required to make older H5LMT files
compatible with the TOKIO HDF5 schemas and API.
"""

import numpy
import h5py

TIMESTAMP_KEY = 'timestamps'
DEFAULT_TIMESTAMP_DATASET = 'timestamps' # this CANNOT be an absolute location
COLUMN_NAME_KEY = 'columns'

class MappedDataset(h5py.Dataset):
    """
    h5py.Dataset that applies a function to the results of __getitem__
    before returning the data.  Intended to dynamically generate certain
    datasets that are simple derivatives of others.
    """
    def __init__(self, map_function=None, map_kwargs=None, transpose=False, force2d=False,
                 *args, **kwargs):
        """Configure a MappedDatset

        Attach a map function to a h5py.Dataset (or derivative) and store the
        arguments to be fed into that map function whenever this object gets
        sliced.

        Args:
            map_function (function): function to be called on the value returned
                when parent class is sliced
            map_kwargs (dict): kwargs to be passed into map_function
            transpose (bool): when True, transpose the results of map_function
                before returning them.  Required by some H5LMT datasets.
            force2d (bool): when True, convert a 1d array into a 2d array with
                a single column.  Required by some H5LMT datasets.
        """
        if map_kwargs is None:
            map_kwargs = {}

        super(MappedDataset, self).__init__(*args, **kwargs)

        self.map_function = map_function
        self.map_kwargs = map_kwargs
        self.transpose = transpose
        self.force2d = force2d

    def __getitem__(self, key):
        """
        Apply the map function to the result of the parent class and return that
        transformed result instead.  Transpose is very ugly, but required for
        h5lmt support.
        """

        # The following transformations require the entire dataset to be
        # retrieved before it can be sliced, so retrieve it into a memory
        # buffer
        if self.transpose or self.force2d:
            array_buf = numpy.zeros(shape=self.shape, dtype=self.dtype)
            self.read_direct(array_buf)
            if self.transpose:
                array_buf = array_buf.T
            if self.force2d and len(array_buf.shape) == 1:
                array_buf = array_buf.reshape((array_buf.shape[0], 1))
            # We have to __getitem__ *after* applying the transformation or else
            # we won't get transformed indices
            if self.map_function:
                return self.map_function(array_buf, **self.map_kwargs).__getitem__(key)
            else:
                return array_buf.__getitem__(key)
        else:
            # if we didn't have to preload the whole dataset, we get __getitem__
            # then apply the map function
            result = super(MappedDataset, self).__getitem__(key)
            if self.map_function:
                return self.map_function(result, **self.map_kwargs)
            else:
                return result

def _apply_timestep(return_value, parent_dataset, func=lambda x, timestep: x * timestep):
    """Apply a transformation function to a return value

    Transforms the data returned when slicing a h5py.Dataset object by
    applying a function to the dataset's values.  For example if return_value
    are 'counts per timestep' and you want to convert to 'counts per second',
    you would specify func=lambda x, y: x * y

    Args:
        return_value: the value returned when slicing h5py.Dataset
        parent_dataset: the h5py.Dataset which generated return_value
        func: a function which takes two arguments: the first is return_value,
            and the second is the timestep of parent_dataset

    Returns:
        A modified version of return_value (usually a numpy.ndarray)
    """
    hdf5_file = parent_dataset.file
    dataset_name = parent_dataset.name
    timestamps = get_timestamps(hdf5_file, dataset_name)

    if timestamps is None:
        errmsg = "Could not find timestamps for %s in %s" % (dataset_name, hdf5_file.filename)
        raise KeyError(errmsg)

    timestep = timestamps[1] - timestamps[0]

    return func(return_value, timestep)

def _one_column(return_value, col_idx, apply_timestep_func=None, parent_dataset=None):
    """Extract a specific column from a dataset

    Args:
        return_value: the value returned by the parent DataSet object that we
            will modify
        col_idx: the column index for the column we are demultiplexing
        apply_timestep_func (function): if provided, apply this function with
            return_value as the first argument and the timestep of
            parent_dataset as the second.
        parent_dataset (Dataset): if provided, indicates that return_value
            should be divided by the timestep of parent_dataset to convert
            values to rates before returning

    Returns:
        A modified version of return_value (usually a numpy.ndarray)
    """
    modified_values = return_value[:, col_idx:col_idx+1]
    if parent_dataset and apply_timestep_func:
        modified_values = _apply_timestep(modified_values, parent_dataset, func=apply_timestep_func)
    return modified_values

def convert_counts_rates(hdf5_file, from_key, to_rates, *args, **kwargs):
    """Convert a dataset between counts/sec and counts/timestep

    Retrieve a dataset from an HDF5 file, convert it to a MappedDataset, and
    attach a multiply/divide function to it so that subsequent slices return
    a transformed set of data.

    Args:
        hdf5_file (h5py.File): object from which dataset should be loaded
        from_key (str): dataset name key to load from hdf5_file
        to_rates (bool): convert from per-timestep to per-sec (True) or per-sec
            to per-timestep (False)

    Returns:
        A MappedDataset configured to convert to/from rates when dereferenced
    """
    if from_key not in hdf5_file:
        errmsg = "Could not find dataset_name %s in %s" % (from_key, hdf5_file.filename)
        raise KeyError(errmsg)

    dataset = hdf5_file[from_key]
    map_kwargs = {'parent_dataset': dataset}
    if to_rates:
        map_kwargs['func'] = lambda x, timestep: x / timestep
    else:
        map_kwargs['func'] = lambda x, timestep: x * timestep

    return MappedDataset(bind=dataset.id,
                         map_function=_apply_timestep,
                         map_kwargs=map_kwargs,
                         *args,
                         **kwargs)

def map_dataset(hdf5_file, from_key, *args, **kwargs):
    """Create a MappedDataset

    Creates a MappedDataset from an h5py.File (or derivative).  Functionally
    similar to h5py.File.__getitem__.

    Args:
        hdf5_file (h5py.File or connectors.hdf5.Hdf5): file containing dataset of interest
        from_key (str): name of dataset to apply mapping function to
    """
    if from_key not in hdf5_file:
        errmsg = "Could not find dataset_name %s in %s" % (from_key, hdf5_file.filename)
        raise KeyError(errmsg)

    return MappedDataset(bind=hdf5_file[from_key].id,
                         map_function=None,
                         map_kwargs={},
                         *args,
                         **kwargs)

def demux_column(hdf5_file, from_key, column, apply_timestep_func=None, *args, **kwargs):
    """Extract a single column from an HDF5 dataset

    MappedDataset map function to present a single column from a dataset as an
    entire dataset.  Required to bridge the h5lmt metadata table (which encodes
    all metadata ops in a single dataset) and the TOKIO HDF5 format (which
    encodes a single metadata op per dataset)

    Args:
        hdf5_file (h5py.File): the HDF5 file containing the dataset of interest
        from_key (str): the dataset name from which a column should be extracted
        column (str): the column heading to be returned
        transpose (bool): transpose the dataset before returning it

    Returns:
        A MappedDataset configured to extract a single column when dereferenced
    """
    if from_key not in hdf5_file:
        errmsg = "Could not find dataset_name %s in %s" % (from_key, hdf5_file.filename)
        raise KeyError(errmsg)

    column_idx = list(hdf5_file.get_columns(from_key.lstrip('/'))).index(column)
    map_kwargs = {'col_idx': column_idx}
    if apply_timestep_func:
        map_kwargs['parent_dataset'] = hdf5_file[from_key]
        map_kwargs['apply_timestep_func'] = apply_timestep_func

    return MappedDataset(bind=hdf5_file[from_key].id,
                         map_function=_one_column,
                         map_kwargs=map_kwargs,
                         *args,
                         **kwargs)

def get_timestamps_key(hdf5_file, dataset_name):
    """
    Read into an HDF5 file and extract the name of the dataset containing the
    timestamps correspond to the given dataset_name
    """
    # Look for special 'missing' dataset hack
    if len(dataset_name.strip('/').split('/')) == 3:
        dataset_name = dataset_name.rsplit('/', 1)[0]

    # Get dataset out of HDF5 file.  If dataset doesn't exist, throw exception
    hdf5_dataset = hdf5_file[dataset_name]

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
