"""
Helper classes and functions used by the HDF5 connector
"""

import tokio
import h5py

class MappedDataset(h5py.Dataset):
    """
    h5py.Dataset that applies a function to the results of __getitem__
    before returning the data.  Intended to dynamically generate certain
    datasets that are simple derivatives of others.
    """
    def __init__(self, map_function, map_kwargs, *args, **kwargs):
        """
        Attach a map function and arguments to be fed into that map function
        whenever this object gets sliced
        """
        super(MappedDataset, self).__init__(*args, **kwargs)

        self.map_function = map_function
        self.map_kwargs = map_kwargs

    def __getitem__(self, key):
        """
        Apply the map function to the result of the parent class and return that
        transformed result instead
        """
        result = super(MappedDataset, self).__getitem__(key)
        return self.map_function(result, **self.map_kwargs)

def multiply_by_timestep(return_value, parent_dataset, divide=False):
    """
    Transform the data returned when slicing a h5py.Dataset object by
    multiplying or dividing it by that dataset's timestep.

    return_value is what slicing h5py.Dataset returns
    parent_dataset is the h5py.Dataset which generated return_value
    divide is bool whether we want to divide or multiply by timestep
    """
    hdf5_file = parent_dataset.file
    dataset_name = parent_dataset.name
    timestamps, _ = tokio.timeseries.extract_timestamps(hdf5_file, dataset_name)

    if timestamps is None:
        errmsg = "Could not find timestamps for %s in %s" % (dataset_name, hdf5_file.filename)
        raise KeyError(errmsg)

    timestep = timestamps[1] - timestamps[0]

    if divide:
        return return_value / timestep
    else:
        return return_value * timestep

def convert_bytes_rates(hdf5_file, from_key, to_rates):
    """
    Retrieve a dataset from an HDF5 file, convert it to a MappedDataset, and
    attach a multiply/divide function to it so that subsequent slices return
    a transformed set of data.

    hdf5_file is the h5py.File object from which the dataset should be loaded
    from_key is the dataset name key that we wish to load from hdf5_file
    to_rates is True/False--will we convert to rates or to absolute bytes?
    """
    if from_key not in hdf5_file:
        errmsg = "Could not find dataset_name %s in %s" % (from_key, hdf5_file.filename)
        raise KeyError(errmsg)

    dataset = hdf5_file[from_key]
    dataset.__class__ = MappedDataset
    dataset.map_function = multiply_by_timestep
    dataset.map_kwargs = {
        'parent_dataset': dataset,
        'divide': to_rates,
    }

    return dataset
