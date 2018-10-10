#!/usr/bin/env python
"""
Downsample all datasets in a given TOKIO Time Series file
"""

import os
import argparse
import h5py

DATASETS = [
    '/datatargets/readbytes',
    '/datatargets/writebytes',
    '/dataservers/cpuload',
    '/mdservers/cpuload',
    '/mdtargets/opens',
    '/mdtargets/closes',
    '/mdtargets/mknods',
    '/mdtargets/links',
    '/mdtargets/unlinks',
    '/mdtargets/mkdirs',
    '/mdtargets/rmdirs',
    '/mdtargets/renames',
    '/mdtargets/getxattrs',
    '/mdtargets/statfss',
    '/mdtargets/setattrs',
    '/mdtargets/getattrs',
]

def downsample_dataset(input_hdf5, output_hdf5, dataset_name, stride=2):
    """Convert an H5LMT dataset into a TOKIO TimeSeries dataset

    Args:
        input_file (h5py.File): HDF5 file descriptor from which a dataset should
            be read
        output_file (h5py.File): HDF5 file descriptor to which the downsampled
            dataset should be written
        dataset_name (str): Name of dataset in ``input_file`` to downsample
    """
    extra_dataset_args = {
        'dtype': 'f8',
        'chunks': True,
        'compression': 'gzip',
    }

    input_dataset = input_hdf5[dataset_name]
    if len(input_dataset.shape) == 1:
        data = input_dataset[::stride]
    elif len(input_dataset.shape) == 2:
        data = input_dataset[::stride, :]
    else:
        raise ValueError("input dataset had dimensions > 2")

    output_hdf5.create_dataset(name=dataset_name,
                               data=data,
                               **extra_dataset_args)

    for key, value in input_dataset.attrs.items():
        output_hdf5[dataset_name].attrs[key] = value

def copy_group_metadata(input_hdf5, output_hdf5):
    """Copies the attributes between HDF5 files

    Args:
        input_file (h5py.File): HDF5 file descriptor from which group attributes
            should be copied
        output_file (h5py.File): HDF5 file descriptor to which group attributes
            should be copied
    """
    for group_name in ['/'] + input_hdf5.keys():
        for key, value in input_hdf5[group_name].attrs:
            output_hdf5[group_name].attrs[key] = value
            print("%s[%s] copied" % (group_name, key))

def main():
    """Provide simple CLI for downsample_dataset
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--stride", type=int, default=2, help="stride of downsampling (default: 2)")
    parser.add_argument("input", type=str, default=None, help="HDF5 file to repack")
    parser.add_argument("output", type=str, default=None, help="HDF5 file to save output")
#   parser.add_argument("dataset", nargs="+", type=str, help="HDF5 file to save output")
    args = parser.parse_args()

    if not os.path.isfile(args.input):
        raise IOError("File '%s' does not exist" % args.input)

    print("Opening '%s' as input" % args.input)
    input_hdf5 = h5py.File(args.input, 'r')

    print("Opening '%s' as output" % args.output)
    output_hdf5 = h5py.File(args.output, 'w')

    for dataset_name in DATASETS:
        print("Downsampling %s in %s to %s" % (dataset_name, args.input, args.output))
        downsample_dataset(input_hdf5, output_hdf5, dataset_name, stride=args.stride)

    input_hdf5.close()
    output_hdf5.close()

if __name__ == "__main__":
    main()
