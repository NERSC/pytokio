#!/usr/bin/env python
#
# Known issues:
#  - timestamps dataset not correctly being created; old FSStepsDataSet is
#    carried over instead
#  - root version is not being correctly populated
#  - 2d datasets need to have their 17,281st row trimmed
#
# What works:
#  - dataset is transposed
#  - actual data is correctly transferred to new dataset name
#

import os
import warnings
import argparse
import tokio.timeseries
import tokio.connectors.hdf5

DATASETS = [
    '/datatargets/readrates',
    '/datatargets/writerates',
    '/dataservers/cpuload',
    '/mdservers/cpuload',
    '/mdtargets/openrates',
    '/mdtargets/closerates',
    '/mdtargets/mknodrates',
    '/mdtargets/linkrates',
    '/mdtargets/unlinkrates',
    '/mdtargets/mkdirrates',
    '/mdtargets/rmdirrates',
    '/mdtargets/renamerates',
    '/mdtargets/getxattrrates',
    '/mdtargets/statfsrates',
    '/mdtargets/setattrrates',
    '/mdtargets/getattrrates',
]

def h5lmt_to_tts(input_hdf5, output_hdf5, dataset_name):
    """Convert an H5LMT dataset into a TOKIO TimeSeries dataset
    
    Args:
        input_file (tokio.connectors.hdf5.Hdf5): h5lmt file descriptor from
            which a dataset should be read
        output_file (tokio.connectors.hdf5.Hdf5): TOKIO TimeSeries file to which
            the converted dataset should be written
        dataset_name (str): Name of dataset in ``input_file`` to convert
    """

    ts = tokio.timeseries.TimeSeries(dataset_name=dataset_name,
                                     hdf5_file=input_hdf5)

    if ts.dataset is None:
        warnings.warn("Failed to attach to %s" % input_file)

    ts.commit_dataset(output_hdf5)

def main():
    """Provide simple CLI for h5lmt_to_tts
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", type=str, default=None, help="HDF5 file to repack")
    parser.add_argument("-o", "--output", type=str, default=None, help="HDF5 file to save output")
#   parser.add_argument("dataset", nargs="+", type=str, help="HDF5 file to save output")
    args = parser.parse_args()

    if not args.input:
        input_file = "default.h5lmt"
    else:
        input_file = args.input

    if not args.output:
        output_file = "default.h5lmt"
    else:
        output_file = args.output

    if not os.path.isfile(input_file):
        raise IOError("File '%s' does not exist" % input_file)

    print("Opening '%s' as input" % input_file)
    input_hdf5 = tokio.connectors.hdf5.Hdf5(input_file, 'r')

    print("Opening '%s' as output" % output_file)
    output_hdf5 = tokio.connectors.hdf5.Hdf5(output_file, 'a')

    for dataset_name in DATASETS:
        print("Converting %s in %s to %s" % (dataset_name, input_file, output_file))
        h5lmt_to_tts(input_hdf5, output_hdf5, dataset_name)

    input_hdf5.close()
    output_hdf5.close()

if __name__ == "__main__":
    main()
