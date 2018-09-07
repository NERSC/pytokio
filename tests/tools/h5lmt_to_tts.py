#!/usr/bin/env python

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
        input_hdf5 (tokio.connectors.hdf5.Hdf5): h5lmt file descriptor from
            which a dataset should be read
        output_hdf5 (tokio.connectors.hdf5.Hdf5): TOKIO TimeSeries file to which
            the converted dataset should be written
        dataset_name (str): Name of dataset in ``input_file`` to convert
    """

    ts = tokio.timeseries.TimeSeries(dataset_name=dataset_name,
                                     hdf5_file=input_hdf5)

    if ts.dataset is None:
        warnings.warn("Failed to attach to %s" % input_hdf5.filename)

    # Correct semantic incompatibilities between H5LMT and TTS
    ts.dataset = ts.dataset[1:]
    ts.timestamps = ts.timestamps[:-1]

    # Update version
    ts.set_timestamp_key(timestamp_key=None)
    ts.global_version = "1"

    ts.commit_dataset(output_hdf5)

def main():
    """Provide simple CLI for h5lmt_to_tts
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str, default=None, help="HDF5 file to repack")
    parser.add_argument("output", type=str, default=None, help="HDF5 file to save output")
#   parser.add_argument("dataset", nargs="+", type=str, help="HDF5 file to save output")
    args = parser.parse_args()

    if not os.path.isfile(args.input):
        raise IOError("File '%s' does not exist" % args.input)

    print("Opening '%s' as input" % args.input)
    input_hdf5 = tokio.connectors.hdf5.Hdf5(args.input, 'r')

    print("Opening '%s' as output" % args.output)
    output_hdf5 = tokio.connectors.hdf5.Hdf5(args.output, 'a')

    for dataset_name in DATASETS:
        print("Converting %s in %s to %s" % (dataset_name, args.input, args.output))
        h5lmt_to_tts(input_hdf5, output_hdf5, dataset_name)

    input_hdf5.close()
    output_hdf5.close()

if __name__ == "__main__":
    main()
