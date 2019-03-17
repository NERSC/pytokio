#!/usr/bin/env python

import os
import warnings
import argparse
import numpy
import tokio.timeseries
import tokio.connectors.hdf5

VERBOSE = False

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

def vprint(message):
    if VERBOSE:
        print(message)

def h5lmt_to_tts(input_hdf5, output_hdf5, dataset_name):
    """Convert an H5LMT dataset into a TOKIO TimeSeries dataset
    
    Args:
        input_hdf5 (tokio.connectors.hdf5.Hdf5): h5lmt file descriptor from
            which a dataset should be read
        output_hdf5 (tokio.connectors.hdf5.Hdf5): TOKIO TimeSeries file to which
            the converted dataset should be written
        dataset_name (str): Name of dataset in ``input_file`` to convert
    """

    ts = input_hdf5.to_timeseries(dataset_name=dataset_name)

    if ts.dataset is None:
        warnings.warn("Failed to attach to %s" % input_hdf5.filename)

    # Correct semantic incompatibilities between H5LMT and TTS
    ts.dataset = ts.dataset[1:]
    ts.timestamps = ts.timestamps[:-1]

    # Update version
    ts.set_timestamp_key(timestamp_key=None)
    ts.global_version = "1"

    # Encode missing data
    if '/FSMissingGroup/FSMissingDataSet' in input_hdf5:
        missing_dataset = input_hdf5['/FSMissingGroup/FSMissingDataSet'][...].T[1:]
        if missing_dataset.shape == ts.dataset.shape:
            vprint("  Applying missing values to %s" % dataset_name)
            num_zeroed = 0
            num_missing = 0
            iterator = numpy.nditer(ts.dataset, flags=['multi_index'])
            while not iterator.finished:
                # iterator[0] contains the element
                # iterator.multi_index is a tuple of the indices
                if missing_dataset[iterator.multi_index]:
                    if iterator[0] != 0.0:
                        num_zeroed += 1
                    ts.dataset[iterator.multi_index] = -0.0
                    num_missing += 1
                iterator.iternext()
            vprint("  Found %d missing elements; %d contained nonzero values" % (num_missing, num_zeroed))

    output_hdf5.commit_timeseries(timeseries=ts)

def main():
    """Provide simple CLI for h5lmt_to_tts
    """
    global VERBOSE

    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action='store_true', help="Enable verbose messages")
    parser.add_argument("input", type=str, default=None, help="HDF5 file to repack")
    parser.add_argument("output", type=str, default=None, help="HDF5 file to save output")
    parser.add_argument("-d", "--dataset", action="append", help="Datasets to translate (default: all)")
    args = parser.parse_args()

    if not os.path.isfile(args.input):
        raise IOError("File '%s' does not exist" % args.input)

    if args.verbose:
        VERBOSE=True

    if args.dataset:
        datasets = list(args.dataset)
    else:
        datasets = DATASETS

    vprint("Opening '%s' as input" % args.input)
    input_hdf5 = tokio.connectors.hdf5.Hdf5(args.input, 'r')

    vprint("Opening '%s' as output" % args.output)
    output_hdf5 = tokio.connectors.hdf5.Hdf5(args.output, 'a')

    num_processed = 0
    for dataset_name in datasets:
        vprint("Converting %s in %s to %s" % (dataset_name, args.input, args.output))
        h5lmt_to_tts(input_hdf5, output_hdf5, dataset_name)
        num_processed += 1

    input_hdf5.close()
    output_hdf5.close()
    print("Translated %d datasets into %s" % (num_processed, args.output))

if __name__ == "__main__":
    main()
