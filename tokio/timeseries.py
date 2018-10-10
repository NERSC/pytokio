#/usr/bin/env python
"""
TimeSeries class to simplify updating and manipulating the in-memory
representation of time series data.
"""

import re
import math
import time
import datetime
import warnings
import numpy
import tokio.connectors.hdf5
from tokio.common import isstr

class TimeSeries(object):
    """
    In-memory representation of an HDF5 group in a TokioFile.  Can either
    initialize with no datasets, or initialize against an existing HDF5
    group.
    """
    def __init__(self, dataset_name=None,
                 start=None, end=None, timestep=None, num_columns=None,
                 column_names=None, timestamp_key=None,
                 hdf5_file=None, sort_hex=False):

        # numpy.ndarray of timestamp measurements
        self.timestamps = None

        # time between consecutive timestamps
        self.timestep = None
        # numpy.ndarray of the timeseries data itself
        self.dataset = None
        # string containing fully qualified dataset name+path
        self.dataset_name = None
        # list of strings serving as column headings
        self.columns = []
        # map between strings in self.columns and its corresponding column index in self.dataset
        self.column_map = {}
        # key-value of metadata to be stored in HDF5 group.attrs
        self.group_metadata = {}
        # key-value of metadata to be stored in HDF5 dataset.attrs
        self.dataset_metadata = {}
        # path corresponding to self.timestamps dataset in HDF5 file
        self.timestamp_key = timestamp_key
        # True = natural sort columns assuming hex-encoded numbers; False = only recognize decimals
        self.sort_hex = sort_hex
        # string describing dataset version
        self.version = None
        # string describing schema version
        self.global_version = None

        # attempt to attach the object if requested.
        if dataset_name is not None:
            attached = False
            if hdf5_file is not None:
                attached = self.attach(hdf5_file, dataset_name)
            # if attach fails due to dataset being uninitialized, initialize it instead
            if not attached and start and end and timestep and num_columns:
                self.init(start, end, timestep, num_columns, dataset_name,
                          column_names, timestamp_key)

    def init(self, start, end, timestep, num_columns, dataset_name,
             column_names=None, timestamp_key=None):
        """Create a new TimeSeries dataset object

        Responsible for setting self.timestep, self.timestamp_key, and self.timestamps

        Args:
            start (datetime): timestamp to correspond with the 0th index
            end (datetime): timestamp at which timeseries will end (exclusive)
            timestep (int): seconds between consecutive timestamp indices
            num_columns (int): number of columns to initialize in the numpy.ndarray
            dataset_name (str): an HDF5-compatible name for this timeseries
            column_names (list of str, optional): strings by which each column
                should be indexed.  Must be less than or equal to num_columns in
                length; difference remains uninitialized
            timestamp_key (str, optional): an HDF5-compatible name for this timeseries'
                timestamp vector.  Default is /groupname/timestamps
        """
        if column_names is None:
            column_names = []

        # Attach the timestep dataset
        self.timestep = timestep

        # Calculate the hours in a day in epoch-seconds since Python datetime
        # and timedelta doesn't understand DST
        time_list = []
        end_epoch = int(time.mktime(end.timetuple()))
        timestamp = int(time.mktime(start.timetuple()))
        while timestamp < end_epoch:
            time_list.append(timestamp)
            timestamp += timestep

        self.timestamps = numpy.array(time_list)

        # Attach the dataset itself
        self.dataset_name = dataset_name
        self.dataset = numpy.full((len(self.timestamps), num_columns), -0.0)
        self.set_columns(column_names)

        self.set_timestamp_key(timestamp_key, safe=True)

    def set_timestamp_key(self, timestamp_key, safe=False):
        """Set the timestamp key

        Args:
            timestamp_key (str or None): The key for the timestamp dataset.  If
                None, use the default
            safe (bool): If true, do not overwrite an existing timestamp key
        """
        # Root the timestamp_key at the same parent as the dataset
        if not safe or self.timestamp_key is None:
            if timestamp_key is None:
                self.timestamp_key = '/'.join(self.dataset_name.split('/')[0:-1] \
                                     + [tokio.connectors.hdf5.DEFAULT_TIMESTAMP_DATASET])
            else:
                self.timestamp_key = timestamp_key

    def attach(self, hdf5_file, dataset_name, light=False):
        """
        Populate a TimeSeries dataset object with the data from an existing HDF5
        dataset.  If light is True, don't actually load datasets into memory;
        reference them directly into the HDF5 file

        Responsible for setting self.dataset_name, self.columns, self.dataset,
        self.dataset_metadata, self.group_metadata, self.timestamp_key
        """
        self.dataset_name = dataset_name

        try:
            dataset = hdf5_file[dataset_name]
        except KeyError:
            # can't attach because dataset doesn't exist; pass this back to caller so it can init
            return False

        self.dataset = dataset if light else dataset[:, :]

        # load and decode version of dataset and file schema
        self.global_version = hdf5_file['/'].attrs.get('version')
        self.version = hdf5_file.get_version(dataset_name)
        if isinstance(self.version, bytes):
            self.version = self.version.decode()

        # copy columns into memory
        columns = hdf5_file.get_columns(dataset_name)
        self.set_columns(columns)

        # copy metadata into memory
        for key, value in dataset.attrs.items():
            if isinstance(value, bytes):
                self.dataset_metadata[key] = value.decode()
            else:
                self.dataset_metadata[key] = value
        for key, value in dataset.parent.attrs.items():
            if isinstance(value, bytes):
                self.group_metadata[key] = value.decode()
            else:
                self.group_metadata[key] = value

        self.timestamp_key = tokio.connectors.hdf5.get_timestamps_key(hdf5_file, dataset_name)
        self.timestamps = hdf5_file[self.timestamp_key]
        self.timestamps = self.timestamps if light else self.timestamps[:]

        self.timestep = self.timestamps[1] - self.timestamps[0]
        return True

    def commit_dataset(self, hdf5_file, **kwargs):
        """
        Write contents of this object into an HDF5 file group
        """
        extra_dataset_args = {
            'dtype': 'f8',
            'chunks': True,
            'compression': 'gzip',
        }
        extra_dataset_args.update(kwargs)

        # Create the dataset in the HDF5 file (if necessary)
        if self.dataset_name in hdf5_file:
            dataset_hdf5 = hdf5_file[self.dataset_name]
        else:
            dataset_hdf5 = hdf5_file.create_dataset(name=self.dataset_name,
                                                    shape=self.dataset.shape,
                                                    **extra_dataset_args)

        # Create the timestamps in the HDF5 file (if necessary) and calculate
        # where to insert our data into the HDF5's dataset
        if self.timestamp_key not in hdf5_file:
            timestamps_hdf5 = hdf5_file.create_dataset(name=self.timestamp_key,
                                                       shape=self.timestamps.shape,
                                                       dtype='i8')
            # Copy the in-memory timestamp dataset into the HDF5 file
            timestamps_hdf5[:] = self.timestamps[:]
            t_start = 0
            t_end = self.timestamps.shape[0]
            start_timestamp = self.timestamps[0]
            end_timestamp = self.timestamps[-1] + self.timestep
        else:
            existing_timestamps = tokio.connectors.hdf5.get_timestamps(hdf5_file, self.dataset_name)
            t_start, t_end = get_insert_indices(self.timestamps, existing_timestamps)

            if t_start < 0 \
            or t_start > (len(existing_timestamps) - 2) \
            or t_end < 1 \
            or t_end > len(existing_timestamps):
                raise IndexError("cannot commit dataset that is not a subset of existing data")

            start_timestamp = existing_timestamps[0]
            end_timestamp = existing_timestamps[-1] + self.timestep

        # Make sure that the start/end timestamps are consistent with the HDF5
        # file's global time range
        if 'start' not in hdf5_file.attrs:
            hdf5_file.attrs['start'] = start_timestamp
            hdf5_file.attrs['end'] = end_timestamp
        else:
            if hdf5_file.attrs['start'] != start_timestamp \
            or hdf5_file.attrs['end'] != end_timestamp:
#               warnings.warn(
                raise IndexError("Mismatched start or end values:  %d != %d or %d != %d" % (
                    start_timestamp, hdf5_file.attrs['start'],
                    end_timestamp, hdf5_file.attrs['end']))

        # If we're updating an existing HDF5, use its column names and ordering.
        # Otherwise sort the columns before committing them.
        if tokio.connectors.hdf5.COLUMN_NAME_KEY in dataset_hdf5.attrs:
            self.rearrange_columns(self.columns)
        else:
            self.sort_columns()

        # Copy the in-memory dataset into the HDF5 file
        dataset_hdf5[t_start:t_end, :] = self.dataset[:, :]

        # Copy column names into metadata before committing metadata
        self.dataset_metadata[tokio.connectors.hdf5.COLUMN_NAME_KEY] = self.columns
        self.dataset_metadata['updated'] = int(time.mktime(datetime.datetime.now().timetuple()))

        # If self.version was never set, don't set a dataset-level version in the HDF5
        if self.version is not None:
            hdf5_file.set_version(self.version, dataset_name=self.dataset_name)

        # Set the file's global version to indicate its schema
        if self.global_version is not None:
            hdf5_file['/'].attrs['version'] = self.global_version

        # Insert/update dataset metadata
        for key, value in self.dataset_metadata.items():
            # special hack for column names
            if (key == tokio.connectors.hdf5.COLUMN_NAME_KEY):
                # note: the behavior of numpy.string_(x) where
                # type(x) == numpy.array is _different_ in python2 vs. python3.
                # Python3 happily converts each element to a numpy.string_,
                # while Python2 first calls a.__repr__ to turn it into a single
                # string, then converts that to numpy.string_.
                dataset_hdf5.attrs[key] = numpy.array([numpy.string_(x) for x in value])
            elif isstr(value):
                dataset_hdf5.attrs[key] = numpy.string_(value)
            elif value is None:
                warnings.warn("Skipping attribute %s (null value) for %s" % (key, self.dataset_name))
            else:
                dataset_hdf5.attrs[key] = value

        # Insert/update group metadata
        for key, value in self.group_metadata.items():
            if isstr(value):
                dataset_hdf5.parent.attrs[key] = numpy.string_(value)
            else:
                dataset_hdf5.parent.attrs[key] = value

    def update_column_map(self):
        """
        Create the mapping of column names to column indices
        """
        self.column_map = {}
        for index, column_name in enumerate(self.columns):
            self.column_map[column_name] = index

    def set_columns(self, column_names):
        """
        Set the list of column names
        """
        num_columns = len(column_names)
        # handle case where HDF5 has more column names than dataset columns via truncation
        if num_columns > self.dataset.shape[1]:
            column_names = self.columns[0:self.dataset.shape[1]]
            truncated = self.columns[self.dataset.shape[1]:]
            warnings.warn(
                "Dataset has %d column names but %d columns; dropping columns %s"
                % (num_columns, self.dataset.shape[1], ', '.join(truncated)))
        self.columns = [str(x) for x in column_names] # ensure that elements are mutable and not unicode
        self.update_column_map()

    def add_column(self, column_name):
        """
        Add a new column and update the column map
        """
        index = len(self.columns)
        if column_name in self.column_map:
            warnings.warn("Adding degenerate column '%s' at %d (exists at %d)"
                          % (column_name, index, self.column_map[column_name]))
        self.column_map[column_name] = index
        if index >= (self.dataset.shape[1]):
            errmsg = "new index %d (%s) exceeds number of columns %d (%s)" % (
                index, column_name, self.dataset.shape[1], self.columns[-1])
            raise IndexError(errmsg)
        self.columns.append(str(column_name)) # convert from unicode to str for numpy
        return index

    def sort_columns(self):
        """
        Rearrange the dataset's column data by sorting them by their headings
        """
        self.rearrange_columns(sorted_nodenames(self.columns, sort_hex=self.sort_hex))

    def rearrange_columns(self, new_order):
        """
        Rearrange the dataset's columnar data by an arbitrary column order given
        as an enumerable list
        """
        # validate the new order - new_order must contain at least all of
        # the elements in self.columns, but may contain more than that
        for new_key in new_order:
            if new_key not in self.columns:
                raise Exception("key %s in new_order not in columns" % new_key)

        # walk the new column order
        for new_index, new_column in enumerate(new_order):
            # new_order can contain elements that don't exist; this happens when
            # re-ordering a small dataset to be inserted into an existing,
            # larger dataset
            if new_column not in self.columns:
                warnings.warn("Column '%s' in new order not present in TimeSeries" % new_column)
                continue

            old_index = self.column_map[new_column]
            self.swap_columns(old_index, new_index)

    def swap_columns(self, index1, index2):
        """
        Swap two columns of the dataset in-place
        """
        # save the data from the column we're about to swap
        saved_column_data = self.dataset[:, index2].copy()
        saved_column_name = self.columns[index2]

        # swap column data
        self.dataset[:, index2] = self.dataset[:, index1]
        self.dataset[:, index1] = saved_column_data[:]

        # swap column names too
        self.columns[index2] = self.columns[index1]
        self.columns[index1] = saved_column_name

        # update the column map
        self.column_map[self.columns[index2]] = index2
        self.column_map[self.columns[index1]] = index1

    def get_insert_pos(self, timestamp, column_name, create_col=False):
        """Determine col and row indices corresponding to timestamp and col name

        Args:
            timestamp (datetime): Timestamp to map to a row index
            column_name (str): Name of column to map to a column index
            create_col (bool): If column_name does not exist, create it?
        Returns:
            (t_index, c_index) (long or None)
        """
        timestamp_epoch = int(time.mktime(timestamp.timetuple()))
        t_index = (timestamp_epoch - self.timestamps[0]) // self.timestep
        if t_index >= self.timestamps.shape[0]: # check bounds
            return None, None

        # create a new column label if necessary
        c_index = self.column_map.get(column_name)
        if c_index is None and create_col:
            c_index = self.add_column(column_name)
        return t_index, c_index

    def insert_element(self, timestamp, column_name, value, reducer=None):
        """
        Given a timestamp (datetime.datetime object) and a column name (string),
        update an element of the dataset.  If a reducer function is provided,
        use that function to reconcile any existing values in the element to be
        updated.
        """
        t_index, c_index = self.get_insert_pos(timestamp,
                                               column_name,
                                               create_col=True)
        if t_index is None or c_index is None:
            return False

        # actually copy the two data points into the datasets
        old_value = self.dataset[t_index, c_index]
        if (self.dataset[t_index, c_index] != 0.0 \
        or math.copysign(1, old_value) < 0.0) \
        and reducer is not None:
            self.dataset[t_index, c_index] = reducer(old_value, value)
        else:
            self.dataset[t_index, c_index] = value
        return True

    def missing_matrix(self, inverse=False):
        """
        Because we initialize datasets with -0.0, we can scan the sign bit of every
        element of an array to determine how many data were never populated.  This
        converts negative zeros to ones and all other data into zeros then count up
        the number of missing elements in the array.
        """
        return tokio.connectors.hdf5.missing_values(self.dataset, inverse)

    def convert_to_deltas(self):
        """
        Convert a matrix of monotonically increasing rows into deltas.  Replaces
        self.dataset with a matrix with the same number of columns but one fewer
        row (taken off the bottom of the matrix).  Also adjusts the timestamps
        dataset.
        """
        self.dataset = timeseries_deltas(self.dataset)
        self.timestamps = self.timestamps[0:-1]

    def trim_rows(self, num_rows=1):
        """
        Trim some rows off the end of self.dataset and self.timestamps
        """
        self.dataset = self.dataset[0:-1*num_rows]
        self.timestamps = self.timestamps[0:-1*num_rows]

    def add_rows(self, num_rows=1):
        """
        Add additional rows to the end of self.dataset and self.timestamps
        """
        new_dataset_rows = numpy.full((num_rows, self.dataset.shape[1]), -0.0)
        new_timestamps = []
        for index in range(num_rows):
            new_timestamps.append(self.timestamps[-1] + (index + 1) * self.timestep)
        new_timestamp_rows = numpy.array(new_timestamps)

        self.dataset = numpy.vstack((self.dataset, new_dataset_rows))
        self.timestamps = numpy.hstack((self.timestamps, new_timestamp_rows))

def sorted_nodenames(nodenames, sort_hex=False):
    """
    Gnarly routine to sort nodenames naturally.  Required for nodes named things
    like 'bb23' and 'bb231'.
    """
    def extract_int(string):
        """
        Convert input into an int if possible; otherwise return unmodified
        """
        try:
            if sort_hex:
                return int(string, 16)
            return int(string)
        except ValueError:
            return string

    def natural_compare(string):
        """
        Tokenize string into alternating strings/ints if possible
        """
        return list(map(extract_int, re.findall(r'(\d+|\D+)', string)))

    def natural_hex_compare(string):
        """
        Tokenize string into alternating strings/ints if possible.  Also
        recognizes hex, so be careful with ambiguous nodenames like "bb234",
        which is valid hex.
        """
        return list(map(extract_int, re.findall(r'([0-9a-fA-F]+|[^0-9a-fA-F]+)', string)))

#   def natural_comp(arg1, arg2):
#       """
#       Cast the parts of a string that look like integers into integers, then
#       sort based on strings and integers rather than only strings
#       """
#       return cmp(natural_compare(arg1), natural_compare(arg2))

#   def natural_hex_comp(arg1, arg2):
#       """
#       Cast the parts of a string that look like hex into integers, then
#       sort based on strings and integers rather than only strings.
#       """
#       return cmp(natural_hex_compare(arg1), natural_hex_compare(arg2))

    if sort_hex:
        return sorted(nodenames, key=natural_hex_compare)
    return sorted(nodenames, key=natural_compare)

def timeseries_deltas(dataset):
    """Convert monotonically increasing values into deltas

    Subtract every row of the dataset from the row that precedes it to
    convert a matrix of monotonically increasing rows into deltas.  This is a
    lossy process because the deltas for the final measurement of the time
    series cannot be calculated.

    Args:
        dataset (numpy.ndarray): The dataset to convert from absolute values
            into deltas.  rows should correspond to time, and columns to
            individual components

    Returns:
        numpy.ndarray: The deltas between each row in the given input dataset.
            Will have the same number of columns as the input dataset and one
            fewer rows.
    """
    diff_matrix = numpy.full((dataset.shape[0] - 1, dataset.shape[1]), -0.0)

    prev_nonzero = [None] * dataset.shape[1] # the last known valid measurement
    searching = [True] * dataset.shape[1] # are we spanning a gap in data?
    for irow in range(dataset.shape[0]):
        for icol in range(dataset.shape[1]):
            this_element = dataset[irow, icol]

            if irow == 0:
                if this_element != 0.0:
                    prev_nonzero[icol] = this_element
            elif searching[icol]:
                if this_element != 0.0:
                    if prev_nonzero[icol] is not None and this_element >= prev_nonzero[icol]:
                        diff_matrix[irow - 1, icol] = this_element - prev_nonzero[icol]
                        searching[icol] = False
                    prev_nonzero[icol] = this_element
            else:
                if this_element < dataset[irow - 1, icol]: # found a missing data point
                    searching[icol] = True
                else:
                    diff_matrix[irow - 1, icol] = this_element - dataset[irow - 1, icol]
                    prev_nonzero[icol] = this_element

    return diff_matrix

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
