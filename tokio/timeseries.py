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
                 sort_hex=False):

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

        # attempt to initialize the object if fields are supplied
        if dataset_name is not None and start and end and timestep and num_columns:
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
            timestamp_key (str): The key for the timestamp dataset.
            safe (bool): If true, do not overwrite an existing timestamp key
        """
        # Root the timestamp_key at the same parent as the dataset
        if not safe or self.timestamp_key is None:
            self.timestamp_key = timestamp_key

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
