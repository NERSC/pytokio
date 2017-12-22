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

TIMESTAMP_KEY = 'timestamps'
COLUMN_NAME_KEY = 'columns'

class TimeSeries(object):
    """
    In-memory representation of an HDF5 group in a TokioFile.  Can either
    initialize with no datasets, or initialize against an existing HDF5
    group.
    """
    def __init__(self, start=None, end=None, timestep=None, group=None,
                 timestamp_key=TIMESTAMP_KEY):
        self.timestamps = None
        self.time0 = None
        self.timef = None
        self.timestep = None

        self.dataset = None
        self.dataset_name = None
        self.columns = []
        self.num_columns = 0
        self.column_map = {}
        self.timestamp_key = timestamp_key

        if group is not None:
            self.attach_group(group)
        else:
            if start is None or end is None or timestep is None:
                raise Exception("Must specify either ({start,end,timestep}|group)")
            else:
                self.init_group(start, end, timestep)

    def attach_group(self, group):
        """
        Attach to an existing h5py Group object
        """
        if self.timestamp_key not in group:
            raise Exception("Existing dataset contains no timestamps")

        self.timestamps = group[self.timestamp_key][:]
        self.time0 = self.timestamps[0]
        self.timef = self.timestamps[-1]
        self.timestep = self.timestamps[1] - self.timestamps[0]

    def init_group(self, start, end, timestep):
        """
        Initialize the object from scratch
        """
        if start is None or end is None or timestep is None:
            raise Exception("Must specify either ({start,end,timestep}|group)")
        self.time0 = long(time.mktime(start.timetuple()))
        self.timef = long(time.mktime(end.timetuple()))
        self.timestep = timestep

        time_list = []
        timestamp = start
        while timestamp < end:
            time_list.append(long(time.mktime(timestamp.timetuple())))
            timestamp += datetime.timedelta(seconds=timestep)

        self.timestamps = numpy.array(time_list)

    def update_column_map(self):
        """
        Create the mapping of column names to column indices
        """
        self.column_map = {}
        for index, column_name in enumerate(self.columns):
            self.column_map[column_name] = index

    def init_dataset(self, *args, **kwargs):
        """
        Dimension-independent wrapper around init_dataset2d
        """
        self.init_dataset2d(*args, **kwargs)

    def attach_dataset(self, *args, **kwargs):
        """
        Dimension-independent wrapper around attach_dataset2d
        """
        self.attach_dataset2d(*args, **kwargs)

    def init_dataset2d(self, dataset_name, num_columns, default_value=-0.0):
        """
        Initialize the dataset from scratch
        """
        self.dataset_name = dataset_name
        self.columns = []
        self.num_columns = len(self.columns)
        self.dataset = numpy.full((len(self.timestamps), num_columns), default_value)
        self.update_column_map()

    def attach_dataset2d(self, dataset):
        """
        Initialize the dataset from an existing h5py Dataset object
        """
        self.dataset_name = dataset.name
        self.dataset = dataset[:, :]
        if COLUMN_NAME_KEY in dataset.attrs:
            columns = list(dataset.attrs[COLUMN_NAME_KEY])
            num_columns = len(columns)
            # handle case where HDF5 has more column names than dataset columns via truncation
            if num_columns > self.dataset.shape[1]:
                columns = columns[0:self.dataset.shape[1]]
                truncated = columns[self.dataset.shape[1]:]
                warnings.warn(
                    "Dataset has %d column names but %d columns; truncating columns %s"
                    % (num_columns, self.dataset.shape[1], ', '.join(truncated)))
            # add columns one by one
            for column in columns:
                self.add_column(column)
        else:
            warnings.warn("attaching to a columnless dataset (%s)" % self.dataset_name)

    def commit_dataset(self, *args, **kwargs):
        """
        Dimension-independent wrapper around commit_dataset2d
        """
        self.commit_dataset2d(*args, **kwargs)

    def commit_dataset2d(self, hdf5_file):
        """
        Write contents of this object into an HDF5 file group
        """
        # Create the timestamps dataset
        group_name = '/'.join(self.dataset_name.split('/')[0:-1])
        timestamps_dataset_name = group_name + '/' + self.timestamp_key

        # If we are creating a new group, first insert the new timestamps
        if timestamps_dataset_name not in hdf5_file:
            hdf5_file.create_dataset(name=timestamps_dataset_name,
                                     shape=self.timestamps.shape,
                                     dtype='i8')
            # Copy the in-memory timestamp dataset into the HDF5 file
            hdf5_file[timestamps_dataset_name][:] = self.timestamps[:]
        # Otherwise, verify that our dataset will fit into the existing timestamps
        else:
            if not numpy.array_equal(self.timestamps, hdf5_file[timestamps_dataset_name][:]):
                raise Exception("Attempting to commit to a group with different timestamps")

        # Create the dataset in the HDF5 file
        if self.dataset_name not in hdf5_file:
            hdf5_file.create_dataset(name=self.dataset_name,
                                     shape=self.dataset.shape,
                                     dtype='f8',
                                     chunks=True,
                                     compression='gzip')

        # If we're updating an existing HDF5, use its column names and ordering.
        # Otherwise sort the columns before committing them.
        if COLUMN_NAME_KEY in hdf5_file[self.dataset_name].attrs:
            self.rearrange_columns(list(hdf5_file[self.dataset_name].attrs[COLUMN_NAME_KEY]))
        else:
            self.sort_columns()

        # Copy the in-memory dataset into the HDF5 file
        hdf5_file[self.dataset_name][:, :] = self.dataset[:, :]
        hdf5_file[self.dataset_name].attrs[COLUMN_NAME_KEY] = self.columns

    def add_column(self, column_name):
        """
        Add a new column and update the column map
        """
        index = self.num_columns
        if column_name in self.column_map:
            warnings.warn("Adding degenerate column '%s' at %d (exists at %d)"
                          % (column_name, index, self.column_map[column_name]))
        self.column_map[column_name] = index
        if index >= (self.num_columns + 1):
            raise IndexError("index %d exceeds length of columns %d" % (index, self.num_columns))
        self.columns.append(str(column_name)) # convert from unicode to str for numpy
        self.num_columns += 1
        return index

    def sort_columns(self):
        """
        Rearrange the dataset's column data by sorting them by their headings
        """
        self.rearrange_columns(sorted_nodenames(self.columns))

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

    def insert_element(self, timestamp, column_name, value, reducer=None):
        """
        Given a timestamp (datetime.datetime object) and a column name (string),
        update an element of the dataset.  If a reducer function is provided,
        use that function to reconcile any existing values in the element to be
        updated.
        """
        # calculate the timeseries index and check bounds
        timestamp_epoch = long(time.mktime(timestamp.timetuple()))
        t_index = (timestamp_epoch - self.time0) / self.timestep
        if t_index >= self.timestamps.shape[0]:
            return False

        # if this is a new hostname, create a new column for it
        c_index = self.column_map.get(column_name)
        if c_index is None:
            c_index = self.add_column(column_name)

        # actually copy the two data points into the datasets
        old_value = self.dataset[t_index, c_index]
        if self.dataset[t_index, c_index] == 0.0 \
        and math.copysign(1, old_value) < 0.0 \
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
        if inverse:
            converter = numpy.vectorize(lambda x: 0 if (x == 0.0 and math.copysign(1, x) < 0.0) else 1)
        else:
            converter = numpy.vectorize(lambda x: 1 if (x == 0.0 and math.copysign(1, x) < 0.0) else 0)
        return converter(self.dataset)


def sorted_nodenames(nodenames):
    """
    Gnarly routine to sort nodenames naturally.  Required for nodes named things
    like 'bb23' and 'bb231'.
    """
    def extract_int(string):
        """
        Convert input into an int if possible; otherwise return unmodified
        """
        try:
            return int(string)
        except ValueError:
            return string

    def natural_compare(string):
        """
        Tokenize string into alternating strings/ints if possible
        """
        return map(extract_int, re.findall(r'(\d+|\D+)', string))

    def natural_comp(arg1, arg2):
        """
        Cast the parts of a string that look like integers into integers, then
        sort based on strings and integers rather than only strings
        """
        return cmp(natural_compare(arg1), natural_compare(arg2))

    return sorted(nodenames, natural_comp)
