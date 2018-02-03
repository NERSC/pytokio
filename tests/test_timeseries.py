#!/usr/bin/env python
"""
tokio.timeseries.TimeSeries methods
"""

import random
import shutil
import warnings
import h5py
import nose
import numpy
import tokio
import tokiotest

def compare_timeseries(timeseries1, timeseries2, verbose=False):
    """
    Compare two TimeSeries objects' datasets column by column
    """
    for timeseries1_index, column in enumerate(list(timeseries1.columns)):
        timeseries2_index = timeseries2.column_map[column]
        if verbose:
            col_sum1 = timeseries1.dataset[:, timeseries1_index].sum()
            col_sum2 = timeseries2.dataset[:, timeseries2_index].sum()
            print "%-14s: %.16e vs. %.16e" % (column, col_sum1, col_sum2)
        assert numpy.array_equal(timeseries1.dataset[:, timeseries1_index],
                                 timeseries2.dataset[:, timeseries2_index])

def generate_timeseries(file_name=tokiotest.SAMPLE_COLLECTDES_HDF5):
    """
    Return a TimeSeries object that's initialized against the tokiotest sample
    input
    """
    output_hdf5 = h5py.File(file_name, 'r')
    target_dset_name = tokiotest.SAMPLE_COLLECTDES_DSET
    target_group_name = '/'.join(tokiotest.SAMPLE_COLLECTDES_DSET.split('/')[0:-1])
    target_group = output_hdf5[target_group_name]
    timeseries = tokio.timeseries.TimeSeries(dataset_name=target_dset_name, hdf5_file=output_hdf5)
    return timeseries

def test_rearrange():
    """
    TimeSeries.rearrange_columns()
    """
    timeseries1 = generate_timeseries()
    timeseries2 = generate_timeseries()

    # test random reordering
    new_col_order = list(timeseries2.columns[:])
    print new_col_order
    random.shuffle(new_col_order)
    print new_col_order
    timeseries2.rearrange_columns(new_col_order)

    compare_timeseries(timeseries2, timeseries1, verbose=True)

def test_sort():
    """
    TimeSeries.sort_columns()
    """
    timeseries1 = generate_timeseries()
    timeseries2 = generate_timeseries()

    timeseries2.sort_columns()
    compare_timeseries(timeseries2, timeseries1, verbose=True)

def test_timeseries_deltas():
    """
    TimeSeries.timeseries_deltas()
    """

    MAX_DELTA = 9
    NUM_COLS = 16
    NUM_ROWS = 20

    numpy.set_printoptions(formatter={'float': '{: 0.1f}'.format})
    random.seed(0)

    # Create an array of random deltas as our ground truth
    actual_deltas = numpy.random.random(size=(NUM_ROWS, NUM_COLS)) * MAX_DELTA
    first_row = numpy.random.random(size=(1, NUM_COLS)) * MAX_DELTA
#   actual_deltas = numpy.full((NUM_ROWS, NUM_COLS), 2.0)
#   first_row = numpy.full((1, NUM_COLS), 2.0)

    # Calculate the monotonically increasing dataset that would result in these deltas
    monotonic_values = actual_deltas.copy()
    monotonic_values = numpy.vstack((first_row, actual_deltas)).copy()
    for irow in range(1, monotonic_values.shape[0]):
        monotonic_values[irow, :] += monotonic_values[irow - 1, :]

    print "Actual monotonic values:"
    print monotonic_values
    print

    print "Actual deltas:"
    print actual_deltas
    print

    # Delete some data from our sample monotonically increasing dataset
    # Columns 0-3 are hand-picked to exercise all edge cases
    delete_data = [
        ( 1,  0),
        ( 2,  0),
        ( 5,  0),
        ( 0,  1),
        ( 1,  1),
        ( 1,  2),
        ( 3,  2),
        (-1,  2),
        (-2,  2),
        ( 0,  3),
        (-1,  3),
    ]
    # Columns 4-7 are low density errors
    for _ in range(int(NUM_COLS * NUM_ROWS / 4)):
        delete_data.append((numpy.random.randint(0, NUM_ROWS), numpy.random.randint(4, 8)))

    # Columns 8-11 are high density errors
    for _ in range(int(3 * NUM_COLS * NUM_ROWS / 4)):
        delete_data.append((numpy.random.randint(0, NUM_ROWS), numpy.random.randint(8, 12)))

    # Columns 12-15 are nonzero but non-monotonic flips
    START_FLIP = 12
    flip_data = []
    for _ in range(int(3 * NUM_COLS * NUM_ROWS / 4)):
        flip_data.append((numpy.random.randint(0, NUM_ROWS), numpy.random.randint(12, 16)))

    for coordinates in delete_data:
        monotonic_values[coordinates] = 0.0

    print "Matrix after introducing data loss:"
    print monotonic_values
    print

    for irow, icol in flip_data:
        if irow == 0:
            irow_complement = irow + 1
        else:
            irow_complement = irow - 1
        temp = monotonic_values[irow, icol]
        monotonic_values[irow, icol] = monotonic_values[irow_complement, icol]
        monotonic_values[irow_complement, icol] = temp

    print "Matrix after flipping data order:"
    print monotonic_values
    print

    # Call the routine being tested to regenerate the deltas matrix
    calculated_deltas = tokio.timeseries.timeseries_deltas(monotonic_values)

    # Check to make sure that the total data moved according to our function
    # matches the logical total obtained by subtracting the largest absolute
    # measurement from the smallest
    print "Checking each column's sum (missing data)"
    for icol in range(START_FLIP):
        truth = actual_deltas[:, icol].sum()
        calculated = calculated_deltas[:, icol].sum()
        total_delta = monotonic_values[:, icol].max() - numpy.matrix([x for x in monotonic_values[:, icol] if x > 0.0]).min()
        print 'truth=', truth, \
              'from piecewise deltas=', calculated, \
              'from total delta=', total_delta
        assert numpy.isclose(calculated, total_delta)

        # Calculated delta should either be equal to (no data loss) or less than
        # (data lost) than ground truth.  It should never reflect MORE total
        # than the ground truth.
        assert numpy.isclose(truth - calculated, 0.0) or ((truth - calculated) > 0)

    print "Checking each column's sum (flipped data)"
    for icol in range(START_FLIP, actual_deltas.shape[1]):
        truth = actual_deltas[:, icol].sum()
        calculated = calculated_deltas[:, icol].sum()
        total_delta = monotonic_values[:, icol].max() - numpy.matrix([x for x in monotonic_values[:, icol] if x > 0.0]).min()
        print 'truth=', truth, \
              'from piecewise deltas=', calculated, \
              'from total delta=', total_delta
        assert numpy.isclose(calculated, total_delta) or ((total_delta - calculated) > 0)
        assert numpy.isclose(truth, calculated) or ((truth - calculated) > 0)


    # Now do an element-by-element comparison
    close_matrix = numpy.isclose(calculated_deltas, actual_deltas)
    print "Is each calculated delta close to the ground-truth deltas?"
    print close_matrix
    print

    # Some calculated values will _not_ be the same because the data loss we
    # induced, well, loses data.  However we can account for known differences
    # and ensure that nothing unexpected is different.
    fix_matrix = numpy.full(close_matrix.shape, False)
    for irow, icol in delete_data + flip_data:
        fix_matrix[irow, icol] = True
        if irow - 1 >= 0:
            fix_matrix[irow - 1, icol] = True

    for irow, icol in flip_data:
        if irow == 0:
            fix_matrix[irow + 1, icol] = True

    print "Matrix of known deviations from the ground truth:"
    print fix_matrix
    print
    assert (close_matrix | fix_matrix).all()


@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_commit_dataset():
    """
    TimeSeries.commit_dataset()
    """
    tokiotest.TEMP_FILE.close()

    # Connect to the sample input file
    timeseries1 = generate_timeseries()
    hdf5_file = h5py.File(tokiotest.TEMP_FILE.name, 'w')
    # Write the output out as a new HDF5 file
    timeseries1.commit_dataset(hdf5_file)
    hdf5_file.close()

    # Read that newly generated HDF5 file back in
    timeseries2 = generate_timeseries(file_name=tokiotest.TEMP_FILE.name)

    # Compare the original to the reprocessed
    compare_timeseries(timeseries2, timeseries1, verbose=True)

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_uneven_columns():
    """
    TimeSeries.attach_dataset with uneven columns
    """

    # We expect to trigger some warnings here
    warnings.filterwarnings('ignore')

    tokiotest.TEMP_FILE.close()
    # copy the input file into a new temporary file with which we can tinker
    with open(tokiotest.SAMPLE_COLLECTDES_HDF5, 'r') as input_file:
        with open(tokiotest.TEMP_FILE.name, 'w') as output_file:
            shutil.copyfileobj(input_file, output_file)

    print "Ensure that the input dataset has even column lengths before we make them uneven"
    h5_file = h5py.File(tokiotest.TEMP_FILE.name, 'r+')
    dataset = h5_file[tokiotest.SAMPLE_COLLECTDES_DSET]
    orig_col_names = list(dataset.attrs[tokio.timeseries.COLUMN_NAME_KEY])
    result = len(orig_col_names) == dataset.shape[1]
    print "%-3d == %3d? %s" % (len(orig_col_names), dataset.shape[1], result)
    assert result 

    print "Test case where there are more column names than shape of dataset"
    extra_columns = orig_col_names + ['argle', 'bargle', 'fffff']
    timeseries = generate_timeseries(file_name=tokiotest.TEMP_FILE.name)
    timeseries.set_columns(extra_columns)
    result = len(timeseries.columns) == timeseries.dataset.shape[1]
    print "%-3d == %3d? %s" % (len(timeseries.columns), timeseries.dataset.shape[1], result)
    assert result
    for icol in range(timeseries.dataset.shape[1]):
        print "%-15s == %15s? %s" % (extra_columns[icol], timeseries.columns[icol],
                                extra_columns[icol] == timeseries.columns[icol])
        assert extra_columns[icol] == timeseries.columns[icol]

    print "Test cases where column names are incomplete compared to shape of dataset"
    fewer_columns = orig_col_names[0:-(3*len(orig_col_names)/4)]
    timeseries = generate_timeseries(file_name=tokiotest.TEMP_FILE.name)
    timeseries.set_columns(fewer_columns)
    result = len(timeseries.columns) < timeseries.dataset.shape[1]
    print "%-3d < %3d? %s" % (len(timeseries.columns), timeseries.dataset.shape[1], result)
    assert result
    for icol in range(len(fewer_columns)):
        print "%-15s == %15s? %s" % (fewer_columns[icol], timeseries.columns[icol],
                                fewer_columns[icol] == timeseries.columns[icol])
        assert fewer_columns[icol] == timeseries.columns[icol]
