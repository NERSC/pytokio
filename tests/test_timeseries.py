#!/usr/bin/env python
"""
tokio.timeseries.TimeSeries methods
"""

import datetime
import random
import pandas

import nose
import numpy
import tokio
from tokiotest import generate_timeseries, compare_timeseries

START = datetime.datetime(2019, 5, 28, 1, 0, 0)
END = datetime.datetime(2019, 5, 28, 2, 0, 0)
DELTIM = datetime.timedelta(seconds=10)

def to_dataframe(timeseries):
    return pandas.DataFrame(
        timeseries.dataset,
        index=[datetime.datetime.fromtimestamp(x) for x in timeseries.timestamps],
        columns=timeseries.columns)

def test_rearrange():
    """
    TimeSeries.rearrange_columns()
    """
    timeseries1 = generate_timeseries()
    timeseries2 = generate_timeseries()

    # test random reordering
    new_col_order = list(timeseries2.columns[:])
    print(new_col_order)
    random.shuffle(new_col_order)
    print(new_col_order)
    timeseries2.rearrange_columns(new_col_order)

    print("Comparing before/after rearrange_columns()")
    compare_timeseries(timeseries2, timeseries1, verbose=True)

def test_sort():
    """
    TimeSeries.sort_columns()
    """
    timeseries1 = generate_timeseries()
    timeseries2 = generate_timeseries()

    timeseries2.sort_columns()
    print("Comparing before/after sort_columns()")
    compare_timeseries(timeseries2, timeseries1, verbose=True)

def test_timeseries_deltas():
    """
    TimeSeries.timeseries_deltas()
    """

    max_delta = 9
    num_cols = 16
    num_rows = 20

    numpy.set_printoptions(formatter={'float': '{: 0.1f}'.format})
    random.seed(0)

    # Create an array of random deltas as our ground truth
    actual_deltas = numpy.random.random(size=(num_rows, num_cols)) * max_delta
    first_row = numpy.random.random(size=(1, num_cols)) * max_delta
#   actual_deltas = numpy.full((num_rows, num_cols), 2.0)
#   first_row = numpy.full((1, num_cols), 2.0)

    # Calculate the monotonically increasing dataset that would result in these deltas
    monotonic_values = actual_deltas.copy()
    monotonic_values = numpy.vstack((first_row, actual_deltas)).copy()
    for irow in range(1, monotonic_values.shape[0]):
        monotonic_values[irow, :] += monotonic_values[irow - 1, :]

    print("Actual monotonic values:")
    print(monotonic_values)
    print()

    print("Actual deltas:")
    print(actual_deltas)
    print()

    # Delete some data from our sample monotonically increasing dataset
    # Columns 0-3 are hand-picked to exercise all edge cases
    delete_data = [
        (1, 0),
        (2, 0),
        (5, 0),
        (0, 1),
        (1, 1),
        (1, 2),
        (3, 2),
        (-1, 2),
        (-2, 2),
        (0, 3),
        (-1, 3),
    ]
    # Columns 4-7 are low density errors
    for _ in range(int(num_cols * num_rows / 4)):
        delete_data.append((numpy.random.randint(0, num_rows), numpy.random.randint(4, 8)))

    # Columns 8-11 are high density errors
    for _ in range(int(3 * num_cols * num_rows / 4)):
        delete_data.append((numpy.random.randint(0, num_rows), numpy.random.randint(8, 12)))

### Note that this method produces bad data if the input data is non-monotonic
### in time.  It would need some small tweaks to deal with that, so in the meantime,
### just don't do it.
    # Columns 12-15 are nonzero but non-monotonic flips
    start_flip = 12
#   flip_data = []
#   for _ in range(int(3 * num_cols * num_rows / 4)):
#       flip_data.append((numpy.random.randint(0, num_rows), numpy.random.randint(12, 16)))

    for coordinates in delete_data:
        monotonic_values[coordinates] = 0.0

    print("Matrix after introducing data loss:")
    print(monotonic_values)
    print()

#   for irow, icol in flip_data:
#       if irow == 0:
#           irow_complement = irow + 1
#       else:
#           irow_complement = irow - 1
#       temp = monotonic_values[irow, icol]
#       monotonic_values[irow, icol] = monotonic_values[irow_complement, icol]
#       monotonic_values[irow_complement, icol] = temp

#   print "Flipping the following:"
#   print flip_data
#   print
#   print "Matrix after flipping data order:"
#   print monotonic_values
#   print

    # Call the routine being tested to regenerate the deltas matrix
    calculated_deltas = tokio.timeseries.timeseries_deltas(monotonic_values)

    # Check to make sure that the total data moved according to our function
    # matches the logical total obtained by subtracting the largest absolute
    # measurement from the smallest
    print("Checking each column's sum (missing data)")
    for icol in range(start_flip):
        truth = actual_deltas[:, icol].sum()
        calculated = calculated_deltas[:, icol].sum()
        total_delta = monotonic_values[:, icol].max() \
                      - numpy.matrix([x for x in monotonic_values[:, icol] if x > 0.0]).min()
        print('truth=%s from piecewise deltas=%s from total delta=%s' % (
            truth,
            calculated,
            total_delta))
        assert numpy.isclose(calculated, total_delta)

        # Calculated delta should either be equal to (no data loss) or less than
        # (data lost) than ground truth.  It should never reflect MORE total
        # than the ground truth.
        assert numpy.isclose(truth - calculated, 0.0) or ((truth - calculated) > 0)

    print("Checking each column's sum (flipped data)")
    for icol in range(start_flip, actual_deltas.shape[1]):
        truth = actual_deltas[:, icol].sum()
        calculated = calculated_deltas[:, icol].sum()
        total_delta = monotonic_values[:, icol].max() \
                      - numpy.matrix([x for x in monotonic_values[:, icol] if x > 0.0]).min()
        print('truth=%s from piecewise deltas=%s from total delta=%s' % (
            truth,
            calculated,
            total_delta))
        assert numpy.isclose(calculated, total_delta) or ((total_delta - calculated) > 0)
        assert numpy.isclose(truth, calculated) or ((truth - calculated) > 0)


    # Now do an element-by-element comparison
    close_matrix = numpy.isclose(calculated_deltas, actual_deltas)
    print("Is each calculated delta close to the ground-truth deltas?")
    print(close_matrix)
    print()

    # Some calculated values will _not_ be the same because the data loss we
    # induced, well, loses data.  However we can account for known differences
    # and ensure that nothing unexpected is different.
    fix_matrix = numpy.full(close_matrix.shape, False)
    for irow, icol in delete_data: #+ flip_data:
        fix_matrix[irow, icol] = True
        if irow - 1 >= 0:
            fix_matrix[irow - 1, icol] = True

#   for irow, icol in flip_data:
#       if irow == 0:
#           fix_matrix[irow + 1, icol] = True

    print("Matrix of known deviations from the ground truth:")
    print(fix_matrix)
    print()

    print("Non-missing and known-missing data (everything should be True):")
    print(close_matrix | fix_matrix)
    print()
    assert (close_matrix | fix_matrix).all()

def test_add_rows():
    """
    TimeSeries.add_rows()
    """
    add_rows = 5
    timeseries = generate_timeseries()
    orig_row = timeseries.timestamps.copy()
    orig_row_count = timeseries.timestamps.shape[0]
    timeseries.add_rows(add_rows)

    prev_deltim = None
    for index in range(1, timeseries.dataset.shape[0]):
        new_deltim = timeseries.timestamps[index] - timeseries.timestamps[index - 1]
        if prev_deltim is not None:
            assert new_deltim == prev_deltim
        prev_deltim = new_deltim

    print("Orig timestamps: %s" % orig_row[-5: -1])
    print("Now timestamps:  %s" % timeseries.timestamps[-5 - add_rows: -1])
    assert prev_deltim > 0
    assert timeseries.timestamps.shape[0] == timeseries.dataset.shape[0]
    assert (timeseries.timestamps.shape[0] - add_rows) == orig_row_count

def _test_insert_element(timeseries, timestamp, column_name, value, reducer, expect_failure):
    worked = timeseries.insert_element(
        timestamp=timestamp,
        column_name=column_name,
        value=value,
        reducer=reducer)

    print("worked? ", worked)
    if expect_failure:
        assert not worked
    else:
        assert worked

def test_insert_element():
    """TimeSeries.insert_element()
    """
    timeseries = tokio.timeseries.TimeSeries(
        dataset_name='test_dataset',
        start=START,
        end=END,
        timestep=DELTIM.total_seconds(),
        num_columns=5,
        column_names=['a', 'b', 'c', 'd', 'e'])

    func = _test_insert_element
    func.description = "TimeSeries.insert_element(): insert element at first position"
    yield func, timeseries, START, 'a', 1.0, None, False

    func.description = "TimeSeries.insert_element(): insert element at last position"
    yield func, timeseries, END - DELTIM, 'a', 1.0, None, False

    func.description = "TimeSeries.insert_element(): insert element at negative position"
    yield func, timeseries, START - DELTIM, 'a', 1.0, None, True

    func.description = "TimeSeries.insert_element(): insert element beyond end position"
    yield func, timeseries, END, 'a', 1.0, None, True

def test_insert_element_columns():
    """TimeSeries.insert_element(): insert element in column that does fit"""
    timeseries = tokio.timeseries.TimeSeries(
        dataset_name='test_dataset',
        start=START,
        end=END,
        timestep=DELTIM.total_seconds(),
        num_columns=6,
        column_names=['a', 'b', 'c', 'd', 'e'])

    _test_insert_element(timeseries, START + DELTIM, 'f', 1.0, None, False)

    assert 'f' in timeseries.columns

@nose.tools.raises(IndexError)
def test_insert_element_column_overflow():
    """TimeSeries.insert_element(): insert element in column that doesn't fit"""
    timeseries = tokio.timeseries.TimeSeries(
        dataset_name='test_dataset',
        start=START,
        end=END,
        timestep=DELTIM.total_seconds(),
        num_columns=5,
        column_names=['a', 'b', 'c', 'd', 'e'])

    _test_insert_element(timeseries, START + DELTIM, 'f', 1.0, None, True)

def test_align():
    """TimeSeries.insert_element() and TimeSeries.convert_deltas()
    """
    # Test the case:
    #
    #  +-----+-----+---
    #  | x   | y   |
    #  +-----+-----+---
    #  ^-t0  ^-t1
    #
    # yields
    #
    #  +-----+-----+---
    #  | y-x |   0 |
    #  +-----+-----+---
    #  ^-t0  ^-t1
    #
    timeseries0 = tokio.timeseries.TimeSeries(
        dataset_name='test_dataset',
        start=START,
        end=START + DELTIM * 3,
        timestep=DELTIM.total_seconds(),
        num_columns=5,
        column_names=['a', 'b', 'c', 'd', 'e'])

    assert timeseries0.insert_element(
        timestamp=START,
        column_name='a',
        value=1.0,
        reducer=None,
        align='l')

    assert timeseries0.insert_element(
        timestamp=START + DELTIM,
        column_name='a',
        value=2.0,
        reducer=None,
        align='l')

    print("\nDataset before delta conversion:")
    print(to_dataframe(timeseries0))
    timeseries0.convert_to_deltas(align='l')
    print("\nDataset after delta conversion:")
    print(to_dataframe(timeseries0))
    assert timeseries0.dataset[0, 0]
    assert not timeseries0.dataset[1, 0]

    df0 = to_dataframe(timeseries0)

    # Test the case:
    #
    #  +-----+-----+---
    #  | x   | y   |
    #  +-----+-----+---
    #        ^-t0  ^-t1
    #
    # yields
    #
    #  +-----+-----+---
    #  | y-x |   0 |
    #  +-----+-----+---
    #  ^-t0  ^-t1
    #
    timeseries1 = tokio.timeseries.TimeSeries(
        dataset_name='test_dataset',
        start=START,
        end=START + DELTIM * 3,
        timestep=DELTIM.total_seconds(),
        num_columns=5,
        column_names=['a', 'b', 'c', 'd', 'e'])

    assert timeseries1.insert_element(
        timestamp=START + DELTIM,
        column_name='a',
        value=1.0,
        reducer=None,
        align='r')

    assert timeseries1.insert_element(
        timestamp=START + 2 * DELTIM,
        column_name='a',
        value=2.0,
        reducer=None,
        align='r')

    print("\nDataset before delta conversion:")
    print(to_dataframe(timeseries1))
    timeseries1.convert_to_deltas(align='l')
    print("\nDataset after delta conversion:")
    print(to_dataframe(timeseries1))
    assert timeseries1.dataset[0, 0]
    assert not timeseries1.dataset[1, 0]

    df1 = to_dataframe(timeseries1)

    assert (df0.index == df1.index).all()
    assert (df0.all() == df1.all()).all()
