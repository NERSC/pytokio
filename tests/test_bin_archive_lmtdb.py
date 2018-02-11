#!/usr/bin/env python
"""
Test the archive_lmtdb.py tool
"""

import os
import time
import datetime
import warnings
import nose
import h5py
import numpy
import tokio
import tokiotest
import tokiobin.archive_lmtdb

# expressed as fraction, not percent; used to account for differences in how
# pylmt and pytokio handle missing data
TOLERANCE_PCT = 0.005 # expressed as fraction, not percent

# start and end of file to overwrite; 0.0 is beginning and 1.0 is end of file
TEST_RANGES = [
    (0.00, 1.00),
    (0.00, 0.50),
    (0.50, 1.00),
    (0.25, 0.75),
]

################################################################################
### Test data placement correctness ############################################
################################################################################

def generate_tts(output_file):
    """Create a TokioTimeSeries output file
    """
    init_start = tokiotest.SAMPLE_LMTDB_START_STAMP
    init_end = tokiotest.SAMPLE_LMTDB_END_STAMP
    argv = ['--init-start', init_start,
            '--init-end', init_end,
            '--input', tokiotest.SAMPLE_LMTDB_FILE,
            '--timestep', str(tokiotest.SAMPLE_LMTDB_TIMESTEP),
            '--output', output_file,
            '--debug',
            init_start,
            init_end]
    print "Running [%s]" % ' '.join(argv)
    tokiobin.archive_lmtdb.main(argv)
    print "Created", output_file

def update_tts(output_file, q_start, q_end):
    """
    Append to an existing tts file
    """
    assert os.path.isfile(output_file) # must update an existing file

    argv = ['--input', tokiotest.SAMPLE_LMTDB_FILE,
            '--output', output_file,
            '--debug',
            q_start.strftime(tokiotest.SAMPLE_TIMESTAMP_DATE_FMT),
            q_end.strftime(tokiotest.SAMPLE_TIMESTAMP_DATE_FMT)]

    print "Running [%s]" % ' '.join(argv)
    tokiobin.archive_lmtdb.main(argv)
    print "Updated", output_file

def summarize_hdf5(hdf5_file):
    """
    Return some summary metrics of an hdf5 file in a mostly content-agnostic way
    """
    # characterize the h5file in a mostly content-agnostic way
    summary = {
        'sums': {},
        'shapes': {},
        'updates': {},
    }

    def characterize_object(obj_name, obj_data):
        """retain some properties of each dataset in an hdf5 file"""
        if isinstance(obj_data, h5py.Dataset):
            summary['shapes'][obj_name] = obj_data.shape
            # note that this will break if the hdf5 file contains non-numeric datasets
            if len(obj_data.shape) == 1:
                summary['sums'][obj_name] = obj_data[:].sum()
            elif len(obj_data.shape) == 2:
                summary['sums'][obj_name] = obj_data[:, :].sum()
            elif len(obj_data.shape) == 3:
                summary['sums'][obj_name] = obj_data[:, :, :].sum()
            summary['updates'][obj_name] = obj_data.attrs.get('updated')

    hdf5_file.visititems(characterize_object)

    return summary

def identical_datasets(summary0, summary1):
    """
    compare the contents of two HDF5s for similarity
    """
    # ensure that updating the overlapping data didn't change the contents of the TimeSeries
    num_compared = 0
    for metric in 'sums', 'shapes':
        for key, value in summary0[metric].iteritems():
            num_compared += 1
            assert key in summary1[metric]
            print "%s->%s->[%s] == [%s]?" % (metric, key, summary1[metric][key], value)
            assert summary1[metric][key] == value

    # check to make sure summary1 was modified after summary0 was generated
    for obj, update in summary0['updates'].iteritems():
        if update is not None:
            result = summary1['updates'][obj] > summary0['updates'][obj]
            print "%s newer than %s? %s" % (summary1['updates'][obj],
                                            summary0['updates'][obj],
                                            result)
            assert result

    assert num_compared > 0

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_bin_archive_lmtdb_overlaps():
    """
    bin/archive_lmtdb.py write + overwrite correctness

    1. initialize a new HDF5 and pull down a large window
    2. pull down a complete subset of that window to this newly minted HDF5
    3. ensure that the hdf5 between #1 and #2 doesn't change
    """
    tokiotest.TEMP_FILE.close()

    start = datetime.datetime.fromtimestamp(tokiotest.SAMPLE_LMTDB_START)
    end = datetime.datetime.fromtimestamp(tokiotest.SAMPLE_LMTDB_END)
    delta = (end - start).total_seconds()

    for test_range in TEST_RANGES:
        # initialize a new TimeSeries, populate it, and write it out as HDF5
        generate_tts(tokiotest.TEMP_FILE.name)
        h5_file = h5py.File(tokiotest.TEMP_FILE.name, 'r')
        summary0 = summarize_hdf5(h5_file)
        h5_file.close()

        q_start = start + datetime.timedelta(seconds=int(test_range[0] * delta))
        q_end = start + datetime.timedelta(seconds=int(test_range[1] * delta))
        print "Overwriting %s to %s" % (q_start, q_end)
        time.sleep(1.5)

        # append an overlapping subset of data to the same HDF5
        update_tts(tokiotest.TEMP_FILE.name, q_start, q_end)
        h5_file = h5py.File(tokiotest.TEMP_FILE.name, 'r')
        summary1 = summarize_hdf5(h5_file)
        h5_file.close()

        func = identical_datasets
        func.description = ("bin/archive_lmtdb.py overlap %s" % str(test_range))
# yield doesn't work because tokiotest.TEMP_FILE doesn't propagate
#       yield func, summary0, summary1
        func(summary0, summary1)

################################################################################
### Compare generated dataset to ground-truth datasets and pytokio H5LMT file ##
################################################################################

class TestArchiveLmtdbCorrectness(object):
    """Compare generated dataset to ground-truth datasets
    """
    @classmethod
    def setup_class(cls):
        """Compare TOKIO HDF5 to reference files

        Generate a new HDF5 file from a full day's worth of LMT data stored in
        SQLite and compare it to the reference file to ensure correctness.
        """
        tokiotest.create_tempfile()
        cls.output_file = tokiotest.TEMP_FILE.name

        warnings.simplefilter("always")

        if os.path.isfile(cls.output_file):
            os.unlink(cls.output_file)
        argv = ['--input', tokiotest.SAMPLE_LMTDB_FILE,
                '--output', cls.output_file,
                '--init-start', tokiotest.SAMPLE_LMTDB_START_STAMP,
                '--init-end', tokiotest.SAMPLE_LMTDB_END_STAMP,
                '--timestep', '5',
                tokiotest.SAMPLE_LMTDB_START_STAMP,
                tokiotest.SAMPLE_LMTDB_END_STAMP
               ]

        print "Running [%s]" % ' '.join(argv)
        tokiobin.archive_lmtdb.main(argv)
        print "Created", cls.output_file

        cls.generated = h5py.File(cls.output_file, 'r')
        cls.generated_tts = tokio.connectors.hdf5.Hdf5(cls.output_file, 'r')

        cls.ref_h5lmt = tokio.connectors.hdf5.Hdf5(tokiotest.SAMPLE_LMTDB_H5LMT, 'r')

#TODO   cls.ref_hdf5 = h5py.File(, 'r')

    @classmethod
    def teardown_class(cls):
        """Close reference files and unlink temp file
        """
        cls.generated.close()
#TODO   cls.ref_hdf5.close()
        cls.ref_h5lmt.close()

        tokiotest.delete_tempfile()

    def test_compare_h5lmt(self):
        """Compare a TOKIO HDF5 file to a pylmt H5LMT file

        Args:
            generated (h5py.File): newly generated HDF5 file
            reference (tokio.connectors.hdf5.Hdf5): reference H5LMT file
        """
        dataset_names = []
        for group_name, group_data in self.generated.iteritems():
            for dataset_name, dataset in group_data.iteritems():
                if isinstance(dataset, h5py.Dataset):
                    dataset_names.append("/%s/%s" % (group_name, dataset_name))

        checked_ct = 0
        for dataset_name in dataset_names:
            if dataset_name.endswith('timestamps'):
                # don't bother with timestamps datasets directly; we handle them
                # when we encounter the parent dataset
                continue

            # check the dataset
            func = compare_h5lmt_dataset
            func.description = "bin/archive_h5lmt.py: comparing %s dataset inside h5lmt" % dataset_name
            print func.description
            yield func, self.generated, self.ref_h5lmt, dataset_name, False

#           # check the timestamp
            func.description = "bin/archive_h5lmt.py: comparing %s's timestamps dataset inside h5lmt" % dataset_name
            print func.description
            yield func, self.generated_tts, self.ref_h5lmt, dataset_name, True
            checked_ct += 1

        print "Verified %d datasets against reference" % checked_ct
        assert checked_ct > 0

#TODOef test_compare_tts(self):
#       """Compare two TOKIO HDF5 files
#       """
#       checked_ct = 0
#       for group_name, group_data in self.generated.iteritems():
#           for dataset in group_data.itervalues():
#               if not isinstance(dataset, h5py.Dataset):
#                   continue
#               dataset_name = dataset.name
#
#               func = compare_tts_dataset
#               func.description = "bin/archive_h5mlt.py: comparing %s to hdf5 reference" % dataset_name
#               print func.description
#               yield func, self.generated, self.ref_hdf5, dataset_name
#               checked_ct += 1
#
#       assert checked_ct > 0

def compare_tts_dataset(generated, reference, dataset_name):
    """Compare a single dataset between a generated HDF5 and a reference HDF5
    """
    print "Comparing %s in %s to %s" % (dataset_name, generated.filename, reference.filename)
    ref_dataset = reference.get(dataset_name)
    gen_dataset = generated.get(dataset_name)
    if ref_dataset is None:
        errmsg = "Cannot compare dataset %s (not in reference)" % dataset_name
        nose.SkipTest(errmsg)
        return
    elif gen_dataset is None:
        errmsg = "Cannot compare dataset %s (not in generated)" % dataset_name
        nose.SkipTest(errmsg)
        return

    print "Checking shape: generated %s == reference %s? %s" \
        % (gen_dataset.shape, ref_dataset.shape, gen_dataset.shape == ref_dataset.shape)
    assert gen_dataset.shape == ref_dataset.shape

    if len(gen_dataset.shape) == 1:
        assert (numpy.isclose(gen_dataset[:], ref_dataset[:])).all()
        sum_gen = gen_dataset[:].sum()
        sum_ref = ref_dataset[:].sum()
        print "generated %f == reference %f? %s" % (sum_gen, sum_ref,
                                                    numpy.isclose(sum_gen, sum_ref))
        assert numpy.isclose(sum_gen, sum_ref)
        assert sum_gen > 0
    elif len(gen_dataset.shape) == 2:
        assert (numpy.isclose(gen_dataset[:, :], ref_dataset[:, :])).all()
        sum_gen = gen_dataset[:].sum().sum()
        sum_ref = ref_dataset[:].sum().sum()
        print "%f == %f? %s" % (sum_gen, sum_ref, numpy.isclose(sum_gen, sum_ref))
        assert numpy.isclose(sum_gen, sum_ref)
        assert sum_gen > 0

def compare_h5lmt_dataset(generated, ref_h5lmt, dataset_name, check_timestamps=False):
    """Compare a single dataset between an H5LMT and TOKIO HDF5 file
    """
    print "Comparing %s in %s to %s" % (dataset_name, generated.filename, ref_h5lmt.filename)

    ref_dataset = ref_h5lmt.get(dataset_name)
    gen_dataset = generated.get(dataset_name)
    if ref_dataset is None:
        errmsg = "Cannot compare dataset %s (not in reference)" % dataset_name
        nose.SkipTest(errmsg)
        return
    elif gen_dataset is None:
        errmsg = "Cannot compare dataset %s (not in generated)" % dataset_name
        nose.SkipTest(errmsg)
        return

    if check_timestamps:
        ref_dataset = ref_h5lmt.get_timestamps(dataset_name)
        gen_dataset = generated.get_timestamps(dataset_name)

    gen_shape = gen_dataset.shape

    if len(gen_shape) == 1 or check_timestamps:
        sum_gen = gen_dataset[:tokiotest.SAMPLE_LMTDB_MAX_INDEX].sum()
        sum_ref = ref_dataset[:tokiotest.SAMPLE_LMTDB_MAX_INDEX].sum()
        print "%f == %f? %s" % (sum_gen, sum_ref, numpy.isclose(sum_gen, sum_ref))
        assert numpy.isclose(sum_gen, sum_ref)
        assert sum_gen > 0
        assert (numpy.isclose(gen_dataset[:tokiotest.SAMPLE_LMTDB_MAX_INDEX],
                              ref_dataset[:tokiotest.SAMPLE_LMTDB_MAX_INDEX])).all()

    elif len(gen_shape) == 2:
        # H5LMT is semantically inconsistent across its own datasets!  Must
        # slice them differently
        if 'OSTBulk' in ref_dataset.name or 'MDSOpsDataSet' in ref_dataset.name:
            ref_slice = (slice(1, tokiotest.SAMPLE_LMTDB_MAX_INDEX + 1), slice(None, None))
            gen_slice = (slice(None, tokiotest.SAMPLE_LMTDB_MAX_INDEX), slice(None, None))
            fudged = True
            print "%s is fudged; doing imperfect comparisons" % ref_dataset.name
        else:
            ref_slice = (slice(None, tokiotest.SAMPLE_LMTDB_MAX_INDEX), slice(None, None))
            gen_slice = (slice(None, tokiotest.SAMPLE_LMTDB_MAX_INDEX), slice(None, None))
            fudged = False
            print "%s is not fudged; doing exact comparisons" % ref_dataset.name

        print "Comparing gen(%s, %s) to ref(%s, %s)" % (gen_dataset.name,
                                                        gen_dataset[gen_slice].shape,
                                                        ref_dataset.name,
                                                        ref_dataset[ref_slice].shape)
        match_matrix = numpy.isclose(gen_dataset[gen_slice], ref_dataset[ref_slice])
        nmatch = match_matrix.sum()
        nelements = match_matrix.shape[0] * match_matrix.shape[1]

        sum_gen = gen_dataset[gen_slice].sum()
        sum_ref = ref_dataset[ref_slice].sum()

        if fudged:
            tol = TOLERANCE_PCT
        else:
            tol = TOLERANCE_PCT / 10.0

        # how big is the skew as a percent of the true value?
        if sum_ref == 0.0:
            raise nose.SkipTest("reference data does not contain any values")
        pct_diff = abs(sum_gen - sum_ref) / sum_ref
        print "pct_diff %e < tolerance %e? %s" % (pct_diff, tol, pct_diff < tol)
        assert pct_diff < tol

        # what fraction of elements match?
        pct_diff = (1.0 - float(nmatch) / float(nelements))
        assert pct_diff < tol

# isclose doesn't work in the presence of missing data for OSSCPUDataSet
#       print "generated %f == reference %f? %s" % (sum_gen, sum_ref,
#                                                   numpy.isclose(sum_gen, sum_ref))
#       isclose = numpy.isclose(sum_gen, sum_ref)
        # h5lmt includes interpolated values rather than mark them missing;
        # mask those off
#       missing_data = tokio.timeseries.negative_zero_matrix(gen_dataset[gen_slice]) > 0.5
#       assert (isclose | missing_data).all()
#       assert nmatch == nelements

        assert sum_gen > 0
