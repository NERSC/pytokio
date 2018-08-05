import os
import datetime
import tokio.tools
import tokiotest

FAKE_FS_NAME = "fakefs"
TEST_FILES = ["file1", "file2"]
TEST_BASE = os.path.join(tokiotest.INPUT_DIR, "%Y-%m-%d")

TEST_FILE = os.path.join(TEST_BASE, TEST_FILES[0])
TEST_LIST = [os.path.join(TEST_BASE, x) for x in TEST_FILES]
TEST_DICT = {
    FAKE_FS_NAME: TEST_FILE,
    FAKE_FS_NAME + "2": os.path.join(TEST_BASE, TEST_FILES[1])
}
TEST_RECURSE = {
    FAKE_FS_NAME: TEST_LIST
}

START = datetime.datetime.strptime(tokiotest.SAMPLE_H5LMT_DATES[0], "%Y-%m-%d")
END = datetime.datetime.strptime(tokiotest.SAMPLE_H5LMT_DATES[-1], "%Y-%m-%d")
EXPECTED_DAYS = (END - START).days + 1

def test_enumerate_dated_files_str():
    """enumerate_dated_files with str
    """
    matches = tokio.tools.common.enumerate_dated_files(
        start=START, end=END, template=TEST_FILE)
    print "Found %d matches: %s" % (len(matches), matches)
    assert len(matches) == 1

def test_enumerate_dated_files_list():
    """enumerate_dated_files with list
    """
    # match_first=True returns, at maximum, 1 entry per day
    matches = tokio.tools.common.enumerate_dated_files(
        start=START, end=END, template=TEST_LIST, match_first=True)
    print "Found %d matches: %s" % (len(matches), matches)
    assert len(matches) == EXPECTED_DAYS 

    # match_first=False can return multiple entries per day
    matches = tokio.tools.common.enumerate_dated_files(
        start=START, end=END, template=TEST_LIST, match_first=False)
    print "Found %d matches: %s" % (len(matches), matches)
    assert len(matches) > EXPECTED_DAYS

def test_enumerate_dated_files_dict_scalar():
    """enumerate_dated_files with dict, scalar value
    """
    matches = tokio.tools.common.enumerate_dated_files(
        start=START, end=END, lookup_key=FAKE_FS_NAME, template=TEST_DICT)

    print "Found %d matches: %s" % (len(matches), matches)
    assert len(matches) == 1

def test_enumerate_dated_files_dict_scalar_nokey():
    """enumerate_dated_files with dict, scalar value, no key
    """
    matches = tokio.tools.common.enumerate_dated_files(
        start=START, end=END, lookup_key=None, template=TEST_DICT)

    print "Found %d matches: %s" % (len(matches), matches)
    assert len(matches) == 2

def test_enumerate_dated_files_dict_list():
    """enumerate_dated_files with dict, list value
    """

    # match_first=True returns, at maximum, 1 entry per day
    matches = tokio.tools.common.enumerate_dated_files(
        start=START, end=END, lookup_key=FAKE_FS_NAME, template=TEST_RECURSE,
        match_first=True)
    print "Found %d matches without multimatch: %s" % (len(matches), matches)
    assert len(matches) == EXPECTED_DAYS

    # match_first=False can return multiple entries per day
    matches = tokio.tools.common.enumerate_dated_files(
        start=START, end=END, lookup_key=FAKE_FS_NAME, template=TEST_RECURSE,
        match_first=False)
    print "Found %d matches with multimatch: %s" % (len(matches), matches)
    assert len(matches) > EXPECTED_DAYS
