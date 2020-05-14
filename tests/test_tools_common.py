import os
import datetime
import nose
import tokio.tools
import tokiotest

DATE_FMT = "%Y-%m-%d"

FAKE_FS_NAME = "fakefs"
TEST_FILES = ["file1", "file2"]
TEST_BASE = os.path.join(tokiotest.INPUT_DIR, DATE_FMT)

TEST_FILE = os.path.join(TEST_BASE, TEST_FILES[0])
TEST_LIST = [os.path.join(TEST_BASE, x) for x in TEST_FILES]
TEST_DICT = {
    FAKE_FS_NAME: TEST_FILE,
    FAKE_FS_NAME + "2": os.path.join(TEST_BASE, TEST_FILES[1])
}
TEST_RECURSE = {
    FAKE_FS_NAME: TEST_LIST
}

START = datetime.datetime.strptime(tokiotest.SAMPLE_H5LMT_DATES[0], DATE_FMT)
END = datetime.datetime.strptime(tokiotest.SAMPLE_H5LMT_DATES[-1], DATE_FMT)
EXPECTED_DAYS = (END - START).days + 1

def test_enumerate_dated_files_str():
    """enumerate_dated_files with str
    """
    matches = tokio.tools.common.enumerate_dated_files(
        start=START, end=END, template=TEST_FILE)
    print("Expected %d matches and found %d matches: %s" % (1, len(matches), matches))
    assert len(matches) == 1

def test_enumerate_dated_files_list():
    """enumerate_dated_files with list
    """
    # match_first=True returns, at maximum, 1 entry per day
    matches = tokio.tools.common.enumerate_dated_files(
        start=START, end=END, template=TEST_LIST, match_first=True)
    print("Expected %d matches and found %d matches: %s" % (EXPECTED_DAYS, len(matches), matches))
    assert len(matches) == EXPECTED_DAYS 

    # match_first=False can return multiple entries per day
    matches = tokio.tools.common.enumerate_dated_files(
        start=START, end=END, template=TEST_LIST, match_first=False)
    print("Expected > %d matches and found %d matches: %s" % (EXPECTED_DAYS, len(matches), matches))
    assert len(matches) > EXPECTED_DAYS

def test_enumerate_dated_files_dict_scalar():
    """enumerate_dated_files with dict, scalar value
    """
    matches = tokio.tools.common.enumerate_dated_files(
        start=START, end=END, lookup_key=FAKE_FS_NAME, template=TEST_DICT)

    print("Expected %d matches and found %d matches: %s" % (EXPECTED_DAYS, len(matches), matches))
    assert len(matches) == 1

def test_enumerate_dated_files_dict_scalar_nokey():
    """enumerate_dated_files with dict, scalar value, no key
    """
    matches = tokio.tools.common.enumerate_dated_files(
        start=START, end=END, lookup_key=None, template=TEST_DICT)

    print("Expected %d matches and found %d matches: %s" % (2, len(matches), matches))
    assert len(matches) == 2

def test_enumerate_dated_files_dict_list():
    """enumerate_dated_files with dict, list value
    """

    # match_first=True returns, at maximum, 1 entry per day
    matches = tokio.tools.common.enumerate_dated_files(
        start=START, end=END, lookup_key=FAKE_FS_NAME, template=TEST_RECURSE,
        match_first=True)
    print("Expected %d matches and found %d matches without multimatch: %s" % (EXPECTED_DAYS, len(matches), matches))
    assert len(matches) == EXPECTED_DAYS

    # match_first=False can return multiple entries per day
    matches = tokio.tools.common.enumerate_dated_files(
        start=START, end=END, lookup_key=FAKE_FS_NAME, template=TEST_RECURSE,
        match_first=False)
    print("Expected > %d matches and found %d matches with multimatch: %s" % (EXPECTED_DAYS, len(matches), matches))
    assert len(matches) > EXPECTED_DAYS

#
# This dynamically creates and destroys the search tree.  The tests above should
# be retrofitted with this mechanism.
#
@nose.tools.with_setup(tokiotest.create_tempdir, tokiotest.delete_tempdir)
def test_enumerate_dated_files_glob():
    """enumerate_dated_files with globbing
    """
    MAX_MATCH = 10
    NUM_SUBDIRS = 4

    match_config = {}
    for match_idx, match_num in enumerate(range(MAX_MATCH)):
        match_date = (datetime.datetime.now() - datetime.timedelta(days=match_idx)).replace(hour=0, minute=0, second=0)
        base_dir = os.path.join(tokiotest.TEMP_DIR, match_date.strftime(DATE_FMT))
        match_config[match_num] = {
            "date": match_date,
            "base_dir": base_dir,
        }

        for subdir_idx in range(NUM_SUBDIRS):
            os.makedirs(os.path.join(base_dir, "dir%d" % subdir_idx))

        for num_created in range(match_idx):
            create_file = os.path.join(base_dir, "dir%d" % (num_created % NUM_SUBDIRS), "file%d" % num_created)
            open(create_file, 'a').close()

    print("\nEnsure globbing works\n")
    for match_num in range(MAX_MATCH):
        template = os.path.join(match_config[match_num]['base_dir'], "*", "file*")
        start = match_config[match_num]['date']
        end = match_config[match_num]['date'] + datetime.timedelta(days=1, seconds=-1)
        print("Start   : ", start)
        print("End     : ", end)
        print("Template: ", template)
        matches = tokio.tools.common.enumerate_dated_files(
            start=start,
            end=end,
            template=template,
            use_glob=True,
            match_first=False)
        print("Expected %d matches and found %d matches with globbing:\n  %s" % (match_num, len(matches), "\n  ".join(matches)))
        assert len(matches) == match_num

    print("\nEnsure not globbing doesn't work\n")
    for match_num in range(MAX_MATCH):
        template = os.path.join(match_config[match_num]['base_dir'], "*", "file*")
        start = match_config[match_num]['date']
        end = match_config[match_num]['date'] + datetime.timedelta(days=1, seconds=-1)
        print("Start   : ", start)
        print("End     : ", end)
        print("Template: ", template)
        matches = tokio.tools.common.enumerate_dated_files(
            start=start,
            end=end,
            template=template,
            use_glob=False,
            match_first=False)
        print("Expected %d matches and found %d matches without globbing:\n  %s" % (0, len(matches), "\n  ".join(matches)))
        assert len(matches) == 0

    print("\nEnsure match_first with globbing works\n")
    for match_num in range(1, MAX_MATCH):
        template = os.path.join(match_config[match_num]['base_dir'], "*", "file*")
        start = match_config[match_num]['date']
        end = match_config[match_num]['date'] + datetime.timedelta(days=1, seconds=-1)
        print("Start   : ", start)
        print("End     : ", end)
        print("Template: ", template)
        matches = tokio.tools.common.enumerate_dated_files(
            start=start,
            end=end,
            template=template,
            use_glob=True,
            match_first=True)
        print("Expected %d matches and found %d matches with globbing and match_first:\n  %s" % (1, len(matches), "\n  ".join(matches)))
        assert len(matches) == 1
