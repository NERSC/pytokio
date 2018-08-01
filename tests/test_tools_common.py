import datetime
import tokio.tools
import tokiotest

def test_enumerate_dated_files_list():
    """enumerate_dated_files with a list
    """
    start = datetime.datetime.strptime(tokiotest.SAMPLE_H5LMT_DATES[0], "%Y-%m-%d")
    end = datetime.datetime.strptime(tokiotest.SAMPLE_H5LMT_DATES[-1], "%Y-%m-%d")
    matches = tokio.tools.common.enumerate_dated_files(
        start=start,
        end=end,
        template=tokio.config.TEST_LIST)

    assert len(matches) > 1

def test_enumerate_dated_files_match_multi():
    """enumerate_dated_files with multiple matches per date
    """
    start = datetime.datetime.strptime(tokiotest.SAMPLE_H5LMT_DATES[0], "%Y-%m-%d")
    sample_templates = [
        tokiotest.SAMPLE_LMTDB_FILE,
        tokiotest.SAMPLE_H5LMT_FILE,
        tokiotest.SAMPLE_XTDB2PROC_FILE
    ]
    matches = tokio.tools.common.enumerate_dated_files(
        start=start,
        end=start,
        template=sample_templates,
        match_first=True)

    print len(matches), matches
    assert len(matches) == 1

    matches = tokio.tools.common.enumerate_dated_files(
        start=start,
        end=start,
        template=sample_templates,
        match_first=False)

    print len(matches), matches
    assert len(matches) > 1
