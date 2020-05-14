import datetime

import tokio.tools.nersc_mmperfmon

import tokiotest

def test_enumerate_mmperfmon_txt():
    """tools.nersc_mmperfmon.enumerate_mmperfmon_txt()"""
    start = datetime.datetime.strptime(tokiotest.SAMPLE_MMPERFMON_MINI_START, "%Y-%m-%dT%H:%M:%S")
    end = datetime.datetime.strptime(tokiotest.SAMPLE_MMPERFMON_MINI_END, "%Y-%m-%dT%H:%M:%S")

    results = tokio.tools.nersc_mmperfmon.enumerate_mmperfmon_txt(
        fsname='testfs-mini',
        datetime_start=start,
        datetime_end=end)

    print("Looking for %s to %s" % (start, end))
    print(results)
    assert len(results)
    assert len(results) == 1

def test_enumerate_mmperfmon_edge():
    """tools.nersc_mmperfmon.enumerate_mmperfmon_txt(), exactly one day"""
    start = datetime.datetime.strptime(tokiotest.SAMPLE_MMPERFMON_MINI_START.split('T', 1)[0], "%Y-%m-%d")
    end = start + datetime.timedelta(days=1, seconds=-1)

    results = tokio.tools.nersc_mmperfmon.enumerate_mmperfmon_txt(
        fsname='testfs-mini',
        datetime_start=start,
        datetime_end=end)

    print("Looking for %s to %s" % (start, end))
    print(results)
    assert len(results)
    assert len(results) == 1

def test_enumerate_mmperfmon_twoday():
    """tools.nersc_mmperfmon.enumerate_mmperfmon_txt(), 24 hours"""
    start = datetime.datetime.strptime(tokiotest.SAMPLE_MMPERFMON_MINI_START.split('T', 1)[0], "%Y-%m-%d")
    end = start + datetime.timedelta(days=1)

    results = tokio.tools.nersc_mmperfmon.enumerate_mmperfmon_txt(
        fsname='testfs-mini',
        datetime_start=start,
        datetime_end=end)

    print("Looking for %s to %s" % (start, end))
    print(results)
    assert len(results)
    assert len(results) == 2
