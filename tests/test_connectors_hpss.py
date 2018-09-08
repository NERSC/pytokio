"""Tests for the HPSS daily report connector
"""

import json
import tokio.connectors.hpss
import tokiotest

TEST_STRINGS = [
    "===== === ==== ======",
    "    ===== === ==== ======",
    "===== === ==== ======    ",
    "========",
    "=",
    "= ",
    " =",
    "= =",
    "= = ",
    " = =",
    "",
]

TEST_TABLE = """
Archive : IO Totals by Client Host
Host             Users      IO_GB       Ops   Write_GB     W_Ops    Read_GB     R_Ops    Copy_GB     C_Ops
===============  =====  =========  ========  =========  ========  =========  ========  =========  ========
Cori                25    74318.5     11237    65835.7      7583     8482.8      3654        0.0         0
Edison              15    52499.4     10948    27635.8     10555    24863.7       393        0.0         0
DTN                 11    40124.8      6140    37761.7      2819     2363.1      3321        0.0         0
Other                2     6517.7       670     3621.1       623     2896.6        47        0.0         0
Nersc_Lan            7     4274.3       650     3339.3       240      935.0       410        0.0         0
LBL                  4      544.5        40      544.5        40        0.0         0        0.0         0
Total               64   178279.2     29685   138738.0     21860    39541.2      7825        0.0         0
"""

def test_find_columns():
    """connectors.hpss._find_columns()
    """
    for test_str in TEST_STRINGS:
        cols = tokio.connectors.hpss._find_columns(test_str) #pylint: disable=protected-access

        print("Test string: [%s]" % test_str)
        for istart, length in cols:
            sliced_str = test_str[istart:istart+length]
            print("%2d, %2d, = [%s]" % (istart, length, sliced_str))
            assert sliced_str.strip().rstrip("=") == ""

def test_find_columns_strict():
    """connectors.hpss._find_columns(strict=True)
    """
    for test_str in TEST_STRINGS:
        cols = tokio.connectors.hpss._find_columns(test_str, strict=True) #pylint: disable=protected-access

        print("Test string: [%s]" % test_str)
        for istart, length in cols:
            sliced_str = test_str[istart:istart+length]
            print("%2d, %2d, = [%s]" % (istart, length, sliced_str))
            assert sliced_str.rstrip("=") == ""

def test_parse_section():
    """connectors.hpss._parse_section()
    """
    records = tokio.connectors.hpss._parse_section(TEST_TABLE.splitlines()) #pylint: disable=protected-access
    print(json.dumps(records[0], indent=4, sort_keys=True))

def test_load():
    """connectors.hpss.HpssDailyReport.load()
    """
    results = tokio.connectors.hpss.HpssDailyReport(cache_file=tokiotest.SAMPLE_HPSS_REPORT)
    print(json.dumps(results, indent=4, sort_keys=True))
    assert results
