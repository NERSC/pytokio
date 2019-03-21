#!/usr/bin/env python
"""Test the archive_nersc_globuslogs.py tool
"""

import os
import sqlite3

import nose

import tokio.cli.archive_nersc_globuslogs
import tokiotest

def generate_sqlite(output_file,
                    input_file=tokiotest.SAMPLE_GLOBUSLOGS,
                    query_start=tokiotest.SAMPLE_GLOBUSLOGS_START,
                    query_end=tokiotest.SAMPLE_GLOBUSLOGS_END):
    """Create a TokioTimeSeries output file
    """
    argv = [
        '--input', input_file,
        '--output', output_file,
        query_start,
        query_end
    ]
    print("Running [%s]" % ' '.join(argv))
    tokio.cli.archive_nersc_globuslogs.main(argv)
    print("Created %s" % output_file)

def validate_sqlite(sqlite_file, table_name='transfers'):
    """Ensures that the SQLite file is reasonable
    """
    assert os.path.isfile(sqlite_file)
    conn = sqlite3.connect(sqlite_file)
    rows = conn.execute("SELECT * from %s" % table_name).fetchall()
    conn.close()

    assert len(rows) > 1

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_basic():
    """connectors.cli.archive_nersc_globuslogs
    """
    tokiotest.TEMP_FILE.close()

    generate_sqlite(output_file=tokiotest.TEMP_FILE.name)

    validate_sqlite(tokiotest.TEMP_FILE.name)
