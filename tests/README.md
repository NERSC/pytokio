This directory contains unit tests and integration tests that should be all
completely self-contained.  Several tests require external binaries including
`darshan-parser` and `sacct` and cannot pass in their absence.

To run the unit tests, use the standard

    nosetests -v

Those tests which fail due to third-party binaries being absent will generate
failed assertions that look like

    ======================================================================
    FAIL: Integration of lfsstatus
    ----------------------------------------------------------------------
    Traceback (most recent call last):
      File "/Users/glock/.apps/anaconda2/lib/python2.7/site-packages/nose/case.py", line 197, in runTest
        self.test(*self.arg)
      File "/Users/glock/src/git/work/pytokio/tests/test_bin_summarize_job.py", line 76, in test_darshan_summary_with_lfsstatus
        assert p.returncode == 0
    AssertionError: 

The coverage of these tests is far from complete, but many components of pytokio
were created used a test-driven approach.  Increasing coverage is an ongoing
effort.
