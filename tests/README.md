This directory contains unit tests and integration tests that should be all
completely self-contained.  Several tests require external binaries including
`darshan-parser` and `sacct` and will be skipped in their absence.

To run the unit tests, use the standard

    nosetests -v

There are also several modules that look for specific environment variables
to test functionality that depends on external databases.  Specifically,
the following variables:

are required to test the following connectors' remote capabilities:

* `tokio.connectors.nersc_jobsdb` - the NERSC jobs database
    * `NERSC_JOBSDB_HOST` - hostname of NERSC jobs database
    * `NERSC_JOBSDB_USER` - username to log into NERSC jobs database
    * `NERSC_JOBSDB_PASSWORD` - password for `NERSC_JOBSDB_USER`
    * `NERSC_JOBSDB_DB` - database to use
* `tokio.connectors.lmtdb` - an LMT MySQL database
* `tokio.connectors.cachingdb` - a generic database connector that uses the
  schema of an LMT database to exercise its functionality
    * `TEST_DB_HOST` - hostname of LMT database
    * `TEST_DB_USER` - username to log into LMT database
    * `TEST_DB_PASSWORD` - password for `TEST_DB_USER`
    * `TEST_DB_DBNAME` - database to use (e.g., `filesystem_snx12345`)


