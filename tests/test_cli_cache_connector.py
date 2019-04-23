#!/usr/bin/env python
"""
Test each connector's standalone CLI cache tool
"""

import os
import json
import sqlite3
try:
    import StringIO as io
except ImportError:
    import io
import datetime
import pandas
import nose

try:
    import elasticsearch.exceptions
    _HAVE_ELASTICSEARCH = True
except ImportError:
    _HAVE_ELASTICSEARCH = False

try:
    import requests.exceptions
    _HAVE_REQUESTS = True
except ImportError:
    _HAVE_REQUESTS = False

import tokiotest
import tokio.cli.cache_collectdes
import tokio.cli.cache_darshan
import tokio.cli.cache_esnet_snmp
import tokio.cli.cache_isdct
import tokio.cli.cache_lfsstatus
import tokio.cli.cache_lmtdb
import tokio.cli.cache_mmperfmon
import tokio.cli.cache_nersc_globuslogs
import tokio.cli.cache_nersc_jobsdb
import tokio.cli.cache_slurm
import tokio.cli.cache_topology

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def verify_sqlite(output_str):
    """
    Ensure that the database contains at least one table, and that table
    contains at least one row.
    """
    ### Try to find the caching file name from the application's stdout
    output_file = None
    for line in output_str.splitlines():
        if line.startswith('Caching to'):
            output_file = line.strip().split(None, 3)[-1]
            break
    if output_file is None:
        print("Could not find cache file name in output:")
        print(output_str)
        assert output_file is not None
    print("Using output_file [%s]" % output_file)
    assert os.path.isfile(output_file)
    tmpdb = sqlite3.connect(output_file)
    cursor = tmpdb.cursor()
    ## Count number of tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    tables = cursor.fetchall()
    print("Found %d tables in %s" % (len(tables), output_file))
    assert len(tables) > 0
    for table in [x[0] for x in tables]:
        cursor.execute('SELECT count(*) FROM %s' % table)
        rows = cursor.fetchall()
        num_rows = rows[0][0]
        print("Found %d rows in %s" % (num_rows, table))
        assert len(rows) > 0

def verify_json_zero_ok(json_str):
    """Ensure that json is loadable

    Args:
        json_str (str): string containing json text
    """
    data = json.loads(json_str)
    assert data is not None

def verify_json(json_str):
    """Ensure that json is loadable and contains something

    Args:
        json_str (str): string containing json text
    """
    data = json.loads(json_str)
    assert len(data) > 0

def verify_csv(csv_str):
    """
    Ensure that csv is loadable by Pandas
    """
    data = pandas.read_csv(io.StringIO(csv_str))
    assert len(data) > 0

def verify_sacct(csv_str):
    """
    Ensure that native format is vaguely valid (treat it as a |-separated csv)
    """
    data = pandas.read_csv(io.StringIO(csv_str), sep="|")
    assert len(data) > 0

def run_connector(binary, argv):
    """Default cache_connector run function

    Args:
        binary (module): tokio.cli module that contains a main() function
        argv (list of str): list of CLI arguments to pass to connector

    Returns:
        Stdout of cache connector script as a string
    """
    return tokiotest.run_bin(binary, argv)

def run_elasticsearch(binary, argv):
    """Run function that traps connection errors from ElasticSearch

    Args:
        binary (module): tokio.cli module that contains a main() function
        argv (list of str): list of CLI arguments to pass to connector

    Returns:
        Stdout of cache connector script as a string
    """
    if not _HAVE_ELASTICSEARCH:
        raise nose.SkipTest("elasticsearch module not available")

    try:
        return tokiotest.run_bin(binary, argv)
    except elasticsearch.exceptions.ConnectionError as error:
        raise nose.SkipTest(error)

def run_requests(binary, argv):
    """Run function that traps connection errors from REST

    Args:
        binary (module): tokio.cli module that contains a main() function
        argv (list of str): list of CLI arguments to pass to connector

    Returns:
        Stdout of cache connector script as a string
    """

    if not _HAVE_REQUESTS:
        raise nose.SkipTest("requests module not available")

    try:
        return tokiotest.run_bin(binary, argv)
    except requests.exceptions.ConnectionError as error:
        raise nose.SkipTest(error)

@nose.tools.raises(ValueError)
def run_raises_valueerror(binary, argv):
    return tokiotest.run_bin(binary, argv)

@nose.tools.raises(SystemExit)
def run_raises_systemexit(binary, argv):
    return tokiotest.run_bin(binary, argv)


CACHE_CONNECTOR_CONFIGS = [
    {
        'name':       'cli.cache_mmperfmon',
        'description': 'cli.cache_mmperfmon, gzipped text input',
        'binary':     tokio.cli.cache_mmperfmon,
        'args':       [tokiotest.SAMPLE_MMPERFMON_USAGE_INPUT],
        'validators': [verify_json,],
    },
    {
        'name':       'cli.cache_mmperfmon',
        'description': 'cli.cache_mmperfmon --csv --metric, gzipped text input',
        'binary':     tokio.cli.cache_mmperfmon,
        'args':       ['--csv', '--metric', tokiotest.SAMPLE_MMPERFMON_METRICS[0], tokiotest.SAMPLE_MMPERFMON_USAGE_INPUT],
        'validators': [verify_csv,],
    },
    {
        'name':       'cli.cache_mmperfmon',
        'description': 'cli.cache_mmperfmon --csv --host, gzipped text input',
        'binary':     tokio.cli.cache_mmperfmon,
        'args':       ['--csv', '--host', tokiotest.SAMPLE_MMPERFMON_HOSTS[0], tokiotest.SAMPLE_MMPERFMON_USAGE_INPUT],
        'validators': [verify_csv,],
    },
    {
        'name':       'cli.cache_mmperfmon',
        'description': 'cli.cache_mmperfmon --csv without --host/--metric',
        'binary':     tokio.cli.cache_mmperfmon,
        'runfunction': run_raises_systemexit,
        'args':       ['--csv', tokiotest.SAMPLE_MMPERFMON_USAGE_INPUT],
    },
    {
        'name':       'cli.cache_mmperfmon',
        'description': 'cli.cache_mmperfmon, tarfile input',
        'binary':     tokio.cli.cache_mmperfmon,
        'args':       [tokiotest.SAMPLE_MMPERFMON_TGZ_INPUT],
        'validators': [verify_json,],
    },
    {
        'name':       'cli.cache_mmperfmon',
        'description': 'cli.cache_mmperfmon, multiple inputs',
        'binary':     tokio.cli.cache_mmperfmon,
        'args':       [tokiotest.SAMPLE_MMPERFMON_USAGE_INPUT, tokiotest.SAMPLE_MMPERFMON_USAGE_INPUT],
        'validators': [verify_json,],
    },
    {
        'name':       'cli.cache_nersc_globuslogs',
        'description': 'cli.cache_nersc_globuslogs, cached input',
        'binary':     tokio.cli.cache_nersc_globuslogs,
        'args':       ['--input', tokiotest.SAMPLE_GLOBUSLOGS,
                       tokiotest.SAMPLE_TIMESTAMP_START_NOW,
                       tokiotest.SAMPLE_TIMESTAMP_END_NOW],
        'validators': [verify_json,],
    },
    {
        'name':       'cli.cache_nersc_globuslogs',
        'description': 'cli.cache_nersc_globuslogs, remote connection',
        'binary':     tokio.cli.cache_nersc_globuslogs,
        'args':       ['--input', tokiotest.SAMPLE_GLOBUSLOGS,
                       tokiotest.SAMPLE_TIMESTAMP_START_NOW,
                       tokiotest.SAMPLE_TIMESTAMP_END_NOW],
        'runfunction': run_elasticsearch,
        'validators': [verify_json_zero_ok,],
    },
    {
        'name':       'cli.cache_nersc_globuslogs',
        'description': 'cli.cache_nersc_globuslogs --csv',
        'binary':     tokio.cli.cache_nersc_globuslogs,
        'args':       ['--input', tokiotest.SAMPLE_GLOBUSLOGS,
                       '--csv',
                       tokiotest.SAMPLE_TIMESTAMP_START_NOW,
                       tokiotest.SAMPLE_TIMESTAMP_END_NOW],
        'validators': [verify_csv,],
    },
    {
        'name':       'cli.cache_isdct',
        'binary':     tokio.cli.cache_isdct,
        'args':       ['--json', tokiotest.SAMPLE_NERSCISDCT_FILE],
        'validators': [verify_json,],
    },
    {
        'name':       'cli.cache_isdct',
        'binary':     tokio.cli.cache_isdct,
        'args':       ['--csv', tokiotest.SAMPLE_NERSCISDCT_FILE],
        'validators': [verify_csv,],
    },
    {
        'name':       'cli.cache_collectdes',
        'description': 'cli.cache_collectdes, cached input',
        'binary':     tokio.cli.cache_collectdes,
        'args':       ['--input', tokiotest.SAMPLE_COLLECTDES_FILE,
                       tokiotest.SAMPLE_COLLECTDES_START,
                       tokiotest.SAMPLE_COLLECTDES_END],
        'validators': [verify_json,],
    },
    {
        'name':       'cli.cache_collectdes',
        'description': 'cli.cache_collectdes, remote connection',
        'binary':     tokio.cli.cache_collectdes,
        'args':       [tokiotest.SAMPLE_TIMESTAMP_START_NOW,
                       tokiotest.SAMPLE_TIMESTAMP_END_NOW],
        'runfunction': run_elasticsearch,
        'validators': [verify_json_zero_ok,],
    },
    {
        'name':       'cli.cache_darshan',
        'binary':     tokio.cli.cache_darshan,
        'args':       ['--base', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'name':       'cli.cache_darshan',
        'binary':     tokio.cli.cache_darshan,
        'args':       ['--perf', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'name':       'cli.cache_darshan',
        'binary':     tokio.cli.cache_darshan,
        'args':       ['--total', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'name':       'cli.cache_darshan',
        'binary':     tokio.cli.cache_darshan,
        'args':       ['--base', '--perf', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'name':       'cli.cache_darshan',
        'binary':     tokio.cli.cache_darshan,
        'args':       ['--base', '--total', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'name':       'cli.cache_darshan',
        'binary':     tokio.cli.cache_darshan,
        'args':       ['--perf', '--total', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'name':       'cli.cache_darshan',
        'binary':     tokio.cli.cache_darshan,
        'args':       ['--base', '--perf', '--total', tokiotest.SAMPLE_DARSHAN_LOG],
        'validators': [verify_json,],
    },
    {
        'name':        'cli.cache_esnet_snmp',
        'description': 'cli.cache_esnet_snmp default args',
        'binary':      tokio.cli.cache_esnet_snmp,
        'args':        ["nersc"],
        'runfunction': run_requests,
        'validators':  [verify_json,],
    },
    {
        'name':        'cli.cache_esnet_snmp',
        'description': 'cli.cache_esnet_snmp --json, remote connection',
        'binary':      tokio.cli.cache_esnet_snmp,
        'args':        ["--json", "nersc"],
        'runfunction': run_requests,
        'validators':  [verify_json,],
    },
    {
        'name':        'cli.cache_esnet_snmp',
        'description': 'cli.cache_esnet_snmp --csv, remote connection',
        'binary':      tokio.cli.cache_esnet_snmp,
        'args':        ["--csv", "nersc"],
        'runfunction': run_requests,
        'validators':  [verify_csv,],
    },
    {
        'name':        'cli.cache_esnet_snmp',
        'description': 'cli.cache_esnet_snmp --json, cached input',
        'binary':      tokio.cli.cache_esnet_snmp,
        'args':        ["--input", tokiotest.SAMPLE_ESNET_SNMP_FILE, "--json", "nersc"],
        'validators':  [verify_json,],
    },
    {
        'name':        'cli.cache_esnet_snmp',
        'description': 'cli.cache_esnet_snmp --csv, cached input',
        'binary':      tokio.cli.cache_esnet_snmp,
        'args':        ["--input", tokiotest.SAMPLE_ESNET_SNMP_FILE, "--csv", "nersc"],
        'validators':  [verify_csv,],
    },
    {
        'name':        'cli.cache_esnet_snmp',
        'description': 'cli.cache_esnet_snmp, explicit endpoint:interface',
        'binary':      tokio.cli.cache_esnet_snmp,
        'args':        ["--input", tokiotest.SAMPLE_ESNET_SNMP_FILE, "blah0:interf0"],
        'validators':  [verify_json,],
    },
    {
        'name':        'cli.cache_esnet_snmp',
        'description': 'cli.cache_esnet_snmp, invalid endpoint:interface',
        'binary':      tokio.cli.cache_esnet_snmp,
        'args':        ["--input", tokiotest.SAMPLE_ESNET_SNMP_FILE, "blah"],
        'runfunction': run_raises_valueerror,
    },
    {
        'name':        'cli.cache_esnet_snmp',
        'description': 'cli.cache_esnet_snmp, invalid --start format',
        'binary':      tokio.cli.cache_esnet_snmp,
        'args':        ["--input", tokiotest.SAMPLE_ESNET_SNMP_FILE, "--start", "invalid", "nersc"],
        'runfunction': run_raises_valueerror,
    },
    {
        'name':        'cli.cache_esnet_snmp',
        'description': 'cli.cache_esnet_snmp, --end without --start',
        'binary':      tokio.cli.cache_esnet_snmp,
        'args':        ["--input", tokiotest.SAMPLE_ESNET_SNMP_FILE, "--end", "2019-01-01T00:00:00", "nersc"],
        'runfunction': run_raises_systemexit,
    },
    {
        'name':        'cli.cache_esnet_snmp',
        'description': 'cli.cache_esnet_snmp, --start > --end',
        'binary':      tokio.cli.cache_esnet_snmp,
        'args':        ["--input", tokiotest.SAMPLE_ESNET_SNMP_FILE, "--start", "2019-01-02T00:00:00", "--end", "2019-01-01T00:00:00", "nersc"],
        'runfunction': run_raises_valueerror,
    },
    {
        'name':       'cli.cache_slurm',
        'binary':     tokio.cli.cache_slurm,
        'args':       ['--json', tokiotest.SAMPLE_SLURM_CACHE_FILE],
        'validators': [verify_json,],
    },
    {
        'name':       'cli.cache_slurm',
        'binary':     tokio.cli.cache_slurm,
        'args':       ['--csv', tokiotest.SAMPLE_SLURM_CACHE_FILE],
        'validators': [verify_csv,],
    },
    {
        'name':       'cli.cache_slurm',
        'binary':     tokio.cli.cache_slurm,
        'args':       ['--native', tokiotest.SAMPLE_SLURM_CACHE_FILE],
        'validators': [verify_sacct,],
    },
    {
        'name':       'cli.cache_topology',
        'description': 'cli.cache_topology',
        'binary':     tokio.cli.cache_topology,
        'args':       [
            '--nodemap-cache', tokiotest.SAMPLE_XTDB2PROC_FILE,
            '--jobinfo-cache', tokiotest.SAMPLE_SLURM_CACHE_FILE,
        ],
        'validators': [verify_json,],
    },
    {
        'description': 'cli.cache_lfsstatus --fullness, no cache',
        'binary':     tokio.cli.cache_lfsstatus,
        'args':        [
            '--fullness',
            '--',
            tokiotest.SAMPLE_DARSHAN_SONEXION_ID,
            tokiotest.SAMPLE_DARSHAN_START_TIME.replace(' ', 'T'),
        ],
        'validators':  [verify_json,],
    },
    {
        'description': 'cli.cache_lfsstatus --fullness, explicit cache',
        'binary':     tokio.cli.cache_lfsstatus,
        'args':        [
            '--fullness',
            tokiotest.SAMPLE_OSTFULLNESS_FILE,
            tokiotest.SAMPLE_DARSHAN_SONEXION_ID,
            tokiotest.SAMPLE_DARSHAN_START_TIME.replace(' ', 'T'),
        ],
        'validators':  [verify_json,],
    },
    {
        'description': 'cli.cache_lfsstatus --failure, no cache',
        'binary':     tokio.cli.cache_lfsstatus,
        'args':        [
            '--failure',
            '--',
            tokiotest.SAMPLE_DARSHAN_SONEXION_ID,
            tokiotest.SAMPLE_DARSHAN_START_TIME.replace(' ', 'T'),
        ],
        'validators':  [verify_json,],
    },
    {
        'description': 'cli.cache_lfsstatus --failure, explicit cache',
        'binary':     tokio.cli.cache_lfsstatus,
        'args':        [
            '--failure',
            tokiotest.SAMPLE_OSTMAP_FILE,
            tokiotest.SAMPLE_DARSHAN_SONEXION_ID,
            tokiotest.SAMPLE_DARSHAN_START_TIME.replace(' ', 'T'),
        ],
        'validators':  [verify_json,],
    },
    {
        'description': 'cli.cache_nersc_jobsdb',
        'binary':     tokio.cli.cache_nersc_jobsdb,
        'args':        [
            '-i', tokiotest.SAMPLE_NERSCJOBSDB_FILE,
            datetime.datetime.fromtimestamp(
                tokiotest.SAMPLE_NERSCJOBSDB_START).strftime("%Y-%m-%dT%H:%M:%S"),
            datetime.datetime.fromtimestamp(
                tokiotest.SAMPLE_NERSCJOBSDB_END).strftime("%Y-%m-%dT%H:%M:%S"),
            'edison',
        ],
        'validators': [verify_sqlite,],
        'to_file':    [True],
        'validate_contents': False,
    },
    {
        'description': 'cli.cache_lmtdb',
        'binary':     tokio.cli.cache_lmtdb,
        'args':        [
            '-i', tokiotest.SAMPLE_LMTDB_FILE,
            datetime.datetime.fromtimestamp(
                tokiotest.SAMPLE_LMTDB_START).strftime("%Y-%m-%dT%H:%M:%S"),
            datetime.datetime.fromtimestamp(
                tokiotest.SAMPLE_LMTDB_END).strftime("%Y-%m-%dT%H:%M:%S"),
        ],
        'validators': [verify_sqlite,],
        'to_file':    [True],
        'validate_contents': False,
    },
    {
        'description': 'cli.cache_lmtdb --limit',
        'binary':     tokio.cli.cache_lmtdb,
        'args':        [
            '--limit', '2',
            '-i', tokiotest.SAMPLE_LMTDB_FILE,
            datetime.datetime.fromtimestamp(
                tokiotest.SAMPLE_LMTDB_START).strftime("%Y-%m-%dT%H:%M:%S"),
            datetime.datetime.fromtimestamp(
                tokiotest.SAMPLE_LMTDB_END).strftime("%Y-%m-%dT%H:%M:%S"),
        ],
        'validators': [verify_sqlite,],
        'to_file':    [True],
        'validate_contents': False,
    },
]

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def run_cache_connector(config, to_file=False):
    """
    Test a connector cache (cache_*.py) CLI interface
    """
    if config['binary'] == tokio.cli.cache_darshan:
        tokiotest.check_darshan()

    runfunction = config.get('runfunction', run_connector)

    if to_file:
        argv = ['-o', tokiotest.TEMP_FILE.name] + config['args']
        print("Caching to %s" % tokiotest.TEMP_FILE.name)
        print("Executing: %s" % ' '.join(argv))
        output_str = runfunction(config['binary'], argv)

        # (validate_contents == True) means the associated validator function
        # expects the contents of the output file rather than the name of the
        # output file
        if config.get('validate_contents', True):
            output_str = tokiotest.TEMP_FILE.read()
    else:
        argv = config['args']
        print("Caching to stdout")
        print("Executing: %s" % ' '.join(argv))
        output_str = runfunction(config['binary'], argv)

    for validator in config.get('validators', []):
        if isinstance(output_str, bytes):
            validator(output_str.decode())
        else:
            validator(output_str)

def craft_description(config, suffix):
    """
    Take a cache_*.py command invocation and craft a clever test description
    """
    if 'description' in config:
        result = "%s %s" % (config['description'], suffix)
    elif 'name' in config:
        result = "%s %s %s" % (
            config['name'],
            ' '.join(config['args'][0:-1]),
            suffix)
    else:
        result = "%s %s %s" % (
            config['binary'],
            ' '.join(config['args'][0:-1]),
            suffix)
    return result

@tokiotest.needs_darshan
def test():
    """
    Test all connector cache scripts
    """
    for config in CACHE_CONNECTOR_CONFIGS:
        func = run_cache_connector

        for to_file in config.get('to_file', [True, False]):
            if to_file:
                func.description = craft_description(config, '(to file)')
            else:
                func.description = craft_description(config, '(to stdout)')
            yield func, config, to_file
