#!/usr/bin/env python
"""
Test the cli.summarize_darshanlogs and cli.darshan_scoreboard tools
"""

import json
import nose
import tokiotest
import tokio.cli.darshan_scoreboard

INDEXDB = tokiotest.SAMPLE_DARSHAN_INDEX_DB
INDEXDB_USER = tokiotest.SAMPLE_DARSHAN_INDEX_DB_USER
INDEXDB_ALL_MOUNTS = tokiotest.SAMPLE_DARSHAN_INDEX_DB_ALL_MOUNTS_LOGICAL
INDEXDB_EXES = tokiotest.SAMPLE_DARSHAN_INDEX_DB_EXES

def test_scoreboard():
    """cli.darshan_scoreboard ascii output
    """
    argv = [INDEXDB]
    print("Executing: %s" % " ".join(argv))
    output_str = tokiotest.run_bin(tokio.cli.darshan_scoreboard, argv)
    assert len(output_str.splitlines()) > 5

def test_scoreboard_json():
    """cli.darshan_scoreboard --json
    """
    argv = ['--json', INDEXDB]
    print("Executing: %s" % " ".join(argv))
    output_str = tokiotest.run_bin(tokio.cli.darshan_scoreboard, argv)
    decoded_result = json.loads(output_str)
    print("Result: %s" % decoded_result)
    assert len(decoded_result) > 0

def test_scoreboard_limit_user():
    """cli.darshan_scoreboard --limit-user
    """
    argv = ['--json', '--limit-user', INDEXDB_USER, INDEXDB]
    print("Executing: %s" % " ".join(argv))
    output_str = tokiotest.run_bin(tokio.cli.darshan_scoreboard, argv)
    decoded_result = json.loads(output_str)
    print("Result: %s" % decoded_result)
    assert INDEXDB_USER in [x[3] for x in decoded_result['per_user']]

def test_scoreboard_exclude_user():
    """cli.darshan_scoreboard --exclude-user
    """
    argv = ['--json', '--exclude-user', "%s" % INDEXDB_USER, INDEXDB]
    print("Executing: %s" % " ".join(argv))
    output_str = tokiotest.run_bin(tokio.cli.darshan_scoreboard, argv)
    decoded_result = json.loads(output_str)
    print("Result: %s" % decoded_result)
    assert INDEXDB_USER not in [x[3] for x in decoded_result['per_user']]

def limit_fs(indexdb_mounts):
    """cli.darshan_scoreboard --limit-fs
    """
    # include ALL file systems - expect everything to return
    argv = ['--json', '--limit-fs', ",".join(indexdb_mounts), INDEXDB]
    print("Executing: %s" % " ".join(argv))
    output_str = tokiotest.run_bin(tokio.cli.darshan_scoreboard, argv)
    decoded_result = json.loads(output_str)
    print("Result: %s" % decoded_result)
    assert decoded_result['per_user']
    assert decoded_result['per_fs']
    assert decoded_result['per_exe']

    # include only one file system - expect a subset of above results
    wanted_fs = indexdb_mounts[0]
    argv = ['--json', '--limit-fs', wanted_fs, INDEXDB]
    print("Executing: %s" % " ".join(argv))
    output_str = tokiotest.run_bin(tokio.cli.darshan_scoreboard, argv)
    decoded_result2 = json.loads(output_str)
    print("Result: %s" % decoded_result2)

    # make sure we got something back
    assert len(decoded_result2['per_fs'])

    # make sure that we got back one and only one file system
    assert len(decoded_result2['per_fs']) == 1

    # make sure that each app we wanted is defined in the results
    all_fs = [x[3] for x in decoded_result2['per_fs']] + [x[4] for x in decoded_result2['per_fs']]
    assert wanted_fs in all_fs

def exclude_fs(indexdb_mounts):
    """cli.darshan_scoreboard --exclude-fs
    """
    argv = ['--json', '--exclude-fs', ",".join(indexdb_mounts), INDEXDB]
    print("Executing: %s" % " ".join(argv))
    output_str = tokiotest.run_bin(tokio.cli.darshan_scoreboard, argv)
    decoded_result = json.loads(output_str)
    print(decoded_result)
    assert not decoded_result['per_user']
    assert not decoded_result['per_exe']
    assert not decoded_result['per_fs']

    # now test excluding just one file system
    excluded_fs = indexdb_mounts[0]
    argv = ['--json', '--exclude-fs', "%s" % excluded_fs, INDEXDB]
    print("Executing: %s" % " ".join(argv))
    output_str = tokiotest.run_bin(tokio.cli.darshan_scoreboard, argv)
    decoded_result2 = json.loads(output_str)
    print(decoded_result2)

    # make sure we got something back
    assert len(decoded_result2['per_fs'])

    # make sure that results don't include the thing we excluded
    all_fs = [x[3] for x in decoded_result2['per_fs']] + [x[4] for x in decoded_result2['per_fs']]
    assert excluded_fs not in all_fs 

def test_scoreboard_fs_filters():
    tests = [
        (
            exclude_fs,
            tokiotest.SAMPLE_DARSHAN_INDEX_DB_ALL_MOUNTS,
            "cli.darshan_scoreboard --exclude-fs"
        ),
        (
            exclude_fs,
            tokiotest.SAMPLE_DARSHAN_INDEX_DB_ALL_MOUNTS_LOGICAL,
            "cli.darshan_scoreboard --exclude-fs, logical fs name"
        ),
        (
            limit_fs,
            tokiotest.SAMPLE_DARSHAN_INDEX_DB_ALL_MOUNTS,
            "cli.darshan_scoreboard --limit-fs"
        ),
        (
            limit_fs,
            tokiotest.SAMPLE_DARSHAN_INDEX_DB_ALL_MOUNTS_LOGICAL,
            "cli.darshan_scoreboard --limit-fs, logical fs name"
        ),
    ]
    for test in tests:
        test_func = test[0]
        test_func.description = test[2]
        yield test_func, test[1]

def test_scoreboard_limit_exe():
    """cli.darshan_scoreboard --limit-exe
    """
    # try to get back a few specific exes that we know exist (INDEXDB_EXES)
    for appname in INDEXDB_EXES + [','.join(INDEXDB_EXES)]:
        argv = ['--json', '--limit-exe', appname, INDEXDB]
        print("Executing: %s" % " ".join(argv))
        output_str = tokiotest.run_bin(tokio.cli.darshan_scoreboard, argv)
        decoded_result = json.loads(output_str)
        print("Result: %s" % decoded_result)

        # make sure we got something back
        assert len(decoded_result['per_exe'])

        wanted_apps = appname.split(',')
        # make sure that we got back the same number of apps as we queried for
        assert len(decoded_result['per_exe']) == len(wanted_apps)

        # make sure that each app we wanted is defined in the results
        for wanted_app in wanted_apps:
            assert wanted_app in [x[3] for x in decoded_result['per_exe']]

def test_scoreboard_exclude_exe():
    """cli.darshan_scoreboard --exclude-exe
    """
    # get a reference dataset without anything removed
    argv = ['--json', INDEXDB]
    print("Executing: %s" % " ".join(argv))
    output_str = tokiotest.run_bin(tokio.cli.darshan_scoreboard, argv)
    reference_result = json.loads(output_str)
    print("Result: %s" % reference_result)

    for appname in INDEXDB_EXES + [','.join(INDEXDB_EXES)]:
        argv = ['--json', '--exclude-exe', appname, INDEXDB]
        print("Executing: %s" % " ".join(argv))
        output_str = tokiotest.run_bin(tokio.cli.darshan_scoreboard, argv)
        decoded_result = json.loads(output_str)
        print("Result: %s" % decoded_result)

        # make sure we got SOME results
        assert len(decoded_result['per_exe'])

        # make sure the app is not included in the results
        assert appname not in [x[3] for x in decoded_result['per_exe']]

        # make sure that we successfully removed something that was present in
        # the unfiltered reference
        assert len(decoded_result['per_exe']) < len(reference_result['per_exe'])

def test_scoreboard_combo():
    """cli.darshan_scoreboard with multiple excludes/limits

    Tests the combinatorial logic when specifying multiple things to include
    (OR) and things to exclude (AND).
    """
    argv = ['--json', INDEXDB]
    print("Executing: %s" % " ".join(argv))
    output_str = tokiotest.run_bin(tokio.cli.darshan_scoreboard, argv)
    decoded_result = json.loads(output_str)
    print("Result: %s" % json.dumps(decoded_result))

    for user_exe_fs in decoded_result['per_user_exe_fs']:
        username, exename, mountpt = user_exe_fs[3].split('|')
        # jobcount = user_exe_fs[-1]
        argv = ['--json', INDEXDB]
        argv += ['--limit-user', username]
        argv += ['--limit-exe', exename]
        argv += ['--limit-fs', mountpt]
        print("Executing: %s" % " ".join(argv))
        output_str = tokiotest.run_bin(tokio.cli.darshan_scoreboard, argv)
        subresult = json.loads(output_str)

        for category in ['per_user', 'per_exe', 'per_fs', 'per_user_exe_fs']:
            assert len(subresult[category]) == 1
