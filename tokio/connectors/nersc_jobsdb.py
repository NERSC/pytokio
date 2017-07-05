#!/usr/bin/env python
"""
Extract job info from the NERSC jobs database.  Requires the following
environment variables to be set:

    MYSQL_HOST
    MYSQL_USER
    MYSQL_PASSWORD

On Edison, it also requires that the mysql module is loaded.
"""

import os
import sys
import subprocess
import extract_darshan_perf
import darshan
try:
    import MySQLdb
except:
    pass

_CONCURRENT_JOBS_XATTR = "user.concurrent_slurm_jobs"
MYSQL_HOST = os.environ.get('MYSQL_HOST')
MYSQL_USER = os.environ.get('MYSQL_USER')
MYSQL_PASSWD = os.environ.get('MYSQL_PASSWORD')

def get_concurrent_jobs_mysql(start_timestamp, end_timestamp):
    """
    Connect to the NERSC jobs database and grab the number of jobs that were
    running, in part or in full, during the time window bounded by
    start_timestamp and end_timestamp

    """
    if MYSQL_HOST is None or MYSQL_USER is None or MYSQL_PASSWD is None:
        raise Exception("Must define MYSQL_HOST, MYSQL_USER, and MYSQL_PASSWORD")

    db = MySQLdb.connect(host=MYSQL_HOST, user=MYSQL_USER,
                         passwd=MYSQL_PASSWD, db='jobs')
    cur = db.cursor()
    cur.execute("""
    SELECT
        count(s.hostname)
    FROM
        summary AS s
    WHERE
        s.hostname = "edison"
    AND s.`completion` > %d # end time is after darshan_start_time
    AND s.`start` < %d # start time is before darshan_end_time
    """ % (start_timestamp, end_timestamp))

    result = cur.fetchone()[0]
    db.close()
    return result

def get_concurrent_jobs_cached(darshan_log):
    """
    Read the concurrent job count from a darshan log
    
    """
    p = subprocess.Popen(['getfattr', '-n', _CONCURRENT_JOBS_XATTR, darshan_log], stdout=subprocess.PIPE)
    value = None
    for line in p.stdout:
        line_strip = line.strip()
        if len(line_strip) == 0 or line_strip.startswith('#'):
            continue
        if '=' in line_strip:
            key, value = line_strip.split('=', 2)
            value = int(value.strip('"')) #TODO verify if this version is correct
    return value

def set_concurrent_jobs_cached(darshan_log, value):
    """
    Set the concurrent job count on the darshan log file
    
    """
    return subprocess.call(['setfattr', '-n', _CONCURRENT_JOBS_XATTR, '-v', '"%d"' % value, darshan_log])


def get_concurrent_jobs(darshan_log):
    value = get_concurrent_jobs_cached(darshan_log)
    if value is None:
        d = darshan.DARSHAN(darshan_log)
        darshan_perf = d.darshan_parser_perf()
        value = get_concurrent_jobs_mysql(int(darshan_perf['start_time']), int(darshan_perf['end_time']))
        set_concurrent_jobs_cached(darshan_log, value)
    return value
