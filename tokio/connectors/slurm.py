#!/usr/bin/env python

import sys
import subprocess
import datetime

def get_job_nodes(jobid):
    """
    sacct -j 4295762 --format=nodelist -p -n
    """
    node_strings = []
    p = subprocess.Popen(['sacct', '-j', str(jobid), '--format=nodelist', '-p', '-n'], stdout=subprocess.PIPE)
    for line in p.stdout:
        node_strings.append(line.split('|')[0])

    node_names = set([])
    for node_string in node_strings:
        for node_name in expand_nodelist(node_string):
            node_names.add(node_name)

    return node_names

def get_job_startstop(jobid):
    """
    sacct -j 4295762 --format=Start,End -p -n
    """
    min_start = None
    max_stop = None
    for _, startstop in get_task_startstop(jobid).iteritems():
        start, stop = startstop
        if min_start is None or min_start > start:
            min_start = start
        if max_stop is None or max_stop < stop:
            max_stop = stop

    return min_start, max_stop

def get_task_startstop(jobid):
    """
    sacct -j 4773134 -o jobidraw,start,end -p -n

    4773134|2017-05-01T02:00:17|2017-05-01T02:11:08|
    4773134.batch|2017-05-01T02:00:17|2017-05-01T02:11:08|
    4773134.extern|2017-05-01T02:00:17|2017-05-01T02:11:09|
    4773134.0|2017-05-01T02:00:23|2017-05-01T02:01:28|
    4773134.1|2017-05-01T02:01:28|2017-05-01T02:01:30|
    4773134.2|2017-05-01T02:01:33|2017-05-01T02:02:42|
    """
    p = subprocess.Popen(['sacct', '-j', str(jobid), '--format=jobidraw,start,end', '-p', '-n'], stdout=subprocess.PIPE)
    tasks = {}
    for line in p.stdout:
        jobid_raw, start, stop = line.split('|')[0:3]
        start = datetime.datetime.strptime(start, "%Y-%m-%dT%H:%M:%S")
        stop = datetime.datetime.strptime(stop, "%Y-%m-%dT%H:%M:%S")
        if '.' not in jobid_raw:
            jobid = jobid_raw
            taskid = '_overall'
        else:
            jobid, taskid = jobid_raw.split('.',1)

        tasks[taskid] = ( start, stop )
    return tasks

def expand_nodelist(node_string):
    """
    scontrol show hostname nid0[5032-5159]
    """
    node_names = set([])
    p = subprocess.Popen(['scontrol', 'show', 'hostname', node_string], stdout=subprocess.PIPE)
    for line in p.stdout:
        node_name = line.strip()
        if len(node_name) > 0:
            node_names.add(node_name)
    return node_names

if __name__ == "__main__":
    print get_job_nodes(sys.argv[1])
    print get_job_startstop(sys.argv[1])
