#!/usr/bin/env python

import sys
import subprocess
import datetime
import json

class Slurm(dict):
    
    def __init__(self, jobid=None, cache_file=None):
        super(Slurm, self).__init__(self)
        self.cache_file = cache_file
        self.jobid = jobid
        self.load()
        
    def __repr__(self):
        return json.dumps(self.values())
        
    def load(self):
        if self.cache_file is None and self.jobid is None:
            raise Exception("parameters should be provided (at least jobid or cache_file)")
        
        if self.cache_file:
            self.__setitem__(json.load(cache_file))
    
    def save_cache(self, output_file=None):
        """
        Save the dictionnary in a json file
        """
        if output_file is None:
            self._save_cache(sys.stdout)
        else:
            with open(output_file, 'w') as fp:
                self.save_cache(json.dumps(self))

    def _save_cache(self, output):
        output.write(str(self))
   
    
    #======================================================#
    
    def _expand_nodelist(self, node_string):
        """
        scontrol show hostname nid0[5032-5159]
        """
        node_names = set([])
        p = subprocess.Popen(['scontrol', 'show', 
                              'hostname', node_string], stdout=subprocess.PIPE)
        for line in p.stdout:
            node_name = line.strip()
            if len(node_name) > 0:
                node_names.add(node_name)
        return node_names

    
    #======================================================#
    
    def get_task_startstop(self):
        """
        sacct -j 4773134 -o self.jobidraw,start,end -p -n
        
        4773134|2017-05-01T02:00:17|2017-05-01T02:11:08|
        4773134.batch|2017-05-01T02:00:17|2017-05-01T02:11:08|
        """
        p = subprocess.Popen(['sacct', '-j', 
                              str(self.jobid), '--format=self.jobidraw,start,end', 
                              '-p', '-n'], stdout=subprocess.PIPE)
        tasks = {}
        for line in p.stdout:
            self.jobid_raw, start, stop = line.split('|')[0:3]
            start = datetime.datetime.strptime(start, "%Y-%m-%dT%H:%M:%S")
            stop = datetime.datetime.strptime(stop, "%Y-%m-%dT%H:%M:%S")
            if '.' not in self.jobid_raw:
                self.jobid = self.jobid_raw
                taskid = '_overall'
        else:
            self.jobid, taskid = self.jobid_raw.split('.',1)

        tasks[taskid] = (start, stop)        
        self.__setitem__('task_startstop', task_startstop)
        return self
    
    def get_job_nodes(self):
        """
        sacct -j 4295762 --format=nodelist -p -n
        """
        node_strings = []
        p = subprocess.Popen(['sacct', '-j', str(self.jobid), '--format=nodelist', '-p', '-n'], stdout=subprocess.PIPE)
        for line in p.stdout:
            node_strings.append(line.split('|')[0])
            
        node_names = set([])
        for node_string in node_strings:
            for node_name in _expand_nodelist(node_string):
                node_names.add(node_name)
        
        self.__setitem__('node_names', node_names)
        return self

    def get_job_startstop(self):
        min_start = None
        max_stop = None
        for _, startstop in get_task_startstop(self.jobid).iteritems():
            start, stop = startstop
            if min_start is None or min_start > start:
                min_start = start
            if max_stop is None or max_stop < stop:
                max_stop = stop
             
        self.__setitem__('min_start', min_start)
        self.__setitem__('max_stop', max_stop)
        return self
