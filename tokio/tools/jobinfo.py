"""Site-independent interface to retrieve job info
"""

import os
import tokio.config as config

try:
    import tokio.connectors.slurm
except ImportError:
    pass

try:
    import tokio.connectors.nersc_jobsdb
except ImportError:
    pass

DEFAULT_JOBID_PROVIDERS = ['slurm']
DEFAULT_JOBNODES_PROVIDERS = ['slurm']

def get_job_startend(jobid, cache_file=None):
    """Find earliest start and latest end time for a job.

    Returns:
        tuple: Two-item tuple of (earliest start time, latest end time) in
            whatever type the underlying module returns
    """
    jobid_providers = config.CONFIG.get('jobinfo_jobid_providers', DEFAULT_JOBID_PROVIDERS)
    for jobid_provider in jobid_providers:
        if jobid_provider == 'slurm':
            slurm_job = tokio.connectors.slurm.Slurm(jobid=jobid, cache_file=cache_file)
            return slurm_job.get_job_startend()
        elif jobid_provider == 'nersc_jobsdb':
            nersc_host = config.CONFIG.get('nersc_host')
            if nersc_host is None:
                nersc_host = os.environ.get('NERSC_HOST')
            if nersc_host is None:
                raise KeyError("NERSC_HOST not defined in environment or pytokio config")
            nersc_jobsdb = tokio.connectors.nersc_jobsdb.NerscJobsDb(cache_file=cache_file)
            return nersc_jobsdb.get_job_startend(jobid=jobid, nersc_host=nersc_host)
        else:
            raise Exception("No valid jobid providers found")

#def get_job_ids():
#    pass

#def get_job_nodes():
#    pass
