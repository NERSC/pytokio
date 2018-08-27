import tokio.tools.jobinfo
import tokiotest

def test_get_job_startend_slurm():
    """tools.jobinfo.get_job_startend, Slurm
    """
    tokio.config.CONFIG["jobinfo_jobid_providers"] = ["slurm"]
    start, end = tokio.tools.jobinfo.get_job_startend(
        jobid=tokiotest.SAMPLE_DARSHAN_JOBID,
        cache_file=tokiotest.SAMPLE_SLURM_CACHE_FILE)

    print start, end
    assert start
    assert end
    assert start <= end

def test_get_job_startend_slurm():
    """tools.jobinfo.get_job_startend, NerscJobsDb
    """
    tokio.config.CONFIG["jobinfo_jobid_providers"] = ["nersc_jobsdb"]
    start, end = tokio.tools.jobinfo.get_job_startend(
        jobid=tokiotest.SAMPLE_DARSHAN_JOBID)

    print start, end
    assert start
    assert end
    assert start <= end


if __name__ == "__main__":
    test_get_job_startend()
