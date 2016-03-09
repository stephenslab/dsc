#!/usr/bin/env python3
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

from dsc_file import DSCData

class Job:
    '''
    Convert dsc input information in a dictionary and properly prepare a job.

    This includes:
      * Figuring out and loading required data
      * Prepare environments to run R, Python or command line
      * Putting together arguments / parameters for the job
      * Run a job, if asked, and properly store results

    There are some identification variables:
      * SA: ID for scenario
      * R: ID for replicate
      * M: ID for method
      * SO: ID for scoring measure
    These identification variables should be determined by Updater
    '''
    def __init__(self):
        pass

    def __str__(self):
        return None

    def execute(self):
        pass

class Jobs:
    '''
    Takes a list of jobs (from input list or a job file) and figure out the proper
    sequence to run these jobs.

    Suppose given these jobs
      [SA1R1, SA1R2, SA1R1M1, SA1R2M1, SA1R1M2, SA1R2M2,
       SA1R1M1SO, SA1R2M1SO, SA1R1M2SO, SA1R2M2SO]
    they should be organized as
      {1: [SA1R1, SA1R2],
       2: [SA1R1M1, SA1R2M1, SA1R1M2, SA1R2M2],
       3: [SA1R1M1SO, SA1R2M1SO, SA1R1M2SO, SA1R2M2SO]
      }

    Or, multiple independent units:
      {1: [SA1R1],
       2: [SA1R1M1, SA1R1M2],
       3: [SA1R1M1SO, SA1R1M2SO]
      },
      {1: [SA1R2],
       2: [SA1R2M1, SA1R2M2],
       3: [SA1R2M1SO, SA1R2M2SO]
      }

    where step 1 ~ 3 each depends on output from previous step(s),
    and within each step commands should run in parallel. Warnings
    or error messages should be given if there are dependency problems
    '''
    def __init__(self):
        pass

    def __str__(self):
        return None

    def save(self, fname, split = None):
        '''
        Save job objects to file.

        If split > 1, it will create multiple independent job units and
        save each of them to a file.
        '''
        pass

    def execute(self):
        pass

class JobFilter:
    '''
    Checks jobs against archive and determine what to execute
    '''
    def __init__(self):
        pass

    def __str__(self):
        return None

class DSC:
    '''
    The DSC object

    Input is DSC settings; DSC object contains all information necessary to
    run DSC
    '''
    def __init__(self, fname, nodes, threads, verbosity):
        self.job_inits = DSCData(fname)
        self.job_updates = JobFilter(self.job_inits)
        self.raw_jobs = [Job(item) for item in self.job_updates]
        self.jobs = Jobs(self.raw_jobs)

    def __call__(self):
        pass
