#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

from dsc_file import DSCData
from dsc_steps import DSCJobs, DSC2SoS
from pysos import env

def execute(args):
    dsc_data = DSCData(args.dsc_file)
    # print(dsc_data)
    dsc_jobs = DSCJobs(dsc_data)
    print(dsc_jobs)
    sos_jobs = DSC2SoS(dsc_jobs)
    print(sos_jobs)

def submit(args):
    pass

def show(args):
    pass
