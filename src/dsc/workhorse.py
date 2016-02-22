#!/usr/bin/env python3
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import sys, os

def execute(args):
    try:
        env.verbosity = args.verbosity
        dsc = DSC(args.dsc_file, nodes = args.nodes, threads = args.threads)
        if args.nodes != 1:
            # Jobs are not executed. Rather a number of DSC job files in YAML format
            env.logger.info("Jobs saved for submission")
        else:
            # execute one or all of ['scenario', 'method', 'score']
            dsc.execute(args.jobs)
    except Exception as e:
        env.unlock_all()
        env.logger.error(e)
        sys.exit(1)

def submit(args):
    try:
        env.verbosity = args.verbosity
        # load DSC job file and run
        dsc = Jobs(args.file)
        dsc.execute()
    except Exception as e:
        env.unlock_all()
        env.logger.error(e)
        sys.exit(1)
