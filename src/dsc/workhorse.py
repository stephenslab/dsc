#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

from .dsc_file import DSCData
from .dsc_steps import DSCJobs, DSC2SoS, MetaDB
from pysos.utils import env, print_traceback
from pysos.sos_script import SoS_Script
import os, sys, atexit, shutil

def sos_run(args, workflow_args):
    env.verbosity = args.verbosity
    env.max_jobs = args.__max_jobs__
    # kill all remainging processes when the master process is killed.
    atexit.register(env.cleanup)
    try:
        script = SoS_Script(content=args.script)
        workflow = script.workflow(args.workflow)
        if args.__dryrun__:
            env.run_mode = 'dryrun'
        workflow.run(workflow_args, cmd_name=args.dsc_file)
    except Exception as e:
        if args.verbosity and args.verbosity > 2:
            print_traceback()
        env.logger.error(e)
        sys.exit(1)

def sos_dryrun(args, workflow_args):
    run_mode = env.run_mode
    max_jobs = args.__max_jobs__
    dryrun = args.__dryrun__
    args.__max_jobs__ = 1
    args.__dryrun__ = True
    sos_run(args, workflow_args)
    args.__max_jobs__ = max_jobs
    args.__dryrun__ = dryrun
    env.run_mode = run_mode

def execute(args, argv):
    verbosity = args.verbosity
    env.verbosity = args.verbosity
    # Archive scripts
    env.logger.info("Parsing DSC configuration ``{}`` ...".format(args.dsc_file))
    if not os.path.exists('.sos/.dsc'):
        os.makedirs('.sos/.dsc')
    dsc_data = DSCData(args.dsc_file)
    db_name = dsc_data['DSC']['output'][0]
    if os.path.exists('.sos/.dsc/{}.yaml'.format(db_name)):
        os.remove('.sos/.dsc/{}.yaml'.format(db_name))
    with open('.sos/.dsc/{}.data'.format(db_name), 'w') as f:
        f.write(str(dsc_data))
    dsc_jobs = DSCJobs(dsc_data)
    with open('.sos/.dsc/{}.jobs'.format(db_name), 'w') as f:
        f.write(str(dsc_jobs))
    sos_jobs = DSC2SoS(dsc_jobs, echo = True if args.debug else False)
    with open('.sos/.dsc/{}.sos'.format(db_name), 'w') as f:
        f.write(str(sos_jobs))
    # Dryrun for sanity checks
    env.logger.info("Building meta-info database ``{}.db`` ...".format(db_name))
    args.workflow = 'DSC'
    args.verbosity = 1
    for script in sos_jobs.data:
        args.script = script
        sos_dryrun(args, argv)
    env.verbosity = args.verbosity = verbosity
    if args.__dryrun__:
        return
    # Build meta-db
    MetaDB('.sos/.dsc/{}.yaml'.format(db_name), db_name).build()
    # Wetrun
    env.logger.info("Running DSC jobs ...")
    args.verbosity = verbosity - 1 if verbosity > 0 else verbosity
    for script in sos_jobs.data:
        args.script = script
        sos_run(args, argv)
    env.verbosity = args.verbosity = verbosity
    env.logger.info("DSC jobs complete!")

def submit(args):
    pass

def show(args):
    pass
