#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import os, sys, atexit
from copy import deepcopy
from pysos.sos_script import SoS_Script
from pysos.sos_executor import Sequential_Executor
from pysos.utils import env, get_traceback
from .dsc_file import DSCData
from .dsc_steps import DSCJobs, DSC2SoS
from .dsc_database import ResultDB, ConfigDB

def sos_run(args, workflow_args):
    env.max_jobs = args.__max_jobs__
    env.verbosity = args.verbosity
    # kill all remainging processes when the master process is killed.
    atexit.register(env.cleanup)
    if args.__dryrun__:
        env.run_mode = 'dryrun'
    if args.__rerun__:
        env.sig_mode = 'ignore'
    try:
        script = SoS_Script(content=args.script)
        executor = Sequential_Executor(script.workflow(args.workflow))
        executor.run(workflow_args, cmd_name=args.dsc_file, config_file = args.__config__)
    except Exception as e:
        if args.verbosity and args.verbosity > 2:
            sys.stderr.write(get_traceback())
        env.logger.error(e)
        sys.exit(1)

def sos_drillrun(args, workflow_args):
    verbosity = args.verbosity
    args.verbosity = 0
    run_mode = env.run_mode
    max_jobs = args.__max_jobs__
    dryrun = args.__dryrun__
    args.__max_jobs__ = 1
    args.__dryrun__ = False
    sos_run(args, workflow_args)
    args.__max_jobs__ = max_jobs
    args.__dryrun__ = dryrun
    env.run_mode = run_mode
    env.verbosity = args.verbosity = verbosity

def execute(args, argv):
    def setup():
        if os.path.dirname(dsc_data['DSC']['output'][0]):
            os.makedirs(os.path.dirname(dsc_data['DSC']['output'][0]), exist_ok=True)
        os.makedirs('.sos/.dsc/md5', exist_ok = True)
    def log():
        with open('.sos/.dsc/{}.data'.format(db_name), 'w') as f:
            f.write(str(dsc_data))
        with open('.sos/.dsc/{}.jobs'.format(db_name), 'w') as f:
            f.write(str(dsc_jobs))
        with open('.sos/.dsc/{}.exec'.format(db_name), 'w') as f:
            f.write(str(run_jobs))
    #
    verbosity = args.verbosity
    env.verbosity = args.verbosity
    args.workflow = 'DSC'
    args.__config__ = None
    # Archive scripts
    env.logger.info("Constructing DSC from ``{}`` ...".format(args.dsc_file))
    dsc_data = DSCData(args.dsc_file, args.sequence)
    db_name = os.path.basename(dsc_data['DSC']['output'][0])
    setup()
    dsc_jobs = DSCJobs(dsc_data)
    run_jobs = DSC2SoS(deepcopy(dsc_jobs))
    if verbosity > 3:
        log()
    # Dryrun for sanity checks
    for script in run_jobs.confstr:
        args.script = script
        sos_drillrun(args, argv)
    ConfigDB(dsc_data['DSC']['output'][0]).Build()
    if args.__dryrun__:
        # FIXME export scripts to db_name folder
        return
    # Wetrun
    env.logger.info("Running DSC jobs ...")
    args.verbosity = verbosity - 1 if verbosity > 0 else verbosity
    args.__config__ = '.sos/.dsc/{}.conf'.format(os.path.basename(dsc_data['DSC']['output'][0]))
    for script in run_jobs.jobstr:
        args.script = script
        sos_run(args, argv)
    env.verbosity = args.verbosity = verbosity
    # Extracting information as much as possible
    # For RDS files if the values are trivial (single numbers) I'll just write them here
    env.logger.info("Building output database ``{0}.rds`` ...".\
                    format(dsc_data['DSC']['output'][0]))
    ResultDB(dsc_data['DSC']['output'][0]).Build()
    env.logger.info("DSC complete!")
