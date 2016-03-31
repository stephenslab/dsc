#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import os, sys, atexit, shutil
from .utils import SQLiteMan
from .dsc_file import DSCData
from .dsc_steps import DSCJobs, DSC2SoS
from .dsc_database import MetaDB
from pysos.utils import env, print_traceback
from pysos.sos_script import SoS_Script

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
        if args.__rerun__:
            env.sig_mode = 'ignore'
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
    env.logger.info("Constructing DSC from ``{}`` ...".format(args.dsc_file))
    if not os.path.exists('.sos/.dsc'):
        os.makedirs('.sos/.dsc')
    dsc_data = DSCData(args.dsc_file)
    db_name = dsc_data['DSC']['output'][0]
    if os.path.exists('.sos/.dsc/.{}.tmp'.format(db_name)):
        os.remove('.sos/.dsc/.{}.tmp'.format(db_name))
    with open('.sos/.dsc/{}.data'.format(db_name), 'w') as f:
        f.write(str(dsc_data))
    dsc_jobs = DSCJobs(dsc_data)
    with open('.sos/.dsc/{}.jobs'.format(db_name), 'w') as f:
        f.write(str(dsc_jobs))
    sos_jobs = DSC2SoS(dsc_jobs, echo = True if args.debug else False)
    with open('.sos/.dsc/{}.sos'.format(db_name), 'w') as f:
        f.write(str(sos_jobs))
    # Dryrun for sanity checks
    args.workflow = 'DSC'
    args.verbosity = 0
    for script in sos_jobs.data:
        args.script = script
        sos_dryrun(args, argv)
    os.rename('.sos/.dsc/.{}.tmp'.format(db_name), '.sos/.dsc/{}.yaml'.format(db_name))
    env.verbosity = args.verbosity = verbosity
    if args.__dryrun__:
        # FIXME export scripts
        return
    # Wetrun
    env.logger.info("Running DSC jobs ...")
    args.verbosity = verbosity - 1 if verbosity > 0 else verbosity
    for script in sos_jobs.data:
        args.script = script
        sos_run(args, argv)
    env.verbosity = args.verbosity = verbosity
    # Extracting information as much as possible
    # For RDS files if the values are trivial (single numbers) I'll just write them here
    env.logger.info("Building summary database ``{0}.csv.gz & {0}.db`` ...".format(db_name))
    MetaDB(db_name).build()
    env.logger.info("DSC complete!")

def query(args, argv):
    s = SQLiteMan(args.dsc_db)
    if args.items is None:
        # show fields
        print("\033[1mColumns in table 'DSC':\033[0m")
        print ('\n'.join(['[{}] {}'.format(x[1], x[0]) for x in s.getFields('DSC')]))
    else:
        select_query = 'SELECT {} FROM DSC '.format(', '.join(args.items))
        where_query = '' if args.filter is None else "WHERE {}".format(' '.join(args.filter))
        s.execute(select_query + where_query)
