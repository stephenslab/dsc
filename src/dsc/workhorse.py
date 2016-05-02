#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import os, sys, atexit, re, yaml
from copy import deepcopy
from pysos.sos_script import SoS_Script
from pysos.sos_executor import Sequential_Executor
from pysos.utils import env, get_traceback
from .dsc_file import DSCData
from .dsc_steps import DSCJobs, DSC2SoS
from .dsc_database import ResultDB, ConfigDB
from .utils import get_slice, load_rds, flatten_list, yaml2html

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
    sig_mode = env.sig_mode
    max_jobs = args.__max_jobs__
    dryrun = args.__dryrun__
    rerun = args.__rerun__
    args.__max_jobs__ = 1
    args.__dryrun__ = False
    args.__rerun__ = True
    sos_run(args, workflow_args)
    args.__max_jobs__ = max_jobs
    args.__dryrun__ = dryrun
    args.__rerun__ = rerun
    env.run_mode = run_mode
    env.sig_mode = sig_mode
    env.verbosity = args.verbosity = verbosity

def execute(args, argv):
    def setup():
        if os.path.dirname(dsc_data['DSC']['output'][0]):
            os.makedirs(os.path.dirname(dsc_data['DSC']['output'][0]), exist_ok=True)
        os.makedirs('.sos/.dsc/md5', exist_ok = True)
    def log():
        yaml2html(str(dsc_data), '.sos/.dsc/{}.data'.format(db_name), title = 'DSC data')
        yaml2html(str(dsc_jobs), '.sos/.dsc/{}.jobs'.format(db_name), title = 'DSC jobs')
        yaml2html(str(run_jobs), '.sos/.dsc/{}.exec'.format(db_name), title = 'DSC runs')
    #
    verbosity = args.verbosity
    env.verbosity = args.verbosity
    args.workflow = 'DSC'
    args.__config__ = None
    # Archive scripts
    dsc_script = open(args.dsc_file).read()
    yaml2html(dsc_script, os.path.splitext(args.dsc_file)[0] + '.html', title = args.dsc_file)
    if args.sequence:
        env.logger.info("Load command line DSC sequence: ``{}``".format(', '.join(args.sequence)))
    env.logger.info("Constructing DSC from ``{}`` ...".format(args.dsc_file))
    dsc_data = DSCData(args.dsc_file, args.sequence)
    db_name = os.path.basename(dsc_data['DSC']['output'][0])
    # env.logfile = db_name + '.log'
    setup()
    dsc_jobs = DSCJobs(dsc_data)
    run_jobs = DSC2SoS(deepcopy(dsc_jobs))
    if verbosity > 3:
        log()
    # Dryrun for sanity checks
    for script in run_jobs.confstr:
        args.script = script
        sos_drillrun(args, argv)
    ConfigDB(dsc_data['DSC']['output'][0], vanilla = args.__rerun__).Build()
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
    ResultDB(dsc_data['DSC']['output'][0]).Build(script = dsc_script)
    env.logger.info("DSC complete!")

def remove(args, argv):
    env.verbosity = args.verbosity
    dsc_data = DSCData(args.dsc_file)
    dsc_jobs = DSCJobs(dsc_data)
    filename = os.path.basename(dsc_data['DSC']['output'][0]) + '.rds'
    if not os.path.isfile(filename):
        raise ValueError('Cannot remove output because DSC database ``{}`` is not found!'.format(filename))
    to_remove = []
    for item in args.step:
        block, step_idx = get_slice(item, mismatch_quit = False)
        removed = False
        for sequence in dsc_jobs.data:
            for steps in sequence:
                for step in steps:
                    if step['name'] == block and (step_idx is None or step_idx == step['exe_index']):
                        tmp = re.sub(r'[^\w' + '_.' + ']', '_', step['exe'])
                        if tmp not in to_remove:
                            to_remove.append(tmp)
                        removed = True
        if removed is False:
            env.logger.warning('Cannot find step ``{}`` in DSC run sequence defined in ``{}``; '\
                               'thus not processed.'.format(item, args.dsc_file))
    #
    data = load_rds(filename)
    files_to_remove = ' '.join([' '.join([os.path.join(dsc_data['DSC']['output'][0], '{}.*'.format(x))
                                    for x in data[item]['return']]) for item in to_remove])
    map_to_remove = flatten_list([data[item]['return'].tolist() for item in to_remove])
    # delete files from disk
    os.system('rm -f {}'.format(files_to_remove))
    env.logger.debug('Removing files ``{}``'.format(repr(map_to_remove)))
    # delete files from map file
    filename = os.path.basename(dsc_data['DSC']['output'][0])
    if os.path.isfile('.sos/.dsc/{}.map'.format(filename)):
        maps = yaml.load(open('.sos/.dsc/{}.map'.format(filename)))
        for k, v in list(maps.items()):
            if k == 'NEXT_ID':
                continue
            if os.path.splitext(v)[0] in map_to_remove:
                del maps[k]
        with open('.sos/.dsc/{}.map'.format(filename), 'w') as f:
            f.write(yaml.dump(maps, default_flow_style=True))
    env.logger.info('Force removed ``{}`` files from ``{}``.'.\
                    format(len(map_to_remove),
                           os.path.abspath(os.path.expanduser(dsc_data['DSC']['output'][0]))))
