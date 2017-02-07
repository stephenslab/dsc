#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import os, sys, atexit, re, glob
from collections import OrderedDict
from sos.utils import env, get_traceback
from sos.__main__ import cmd_remove
from .dsc_file import DSCData
from .dsc_steps import DSCJobs, DSC2SoS
from .dsc_database import ResultDB, ResultAnnotator, ResultExtractor
from .utils import get_slice, load_rds, flatten_list, yaml2html, dsc2html, dotdict
from sos.sos_script import SoS_Script
from sos.sos_executor import Base_Executor, MP_Executor
from sos.rq.sos_executor import RQ_Executor

def dsc_run(args, content, workflow = 'DSC', verbosity = 1, jobs = None, queue = None, is_prepare = False):
    env.verbosity = verbosity
    env.max_jobs = args.__max_jobs__ if jobs is None else jobs
    # kill all remaining processes when the master process is killed.
    atexit.register(env.cleanup)
    env.sig_mode = 'default'
    if args.__construct__ and is_prepare == False:
        env.sig_mode = 'build'
    if args.__rerun__:
        env.sig_mode = 'force'
    try:
        script = SoS_Script(content=content, transcript = None)
        workflow = script.workflow(workflow)
        if env.max_jobs == 1 and env.verbosity == 1:
            # Do not use progressbar for single CPU job
            # For better debugging
            env.verbosity = 2
        if queue is None and env.max_jobs == 1:
            # single process executor
            executor_class = Base_Executor
        elif queue is None:
            executor_class = MP_Executor
        else:
            executor_class = RQ_Executor
        executor = executor_class(workflow, args = None,
                                  config = {'config_file': args.__config__,
                                            'output_dag': args.__dag__})
        executor.run()
    except Exception as e:
        if verbosity and verbosity > 2:
            sys.stderr.write(get_traceback())
        env.logger.error(e)
        sys.exit(1)
    env.verbosity = args.verbosity


def remove(dsc_jobs, dsc_data, steps, db, force, debug):
    filename = os.path.basename(db) + '.rds'
    if not os.path.isfile(filename):
        raise ValueError('Cannot remove anything because DSC output meta data ``{}`` is not found!'.format(filename))
    if len(steps) == 0:
        # remove everything
        to_remove = glob.glob('{}/*'.format(os.path.basename(db)))
        print(to_remove)
    else:
        to_remove = []
        for item in steps:
            block, step_idx = get_slice(item, mismatch_quit = False)
            removed = False
            for sequence in dsc_jobs.data:
                for steps in sequence:
                    for step in steps:
                        proceed = False
                        if step['name'] == block:
                            if step_idx is None:
                                proceed = True
                            else:
                                for idx in step_idx:
                                    if idx + 1 == step['exe_id']:
                                        proceed = True
                                        break
                        if proceed:
                            tmp = re.sub(r'[^\w' + '_.' + ']', '_', step['exe'])
                            if tmp not in to_remove:
                                to_remove.append(tmp)
                            removed = True
            if removed is False:
                env.logger.warning('Cannot find step ``{}`` in DSC run sequence specified; '\
                                   'thus not processed.'.format(item))
        #
        data = load_rds(filename)
        to_remove = flatten_list([[glob.glob(os.path.join(dsc_data['DSC']['output'][0], '{}.*'.format(x)))
                                   for x in data[item]['return']]
                                  for item in to_remove if item in data])
    if debug:
        env.logger.info(to_remove)
    else:
        cmd_remove(dotdict({"tracked": False, "untracked": False if not force else true,
                            "targets": to_remove, "__dryrun__": False,
                            "__confirm__": True, "signature": True, "verbosity": env.verbosity}), [])


def execute(args):
    def setup():
        args.__config__ = None
        dsc_data = DSCData(args.dsc_file, sequence = args.sequence, output = args.output)
        db_name = os.path.basename(dsc_data['DSC']['output'][0])
        db_dir = os.path.dirname(dsc_data['DSC']['output'][0])
        args.__dag__ = '.sos/.dsc/{}.dag'.format(db_name)
        if db_dir:
            os.makedirs(db_dir, exist_ok = True)
        # Force rerun if trace of existing project cannot be found and not in recover mode
        if not os.path.isdir('.sos/.dsc') and not args.__construct__:
            args.__rerun__ = True
        os.makedirs('.sos/.dsc', exist_ok = True)
        dsc_jobs = DSCJobs(dsc_data)
        if args.verbosity > 3:
            yaml2html(str(dsc_data), '.sos/.dsc/{}.data'.format(db_name), title = 'DSC data')
            yaml2html(str(dsc_jobs), '.sos/.dsc/{}.jobs'.format(db_name), title = 'DSC jobs')
        run_jobs = DSC2SoS(dsc_jobs, args.dsc_file, args.__rerun__)
        if args.verbosity > 3:
            yaml2html(str(run_jobs), '.sos/.dsc/{}.exec'.format(db_name), title = 'DSC runs')
        # master block for output
        try:
            master = dsc_data['DSC']['master']
        except:
            master = None
        section_content = OrderedDict([(k, dsc_jobs.master_data[k]) for k in dsc_jobs.ordering])
        return run_jobs, dsc_jobs, dsc_data, section_content, dsc_data['DSC']['output'][0], master
    #
    if args.host is not None:
        queue = 'rq'
        args.verbosity = 1 if args.verbosity == 2 else args.verbosity
    else:
        queue = None
    env.verbosity = args.verbosity
    if args.sequence:
        env.logger.info("Load command line DSC sequence: ``{}``".\
                        format(' '.join(', '.join(args.sequence).split())))
    run_jobs, dsc_jobs, dsc_data, section_content, db, master = setup()
    if args.to_remove is not None:
        remove(dsc_jobs, dsc_data, args.to_remove, db, args.__rerun__, args.debug)
        return
    # Archive scripts
    dsc_script = open(args.dsc_file).read()
    try:
        dsc_ann = open('.'.join(args.dsc_file.rsplit('.', 1)[:-1]) + '.ann').read()
    except:
        dsc_ann = None
    dsc2html(dsc_script, dsc_ann, os.path.splitext(args.dsc_file)[0], section_content = section_content)
    env.logger.info("DSC script exported to ``{}``".format(os.path.splitext(args.dsc_file)[0] + '.html'))
    env.logger.info("Constructing DSC from ``{}`` ...".format(args.dsc_file))
    # Setup run for config files
    dsc_run(args, run_jobs.conf_str, workflow = 'INIT', verbosity = 0, jobs = 1, is_prepare = True)
    if args.__dryrun__:
        return
    # Wetrun
    env.logfile = os.path.splitext(args.dsc_file)[0] + '.log'
    if os.path.isfile(env.logfile): os.remove(env.logfile)
    args.__config__ = '.sos/.dsc/{}.conf'.format(os.path.basename(db))
    env.logger.debug("Running command ``{}``".format(' '.join(sys.argv)))
    dsc_run(args, run_jobs.job_str,
            verbosity = (args.verbosity - 1 if args.verbosity > 0 else args.verbosity),
            queue = queue)
    # Extracting information as much as possible
    # For RDS files if the values are trivial (single numbers) I'll just write them here
    env.logger.info("Building output database ``{0}.rds`` ...".format(db))
    ResultDB(db, master).Build(script = dsc_script)
    env.logger.info("DSC complete!")


def annotate(args):
    env.verbosity = args.verbosity
    if len(args.annotation) > 1:
        dsc_file = args.annotation[1]
    else:
        dsc_file = args.annotation[0].rsplit('.', 1)[0] + '.dsc'
    if not os.path.isfile(dsc_file):
        raise ValueError('DSC script ``{}`` does not exist. Please specify it via ``-a annotation_file script_file``'.format(dsc_file))
    dsc_data = DSCData(dsc_file, check_rlibs = False, output = args.output)
    ann = ResultAnnotator(args.annotation[0], args.master, dsc_data)
    ann.ConvertAnnToQuery()
    ann.ApplyAnotation()
    ann.SaveShinyMeta()
    env.logger.info('\n'+ ann.ShowQueries(args.verbosity))
    if len(ann.msg):
        env.logger.warning('\n' + '\n'.join(ann.msg))

def extract(args):
    env.max_jobs = args.__max_jobs__
    env.verbosity = args.verbosity if not args.verbosity == 2 else 1
    atexit.register(env.cleanup)
    env.sig_mode = 'default'
    if args.__rerun__:
        env.sig_mode = 'force'
    #
    ext = ResultExtractor(args.tags, args.master, args.output, args.extract)
    try:
        script = SoS_Script(content = ext.script, transcript = None)
        workflow = script.workflow("Extracting")
        if env.max_jobs == 1:
            # single process executor
            executor_class = Base_Executor
        else:
            executor_class = MP_Executor
        executor = executor_class(workflow)
        executor.run()
    except Exception as e:
        if env.verbosity and env.verbosity > 2:
            sys.stderr.write(get_traceback())
        env.logger.error(e)
        sys.exit(1)
    env.verbosity = args.verbosity
    env.logger.info('Data extracted to ``{}`` for {} for DSC result ``{}``.'.\
                    format(ext.output,
                           'annotations ``{}``'.format(', '.join(args.tags)) if args.tags else "all annotations",
                           ext.master))

def run(args):
    if args.dsc_file is not None:
        execute(args)
    if args.annotation is not None:
        annotate(args)
    if args.extract is not None:
        extract(args)
