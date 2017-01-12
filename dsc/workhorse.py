#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import os, sys, atexit, re
from collections import OrderedDict
from sos.utils import env, get_traceback
from sos.__main__ import cmd_remove
from .dsc_file import DSCData
from .dsc_steps import DSCJobs, DSC2SoS
from .dsc_database import ResultDB, ResultAnnotator, ResultExtractor
from .utils import get_slice, load_rds, flatten_list, yaml2html, dsc2html, dotdict

def dsc_run(args, workflow_args, content, verbosity = 1, jobs = None, queue = None, is_prepare = False):
    from sos.sos_script import SoS_Script
    from sos.sos_executor import Base_Executor, MP_Executor
    from sos.rq.sos_executor import RQ_Executor
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
        workflow = script.workflow(args.workflow)
        if env.max_jobs == 1 and env.verbosity == 1:
            # Do not use progressbar for single CPU job
            # For better debugging
            env.verbosity = 2
        if queue is None or env.max_jobs == 1:
            # single process executor
            executor_class = Base_Executor
        elif queue is None:
            executor_class = MP_Executor
        else:
            executor_class = RQ_Executor
        executor = executor_class(workflow, args = workflow_args,
                                  config = {'config_file': args.__config__,
                                            'output_dag': args.__dag__})
        executor.run()
    except Exception as e:
        if verbosity and verbosity > 2:
            sys.stderr.write(get_traceback())
        env.logger.error(e)
        sys.exit(1)
    env.verbosity = args.verbosity


def remove(dsc_jobs, dsc_data, steps, db):
    filename = os.path.basename(db) + '.rds'
    if not os.path.isfile(filename):
        raise ValueError('Cannot remove output because DSC database ``{}`` is not found!'.format(filename))
    to_remove = []
    for item in steps:
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
            env.logger.warning('Cannot find step ``{}`` in DSC run sequence specified; '\
                               'thus not processed.'.format(item))
    #
    data = load_rds(filename)
    to_remove = flatten_list([[os.path.join(dsc_data['DSC']['output'][0], '{}.*'.format(x))
                               for x in data[item]['return']]
                              for item in to_remove if item in data])
    cmd_remove(dotdict({"__tracked__": to_remove, "targets": to_remove}), [])


def execute(args, argv):
    def setup():
        args.workflow = 'DSC'
        args.__config__ = None
        dsc_data = DSCData(args.dsc_file, sequence = args.sequence, output = args.output)
        db_name = os.path.basename(dsc_data['DSC']['output'][0])
        db_dir = os.path.dirname(dsc_data['DSC']['output'][0])
        args.__dag__ = '.sos/.dsc/{}.dag'.format(db_name)
        if db_dir:
            os.makedirs(db_dir, exist_ok = True)
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
    if args.to_remove:
        remove(dsc_jobs, dsc_data, args.to_remove, db)
        return
    # Archive scripts
    dsc_script = open(args.dsc_file).read()
    dsc2html(dsc_script, os.path.splitext(args.dsc_file)[0] + '.html',
             title = args.dsc_file, section_content = section_content)
    env.logger.info("DSC script exported to ``{}``".format(os.path.splitext(args.dsc_file)[0] + '.html'))
    env.logger.info("Constructing DSC from ``{}`` ...".format(args.dsc_file))
    # Setup run for config files
    dsc_run(args, argv, run_jobs.conf_str, verbosity = 0, jobs = 1, is_prepare = True)
    if args.__dryrun__:
        return
    # Wetrun
    env.logfile = os.path.splitext(args.dsc_file)[0] + '.log'
    if os.path.isfile(env.logfile): os.remove(env.logfile)
    args.__config__ = '.sos/.dsc/{}.conf'.format(os.path.basename(db))
    env.logger.debug("Running command ``{}``".format(' '.join(sys.argv)))
    dsc_run(args, argv, run_jobs.job_str,
            verbosity = (args.verbosity - 1 if args.verbosity > 0 else args.verbosity),
            queue = queue)
    # Extracting information as much as possible
    # For RDS files if the values are trivial (single numbers) I'll just write them here
    env.logger.info("Building output database ``{0}.rds`` ...".format(db))
    ResultDB(db, master).Build(script = dsc_script)
    env.logger.info("DSC complete!")


def annotate(args, argv):
    dsc_data = DSCData(args.dsc_file, check_rlibs = False, output = args.output)
    ann = ResultAnnotator(args.annotation, args.master, dsc_data)
    ann.ConvertAnnToQuery()
    ann.ApplyAnotation()
    env.logger.info(ann.ShowQueries())
    if len(ann.msg):
        env.logger.warning('\n' + '\n'.join(ann.msg))

def extract(args, argv):
    dsc_data = DSCData(args.dsc_file, check_rlibs = False, output = args.output)
    ext = ResultExtractor(args.tags, args.master, dsc_data['DSC']['output'][0], args.dest, args.__rerun__)
    ext.Extract(args.extract)
    env.logger.info('``{}`` data saved to ``{}`` for {} from DSC block ``{}``.'.\
                    format(args.extract, ext.output,
                           'annotations {}'.format(', '.join(args.tags)) if args.tags else "all annotations",
                           ext.master[7:]))

def run(args, argv):
    if args.annotation is not None:
        annotate(args, argv)
    elif args.extract is not None:
        extract(args, argv)
    else:
        execute(args, argv)
