#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import os, sys, atexit, re, glob, argparse
from collections import OrderedDict
from sos.utils import env, get_traceback
from sos.__main__ import cmd_remove
from dsc.dsc_parser import DSC_Script
from dsc.dsc_analyzer import DSC_Analyzer
from dsc.dsc_translator import DSC_Translator
from .dsc_database import ResultDB, ResultAnnotator, ResultExtractor
from .utils import get_slice, load_rds, flatten_list, workflow2html, dsc2html, dotdict, Timer
from sos.sos_script import SoS_Script
from sos.sos_executor import Base_Executor, MP_Executor
from sos.rq.sos_executor import RQ_Executor
from sos.converter import script_to_html
from . import VERSION

def sos2html(sos_files, html_files):
    verbosity = env.verbosity
    env.verbosity = 0
    for x, y in zip(sos_files, html_files):
        script_to_html(x, y)
    env.verbosity = verbosity

def dsc_run(args, content, workflows = ['DSC'], verbosity = 1, queue = None, is_prepare = False):
    env.verbosity = verbosity
    env.max_jobs = args.__max_jobs__
    # kill all remaining processes when the master process is killed.
    atexit.register(env.cleanup)
    env.sig_mode = 'default'
    if args.__construct__ and is_prepare == False:
        env.sig_mode = 'build'
    if args.__rerun__:
        env.sig_mode = 'force'
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
    script = SoS_Script(content=content, transcript = None)
    for w in workflows:
        workflow = script.workflow(w)
        try:
            executor = executor_class(workflow, args = None,
                                      config = {'output_dag': args.__dag__})
            executor.run()
        except Exception as e:
            if verbosity and verbosity > 2:
                sys.stderr.write(get_traceback())
            env.logger.error(e)
            sys.exit(1)
    env.verbosity = args.verbosity

def remove(workflows, steps, db, force, debug):
    filename = os.path.basename(db) + '.rds'
    if not os.path.isfile(filename):
        raise ValueError('Cannot remove anything because DSC output meta data ``{}`` is not found!'.format(filename))
    if len(steps) == 0:
        # remove everything
        to_remove = glob.glob('{}/*'.format(os.path.basename(db)))
    else:
        to_remove = []
        for item in steps:
            block, step_idx = get_slice(item, mismatch_quit = False)
            removed = False
            for workflow in workflows:
                for block in workflow:
                    for step in workflow[block].steps:
                        proceed = False
                        if step.group == block:
                            if step_idx is None:
                                proceed = True
                            else:
                                for idx in step_idx:
                                    if idx + 1 == step.exe_id:
                                        proceed = True
                                        break
                        if proceed:
                            tmp = re.sub(r'[^\w' + '_.' + ']', '_', step.name)
                            if tmp not in to_remove:
                                to_remove.append(tmp)
                            removed = True
            if removed is False:
                env.logger.warning('Cannot find step ``{}`` in DSC run sequence specified; '\
                                   'thus not processed.'.format(item))
        #
        data = load_rds(filename)
        to_remove = flatten_list([[glob.glob(os.path.join(db, '{}.*'.format(x)))
                                   for x in data[item]['return']]
                                  for item in to_remove if item in data])
    if debug:
        env.logger.info(to_remove)
    else:
        cmd_remove(dotdict({"tracked": False, "untracked": False if not force else True,
                            "targets": to_remove, "__dryrun__": False,
                            "__confirm__": True, "signature": False, "verbosity": env.verbosity}), [])


def execute(args):
    # 1. Configure
    if args.host is not None:
        queue = 'rq'
        args.verbosity = 1 if args.verbosity == 2 else args.verbosity
    else:
        queue = None
    env.verbosity = args.verbosity
    if args.sequence:
        env.logger.info("Load command line DSC sequence: ``{}``".\
                        format(' '.join(', '.join(args.sequence).split())))
    # Force rerun if trace of existing project cannot be found and not in recover mode
    if not os.path.isdir('.sos/.dsc') and not args.__construct__:
        args.__rerun__ = True
    os.makedirs('.sos/.dsc', exist_ok = True)
    # 2. Parse DSC script
    script = DSC_Script(args.dsc_file, sequence = args.sequence, output = args.output)
    db = script.runtime.output
    db_name = os.path.basename(db)
    db_dir = os.path.dirname(db)
    args.__dag__ = '.sos/.dsc/{}.dag'.format(db_name)
    if db_dir:
        os.makedirs(db_dir, exist_ok = True)
    workflow = DSC_Analyzer(script)
    if args.verbosity > 3:
        workflow2html('.sos/.dsc/{}.workflow.html'.format(db_name), workflow.workflows, script.dump().values())
    pipeline = DSC_Translator(workflow.workflows, script.runtime, args.__rerun__, args.__max_jobs__)
    if args.verbosity > 3:
        sos2html((pipeline.write_pipeline(1), pipeline.write_pipeline(2)),
                 ('.sos/.dsc/{}.prepare.html'.format(db_name),
                  '.sos/.dsc/{}.run.html'.format(db_name)))
    # 3. remove and return when applicable
    if args.to_remove is not None:
        remove(workflow.workflows, args.to_remove, db, args.__rerun__, args.debug)
        return
    # 4. Archive scripts
    exec_content = OrderedDict([(k, [step.exe for step in script.blocks[k].steps])
                                for k in script.runtime.sequence_ordering])
    dsc_script = open(args.dsc_file).read()
    try:
        dsc_ann = open('.'.join(args.dsc_file.rsplit('.', 1)[:-1]) + '.ann').read()
    except:
        dsc_ann = None
    dsc2html(dsc_script, dsc_ann, db, section_content = exec_content)
    env.logger.info("DSC script exported to ``{}``".format(os.path.splitext(args.dsc_file)[0] + '.html'))
    # 5. Prepare
    env.logger.info("Constructing DSC from ``{}`` ...".format(args.dsc_file))
    # Setup run for config files
    dsc_run(args, pipeline.conf_str, workflows = ['INIT', 'BUILD'], verbosity = 0, is_prepare = True)
    if args.__dryrun__:
        return
    # 6. Run
    env.logfile = os.path.splitext(args.dsc_file)[0] + '.log'
    if os.path.isfile(env.logfile): os.remove(env.logfile)
    env.logger.debug("Running command ``{}``".format(' '.join(sys.argv)))
    dsc_run(args, pipeline.job_str,
            verbosity = (args.verbosity - 1 if args.verbosity > 0 else args.verbosity),
            queue = queue)
    # 7. Construct meta database
    master = list(set([x[list(x.keys())[-1]].name for x in workflow.workflows]))
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
    dsc_data = DSCData(dsc_file, check_rlibs = False, check_pymodules = False, output = args.output)
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

def main():
    p = argparse.ArgumentParser(description = __doc__)
    p.add_argument('--version', action = 'version', version = '{}'.format(VERSION))
    p.add_argument('-v', '--verbosity', type = int, choices = list(range(5)), default = 2,
                   help='''Output error (0), warning (1), info (2), debug (3) and trace (4)
                   information. Default to 2.''')
    p.add_argument('--debug', action='store_true', help = argparse.SUPPRESS)
    p.add_argument('-j', type=int, metavar='N', default=2, dest='__max_jobs__',
                   help='''Number of maximum concurrent processes.''')
    p.add_argument('-o', metavar = "str", dest = 'output',
                   help = '''Output data prefix for -x / -e commands.''')
    p.add_argument('-f', action='store_true', dest='__rerun__',
                   help='''Force re-run -x / -e commands from scratch.''')
    p.add_argument('--target', dest = 'master', metavar = 'str',
                         help = '''The ultimate target of a DSC benchmark is the name of
                         the last block in a DSC sequence. This option is relevant to -a / -e
                         commands when there exists multiple DSC sequences with different targets.''')
    p_execute = p.add_argument_group("Execute DSC")
    p_execute.add_argument('-x', '--execute', dest = 'dsc_file', metavar = "DSC script",
                   help = 'Execute DSC.')
    p_execute.add_argument('--sequence', metavar = "str", nargs = '+',
                   help = '''DSC sequence to be executed. It will override the DSC::run
                   entry when specified. Multiple sequences are allowed. Each input should be
                   a quoted string defining a valid DSC sequence. Multiple such strings should be
                   separated by space.''')
    p_execute.add_argument('-d', action='store_true', dest='__dryrun__', help = argparse.SUPPRESS)
    p_execute.add_argument('--recover', action='store_true', dest='__construct__',
                   help = '''Recover DSC based on names (not contents) of existing files.''')
    p_execute.add_argument('--clean', dest = 'to_remove', metavar = "str", nargs = '*',
                   help = '''Instead of running DSC, output for one or multiple steps from previous DSC
                   runs are to be cleaned. Each step should be a valid DSC step in the format of
                   "block[index]", or "block" for all steps in the block.
                   Multiple steps should be separated by space. When "--clean" is used with "-f",
                   all specified files will be removed regardless of their step execution status.''')
    p_execute.add_argument('--host', metavar='str',
                   help='''URL of Redis server for distributed computation.''')
    p_ann = p.add_argument_group("Annotate DSC")
    p_ann.add_argument('-a', '--annotate', dest = 'annotation', metavar = 'DSC files', nargs = '+',
                       help = '''Annotate DSC. An annotation file name is required and DSC will
                       look for the script file having the same base name but with *.dsc extension.
                       Optionally one can input 2 file names with the first the annotation file name
                       and the second the DSC script name, eg, -a test.ann test.dsc''')
    p_ext = p.add_argument_group("Extract DSC results")
    p_ext.add_argument('-e', '--extract', metavar = 'block:variable', nargs = '+',
                       help = '''Variable(s) to extract.
                       Variable(s) should be specified by "block:variable".
                       Valid `variable` are variables found in `return` of the corresponding
                       DSC block.''')
    p_ext.add_argument('--tags', metavar = 'str', nargs = '+',
                       help = '''Tags to extract. The "&&" symbol can be used to specify intersect
                       of multiple tags. Default to extracting for all tags.''')
    p.set_defaults(func = run)
    args = p.parse_args()
    #
    try:
        with Timer(verbose = True if ('verbosity' in vars(args) and args.verbosity > 0) else False):
            args.func(args)
    except Exception as e:
        if 'verbosity' in args and args.verbosity > 2:
            sys.stderr.write(get_traceback())
        else:
            env.logger.error(e)
        sys.exit(1)

if __name__ == '__main__':
    main()
