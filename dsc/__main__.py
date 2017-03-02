#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import os, sys, atexit, re, glob, pickle
from collections import OrderedDict
import pkg_resources
from sos.utils import env, get_traceback
from sos.__main__ import cmd_remove
from .dsc_parser import DSC_Script
from .dsc_analyzer import DSC_Analyzer
from .dsc_translator import DSC_Translator
from .dsc_database import ResultDB, ResultAnnotator, ResultExtractor
from .utils import get_slice, uniq_list, flatten_list, workflow2html, dsc2html, transcript2html, dotdict, Timer
from sos.sos_script import SoS_Script
from sos.converter import script_to_html
from sos.sos_executor import Base_Executor
from . import VERSION

from argparse import ArgumentParser, SUPPRESS

class ArgumentParserError(Exception): pass

class MyArgParser(ArgumentParser):
    def error(self, message):
        raise ArgumentParserError(message)

def sos2html(sos_files, html_files):
    verbosity = env.verbosity
    env.verbosity = 0
    for x, y in zip(sos_files, html_files):
        script_to_html(x, y)
    env.verbosity = verbosity

def dsc_run(args, content, workflows = ['DSC'], dag = None, verbosity = 1, queue = None, is_prepare = False):
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
    if queue:
        # import all executors
        executor_class = None
        for entrypoint in pkg_resources.iter_entry_points(group='sos_executors'):
            # Grab the function that is the actual plugin.
            name = entrypoint.name
            if name == queue:
                try:
                    executor_class = entrypoint.load()
                except Exception as e:
                    print('Failed to load queue executor {}: {}'.format(entrypoint.name, e))

        if not executor_class:
            sys.exit('Could not locate specified queue executor {}'.format(queue))
    else:
        executor_class = Base_Executor
    script = SoS_Script(content=content, transcript = None)
    for w in workflows:
        workflow = script.workflow(w)
        executor = executor_class(workflow, args = None,
                                  config = {'output_dag': dag})
        executor.run()
    env.verbosity = args.verbosity

def remove(workflows, steps, db, force, debug):
    filename = '.sos/.dsc/{}.db'.format(os.path.basename(db))
    if not os.path.isfile(filename):
        raise ValueError('Cannot remove anything because DSC metadata is not found!')
    if len(steps) == 0:
        # remove everything
        to_remove = glob.glob('{}/*'.format(db))
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
        data = pickle.load(open(filename, 'rb'))
        to_remove = flatten_list([[glob.glob(os.path.join(db, '{}.*'.format(x)))
                                   for x in data[item]['return']]
                                  for item in to_remove if item in data])
    if debug:
        env.logger.info(to_remove)
    else:
        cmd_remove(dotdict({"tracked": True, "untracked": False if not force else True,
                            "targets": to_remove, "__dryrun__": False,
                            "__confirm__": True, "signature": True, "verbosity": env.verbosity}), [])


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
    os.makedirs('.sos/.dsc', exist_ok = True)
    # 2. Parse DSC script
    script = DSC_Script(args.dsc_file, output = args.output, sequence = args.sequence, seeds = args.seeds)
    db = script.runtime.output
    db_name = os.path.basename(db)
    db_dir = os.path.dirname(db)
    manifest = '.sos/.dsc/{}.manifest'.format(db_name)
    if db_dir:
        os.makedirs(db_dir, exist_ok = True)
    workflow = DSC_Analyzer(script)
    if args.verbosity > 3:
        workflow2html('.sos/.dsc/{}.workflow.html'.format(db_name), workflow.workflows, script.dump().values())
    pipeline = DSC_Translator(workflow.workflows, script.runtime, args.__rerun__, args.__max_jobs__, args.try_catch)
    if args.verbosity > 3:
        sos2html((pipeline.write_pipeline(1), pipeline.write_pipeline(2)),
                 ('.sos/.dsc/{}.prepare.html'.format(db_name),
                  '.sos/.dsc/{}.run.html'.format(db_name)))
    # 3. remove and reconstruct at level 2: these will not trigger running anything
    if args.to_remove is not None:
        remove(workflow.workflows, args.to_remove, db, args.__rerun__, args.debug)
        return
    # 3.1 prepare for recover mode
    if args.__construct__ and not os.path.isfile(manifest):
        raise RuntimeError('Project cannot be recovered due to lack of integrity: manifest file is missing!\n'\
                         'Please make sure the benchmark was properly distributed with ``--distribute`` option.')
    # 3.2 recover level 2
    if args.__construct__ == 2:
        master = list(set([x[list(x.keys())[-1]].name for x in workflow.workflows]))
        env.logger.warning("Recovering partially completed DSC benchmark ...\n"\
                           "``--distribute`` option will fail on this recovered benchmark because it is incomplete.")
        ResultDB(db_name, master).Build(script = open(args.dsc_file).read())
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
    env.logger.info("DSC script exported to ``{}.html``".format(db))
    # 5. Prepare
    env.logger.info("Constructing DSC from ``{}`` ...".format(args.dsc_file))
    # Setup run for config files
    dsc_run(args, pipeline.conf_str, workflows = ['INIT', 'BUILD'], dag = None,
            verbosity = args.verbosity if args.__dryrun__ else 0, is_prepare = True)
    # 6. Run
    pipeline.filter_execution()
    if args.verbosity > 3:
        sos2html((pipeline.write_pipeline(2),), ('.sos/.dsc/{}.run.html'.format(db_name),))
    if args.__dryrun__:
        return
    env.logfile = db_name + '.log'
    if os.path.isfile(env.logfile):
        os.remove(env.logfile)
    if os.path.isfile('.sos/transcript.txt'):
        os.remove('.sos/transcript.txt')
    open(manifest, 'w').write('.sos/.dsc/{0}.map.mpk\n.sos/.dsc/{0}.io.mpk'.format(db_name))
    env.logger.debug("Running command ``{}``".format(' '.join(sys.argv)))
    env.logger.info("Building execution graph ...")
    try:
        dsc_run(args, pipeline.job_str, dag = '.sos/.dsc/{}.dag'.format(db_name),
                verbosity = (args.verbosity - 1 if args.verbosity > 0 else args.verbosity),
                queue = queue)
    except Exception as e:
        if env.verbosity and env.verbosity > 2:
            sys.stderr.write(get_traceback())
        transcript2html('.sos/transcript.txt', '{}.transcript.html'.format(db_name), title = db_name)
        env.logger.error(e)
        env.logger.warning("If needed, you can open ``{}.transcript.html`` and "\
                           "use ``ctrl-F`` to search by ``output file name`` "\
                           "for the problematic chunk of code.".\
                           format(db_name))
        sys.exit(1)
    # 7. Construct metadata
    master = list(set([x[list(x.keys())[-1]].name for x in workflow.workflows]))
    env.logger.info("Writing output metadata ...")
    ResultDB(db_name, master).Build(script = dsc_script)
    # 8. Update manifest
    manifest_items = [x.strip() for x in open(manifest).readlines()]
    for x in [args.dsc_file, db + '.html', '.sos/.dsc/{}.db'.format(db_name), manifest]:
        if x not in manifest_items:
            manifest_items.append(x)
    with open(manifest, 'w') as f:
        f.write('\n'.join(manifest_items))
    env.logger.info("DSC complete!")


def annotate(args):
    env.verbosity = args.verbosity
    mfiles = [args.annotation]
    ann = ResultAnnotator(args.annotation, args.master, output = args.output, sequence = args.sequence)
    for master in ann.masters:
        tagfile, shinyfile = ann.Apply(master)
        env.logger.info('Annotation summary for sequence ending with ``{}``\n'.format(master[7:]) + ann.ShowQueries())
        if len(ann.msg):
            env.logger.warning('\n' + '\n'.join(ann.msg))
        mfiles.append(tagfile)
        mfiles.append(shinyfile)
    # update manifest
    manifest = '.sos/.dsc/{}.manifest'.format(ann.dsc.runtime.output)
    manifest_items = [x.strip() for x in open(manifest).readlines()]
    for x in mfiles:
        if x not in manifest_items:
            manifest_items.append(x)
    with open(manifest, 'w') as f:
        f.write('\n'.join(manifest_items))


def extract(args):
    env.max_jobs = args.__max_jobs__
    env.verbosity = args.verbosity if not args.verbosity == 2 else 1
    atexit.register(env.cleanup)
    env.sig_mode = 'default'
    if args.__rerun__:
        env.sig_mode = 'force'
    #
    ext = ResultExtractor(args.tags, args.master, args.output, args.extract)
    script = SoS_Script(content = ext.script, transcript = None)
    workflow = script.workflow("Extracting")
    executor_class = Base_Executor
    executor = executor_class(workflow)
    executor.run()
    env.verbosity = args.verbosity
    env.logger.info('Data extracted to ``{}`` for DSC result ``{}`` via {}'.\
                    format(ext.output, ext.master,
                           'annotations: \n\t``{}``'.format('\n\t'.join(args.tags))
                           if args.tags else "all annotations."))


def distribute(args):
    import tarfile
    #
    if args.dsc_file:
        output = DSC_Script(args.dsc_file, output = args.output).runtime.output
    elif args.output:
        output = args.output
    else:
        raise RuntimeError("Please specify DSC benchmark name, via ``-o`` option.")
    manifest = '.sos/.dsc/{}.manifest'.format(output)
    if not os.path.isfile(manifest):
        raise RuntimeError('Project cannot be distributed due to lack of integrity: manifest file is missing!\n'\
                         'Please run DSC with ``-x`` before applying this command.')
    files = [x.strip() for x in open(manifest).readlines()] + glob.glob("{}/*.*".format(output))
    for item in args.distribute:
        if os.path.isdir(item):
            for root, folder, filenames in os.walk(item):
                for filename in filenames:
                    files.append(os.path.join(root, filename))
        else:
            files.append(item)
    files = uniq_list(files)
    env.verbosity = args.verbosity
    tar_args = {'name': output + '.tar.gz', 'mode': 'w:gz'}
    if os.path.isfile(tar_args['name']) and not args.__rerun__:
        raise RuntimeError('Operation aborted due to existing output file ``{}``. Use ``-f`` to overwrite.'.\
                        format(tar_args['name']))
    env.logger.info('Archiving project ``{}`` ...'.format(output))
    with tarfile.open(**tar_args) as archive:
        for f in files:
            if not os.path.isfile(f):
                raise RuntimeError('Project cannot be distributed due to lack of integrity.\n'\
                         'Please run and annotate DSC before distributing.')
            archive.add(f)

def run(args):
    if args.dsc_file is not None:
        execute(args)
    if args.annotation is not None:
        annotate(args)
    if args.extract is not None:
        extract(args)
    if args.distribute is not None:
        distribute(args)


def main():
    p = MyArgParser(description = __doc__, allow_abbrev = False)
    p.add_argument('--version', action = 'version', version = '{}'.format(VERSION))
    p.add_argument('-v', '--verbosity', type = int, choices = list(range(5)), default = 2,
                   help='''Output error (0), warning (1), info (2), debug (3) and trace (4)
                   information. Default to 2.''')
    p.add_argument('--debug', action='store_true', help = SUPPRESS)
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
                   help = '''DSC sequences to be executed. It will override the DSC::run
                   entry when specified. Multiple sequences are allowed. Each input should be
                   a quoted string defining a valid DSC sequence, or referring to the key of an existing
                   sequence in the DSC script. Multiple such strings should be separated by space.''')
    p_execute.add_argument('--seeds', metavar = "values", nargs = '+', dest = 'seeds',
                   help = '''This will override any "seed" property in the DSC script. This feature
                   is useful for using a small number of seeds for a test run.
                   Example: `--seeds 1`, `--replicate 1 2 3 4`, `--seeds {1..10}`, `--seeds "R(1:10)"`''')
    p_execute.add_argument('-q', action='store_true', dest='__dryrun__', help = SUPPRESS)
    p_execute.add_argument('--recover', type = int,
                           metavar = "levels", choices = [1, 2], dest = '__construct__',
                   help = '''Recover DSC based on names (not contents) of existing files. Level 1 recover
                   will try to reconstruct the entire benchmark skipping existing files. Level 2 recover
                   will only use existing files to reconstruct the benchmark output metadata, making it possible
                   to explore partial benchmark results without having to wait until completion of entire benchmark.''')
    p_execute.add_argument('--ignore-errors', action='store_true', dest='try_catch',
                   help = '''Bypass all errors from computational programs. This will keep the benchmark running but
                   all results will be set to missing values and the problematic script will be saved when applicable.
                   ''')
    p_execute.add_argument('--clean', dest = 'to_remove', metavar = "str", nargs = '*',
                   help = '''Instead of running DSC, output for one or multiple steps from previous DSC
                   runs are to be cleaned. Each step should be a valid DSC step in the format of
                   "block[index]", or "block" for all steps in the block.
                   Multiple steps should be separated by space. When "--clean" is used with "-f",
                   all specified files will be removed regardless of their step execution status.''')
    p_execute.add_argument('--host', metavar='str',
                   help='''URL of Redis server for distributed computation.''')
    p_ann = p.add_argument_group("Annotate DSC")
    p_ann.add_argument('-a', '--annotate', dest = 'annotation', metavar = 'DSC Annotation',
                       help = '''Annotate DSC.''')
    p_ext = p.add_argument_group("Extract DSC results")
    p_ext.add_argument('-e', '--extract', metavar = 'block:variable', nargs = '+',
                       help = '''Variable(s) to extract.
                       Variable(s) should be specified by "block:variable".
                       Valid `variable` are variables found in `return` of the corresponding
                       DSC block.''')
    p_ext.add_argument('--tags', metavar = 'str', nargs = '+',
                       help = '''Tags to extract. The "&&" sign can be used to specify intersect
                       of multiple tags. The "=" sign can be used to rename extracted tags.
                       Default to extracting for all tags.''')
    p_dist = p.add_argument_group("Distribute DSC")
    p_dist.add_argument('--distribute', metavar = 'files', nargs = '*',
                       help = '''Additional files to distribute.
                       This option will create a tarball for the DSC benchmark for distribution.
                       If additional files are given to this option, then those files will also be
                       included in the benchmark.''')
    p.set_defaults(func = run)
    try:
        args = p.parse_args()
    except Exception as e:
        env.logger.error(e)
        env.logger.info("Please type ``{} -h`` to view available options".\
                        format(os.path.basename(sys.argv[0])))
        sys.exit(1)
    #
    with Timer(verbose = True if ('verbosity' in vars(args) and args.verbosity > 0) else False):
        try:
            args.func(args)
        except Exception as e:
            if env.verbosity and env.verbosity > 2:
                sys.stderr.write(get_traceback())
            env.logger.error(e)
            sys.exit(1)

if __name__ == '__main__':
    main()
