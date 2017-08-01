#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import os, sys, atexit, re, glob, pickle
from collections import OrderedDict
import warnings
warnings.filterwarnings("ignore")
from sos.utils import env
from sos.__main__ import cmd_run, cmd_remove
from sos.converter import script_to_html
from .dsc_parser import DSC_Script
from .dsc_analyzer import DSC_Analyzer
from .dsc_translator import DSC_Translator
from .dsc_database import ResultDB, ResultAnnotator, ResultExtractor
from .utils import get_slice, uniq_list, flatten_list, workflow2html, dsc2html, transcript2html, dotdict, Timer
from . import VERSION

class Silencer:
    def __init__(self, verbosity):
        self.verbosity = verbosity
        self.env_verbosity = env.verbosity

    def __enter__(self):
        env.verbosity = self.verbosity

    def __exit__(self, etype, value, traceback):
        env.verbosity = self.env_verbosity

def prepare_args(args, db, script, workflow, mode):
    out = dotdict()
    out.__max_running_jobs__ = out.__max_procs__ = args.__max_jobs__
    # FIXME: should wait when local host
    # no-wait when extern task
    out.__wait__ = True
    out.__no_wait__ = False
    out.__targets__ = []
    # FIXME: add bin dir here
    out.__bin_dirs__ = []
    # FIXME: add more options here
    out.__queue__ = 'localhost'
    # FIXME: when remote is used should make it `no_wait`
    # Yet to observe the behavior there
    out.__remote__ = None
    out.dryrun = False
    out.__sig_mode__ = mode
    out.verbosity = env.verbosity
    # FIXME
    out.__dag__ = '.sos/.dsc/{}.dot'.format(db)
    # FIXME: use config info
    out.__config__ = '.sos/.dsc/{}.conf.yml'.format(db)
    # FIXME: port the entire resume related features
    out.__resume__ = False
    out.script = script
    out.workflow = workflow
    return out

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
        cmd_remove(dotdict({"tracked": True, "untracked": True,
                            "targets": to_remove, "external": True,
                            "__confirm__": True, "signature": True,
                            "verbosity": env.verbosity, "zap": False,
                            "size": None, "age": None, "dryrun": False}), [])

def dsc_init(args, output):
    os.makedirs('.sos/.dsc', exist_ok = True)
    if os.path.dirname(output):
        os.makedirs(os.path.dirname(output), exist_ok = True)
    # FIXME: need to utilize this to properly make global configs
    with open('.sos/.dsc/{}.conf.yml'.format(os.path.basename(output)), 'w') as f:
        f.write('name: dsc')
    if os.path.isfile(env.logfile):
        os.remove(env.logfile)
    if os.path.isfile('.sos/transcript.txt'):
        os.remove('.sos/transcript.txt')

def execute(args):
    if args.sequence:
        env.logger.info("Load command line DSC sequence: ``{}``".\
                        format(' '.join(', '.join(args.sequence).split())))
    script = DSC_Script(args.dsc_file, output = args.output, sequence = args.sequence, seeds = args.seeds,
                        extern = args.host)
    db = os.path.basename(script.runtime.output)
    env.logfile = db + '.log'
    dsc_init(args, script.runtime.output)
    workflow = DSC_Analyzer(script)
    if args.debug:
        workflow2html('.sos/.dsc/{}.workflow.html'.format(db), workflow.workflows, script.dump().values())
    pipeline = DSC_Translator(workflow.workflows, script.runtime, args.__construct__ == "no",
                              args.__max_jobs__, args.try_catch)
    # Apply clean-up
    # FIXME: need to revisit
    if args.to_remove is not None:
        remove(workflow.workflows, args.to_remove, script.runtime.output, args.__construct__ == "no", args.debug)
        return
    # Recover DSC from existing files
    if args.__construct__ and not \
       (os.path.isfile('{1}/{0}.map.mpk'.format(db, script.runtime.output)) \
        and os.path.isfile('{1}/{0}.io.mpk'.format(db, script.runtime.output))):
        raise RuntimeError('Project cannot be safely recovered because no meta-data can be found under ``{}``!'.\
                           format(script.runtime.output))
    if args.__construct__ == "partial":
        master = list(set([x[list(x.keys())[-1]].name for x in workflow.workflows]))
        env.logger.warning("Recovering partially completed DSC benchmark ...\n"\
                           "``--distribute`` option will fail on this recovered benchmark because it is incomplete.")
        ResultDB('{}/{}'.format(script.runtime.output, db), master).Build(script = open(args.dsc_file).read())
        return
    # Archive scripts
    exec_content = OrderedDict([(k, [step.exe for step in script.blocks[k].steps])
                                for k in script.runtime.sequence_ordering])
    dsc_script = open(args.dsc_file).read()
    dsc2html(dsc_script, None, script.runtime.output, section_content = exec_content)
    env.logger.info("DSC script exported to ``{}.html``".format(script.runtime.output))
    # Output file structure setup
    env.logger.info("Constructing DSC from ``{}`` ...".format(args.dsc_file))
    script_prepare = pipeline.write_pipeline(1)
    if args.debug:
        script_to_html(script_prepare, '.sos/.dsc/{}.prepare.html'.format(db))
    mode = "default"
    if args.__construct__ == "no":
        mode = "force"
    with Silencer(env.verbosity if args.debug else 0):
        cmd_run(prepare_args(args, db, script_prepare, "default", mode), [])
    # Run
    env.logger.debug("Running command ``{}``".format(' '.join(sys.argv)))
    env.logger.info("Building execution graph ...")
    pipeline.filter_execution()
    script_run = pipeline.write_pipeline(2)
    if args.debug:
        script_to_html(script_run, '.sos/.dsc/{}.run.html'.format(db))
        return
    if args.__construct__ == "full":
        # For this mode, since file names are properly determined by the `cmd_run` above,
        # then the fact that file name remains the same should be a result of same unique parameter + code
        # in that case it is safe to simply build signatures for them
        # For files having different parameter + code new file names should be generated and used for them
        # The new files should not conflict with existing files, due to the use of `remove_obsolete_output`
        mode = "build"
    try:
        with Silencer(args.verbosity if args.host else min(1, args.verbosity)):
            cmd_run(prepare_args(args, db, script_run, "DSC", mode), [])
    except Exception as e:
        if env.verbosity > 2:
            sys.stderr.write(get_traceback())
        if args.host is None:
            transcript2html('.sos/transcript.txt', '{}.transcript.html'.format(db), title = db)
            env.logger.error(e)
            env.logger.warning("If needed, you can open ``{}.transcript.html`` and "\
                               "use ``ctrl-F`` to search by ``output file name`` "\
                               "for the problematic chunk of code.".\
                               format(db))
        sys.exit(1)
    # Construct metadata
    master = list(set([x[list(x.keys())[-1]].name for x in workflow.workflows]))
    env.logger.info("Writing output metadata ...")
    ResultDB('{}/{}'.format(script.runtime.output, db), master).Build(script = dsc_script)
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
    if args.__construct__ == "no":
        env.sig_mode = 'force'
    #
    if args.output is None:
        raise RuntimeError("Please specify DSC benchmark name, via ``-b`` option.")
    ext = ResultExtractor(args.output, args.tags, args.master, args.extract_to, args.extract)
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
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, SUPPRESS
    class ArgumentParserError(Exception): pass
    class MyArgParser(ArgumentParser):
        def error(self, message):
            raise ArgumentParserError(message)
    #
    p = MyArgParser(description = __doc__, allow_abbrev = False, formatter_class = ArgumentDefaultsHelpFormatter)
    p.add_argument('--debug', action='store_true', help = SUPPRESS)
    p.add_argument('--version', action = 'version', version = '{}'.format(VERSION))
    p.add_argument('-v', '--verbosity', type = int, choices = list(range(5)), default = 2,
                   help='''Output error (0), warning (1), info (2), debug (3) and trace (4)
                   information.''')
    p.add_argument('-c', type = int, metavar = 'N', default = max(int(os.cpu_count() / 2), 1),
                   dest='__max_jobs__', help='''Number of maximum cpu threads.''')
    p.add_argument('-o', metavar = "str", dest = 'output',
                   help = '''Benchmark output. It overwrites "DSC::run::output" specified by configuration file.''')
    # FIXME to be made obsolete
    p.add_argument('--target', dest = 'master', metavar = 'str',
                   help = '''The ultimate target of a DSC benchmark is the name of
                   the last block in a DSC sequence. This option is relevant to -a / -e
                   commands when there exists multiple DSC sequences with different targets.''')
    p_execute = p.add_argument_group("Execute DSC")
    p_execute.add_argument('-x', '--execute', dest = 'dsc_file', metavar = "DSC script",
                           help = 'Execute DSC.')
    p_execute.add_argument('--sequence', metavar = "str", nargs = '+',
                           help = '''DSC sequences to be executed. It overwrites "DSC::run" specified by
                           configuration file. Multiple sequences are allowed. Each input should be
                           a quoted string defining a valid DSC sequence, or referring to the key of an existing
                           sequence in the DSC script. Multiple such strings should be separated by space.''')
    p_execute.add_argument('--seed', metavar = "values", nargs = '+', dest = 'seeds',
                           help = '''It overwrites any "seed" property in the DSC script. This feature
                           is useful for running a quick test with small number of replicates.
                           Example: `--seed 1`, `--seed 1 2 3 4`, `--seed {1..10}`, `--seed "R(1:10)"`''')
    p_execute.add_argument('--recover', metavar = "option", choices = ["default", "no", "full", "partial"],
                           dest = '__construct__', default = "default",
                           help = '''Behavior of how DSC is executed in the presence of existing files.
                           "default" recover will check file signature and skip the ones that matches expected signature.
                           "no" recover will run everything from scratch ignoring any existing file.
                           "full" recover will reconstruct signature for existing files that matches expected input
                           environment, and run jobs to generate non-existing files, to complete the benchmark.
                           "partial" recover will use existing files directly to construct output metadata,
                           making it possible to explore partial benchmark results without having to wait until
                           completion of entire benchmark.''')
    p_execute.add_argument('--ignore-errors', action='store_true', dest='try_catch',
                           help = '''Bypass all errors from computational programs.
                           This will keep the benchmark running but
                           all results will be set to missing values and
                           the problematic script will be saved when possible.''')
    p_execute.add_argument('--remove', dest = 'to_remove', metavar = "str", nargs = '*',
                           help = '''Instead of running DSC, output for one or multiple steps from previous DSC
                           runs are to be cleaned. Each step should be a valid DSC step in the format of
                           "block[index]", or "block" for all steps in the block.
                           Multiple steps should be separated by space. When "--remove" is used with "-f",
                           all specified files will be removed regardless of their step execution status.''')
    p_execute.add_argument('--host', metavar='str',
                           help='''Name of host computer to run jobs.''')
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
    p_ext.add_argument('-f', metavar = "str", dest = 'extract_to',
                       help = '''Output file name.''')
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
    env.verbosity = args.verbosity
    with Timer(verbose = True if ('verbosity' in vars(args) and args.verbosity > 0) else False):
        try:
            args.func(args)
        except Exception as e:
            if args.debug:
                raise
            if env.verbosity and env.verbosity > 2:
                sys.stderr.write(get_traceback())
            env.logger.error(e)
            sys.exit(1)

if __name__ == '__main__':
    main()
