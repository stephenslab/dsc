#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

# some loaded modules are used for exec()
import os, sys, re, glob
import warnings
warnings.filterwarnings("ignore")
import msgpack
from sos.utils import env, get_traceback
from .utils import get_slice, flatten_list, workflow2html, dsc2html, transcript2html, Timer, \
    sos_hash_output, sos_group_input, chunks, OrderedDict
from .addict import Dict as dotdict
from . import VERSION

class Silencer:
    def __init__(self, verbosity):
        self.verbosity = verbosity
        self.env_verbosity = env.verbosity

    def __enter__(self):
        env.verbosity = self.verbosity

    def __exit__(self, etype, value, traceback):
        env.verbosity = self.env_verbosity

def prepare_args(name, content):
    out = dotdict()
    out.verbosity = env.verbosity
    # FIXME: should wait when local host
    # no-wait when extern task
    out.__wait__ = True
    out.__no_wait__ = False
    out.__targets__ = []
    # FIXME: add more options here
    out.__queue__ = 'localhost'
    # FIXME: when remote is used should make it `no_wait`
    # Yet to observe the behavior there
    out.__remote__ = None
    out.dryrun = False
    # FIXME
    out.__dag__ = '.sos/.dsc/{}.dot'.format(name)
    # FIXME: use config info
    out.__config__ = '.sos/.dsc/{}.conf.yml'.format(name)
    if not os.path.isfile(out.__config__):
        with open(out.__config__, 'w') as f:
            f.write('name: dsc')
    # FIXME: port the entire resume related features
    out.__resume__ = False
    out.update(content)
    return out

def remove(workflows, steps, db, debug, replace = False):
    import pickle
    from sos.__main__ import cmd_remove
    to_remove = [x for x in steps if os.path.isfile(x)]
    steps = [x for x in steps if x not in to_remove]
    filename = '{}/{}.db'.format(db, os.path.basename(db))
    if not os.path.isfile(filename):
        env.logger.warning('Cannot remove ``{}``, due to missing output database ``{}``.'.format(repr(steps), filename))
    else:
        remove_steps = []
        for item in steps:
            name, step_idx = get_slice(item, mismatch_quit = False)
            removed = False
            for workflow in workflows:
                for block in workflow:
                    for step in workflow[block].steps:
                        proceed = False
                        if step.group == name:
                            if step_idx is None:
                                proceed = True
                            else:
                                for idx in step_idx:
                                    if idx + 1 == step.exe_id:
                                        proceed = True
                                        break
                        if proceed:
                            tmp = re.sub(r'[^\w' + '_.' + ']', '_', step.name)
                            if tmp not in remove_steps:
                                remove_steps.append(tmp)
                            removed = True
            if removed is False:
                env.logger.warning("Cannot remove target ``{}`` because it is neither files nor " \
                                   "modules defined in \"DSC::run\".".format(item))
        #
        data = pickle.load(open(filename, 'rb'))
        to_remove.extend(flatten_list([[glob.glob(os.path.join(db, '{}.*'.format(x)))
                                        for x in data[item]['return']]
                                       for item in remove_steps if item in data]))
    if len(to_remove):
        cmd_remove(dotdict({"tracked": False, "untracked": False,
                            "targets": to_remove, "external": True,
                            "__confirm__": True, "signature": False,
                            "verbosity": env.verbosity, "zap": True if replace else False,
                            "size": None, "age": None, "dryrun": debug}), [])
    else:
        env.logger.warning("No files found to {}. Please check your ``--target`` option".\
                           format('replace' if replace else 'purge'))

def env_init(args, output):
    os.makedirs('.sos/.dsc', exist_ok = True)
    if os.path.dirname(output):
        os.makedirs(os.path.dirname(output), exist_ok = True)
    env.logfile = os.path.basename(output) + '.log'
    if os.path.isfile(env.logfile):
        os.remove(env.logfile)
    if os.path.isfile('.sos/transcript.txt'):
        os.remove('.sos/transcript.txt')

def execute(args):
    if args.to_remove:
        if args.target is None:
            raise ValueError("``--remove`` must be specified with ``--target``.")
        rm_objects = args.target
        args.target = None
    if args.target:
        env.logger.info("Load command line DSC sequence: ``{}``".\
                        format(' '.join(', '.join(args.target).split())))
    from .dsc_parser import DSC_Script, DSC_Pipeline
    from .dsc_translator import DSC_Translator
    from .dsc_database import ResultDB
    script = DSC_Script(args.dsc_file, output = args.output, sequence = args.target, seeds = args.seeds,
                        extern = args.host)
    db = os.path.basename(script.runtime.output)
    env_init(args, script.runtime.output)
    pipeline_dsc = DSC_Pipeline(script).pipelines
    if args.debug:
        workflow2html(f'.sos/.dsc/{db}.workflow.html', pipeline_dsc, list(script.dump().values()))
    pipeline = DSC_Translator(pipeline_dsc, script.runtime, args.__construct__ == "no",
                              args.__max_jobs__, args.try_catch)
    # Apply clean-up
    if args.to_remove:
        remove(workflow.workflows, rm_objects, script.runtime.output, args.debug, args.to_remove == 'replace')
        return
    # Recover DSC from existing files
    if args.__construct__ in ["full", "partial"] and not \
       (os.path.isfile(f'{script.runtime.output}/{db}.map.mpk') \
        and os.path.isfile(f'{script.runtime.output}/{db}.io.mpk')):
        raise RuntimeError('Project cannot be safely recovered because no meta-data can be found under\n``{}``'.\
                           format(os.path.abspath(script.runtime.output)))
    if args.__construct__ == "partial":
        # FIXME: need test
        master = list(set([x[list(x.keys())[-1]].name for x in workflow.workflows]))
        ResultDB(f'{script.runtime.output}/{db}', master).Build(script = open(args.dsc_file).read())
        return
    # Archive scripts
    from .utils import OrderedDict
    lib_content = [(f"From <code>{k}</code>", sorted(glob.glob(f"{k}/*.*")))
                   for k in script.runtime.options['lib_path'] or []]
    exec_content = [(k, [script.modules[k].exe])
                    for k in script.runtime.sequence_ordering]
    dsc2html(open(args.dsc_file).read(), script.runtime.output,
             section_content = OrderedDict(lib_content + exec_content))
    env.logger.info(f"DSC script exported to ``{script.runtime.output}.html``")
    env.logger.info(f"Constructing DSC from ``{args.dsc_file}`` ...")
    # Setup
    from sos.__main__ import cmd_run
    from sos.converter import script_to_html
    script_prepare = pipeline.write_pipeline(1)
    if args.debug:
        script_to_html(script_prepare, f'.sos/.dsc/{db}.prepare.html')
        with open(f'.sos/.dsc/{db}.prepare.py', 'w') as f:
            f.write(pipeline.conf_str_py)
    mode = "default"
    if args.__construct__ == "no":
        mode = "force"
    import platform
    exec_path = [os.path.join(k, 'mac' if platform.system() == 'Darwin' else 'linux')
                 for k in (script.runtime.options['exec_path'] or [])] + (script.runtime.options['exec_path'] or [])
    exec_path = [x for x in exec_path if os.path.isdir(x)]
    # Get raw IO database
    exec(compile(pipeline.conf_str_py, 'prepare_io', 'exec'))
    return
    # Get mapped IO database
    with Silencer(env.verbosity if args.debug else 0):
        content = {'__max_running_jobs__': args.__max_jobs__,
                   '__max_procs__': args.__max_jobs__,
                   '__sig_mode__': mode,
                   '__bin_dirs__': exec_path,
                   'script': script_prepare,
                   'workflow': "default"}
        cmd_run(prepare_args(db + '.prepare', content), [])
    # Run
    env.logger.debug(f"Running command ``{' '.join(sys.argv)}``")
    env.logger.info("Building execution graph ...")
    pipeline.filter_execution()
    script_run = pipeline.write_pipeline(2)
    if args.debug:
        script_to_html(script_run, f'.sos/.dsc/{db}.run.html')
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
            content = {'__max_running_jobs__': args.__max_jobs__,
                       '__max_procs__': args.__max_jobs__,
                       '__sig_mode__': mode,
                       '__bin_dirs__': exec_path,
                       'script': script_run,
                       'workflow': "DSC"}
            cmd_run(prepare_args(db + '.run', content), [])
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
    # Build database
    master = list(set([x[list(x.keys())[-1]].name for x in workflow.workflows]))
    env.logger.info("Writing output database ...")
    ResultDB('{}/{}'.format(script.runtime.output, db), master).\
        Build(script = open(script.runtime.output + '.html').read())
    env.logger.info("DSC complete!")

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
    p.add_argument('dsc_file', metavar = "DSC script", help = 'DSC script to execute.')
    p.add_argument('-o', metavar = "str", dest = 'output',
                   help = '''Benchmark output. It overwrites "DSC::run::output" defined in configuration file.''')
    p.add_argument('--target', metavar = "str", nargs = '+',
                   help = '''This argument can be used in two contexts:
                   1) When used without "--remove" it specifies DSC sequences to execute.
                   It overwrites "DSC::run" defined in configuration file.
                   Multiple sequences are allowed. Each input should be a quoted string defining
                   a valid DSC sequence, or referring to the key of an existing
                   sequence in the DSC script. Multiple such strings should be separated by space.
                   2) When used along with "--remove" it specifies one or more computational modules,
                   separated by space, whose output are to be removed. They should be 1) valid DSC modules
                   in the format of "module[index]", or 2) "module" for all routines in the module,
                   or 3) simply path to files that needs to be removed.''')
    p.add_argument('--seed', metavar = "values", nargs = '+', dest = 'seeds',
                   help = '''It overwrites any "seed" property in the DSC script. This feature
                   is useful for running a quick test with small number of replicates.
                   Example: `--seed 1`, `--seed 1 2 3 4`, `--seed {1..10}`, `--seed "R(1:10)"`''')
    p.add_argument('--recover', metavar = "option", choices = ["default", "no", "full", "partial"],
                   dest = '__construct__', default = "default",
                   help = '''Behavior of how DSC is executed in the presence of existing files.
                   "default" recover will check file signature and skip the ones that matches expected signature.
                   "no" recover will run everything from scratch ignoring any existing file.
                   "full" recover will reconstruct signature for existing files that matches expected input
                   environment, and run jobs to generate non-existing files, to complete the benchmark.
                   "partial" recover will use existing files directly to construct output metadata,
                   making it possible to explore partial benchmark results without having to wait until
                   completion of entire benchmark.''')
    p.add_argument('--remove', metavar = "option", choices = ["purge", "replace"],
                   dest = 'to_remove',
                   help = '''Behavior of how DSC removes files. "purge" deletes specified files
                   or files generated by specified modules. "replace" replaces these files by
                   dummy files with "*.zapped" extension, instead of removing them
                   (useful to swap out large intermediate files).
                   Files to operate on should be specified by "--target".''')
    p.add_argument('--host', metavar='str',
                   help='''Name of host computer to send tasks to.''')
    p.add_argument('-c', type = int, metavar = 'N', default = max(int(os.cpu_count() / 2), 1),
                   dest='__max_jobs__', help='''Number of maximum cpu threads.''')
    p.add_argument('--ignore-errors', action='store_true', dest='try_catch',
                   help = '''Bypass all errors from computational programs.
                   This will keep the benchmark running but
                   all results will be set to missing values and
                   the problematic script will be saved when possible.''')
    p.add_argument('-v', '--verbosity', type = int, choices = list(range(5)), default = 2,
                   help='''Output error (0), warning (1), info (2), debug (3) and trace (4)
                   information.''')
    p.set_defaults(func = execute)
    try:
        args = p.parse_args()
    except Exception as e:
        env.logger.error(e)
        env.logger.info("Please type ``{} -h`` to view available options".\
                        format(os.path.basename(sys.argv[0])))
        sys.exit(1)
    #
    env.verbosity = args.verbosity
    with Timer(verbose = True if ('verbosity' in vars(args) and args.verbosity > 0) else False) as t:
        try:
            args.func(args)
        except Exception as e:
            if args.debug:
                raise
            if env.verbosity and env.verbosity > 2:
                sys.stderr.write(get_traceback())
            env.logger.error(e)
            t.disable()
            sys.exit(1)

if __name__ == '__main__':
    main()
