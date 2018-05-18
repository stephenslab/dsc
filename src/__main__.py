#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import os, sys, glob, time, yaml
from .version import __version__
from sos.utils import env, get_traceback

class Silencer:
    def __init__(self, verbosity):
        self.verbosity = verbosity
        self.env_verbosity = env.verbosity

    def __enter__(self):
        env.verbosity = self.verbosity

    def __exit__(self, etype, value, traceback):
        env.verbosity = self.env_verbosity

class Timer(object):
    def __init__(self, verbose=False):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        self.msecs = self.secs * 1000  # millisecs
        if self.verbose:
            env.logger.info('Elapsed time ``%.03f`` seconds.' % self.secs)

    def disable(self):
        self.verbose = False

def remove(workflows, groups, modules, db, purge = False):
    if purge:
        from .dsc_database import remove_obsolete_output
        remove_obsolete_output(db)
    else:
        from .dsc_database import zap_unwanted_output
        zap_unwanted_output(workflows, groups, modules, db)

def execute(args):
    from .utils import workflow2html, dsc2html, transcript2html
    if args.to_remove:
        if args.target is None and args.to_remove != 'purge':
            raise ValueError("``--clean`` must be specified with ``--target``.")
        rm_objects = args.target
        args.target = None
    if args.target:
        env.logger.info("Load command line DSC sequence: ``{}``".\
                        format(' '.join(', '.join(args.target).split())))
    from .dsc_parser import DSC_Script, DSC_Pipeline
    from .dsc_translator import DSC_Translator
    from .dsc_database import ResultDB
    script = DSC_Script(args.dsc_file, output = args.output, sequence = args.target, truncate = args.truncate, replicate = 1 if args.truncate else args.replicate)
    script.init_dsc(args, env)
    db = os.path.basename(script.runtime.output)
    pipeline_obj = DSC_Pipeline(script).pipelines
    master_tables = list(set([x[list(x.keys())[-1]].name for x in pipeline_obj]))
    #
    import platform
    # FIXME: always assume args.host is a Linux machine; thus not checking it
    exec_path = [os.path.join(k, 'mac' if platform.system() == 'Darwin' and args.host is None else 'linux')
                 for k in (script.runtime.options['exec_path'] or [])] + (script.runtime.options['exec_path'] or [])
    exec_path = [x for x in exec_path if os.path.isdir(x)]
    conf = None
    if args.host:
        from .dsc_parser import remote_config_parser
        conf = remote_config_parser(args.host, exec_path)
        args.host = conf['default']['queue']
    else:
        if args.to_host:
            raise ValueError('Cannot set option ``--to-host`` without specifying ``--host``!')
    #
    if args.debug:
        workflow2html(f'.sos/.dsc/{db}.workflow.html', pipeline_obj, list(script.dump().values()))
    # FIXME: make sure try_catch works, or justify that it is not necessary to have.
    pipeline = DSC_Translator(pipeline_obj, script.runtime, args.__construct__ == "none" and not args.__recover__,
                              args.__max_jobs__, args.try_catch, conf if conf is None else {k:v for k, v in conf.items() if k != 'DSC'},
                              not args.__print__)
    # Apply clean-up
    if args.to_remove:
        remove(pipeline_obj, {**script.runtime.concats, **script.runtime.groups},
               rm_objects, script.runtime.output,
               args.to_remove == 'purge')
        return
    # Archive scripts
    lib_content = [(f"From <code>{k}</code>", sorted(glob.glob(f"{k}/*.*")))
                   for k in script.runtime.options['lib_path'] or []]
    exec_content = [(k, script.modules[k].exe)
                    for k in script.runtime.sequence_ordering]
    dsc2html('\n'.join(script.transcript), script.runtime.output,
             script.runtime.sequence, exec_content, lib_content)
    env.logger.info(f"DSC script exported to ``{script.runtime.output}.html``")
    # Setup
    env.logger.info(f"Constructing DSC from ``{args.dsc_file}`` ...")
    from sos.__main__ import cmd_run
    from sos.converter import script_to_html
    script_prepare = pipeline.write_pipeline(1)
    if args.debug:
        script_to_html(os.path.abspath(script_prepare), f'.sos/.dsc/{db}.prepare.html')
    mode = "default"
    if args.__construct__ == "none":
        mode = "force"
    content = {'__sig_mode__': mode,
               'script': script_prepare,
               '__bin_dirs__': exec_path,
               'workflow': "deploy"}
    if args.__recover__:
        if not (os.path.isfile(f'{script.runtime.output}/{db}.map.mpk')):
            env.logger.warning(f'Cannot use ``--touch`` option because project meta-data is corrupted.')
        else:
            mode = "build"
    # Get mapped IO database
    with Silencer(env.verbosity if args.debug else 0):
        cmd_run(script.get_sos_options(db, content), [])
    if os.path.isfile('.sos/transcript.txt'):
        os.remove('.sos/transcript.txt')
    # Get the executed pipeline
    pipeline.filter_execution()
    script_run = pipeline.write_pipeline(2)
    # Configure job templates
    if args.host:
        # write default config
        yaml.dump({'localhost':'localhost', 'hosts': conf['DSC']}, open(script_run[:-3] + 'localhost.yml', 'w'),
                  default_flow_style=False)
        if  conf['DSC'][args.host]['address'] == 'localhost':
            # the only version of config file to use
            # make a copy of it to current workdir, for maintenance purpose
            yaml.dump({'localhost':'localhost',
                       'hosts': {k:v for k,v in conf['DSC'].items() if k in [args.host, 'localhost']}},
                      open(script.runtime.output + '.queue.yml', 'w'),
                      default_flow_style=False)
            # set to_host to empty because there is no need for it
            args.to_host = []
        else:
            # need 2 versions, local and remote
            # write local config
            conf['DSC'][args.host]['execute_cmd'] = 'ssh -q {host} -p {port} "bash --login -c \'[ -d {cur_dir} ] || mkdir -p {cur_dir}; cd {cur_dir} && sos run %s DSC -c %s -J %s -v %s\'"' % (script_run, script_run[:-3] + 'remote.yml', args.__max_jobs__, args.verbosity)
            yaml.dump({'localhost':'localhost', 'hosts': conf['DSC']},
                      open(script_run[:-3] + 'local.yml', 'w'),
                      default_flow_style=False)
            # write remote config
            del conf['DSC'][args.host]['execute_cmd']
            conf['DSC'][args.host]['address'] = 'localhost'
            yaml.dump({'localhost':'localhost',
                       'hosts': {k:v for k,v in conf['DSC'].items() if k in [args.host, 'localhost']}},
                      open(script_run[:-3] + 'remote.yml', 'w'),
                      default_flow_style=False)
            args.to_host.extend([script_run,
                                 script_run[:-3] + 'remote.yml',
                                 f'{script.runtime.output}/{db}.db',
                                 f'{script.runtime.output}/{db}.conf.mpk'])
    if args.debug:
        script_to_html(os.path.abspath(script_run), f'.sos/.dsc/{db}.run.html')
        return
    # Recover DSC database alone from meta-file
    if args.host or args.__construct__ == "all":
        if not ((os.path.isfile(f'{script.runtime.output}/{db}.map.mpk')
                 and os.path.isfile(f'.sos/.dsc/{db}.io.mpk'))):
            env.logger.warning('Cannot build DSC database because meta-data for this project is corrupted.')
        else:
            env.logger.info("Building DSC database ...")
            ResultDB(f'{script.runtime.output}/{db}', master_tables).\
                Build(script = open(script.runtime.output + '.html').read(), groups = script.runtime.groups)
        if args.__construct__ == "all":
            return
    # Run
    if args.host:
        # send files to remote
        env.logger.info(f"Syncing & installing resources on ``{args.host}`` (may take a while) ...")
        content = {'__sig_mode__': mode,
                   '__queue__': f'{args.host}-process',
                   '__config__': script_run[:-3] + 'localhost.yml',
                   'script': pipeline.write_pipeline(args.to_host)}
        try:
            with Silencer(args.verbosity):
                cmd_run(script.get_sos_options(db, content), [])
        except Exception as e:
            env.logger.error(f"Failed to communicate with ``{args.host}``")
            env.logger.warning(f"Please ensure 1) you have properly configured ``{args.host}`` via file to ``--host`` option, and 2) you have installed \"scp\", \"ssh\" and \"rsync\" on your computer, and \"dsc\" on ``{args.host}``.")
            raise RuntimeError(e)
    env.logger.debug(f"Running command ``{' '.join(sys.argv)}``")
    env.logger.info(f"Building execution graph & {'running DSC' if args.host is None else 'connecting to ``' + args.host + '`` (may take a while)'} ...")
    cfg_file = (script_run[:-3] + ('local.yml' if len(args.to_host) else 'localhost.yml')) if args.host else f'.sos/.dsc/{db}.conf.yml'
    content = {'__max_running_jobs__': args.__max_jobs__,
               '__max_procs__': args.__max_jobs__,
               '__sig_mode__': mode,
               '__bin_dirs__': exec_path,
               '__remote__': args.host if len(args.to_host) else None,
               '__config__': cfg_file,
               'script': script_run,
               'workflow': "DSC"}
    try:
        with Silencer(args.verbosity if args.host else max(0, args.verbosity - 1)):
            cmd_run(script.get_sos_options(db, content), [])
    except SystemExit:
        if args.host is None:
            transcript2html('.sos/transcript.txt', f'{db}.scripts.html', title = db)
            env.logger.warning(f"Files ``in green`` in the error prompt above contains codes and " \
                               "error info to help debug.\nScripts upstream of the error can be found in " \
                               f"``{db}.scripts.html``.")
        sys.exit(1)
    # Build database
    if args.host is None:
        env.logger.info("Building DSC database ...")
        ResultDB(f'{script.runtime.output}/{db}', master_tables).\
            Build(script = open(script.runtime.output + '.html').read(), groups = script.runtime.groups)
        env.logger.info("DSC complete!")

def main():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, SUPPRESS
    class ArgumentParserError(Exception): pass
    class MyArgParser(ArgumentParser):
        def error(self, message):
            raise ArgumentParserError(message)
    #
    p = MyArgParser(description = __doc__, formatter_class = ArgumentDefaultsHelpFormatter, add_help = False)
    p.add_argument('dsc_file', metavar = "DSC script", help = 'DSC script to execute.')
    ce = p.add_argument_group('Customized execution')
    ce.add_argument('--target', metavar = "str", nargs = '+',
                   help = '''This argument can be used in two contexts:
                   1) When used without "--clean" it overrides "DSC::run" in DSC file.
                   Input should be quoted string(s) defining one or multiple valid DSC pipelines
                   (multiple pipelines should be separated by space).
                   2) When used along with "--clean" it specifies one or more computational modules,
                   separated by space, whose output are to be removed. Alternatively one can specify
                   path(s) of particular DSC output files that needs to be removed.''')
    ce.add_argument('--truncate', action='store_true',
                   help = '''When applied, DSC will only run one value per parameter.
                   For example with "--truncate", "n: R{1:50}" will be truncated to "n: 1".
                   This is useful in exploratory analysis and diagnostics, particularly when used in combination with "--target".''')
    ce.add_argument('--replicate', metavar = 'N', type = int,
                   help = '''Overrides "DSC::replicate" to set number of replicates. Will be set to 1 when "--truncate" is in action.''')
    ce.add_argument('-o', metavar = "str", dest = 'output',
                   help = '''Benchmark output. It overrides "DSC::output" defined in DSC file.''')
    mt = p.add_argument_group('Maintenance')
    mt.add_argument('-s', '--skip', metavar = "option", choices = ["default", "none", "all"],
                   dest = '__construct__', default = "default",
                   help = '''Behavior of how DSC is executed in the presence of existing results.
                   "default": skips modules whose "environment" has not been changed since previous execution.
                   "none": executes DSC from scratch.
                   "all": skips all execution yet build DSC database of what the specified benchmark is
                   supposed to look like, thus making it possible to explore partial benchmark results.''')
    mt.add_argument('--touch', action='store_true', dest='__recover__',
                   help = '''"Touch" output files if exist, to mark them "up-to-date".
                   It will override "--skip" option. Note that time stamp is irrelevant to whether or not a file is up-to-date.
                   Files will be considered to "exist" as long as module name, module parameters and variables,
                   module script name and command arguments remain the same. Module script content do not matter.
                   The output files will be "touched" to match with the current status of module code.''')
    mt.add_argument('--clean', metavar = "option", choices = ["purge", "replace"],
                   dest = 'to_remove',
                   help = '''Behavior of how DSC cleans up output folder to save disk space.
                   "purge" cleans up everything in folder "DSC::output" irrelevant to the most recent successful execution of the benchmark.
                   "replace" deletes specified files, or files from specified (via "--target") modules,
                   and put in placeholder files with "*.zapped" extension. When re-running pipelines these "zapped" files will not trigger
                   rerun of their module unless they are directly required by a downstream module that needs re-execution.
                   In other words this is useful to remove large yet unused intermediate module output.''')
    ro = p.add_argument_group('Runtime behavior')
    ro.add_argument('-c', type = int, metavar = 'N', default = max(int(os.cpu_count() / 2), 1),
                   dest='__max_jobs__',
                   help = '''Number of maximum cpu threads for local runs, or concurrent jobs for remote execution.''')
    ro.add_argument('--ignore-errors', action='store_true', dest='try_catch',
                   help = '''Bypass all errors from computational programs.
                   This will keep the benchmark running but
                   all results will be set to missing values and
                   the problematic script will be saved when possible.''')
    ro.add_argument('-v', '--verbosity', type = int, choices = list(range(5)), default = 2,
                   help='''Output error (0), warning (1), info (2), debug (3) and trace (4)
                   information.''')
    ro.add_argument('-p', dest = '__print__', action='store_true',
                    help='''Print stdout and stderr to screen. Caution that running DSC with and without "-p" will trigger rerun
                    unless "--touch" option is also used.''')
    rt = p.add_argument_group('Remote execution')
    rt.add_argument('--host', metavar='file', help = '''Configuration file for remote computer.''')
    rt.add_argument('--to-host', metavar='dir', dest = 'to_host', nargs = '+', default = [],
                   help = '''Files and directories to be sent to remote host for use in benchmark.''')
    ot = p.add_argument_group('Other options')
    ot.add_argument('--version', action = 'version', version = __version__)
    ot.add_argument("-h", "--help", action="help", help="show this help message and exit")
    ot.add_argument('--debug', action='store_true', help=SUPPRESS)
    p.set_defaults(func = execute)
    if len(sys.argv) > 2 and '-h' in sys.argv:
        try:
            from .dsc_parser import DSC_Script
            script = DSC_Script(sys.argv[1])
            script.print_help('-v' in sys.argv)
            sys.exit(0)
        except Exception as e:
            if '--debug' in sys.argv:
                raise
            else:
                env.logger.error(f'No help information is available for script {sys.argv[1]}: ``{e}``')
                sys.exit(1)
    try:
        args = p.parse_args()
    except Exception as e:
        env.logger.error(e)
        env.logger.info("Please type ``{} -h`` to view available options".\
                        format(os.path.basename(sys.argv[0])))
        sys.exit(1)
    #
    env.verbosity = args.verbosity
    with Timer(verbose = True if (env.verbosity > 0) else False) as t:
        try:
            args.func(args)
        except KeyboardInterrupt:
            t.disable()
            sys.exit('KeyboardInterrupt')
        except Exception as e:
            if args.debug:
                raise
            if env.verbosity > 2:
                sys.stderr.write(get_traceback())
            t.disable()
            if type(e).__name__ == 'RuntimeError':
                env.logger.error("Detailed message:")
                sys.exit(e)
            else:
                env.logger.error(e)
                sys.exit(1)


if __name__ == '__main__':
    main()
