#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import os, sys
import warnings
warnings.filterwarnings("ignore")
from sos.utils import env, get_traceback
from .utils import dotdict
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

def query(args):
    return 0

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
    p.add_argument('dsc_output', metavar = "DSC output folder", help = 'Path to DSC output.')
    p.add_argument('-t', '--target', metavar = "str", nargs = '+',
                   help = '''Query targets.''')
    p.add_argument('-c', '--condition', metavar = "WHERE",
                   help = '''Query condition.''')
    p.add_argument('-o', metavar = "str", dest = 'output', required = True,
                   help = '''Output notebook / data file prefix.''')
    p.add_argument('--title', metavar = 'str', required = True,
                   help='''Title for notebook file.''')
    p.add_argument('--language', metavar = 'str', choices = ['R', 'Python'], default = 'R',
                   help='''Programming language to be embedded to generated notebooks for follow up analysis.''')
    p.add_argument('--addon', metavar = 'str', nargs = '+',
                   help='''Scripts to load to the notebooks for follow up analysis.''')
    p.add_argument('-v', '--verbosity', type = int, choices = list(range(5)), default = 2,
                   help='''Output error (0), warning (1), info (2), debug (3) and trace (4)
                   information.''')
    p.set_defaults(func = query)
    try:
        args = p.parse_args()
    except Exception as e:
        env.logger.error(e)
        env.logger.info("Please type ``{} -h`` to view available options".\
                        format(os.path.basename(sys.argv[0])))
        sys.exit(1)
    #
    env.verbosity = args.verbosity
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
