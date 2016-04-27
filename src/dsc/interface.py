#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
__doc__ = "Implementation of Dynamic Statistical Comparisons"

import sys, argparse
from dsc import PACKAGE, VERSION
from pysos.utils import env, get_traceback
from .workhorse import execute
from .utils import Timer

def main():
    def add_common_args(obj):
        obj.add_argument('-v', '--verbosity', type = int, choices = list(range(5)), default = 2,
                         help='''Output error (0), warning (1), info (2), debug (3) and trace (4)
                         information.''')
        obj.add_argument('--debug', action='store_true', help = argparse.SUPPRESS)
    #
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('--version', action = 'version', version = '{} {}'.format(PACKAGE, VERSION))
    subparsers = parser.add_subparsers(dest = 'subcommands')
    subparsers.required = True
    p = subparsers.add_parser('execute', help = 'Execute DSC',
                              formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('dsc_file', metavar = "dsc_file", help = 'DSC file')
    p.add_argument('-s', '--sequence', metavar = "str", nargs = '+',
                   help = '''DSC sequence to be executed. It will override the DSC::run
                   entry when specified. Multiple sequences are allowed. Each input should be
                   a quoted string defining a valid DSC sequence. Multiple such strings should be
                   separated by space.''')
    p.add_argument('-j', type=int, metavar='jobs', default=1, dest='__max_jobs__',
                   help='''Number of concurrent processes allowed.''')
    p.add_argument('-d', action='store_true', dest='__dryrun__', help = '"dryrun" mode.')
    p.add_argument('-f', action='store_true', dest='__rerun__',
                   help='''Force executing DSC afresh regardless of already created results.''')
    add_common_args(p)
    p.set_defaults(func = execute)
    args, argv = parser.parse_known_args()
    try:
        with Timer(verbose = True if ('verbosity' in vars(args) and args.verbosity > 0) else False):
            args.func(args, argv)
    except Exception as e:
        if args.verbosity > 2:
            sys.stderr.write(get_traceback())
        else:
            env.logger.error(e)
        sys.exit(1)
