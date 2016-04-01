#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
__doc__ = "Implementation of Dynamic Statistical Comparisons"

import sys, os, argparse
from dsc import PACKAGE, VERSION
from .workhorse import execute, query
from .utils import Timer
from pysos.utils import env, print_traceback

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
    p.add_argument('-j', type=int, metavar='jobs', default=1, dest='__max_jobs__',
                   help='''Number of concurrent processes allowed.''')
    p.add_argument('-d', action='store_true', dest='__dryrun__', help = '"dryrun" mode.')
    p.add_argument('-f', action='store_true', dest='__rerun__',
                   help='''Force executing DSC afresh regardless of already created results.''')
    add_common_args(p)
    p.set_defaults(func = execute)
    p = subparsers.add_parser('query', help = 'Explore DSC benchmark data',
                              formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('dsc_db', metavar = "db", help = 'DSC database')
    p.add_argument('-i', metavar = 'item', dest = 'items', default = [],
                   nargs = '*', help = 'Items to display (SQLite functions supported).')
    p.add_argument('-f', metavar = 'filter', dest = 'filter', default = [],
                   nargs = '*', help = 'Filter criteria (in SQLite syntax).')
    p.add_argument('-g', metavar = 'group_by', dest = 'group_by', default = [],
                   nargs = '*', help = 'Items to form groups by which output will be reported.')
    p.add_argument('-d', metavar = 'delimiter', dest = 'delimiter',
                   default = '\t', help = 'Delimiter of output data.')
    add_common_args(p)
    p.set_defaults(func = query)
    args, argv = parser.parse_known_args()
    try:
        with Timer(verbose = True if ('verbosity' in vars(args) and args.verbosity > 0) else False) as t:
            args.func(args, argv)
    except Exception as e:
        if args.verbosity > 2:
            print_traceback()
        else:
            env.logger.error(e)
        sys.exit(1)
