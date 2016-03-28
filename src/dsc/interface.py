#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
__doc__ = "Implementation of Dynamic Statistical Comparisons"

import sys, os, argparse
from dsc.pysos.utils import env
from dsc import PACKAGE, VERSION
from workhorse import execute, show, submit

def main():
    def add_common_args(obj):
        obj.add_argument('-v', '--verbosity', type = int, choices = list(range(5)), default = '2',
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
    add_common_args(p)
    p.set_defaults(func = execute)
    p = subparsers.add_parser('show', help = 'Explore DSC benchmark data',
                              formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    add_common_args(p)
    p.set_defaults(func = show)
    args, argv = parser.parse_known_args()
    env.verbosity = args.verbosity
    try:
        args.func(args)
    except Exception as e:
        env.logger.error(e)
        raise
