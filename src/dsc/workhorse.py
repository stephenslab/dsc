#!/usr/bin/env python3
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import sys, os, argparse
from dsc import PACKAGE, VERSION

def execute(args):
    try:
        env.verbosity = args.verbosity
        dsc = DSC(args.dsc_file, nodes = args.nodes, threads = args.threads)
        # Setup runtime environment here
        ## Check availability of libraries / files / commands
        if args.nodes != 1:
            # Jobs are not executed. Rather a number of DSC job files in YAML format
            env.logger.info("Jobs saved for submission")
        else:
            # execute one or all of ['scenario', 'method', 'score']
            dsc.execute(args.jobs)
    except Exception as e:
        env.unlock_all()
        env.logger.error(e)
        sys.exit(1)

def submit(args):
    try:
        env.verbosity = args.verbosity
        # load DSC job file and run
        dsc = Jobs(args.file)
        dsc.execute()
    except Exception as e:
        env.unlock_all()
        env.logger.error(e)
        sys.exit(1)

def show(args):
    pass

def main():
    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument('--version', action = 'version', version = '{} {}'.format(PACKAGE, VERSION))
    subparsers = parser.add_subparsers(dest = 'subcommands')
    subparsers.required = True
    p = subparsers.add_parser('execute', help = 'Execute DSC benchmark',
                              formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('-c', dest = 'config_file', type = argparse.FileType('r'), required = True,
                   help = 'DSC benchmark settings')
    p.add_argument('-v', '--verbosity', type = int, choices = [0,1], default = 1, help = 'Verbosity level')
    p.set_defaults(func = execute)
    p = subparsers.add_parser('show', help = 'Explore DSC benchmark data',
                              formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    p.set_defaults(func = show)
    args, argv = parser.parse_known_args()
    try:
        args.func(args)
    except Exception as e:
        raise
