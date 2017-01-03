#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
__doc__ = "Implementation of Dynamic Statistical Comparisons"

import sys, argparse
from dsc import PACKAGE, VERSION
from sos.utils import logger, get_traceback
from .workhorse import execute, query
from .utils import Timer

def main():
    p = argparse.ArgumentParser(description = __doc__)
    p.add_argument('--version', action = 'version', version = '{} {}'.format(PACKAGE, VERSION))
    p.add_argument('-v', '--verbosity', type = int, choices = list(range(5)), default = 2,
                   help='''Output error (0), warning (1), info (2), debug (3) and trace (4)
                   information.''')
    p.add_argument('--debug', action='store_true', help = argparse.SUPPRESS)
    p.add_argument('dsc_file', metavar = "DSC file", help = '')
    p.add_argument('-s', '--sequence', metavar = "str", nargs = '+',
                   help = '''DSC sequence to be executed. It will override the DSC::run
                   entry when specified. Multiple sequences are allowed. Each input should be
                   a quoted string defining a valid DSC sequence. Multiple such strings should be
                   separated by space.''')
    p.add_argument('-o', metavar = "str", dest = 'output',
                   help = '''DSC output filename/directory. When used, it will override the
                   specification in DSC script''')
    p_execute = p.add_argument_group("Execute DSC")
    p_execute.add_argument('-d', action='store_true', dest='__dryrun__', help = argparse.SUPPRESS)
    p_execute.add_argument('-f', action='store_true', dest='__rerun__',
                   help='''Force executing DSC ignoring existing results.''')
    p_execute.add_argument('-r', action='store_true', dest='__construct__',
                   help='''Recover DSC based on names (not contents) of existing files.''')
    p_execute.add_argument('-j', type=int, metavar='N', default=2, dest='__max_jobs__',
                   help='''Number of concurrent processes allowed.''')
    p_execute.add_argument('--host', metavar='str',
                   help='''URL of Redis server for distributed computation.''')
    p_remove = p.add_argument_group("Remove DSC")
    p_remove.add_argument('--remove', dest = 'to_remove', metavar = "str", nargs = '+',
                   help = '''DSC steps whose output are to be removed. Multiple steps are allowed.
                   Each step should be a quoted string defining a valid DSC step, in the format of
                   "block_name[step_index]". Multiple such steps should be separated by space.''')
    p.set_defaults(func = execute)
    args, argv = p.parse_known_args()
    try:
        with Timer(verbose = True if ('verbosity' in vars(args) and args.verbosity > 0) else False):
            args.func(args, argv)
    except Exception as e:
        if 'verbosity' in args and args.verbosity > 2:
            sys.stderr.write(get_traceback())
        else:
            logger.error(e)
        sys.exit(1)

def main_viz():
    p = argparse.ArgumentParser(description = __doc__ + " (the visualization module)")
    p.add_argument('--version', action = 'version', version = '{} {}'.format(PACKAGE, VERSION))
    p.add_argument('dsc_db', metavar = 'DSC output database', help = '')
    p_query = p.add_argument_group("Output query")
    p_query.add_argument('-t', dest = 'master', metavar = 'str', required = True,
                   help = 'Name of master table to query from.')
    p_query.add_argument('-q', dest = 'queries', metavar = 'str', nargs = '+', required = True,
                   help = 'Queries to run. Please see DSC2 documentation for details.')
    p_query.add_argument('-o', dest = 'output', metavar = 'str',
                   help = 'Patterns of desired output columns. Please see DSC2 documentation for details.')
    p_query.add_argument('--no-header', dest = 'no_header', action='store_true',
                   help = 'Do not display header in output')
    p_query.set_defaults(func = query)
    args, argv = p.parse_known_args()
    try:
        args.func(args, argv)
    except Exception as e:
        sys.exit(e)
