#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
__doc__ = "Implementation of Dynamic Statistical Comparisons"

import sys, argparse
from dsc import VERSION
from sos.utils import logger, get_traceback
from .workhorse import run
from .utils import Timer

def main():
    p = argparse.ArgumentParser(description = __doc__)
    p.add_argument('--version', action = 'version', version = '{}'.format(VERSION))
    p.add_argument('-v', '--verbosity', type = int, choices = list(range(5)), default = 2,
                   help='''Output error (0), warning (1), info (2), debug (3) and trace (4)
                   information. Default to 2.''')
    p.add_argument('--debug', action='store_true', help = argparse.SUPPRESS)
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
                   help = '''DSC sequence to be executed. It will override the DSC::run
                   entry when specified. Multiple sequences are allowed. Each input should be
                   a quoted string defining a valid DSC sequence. Multiple such strings should be
                   separated by space.''')
    p_execute.add_argument('-d', action='store_true', dest='__dryrun__', help = argparse.SUPPRESS)
    p_execute.add_argument('--recover', action='store_true', dest='__construct__',
                   help = '''Recover DSC based on names (not contents) of existing files.''')
    p_execute.add_argument('--clean', dest = 'to_remove', metavar = "str", nargs = '*',
                   help = '''Instead of running DSC, output for one or multiple steps from previous DSC
                   runs are to be cleaned. Each step should be a valid DSC step in the format of
                   "block[index]", or "block" for all steps in the block.
                   Multiple steps should be separated by space. When "--clean" is used with "-f",
                   all specified files will be removed regardless of their step execution status.''')
    p_execute.add_argument('--host', metavar='str',
                   help='''URL of Redis server for distributed computation.''')
    p_ann = p.add_argument_group("Annotate DSC")
    p_ann.add_argument('-a', '--annotate', dest = 'annotation', metavar = 'DSC files', nargs = '+',
                       help = '''Annotate DSC. An annotation file name is required and DSC will
                       look for the script file having the same base name but with *.dsc extension.
                       Optionally one can input 2 file names with the first the annotation file name
                       and the second the DSC script name, eg, -a test.ann test.dsc''')
    p_ext = p.add_argument_group("Extract DSC results")
    p_ext.add_argument('-e', '--extract', metavar = 'block:variable', nargs = '+',
                       help = '''Variable(s) to extract.
                       Variable(s) should be specified by "block:variable".
                       Valid `variable` are variables found in `return` of the corresponding
                       DSC block.''')
    p_ext.add_argument('--tags', metavar = 'str', nargs = '+',
                       help = '''Tags to extract. The "&&" symbol can be used to specify intersect
                       of multiple tags. Default to extracting for all tags.''')
    p.set_defaults(func = run)
    args = p.parse_args()
    #
    try:
        with Timer(verbose = True if ('verbosity' in vars(args) and args.verbosity > 0) else False):
            args.func(args)
    except Exception as e:
        if 'verbosity' in args and args.verbosity > 2:
            sys.stderr.write(get_traceback())
        else:
            logger.error(e)
        sys.exit(1)
