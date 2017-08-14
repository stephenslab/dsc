#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import os, sys
import warnings
warnings.filterwarnings("ignore")
from sos.utils import env, get_traceback
from . import VERSION

def query(args):
    env.logger.info("Loading database ...")
    from sos.__main__ import AnswerMachine
    from sos.jupyter.converter import notebook_to_html
    from .query_jupyter import get_database_notebook, get_query_notebook
    from .query_engine import Query_Processor
    from .utils import dotdict, uniq_list
    _AM_ = AnswerMachine()
    if os.path.isfile(args.dsc_output):
        args.dsc_output = os.path.dirname(args.dsc_output)
    db = os.path.join(args.dsc_output, os.path.basename(args.dsc_output) + '.db')
    if args.target is None:
        if not args.output.endswith('.ipynb'):
            args.output = args.output.strip('.') + '.ipynb'
        if os.path.isfile(args.output) and not _AM_.get("Overwrite existing file \"{}\"?".format(args.output)):
            sys.exit("Aborted!")
        env.logger.info("Exporting database ...")
        get_database_notebook(db, args.output, args.title, args.description, args.limit)
    else:
        env.logger.info("Running queries ...")
        from .yhat_sqldf import sqldf, PandaSQLException as SQLError
        _QP_ = Query_Processor(db, args.target, args.condition)
        for query in _QP_.get_queries():
            env.logger.debug(query)
        globals().update(**(_QP_.get_data()))
        try:
            output = {'pipeline_{}'.format(i+1) : _QP_.populate_table(sqldf(query)) \
                      for i, query in enumerate(_QP_.get_queries())}
        except SQLError as e:
            raise(e)
        # write output
        if not args.output.endswith('.rds') and not args.output.endswith('.ipynb'):
            args.output = args.output.strip('.') + '.ipynb'
        if args.output.endswith('.rds'):
            frds = args.output
            fnb = None
            args.no_html = True
        else:
            frds = args.output[:-6] + '.rds'
            fnb = args.output
        if os.path.isfile(frds) and not _AM_.get("Overwrite existing file \"{}\"?".format(frds)):
            sys.exit("Aborted!")
        from .utils import save_rds
        save_rds(output, frds)
        env.logger.info("Query results saved to R compatible format ``{}``".format(frds))
        if fnb is not None:
            if os.path.isfile(fnb) and not _AM_.get("Overwrite existing file \"{}\"?".format(fnb)):
                sys.exit("Aborted!")
            desc = (args.description or []) + ['Queries performed for:\n\n* targets: `{}`\n* conditions: `{}`'.\
                                                   format(repr(args.target), repr(args.condition))]
            get_query_notebook(frds, _QP_.get_queries(), fnb, args.title, desc, args.language,
                               uniq_list(args.addon or []), args.limit)
            env.logger.info("Export complete. You can use ``jupyter notebook {0}`` to open it.".format(fnb))
    if not args.no_html:
        html = args.output[:-6] + '.html'
        notebook_to_html(args.output, html, dotdict({"template": "sos-report"}),
                         ["--Application.log_level='CRITICAL'"])

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
    p.add_argument('-o', metavar = "str", dest = 'output', required = True,
                   help = '''Output notebook / data file name.
                   In query applications if file name ends with ".rds" then only data file will be saved
                   as result of query. Otherwise both data file and a notebook that displays the data
                   will be saved.''')
    p.add_argument('--limit', metavar = 'N', type = int, default = -1,
                   help='''Number of rows to display for tables. Default is to display it for all rows
                   (will result in very large HTML output for large benchmarks).''')
    p.add_argument('--title', metavar = 'str', required = True,
                   help='''Title for notebook file.''')
    p.add_argument('--description', metavar = 'str', nargs = '+',
                   help='''Text to add under notebook title. Each string is a standalone paragraph.''')
    p.add_argument('-t', '--target', metavar = "WHAT", nargs = '+',
                   help = '''Query targets.''')
    p.add_argument('-c', '--condition', metavar = "WHERE", nargs = '+',
                   help = '''Query conditions.''')
    p.add_argument('--language', metavar = 'str', choices = ['R', 'Python3'],
                   help='''Language kernel to switch to for follow up analysis in notebook generated.''')
    p.add_argument('--addon', metavar = 'str', nargs = '+',
                   help='''Scripts to load to the notebooks for follow up analysis.
                   Only usable in conjunction with "--language".''')
    p.add_argument('--no-html', action = 'store_true', dest = 'no_html',
                   help='''Do not export to HTML format.''')
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
