#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import os, sys
import pandas as pd
from .utils import logger
from .version import __version__

def query(args):
    logger.info("Loading database ...")
    from sos.__main__ import AnswerMachine
    # from sos_notebook.converter import notebook_to_html
    from .query_jupyter import get_database_notebook, get_query_notebook
    from .query_engine import Query_Processor
    from .utils import uniq_list
    am = AnswerMachine(always_yes = args.force)
    if os.path.isfile(args.dsc_output):
        args.dsc_output = os.path.dirname(args.dsc_output)
    args.output = args.output.strip('.')
    db = os.path.join(args.dsc_output, os.path.basename(args.dsc_output) + '.db')
    if args.target is None:
        if not args.output.endswith('.ipynb'):
            fnb = args.output + '.ipynb'
        else:
            fnb = args.output
        if os.path.isfile(fnb) and not am.get("Overwrite existing file \"{}\"?".format(fnb)):
            sys.exit("Aborted!")
        logger.info("Exporting database ...")
        get_database_notebook(db, fnb, args.title, args.description, args.limit)
    else:
        logger.info("Running queries ...")
        qp = Query_Processor(db, args.target, args.condition, args.groups, args.add_path)
        for query in qp.get_queries():
            logger.debug(query)
        # write output
        if not args.output.endswith('.xlsx') and not args.output.endswith('.ipynb') and not args.output.endswith('.csv'):
            fnb = args.output + '.ipynb'
            fxlsx = args.output + '.xlsx'
            fcsv = None
        elif args.output.endswith('.xlsx'):
            fxlsx = args.output
            fnb = fcsv = None
        elif args.output.endswith('.csv'):
            fcsv = args.output
            fnb = fxlsx = None
        else:
            fnb = args.output
            fcsv = None
            fxlsx = args.output[:-6] + '.xlsx'
        if fxlsx is not None and os.path.isfile(fxlsx) and not am.get(f"Overwrite existing file \"{fxlsx}\"?"):
            sys.exit("Aborted!")
        if fnb is not None and os.path.isfile(fnb) and not am.get(f"Overwrite existing file \"{fnb}\"?"):
            sys.exit("Aborted!")
        if fcsv is not None and os.path.isfile(fcsv) and not am.get(f"Overwrite existing file \"{fcsv}\"?"):
            sys.exit("Aborted!")
        if fxlsx is not None:
            writer = pd.ExcelWriter(fxlsx)
            qp.output_table.to_excel(writer, 'Sheet1', index = False)
            if len(qp.output_tables) > 1:
                for table in qp.output_tables:
                    qp.output_tables[table].to_excel(writer, table, index = False)
            writer.save()
            logger.info(f"Query results saved to spreadsheet ``{fxlsx}``".format(fxlsx))
        if fnb is not None:
            desc = (args.description or []) + ['Queries performed for:\n\n* targets: `{}`\n* conditions: `{}`'.\
                                               format(repr(args.target), repr(args.condition))]
            get_query_notebook(fxlsx, qp.get_queries(), fnb, args.title, desc, args.language,
                               uniq_list(args.addon or []), args.limit)
        if fcsv is not None:
            qp.output_table.to_csv(fcsv, index = False)
    logger.info("Extraction complete!")
    if os.path.isfile(args.output + '.ipynb'):
        logger.info("You can use ``jupyter notebook {0}.ipynb`` to open it and run all cells, "\
                    "or run it from command line with ``jupyter nbconvert --to notebook --execute {0}.ipynb`` "\
                    "first, then use ``jupyter notebook {0}.nbconvert.ipynb`` to open it.".\
                    format(args.output))
    #     html = args.output[:-6] + '.html'
    #     notebook_to_html(args.output, html, dotdict([("template", "sos-report")]),
    #                      ["--Application.log_level='CRITICAL'"])

def main():
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, SUPPRESS
    class ArgumentParserError(Exception): pass
    class MyArgParser(ArgumentParser):
        def error(self, message):
            raise ArgumentParserError(message)
    #
    p = MyArgParser(description = "An internal command to extract meta-table for DSC results (requires 'sos-essentials' package to use notebook output).", formatter_class = ArgumentDefaultsHelpFormatter)
    p.add_argument('--debug', action='store_true', help = SUPPRESS)
    p.add_argument('--version', action = 'version', version = __version__)
    p.add_argument('dsc_output', metavar = "DSC output folder", help = 'Path to DSC output.')
    p.add_argument('-o', metavar = "str", dest = 'output', required = True,
                   help = '''Output notebook / data file name.
                   In query applications if file name ends with ".csv", ".ipynb" or ".xlsx" then only data file will be saved
                   as result of query. Otherwise both data file in ".xlsx" format and a notebook that displays the data
                   will be saved.''')
    p.add_argument('--limit', metavar = 'N', type = int, default = -1,
                   help='''Number of rows to display for tables. Default is to display it for all rows
                   (will result in very large HTML output for large benchmarks).''')
    p.add_argument('--title', metavar = 'str', default = 'DSC summary & query',
                   help='''Title for notebook file.''')
    p.add_argument('--description', metavar = 'str', nargs = '+',
                   help='''Text to add under notebook title. Each string is a standalone paragraph.''')
    p.add_argument('-t', '--target', metavar = "WHAT", nargs = '+',
                   help = '''Query targets.''')
    p.add_argument('-c', '--condition', metavar = "WHERE", nargs = '+',
                   help = '''Query conditions.''')
    p.add_argument('-g', '--groups', metavar = "G:A,B", nargs = '+',
                   help = '''Definition of module groups.''')
    p.add_argument('--add-path', dest = 'add_path', action='store_true',
                   help='''Save the complete path of data files, not just the base name of file.''')
    p.add_argument('--language', metavar = 'str', choices = ['R', 'Python3'],
                   help='''Language kernel to switch to for follow up analysis in notebook generated.''')
    p.add_argument('--addon', metavar = 'str', nargs = '+',
                   help='''Scripts to load to the notebooks for follow up analysis.
                   Only usable in conjunction with "--language".''')
    p.add_argument('-f', '--force', action = 'store_true', dest = 'force',
                    help=SUPPRESS)
    p.add_argument('-v', '--verbosity', type = int, choices = list(range(4)), default = 2,
                   help='''Output error (0), warning (1), info (2) and debug (3)
                   information.''')
    p.set_defaults(func = query)
    try:
        args = p.parse_args()
        logger.verbosity = args.verbosity
    except Exception as e:
        logger.info("Please type ``{} -h`` to view available options".\
                        format(os.path.basename(sys.argv[0])))
        logger.error(e)
    #
    try:
        args.func(args)
    except Exception as e:
        if args.debug:
            raise
        logger.error(e)

if __name__ == '__main__':
    main()
