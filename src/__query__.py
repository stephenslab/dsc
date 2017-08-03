#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import os, sys
import warnings
warnings.filterwarnings("ignore")
from sos.utils import env, get_traceback
from sos.jupyter.converter import notebook_to_html
from .utils import dotdict
from . import VERSION
from .query_jupyter import get_database_summary, get_query_summary
from .query_engine import Query_Processor

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
    from sos.__main__ import AnswerMachine
    _AM_ = AnswerMachine()
    if os.path.isfile(args.dsc_output):
        args.dsc_output = os.path.dirname(args.dsc_output)
    db = os.path.join(args.dsc_output, os.path.basename(args.dsc_output) + '.db')
    if not args.output.endswith('.ipynb'):
        args.output = args.output.strip('.') + '.ipynb'
    if os.path.isfile(args.output) and not _AM_.get("Overwrite existing file \"{}\"?".format(args.output)):
        sys.exit("Aborted!")
    if args.target is None:
        env.logger.info("Exporting database ...")
        get_database_summary(db, args.output, args.title, args.description)
    else:
        env.logger.info("Running queries ...")
        from .yhat_sqldf import sqldf, PandaSQLException as SQLError
        _QP_ = Query_Processor(db, args.target, args.condition)
        globals().update(**(_QP_.get_data()))
        try:
            output = [sqldf(query) for query in _QP_.get_queries()]
        except SQLError as e:
            raise(e)
        fout = args.output[:-6] + '.db'
        if fout == _QP_.db:
            fout = fout[:-3] + '.extracted.db'
        if os.path.isfile(fout) and not _AM_.get("Overwrite existing file \"{}\"?".format(fout)):
            sys.exit("Aborted!")
        import pickle
        pickle.dump({"data": output, "queries": _QP_.get_queries()}, open(fout, 'wb'))
        env.logger.info("Exporting results ...")
        desc = (args.description or []) + ['Queries performed for:\n\n* targets: `{}`\n* conditions: `{}`'.\
                                                   format(repr(args.target), repr(args.condition))]
        get_query_summary(fout, args.output, args.title, desc)
    #
    env.logger.info("Export complete. You can use ``jupyter notebook {0}`` to open it.".format(args.output))
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
    p.add_argument('-t', '--target', metavar = "str", nargs = '+',
                   help = '''Query targets.''')
    p.add_argument('-c', '--condition', metavar = "WHERE",
                   help = '''Query condition.''')
    p.add_argument('-o', metavar = "str", dest = 'output', required = True,
                   help = '''Output notebook / data file prefix.''')
    p.add_argument('--title', metavar = 'str', required = True,
                   help='''Title for notebook file.''')
    p.add_argument('--description', metavar = 'str', nargs = '+',
                   help='''Text to add under notebook title. Each string is a standalone paragraph.''')
    p.add_argument('--kernel', metavar = 'str', choices = ['R', 'Python', 'ir'],
                   help='''Language kernel to switch to for follow up analysis in notebook generated.''')
    p.add_argument('--addon', metavar = 'str', nargs = '+',
                   help='''Scripts to load to the notebooks for follow up analysis.''')
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
