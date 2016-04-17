#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import os, sys, atexit, shutil, re, glob
from .utils import SQLiteMan, round_print, flatten_list
from .dsc_file import DSCData
from .dsc_steps import DSCJobs, DSC2SoS
from .dsc_database import MetaDB
from pysos.utils import env, print_traceback
from pysos.sos_script import SoS_Script

def sos_run(args, workflow_args):
    env.verbosity = args.verbosity
    env.max_jobs = args.__max_jobs__
    # kill all remainging processes when the master process is killed.
    atexit.register(env.cleanup)
    try:
        script = SoS_Script(content=args.script)
        workflow = script.workflow(args.workflow)
        if args.__dryrun__:
            env.run_mode = 'dryrun'
        if args.__rerun__:
            env.sig_mode = 'ignore'
        workflow.run(workflow_args, cmd_name=args.dsc_file)
    except Exception as e:
        if args.verbosity and args.verbosity > 2:
            print_traceback()
        env.logger.error(e)
        sys.exit(1)

def sos_dryrun(args, workflow_args):
    run_mode = env.run_mode
    max_jobs = args.__max_jobs__
    dryrun = args.__dryrun__
    args.__max_jobs__ = 1
    args.__dryrun__ = True
    sos_run(args, workflow_args)
    args.__max_jobs__ = max_jobs
    args.__dryrun__ = dryrun
    env.run_mode = run_mode

def execute(args, argv):
    verbosity = args.verbosity
    env.verbosity = args.verbosity
    # Archive scripts
    env.logger.info("Constructing DSC from ``{}`` ...".format(args.dsc_file))
    if not os.path.exists('.sos/.dsc'):
        os.makedirs('.sos/.dsc')
    dsc_data = DSCData(args.dsc_file)
    db_name = os.path.basename(dsc_data['DSC']['output'][0])
    os.makedirs(os.path.dirname(dsc_data['DSC']['output'][0]), exist_ok=True)
    for item in glob.glob('.sos/.dsc/.*.tmp'):
        os.remove(item)
    dsc_jobs = DSCJobs(dsc_data)
    sos_jobs = DSC2SoS(dsc_jobs, echo = True if args.debug else False)
    if verbosity > 3:
        with open('.sos/.dsc/{}.data'.format(db_name), 'w') as f:
            f.write(str(dsc_data))
        with open('.sos/.dsc/{}.jobs'.format(db_name), 'w') as f:
            f.write(str(dsc_jobs))
        with open('.sos/.dsc/{}.sos'.format(db_name), 'w') as f:
            f.write(str(sos_jobs))
    # Dryrun for sanity checks
    args.workflow = 'DSC'
    args.verbosity = 0
    for script in sos_jobs.data:
        args.script = script
        sos_dryrun(args, argv)
    with open( '.sos/.dsc/{}.yaml'.format(db_name), 'w' ) as fout:
        for file_ in glob.glob('.sos/.dsc/.*.tmp'):
            with open(file_) as fin:
                fout.write(fin.read())
    env.verbosity = args.verbosity = verbosity
    if args.__dryrun__:
        # FIXME export scripts to db_name folder
        return
    # Wetrun
    env.logger.info("Running DSC jobs ...")
    args.verbosity = verbosity - 1 if verbosity > 0 else verbosity
    for script in sos_jobs.data:
        args.script = script
        sos_run(args, argv)
    env.verbosity = args.verbosity = verbosity
    # Extracting information as much as possible
    # For RDS files if the values are trivial (single numbers) I'll just write them here
    env.logger.info("Building summary database ``{0}.csv.gz & {0}.db`` ...".format(dsc_data['DSC']['output'][0]))
    MetaDB(dsc_data['DSC']['output'][0]).build()
    env.logger.info("DSC complete!")

def query(args, argv):
    env.verbosity = args.verbosity
    if not os.path.isfile(args.dsc_db):
        raise IOError('File ``{}`` not found!'.format(args.dsc_db))
    s = SQLiteMan(args.dsc_db)
    fields = sorted(s.getFields('DSC'), key = lambda x: x[0].split('__')[-1])
    field_names = [item[0] for item in fields]
    if len(args.items) == 0:
        # show fields
        env.logger.info("Columns in ``DSC``:")
        print ('\n'.join(['[{}] \033[1m{}\033[0m'.format(x[1], x[0]) for x in fields]))
    else:
        args.items = [x.lower() for x in args.items]
        args.group_by = [x.lower() for x in args.group_by]
        select_query = 'SELECT {} FROM DSC'.format(', '.join(args.group_by + args.items))
        where_query = flatten_list([x.split('AND') for x in args.filter])
        # handle exclude() and include()
        for idx, item in enumerate(where_query):
            groups = re.search('^include\((.*?)\)$', item.lower())
            if groups is not None:
                compiled = re.compile(groups.group(1).replace('*', '(.*?)'))
                where_query[idx] = ' AND '.join(['{} IS NOT NULL'.format(x)
                                    if compiled.search(x)
                                    else '{} IS NULL'.format(x)
                                    for x in [y for y in field_names if not y.endswith('__')]])
            groups = re.search('^exclude\((.*?)\)$', item.lower())
            if groups is not None:
                compiled = re.compile(groups.group(1).replace('*', '(.*?)'))
                where_query[idx] = ' AND '.join(['{} IS NULL'.format(x)
                                    if compiled.search(x)
                                    else '{} IS NOT NULL'.format(x)
                                    for x in [y for y in field_names if not y.endswith('__')]])
        where_query = ' AND '.join(where_query)
        # make sure the items are all not NULL
        fields_involved = []
        for item in args.group_by + args.items:
            # handle function
            groups = re.search('\((.*?)\)', item)
            if groups is not None:
                item = groups.group(1)
            #
            if item in field_names:
                fields_involved.append(item)
        is_null_query = ' AND '.join(['{} IS NOT NULL'.format(item) for item in fields_involved])
        where_query = ' AND '.join([item for item in (where_query, is_null_query) if item])
        if where_query:
            where_query = ' WHERE ' + where_query
        group_query = ' GROUP BY ' + ', '.join(args.group_by) if args.group_by else ''
        order_query = ' ORDER BY ' + ', '.join(fields_involved)
        query = select_query + where_query + group_query + order_query
        env.logger.debug(query)
        text = s.execute(query, display = False, delimiter = args.delimiter)
        if not text:
            env.logger.warning('No results found. If you are expecting otherwise, please ensure proper filter is applied and queried parameters co-exists in the same DSC sequence.')
        else:
            print(args.delimiter.join(args.group_by + (args.items if args.items != ['*'] else field_names)))
            round_print(text, args.delimiter, pc = args.precision)
