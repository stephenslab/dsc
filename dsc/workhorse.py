#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import os, sys, atexit, re, yaml, fnmatch
import pandas as pd
from collections import OrderedDict
from pysos.sos_script import SoS_Script
from pysos.sos_executor import Sequential_Executor
from pysos.utils import env, get_traceback
from .dsc_file import DSCData
from .dsc_steps import DSCJobs, DSC2SoS
from .dsc_database import ResultDB, ConfigDB
from .utils import get_slice, load_rds, flatten_list, yaml2html, dsc2html

def sos_run(args, workflow_args, verbosity = 1, jobs = None,
            sig_mode = 'default', run_mode = 'run', transcript = None):
    env.max_jobs = args.__max_jobs__ if jobs is None else jobs
    # kill all remainging processes when the master process is killed.
    atexit.register(env.cleanup)
    if args.__rerun__:
        sig_mode = 'ignore'
    try:
        script = SoS_Script(content=args.script)
        executor = Sequential_Executor(script.workflow(args.workflow), transcript = transcript)
        executor.run(workflow_args, cmd_name=args.dsc_file, config_file = args.__config__,
                     run_mode = run_mode, sig_mode = sig_mode, verbosity = verbosity)
    except Exception as e:
        if verbosity and verbosity > 2:
            sys.stderr.write(get_traceback())
        env.logger.error(e)
        sys.exit(1)
    env.verbosity = args.verbosity

def execute(args, argv):
    def setup():
        args.workflow = 'DSC'
        args.__config__ = None
        dsc_data = DSCData(args.dsc_file, sequence = args.sequence, output = args.output)
        db_name = os.path.basename(dsc_data['DSC']['output'][0])
        db_dir = os.path.dirname(dsc_data['DSC']['output'][0])
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        os.makedirs('.sos/.dsc/md5', exist_ok = True)
        dsc_jobs = DSCJobs(dsc_data)
        if args.verbosity > 3:
            yaml2html(str(dsc_data), '.sos/.dsc/{}.data'.format(db_name), title = 'DSC data')
            yaml2html(str(dsc_jobs), '.sos/.dsc/{}.jobs'.format(db_name), title = 'DSC jobs')
        run_jobs = DSC2SoS(dsc_jobs)
        if args.verbosity > 3:
            yaml2html(str(run_jobs), '.sos/.dsc/{}.exec'.format(db_name), title = 'DSC runs')
        # master block for output
        try:
            master = dsc_data['DSC']['master']
        except:
            master = None
        return run_jobs, OrderedDict([(k, dsc_jobs.master_data[k]) for k in dsc_jobs.ordering]), dsc_data['DSC']['output'][0], db_name, master
    #
    if args.sequence:
        env.logger.info("Load command line DSC sequence: ``{}``".format(', '.join(args.sequence)))
    env.logger.info("Constructing DSC from ``{}`` ...".format(args.dsc_file))
    run_jobs, section_content, db, db_name, master = setup()
    # Setup run for config files
    for idx, script in enumerate(run_jobs.confstr):
        args.script = script
        sos_run(args, argv, verbosity = 0, jobs = 1, sig_mode = 'ignore', run_mode = 'inspect',
                transcript = '.sos/.dsc/{}.{}.io.tmp'.format(db_name, idx + 1))
    ConfigDB(db, vanilla = args.__rerun__).Build()
    # Archive scripts
    dsc_script = open(args.dsc_file).read()
    dsc2html(dsc_script, os.path.splitext(args.dsc_file)[0] + '.html',
             title = args.dsc_file, section_content = section_content)
    env.logger.info("DSC exported to ``{}``".format(os.path.splitext(args.dsc_file)[0] + '.html'))
    if args.__dryrun__:
        # FIXME save transcript
        return
    # Wetrun
    env.logfile = os.path.splitext(args.dsc_file)[0] + '.log'
    if os.path.isfile(env.logfile): os.remove(env.logfile)
    args.__config__ = '.sos/.dsc/{}.conf'.format(os.path.basename(db))
    for script in run_jobs.jobstr:
        args.script = script
        sos_run(args, argv, verbosity = (args.verbosity - 1 if args.verbosity > 0 else args.verbosity))
    # Extracting information as much as possible
    # For RDS files if the values are trivial (single numbers) I'll just write them here
    env.logger.info("Building output database ``{0}.rds`` ...".format(db))
    ResultDB(db, master).Build(script = dsc_script)
    env.logger.info("DSC complete!")

def remove(args, argv):
    env.verbosity = args.verbosity
    dsc_data = DSCData(args.dsc_file, output = args.output)
    dsc_jobs = DSCJobs(dsc_data)
    filename = os.path.basename(dsc_data['DSC']['output'][0]) + '.rds'
    if not os.path.isfile(filename):
        raise ValueError('Cannot remove output because DSC database ``{}`` is not found!'.format(filename))
    to_remove = []
    for item in args.step:
        block, step_idx = get_slice(item, mismatch_quit = False)
        removed = False
        for sequence in dsc_jobs.data:
            for steps in sequence:
                for step in steps:
                    if step['name'] == block and (step_idx is None or step_idx == step['exe_index']):
                        tmp = re.sub(r'[^\w' + '_.' + ']', '_', step['exe'])
                        if tmp not in to_remove:
                            to_remove.append(tmp)
                        removed = True
        if removed is False:
            env.logger.warning('Cannot find step ``{}`` in DSC run sequence defined in ``{}``; '\
                               'thus not processed.'.format(item, args.dsc_file))
    #
    data = load_rds(filename)
    files_to_remove = ' '.join([' '.join([os.path.join(dsc_data['DSC']['output'][0], '{}.*'.format(x))
                                          for x in data[item]['return']])
                                for item in to_remove if item in data])
    map_to_remove = flatten_list([data[item]['return'].tolist() for item in to_remove if item in data])
    # delete files from disk
    os.system('rm -f {}'.format(files_to_remove))
    env.logger.debug('Removing files ``{}``'.format(repr(map_to_remove)))
    # delete files from map file
    filename = os.path.basename(dsc_data['DSC']['output'][0])
    if os.path.isfile('.sos/.dsc/{}.map'.format(filename)):
        maps = yaml.load(open('.sos/.dsc/{}.map'.format(filename)))
        for k, v in list(maps.items()):
            if k == 'NEXT_ID':
                continue
            if os.path.splitext(v)[0] in map_to_remove:
                del maps[k]
        with open('.sos/.dsc/{}.map'.format(filename), 'w') as f:
            f.write(yaml.dump(maps, default_flow_style=True))
    env.logger.info('Force removed ``{}`` files from ``{}``.'.\
                    format(len(map_to_remove),
                           os.path.abspath(os.path.expanduser(dsc_data['DSC']['output'][0]))))

def query(args, argv):
    def get_id(query, target = None):
        name = master[7:] if master.startswith('master_') else master
        if target is None:
            col_id = data[master].query(query)[name + '_id'].tolist()
        else:
            col_id = [x for x, y in zip(data[master][name + '_id'].tolist(),
                                        data[master][target[1][:-5] + '_id'].\
                                        isin(data[target[0]].query(query)['step_id']).tolist()) if y]
        return col_id
    #
    def get_output(col_id):
        name = master[7:] if master.startswith('master_') else master
        # Get list of files
        lookup = {}
        for x, y in zip(data[master].query('{}_id == @col_id'.format(name))[name + '_name'].tolist(), col_id):
            if x not in lookup:
                lookup[x] = []
            lookup[x].append(y)
        results = []
        files = []
        for k, value in lookup.items():
            # Get output columns
            if output:
                tmp = ['{}_id'.format(name)]
                tmp.extend(flatten_list([[x for x in fnmatch.filter(data[master].columns.values, o)]
                                         for o in output]))
                results.append(data[master].query('{}_id == @value'.format(name))[tmp])
            else:
                results.append(pd.DataFrame())
            # Get output files
            files.append(data[k].query('step_id == @value')[['step_id', 'return']])
        res = []
        for dff, dfr in zip(files, results):
            if len(dfr.columns.values) > 2:
                res.append(pd.merge(dff, dfr, left_on = '{}_id'.format(name), right_on = 'step_id'))
            else:
                res.append(dff.drop('step_id', axis = 1))
        res = pd.concat(res)
        for item in ['{}_id'.format(name), 'step_id']:
            if item in res.columns.values:
                res.drop(item, axis = 1, inplace = True)
        return res
    #
    master = args.master if args.master.startswith('master_') else 'master_{}'.format(args.master)
    queries = args.queries
    output = args.output
    data = {k : pd.DataFrame(v) for k, v in load_rds(args.dsc_db).items() if k != '.dscsrc'}
    #
    return_id = None
    for item in queries:
        pattern = re.search(r'^\[(.*)\](.*)', item)
        if pattern:
            # query from sub-table
            for k in data[master]:
                if pattern.group(1) in data[master][k].tolist():
                    if return_id is None:
                        return_id = get_id(pattern.group(2).strip(), (pattern.group(1).strip(), k))
                    else:
                        return_id = [x for x in get_id(pattern.group(2).strip(),
                                                       (pattern.group(1).strip(), k))
                                     if x in return_id]
                    break
                else:
                    continue
        else:
            # query from master table
            if return_id is None:
                return_id = get_id(item)
            else:
                return_id = [x for x in get_id(item) if x in return_id]
    if len(return_id) == 0:
        env.logger.warning("Cannot find matching entries based on query ``{}``".format(repr(args.queries)))
    else:
        res = get_output(return_id)
        res.to_csv(sys.stdout, index = False, header = not args.no_header)
