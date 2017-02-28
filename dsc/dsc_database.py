#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
import os, msgpack, yaml, re, glob, pickle
from collections import OrderedDict
import pandas as pd
import numpy as np
from sos.utils import Error
from sos.__main__ import cmd_remove
from .utils import load_rds, save_rds, \
     flatten_list, uniq_list, no_duplicates_constructor, \
     cartesian_list, extend_dict, dotdict, chunks
from multiprocessing import Process, Manager

yaml.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, no_duplicates_constructor)


def remove_obsolete_db(fid, additional_files = []):
    map_db = '.sos/.dsc/{}.map.mpk'.format(fid)
    if os.path.isfile(map_db):
        map_data = msgpack.unpackb(open(map_db, 'rb').read(), encoding = 'utf-8',
                                   object_pairs_hook = OrderedDict)
    else:
        map_data = OrderedDict()
    # Remove file signature when files are deleted
    to_remove = []
    for k, x in list(map_data.items()):
        x = os.path.join(fid, x)
        if not os.path.isfile(x):
            to_remove.append(x)
            del map_data[k]
    # Additional files to remove
    for x in additional_files:
        if not os.path.isfile(x):
            to_remove.append(x)
    if len(to_remove):
        open(map_db, "wb").write(msgpack.packb(map_data))
        cmd_remove(dotdict({"tracked": True, "untracked": True,
                            "targets": to_remove, "__dryrun__": False,
                            "__confirm__": True, "signature": True, "verbosity": 0}), [])


def load_mpk(mpk_files, jobs):
    d = Manager().dict()
    def f(d, x):
        for xx in x:
            d.update(msgpack.unpackb(open(xx, "rb").read(), encoding = 'utf-8',
                                     object_pairs_hook = OrderedDict))
    #
    mpk_files = [x for x in chunks(mpk_files, int(len(mpk_files) / jobs) + 1)]
    job_pool = [Process(target = f, args = (d, x)) for x in mpk_files]
    for job in job_pool:
        job.start()
    for job in job_pool:
        job.join()
    return OrderedDict([(x, d[x]) for x in sorted(d.keys())])


def build_config_db(input_files, io_db, map_db, conf_db, vanilla = False, jobs = 4):
    '''
    - collect all output file names in md5 style
    - check if map file should be loaded, and load it
    - update map file: remove irrelevant entries; add new file name mapping (starting from max index)
    - create conf file based on map file and io file
    '''
    def get_names():
        '''Get map names. Also dedup data object'''
        # names has to be ordered dict to make sure
        # map_data is updated non-randomly
        # return is a list of original name and new name mapping
        names = OrderedDict()
        lookup = {}
        seen = set()
        base_ids = {}
        # 1. collect exec names and ID
        for k in list(data.keys()):
            # handle duplicate output
            # FIXME: what if there is partial overlap?
            # Maybe I should prevent this via checking input script
            tmp_output = str(sorted(data[k]['DSC_IO_'][1]))
            if tmp_output in seen:
                del data[k]
                continue
            else:
                seen.add(tmp_output)
            for k1 in data[k]:
                if k1 in ["DSC_EXT_", "DSC_IO_"]:
                    continue
                k1 = k1.split()[0]
                # step_key example:
                # [('rcauchy.R', '71c60831e6ac5e824cb845171bd19933'),
                # ('mean.R', 'dfb0dd672bf5d91dd580ac057daa97b9'),
                # ('MSE.R', '0657f03051e0103670c6299f9608e939')]
                step_key = uniq_list(reversed([x for x in chunks(k1.split(":"), 2)]))
                if k1 in map_data:
                    k_tmp = tuple([x[0] for x in step_key])
                    if not k_tmp in base_ids:
                        base_ids[k_tmp] = {x:set() for x in k_tmp}
                    for x, y in zip(k_tmp, step_key):
                        base_ids[k_tmp][x].add(y[1])
                    continue
                if k1 in names:
                    raise ValueError('\nIdentical computational procedures found: ``{}``!'.format(k1))
                names[k1] = step_key
                for x in names[k1]:
                    if x[0] not in lookup:
                        lookup[x[0]] = []
                    if x[1] not in lookup[x[0]]:
                        lookup[x[0]].append(x[1])
                names[k1].append(data[k]["DSC_EXT_"])
        for k in names:
            k_tmp = tuple([x[0] for x in names[k][:-1]])
            base_id = base_ids[k_tmp] if k_tmp in base_ids else {}
            # 2. replace the UUID of executable environment with a unique index
            names[k] = [[x[0], str(lookup[x[0]].index(x[1]) + 1 + \
                                   (len(base_id[x[0]]) if x[0] in base_id and x[1] not in base_id[x[0]] else 0))]
                        for x in names[k][:-1]] + [names[k][-1]]
            # 3. construct name map
            names[k] = '_'.join(flatten_list(names[k][:-1])) + '.{}'.format(names[k][-1])
        return names

    def update_map(names):
        '''Update maps and write to disk'''
        map_data.update(names)
        open(map_db, "wb").write(msgpack.packb(map_data))

    def find_representative(files):
        '''Input files are exec1:id1:exec2:id2:exec3:id3:....rds
        need to return a list of non-random representative files having unique pattern
        '''
        seen = set()
        res = []
        for fn in files:
            keys = tuple([x[0] for x in chunks(fn.split(":"), 2)])
            if keys not in seen:
                res.append(fn)
                seen.add(keys)
        return res
    #
    if os.path.isfile(map_db) and not vanilla:
        map_data = msgpack.unpackb(open(map_db, 'rb').read(), encoding = 'utf-8',
                                   object_pairs_hook = OrderedDict)
    else:
        map_data = OrderedDict()
    data = load_mpk(input_files, jobs)
    open(io_db, "wb").write(msgpack.packb(data))
    map_names = get_names()
    update_map(map_names)
    # remove *.conf.mpk extension
    fid = os.path.basename(conf_db)[:-9]
    conf = OrderedDict()
    for k in data:
        sid, name = k.split(':')
        if sid not in conf:
            conf[sid] = OrderedDict()
        if name not in conf[sid]:
            conf[sid][name] = OrderedDict()
        conf[sid][name]['input'] = [os.path.join(fid, map_data[item]) \
                                    for item in data[k]['DSC_IO_'][0]]
        conf[sid][name]['output'] = [os.path.join(fid, map_data[item]) \
                                     for item in data[k]['DSC_IO_'][1]]
        conf[sid][name]['input_repr'] = [os.path.join(fid, map_data[item]) \
                                         for item in find_representative(data[k]['DSC_IO_'][0])]
        conf[sid][name]['output_repr'] = [os.path.join(fid, map_data[item]) \
                                          for item in find_representative(data[k]['DSC_IO_'][1])]
    #
    open(conf_db, "wb").write(msgpack.packb(conf))


class ResultDBError(Error):
    """Raised when there is a problem building the database."""
    def __init__(self, msg):
        Error.__init__(self, msg)
        self.args = (msg, )


class ResultDB:
    def __init__(self, db_name, master_names):
        self.dat_prefix = '.sos/.dsc/{}'.format(db_name)
        # If this is None, then the last block will be used
        # As master table
        self.master_names = master_names
        # different tables; one exec per table
        self.data = {}
        # master tables
        self.master = {}
        # list of exec names that are the last step in sequence
        self.last_block = []
        # key = block name, item = exec name
        self.groups = {}
        if os.path.isfile(self.dat_prefix + '.map.mpk'):
            self.maps = msgpack.unpackb(open(self.dat_prefix + '.map.mpk', 'rb').read(), encoding = 'utf-8',
                                        object_pairs_hook = OrderedDict)
        else:
            raise ResultDBError("DSC file name database is corrupted!")

    def load_parameters(self):
        #
        def search_dependent_index(x):
            res = None
            for ii, kk in enumerate(data.keys()):
                if kk.split()[0] == x:
                    res = ii + 1
                    break
            if res is None:
                raise ResultDBError('Cannot find dependency step for output ``{}``!'.format(x))
            return res
        #
        def find_namemap(x):
            if x in self.maps:
                return os.path.splitext(self.maps[x])[0]
            raise ResultDBError('Cannot find name map for ``{}``'.format(x))
        #
        try:
            data_all = msgpack.unpackb(open(self.dat_prefix + ".io.mpk", "rb").read(),
                                    encoding = 'utf-8', object_pairs_hook = OrderedDict)
        except:
            raise ResultDBError('Cannot load source data to build database!')
        seen = []
        data = OrderedDict()
        for k0 in data_all.keys():
            for k in list(data_all[k0].keys()):
                if k == 'DSC_IO_' or k == 'DSC_EXT_':
                    continue
                if not k in seen:
                    data[k] = data_all[k0][k]
                    seen.append(k)
        for idx, (k, v) in enumerate(data.items()):
            # each v is a dict
            # collect some meta info
            table = v['exec']
            block_name = v['step_name'].split("_")[:-1]
            block_name = '_'.join(block_name[:-1]) if (block_name[-1].isdigit() and len(block_name) > 1) \
                         else '_'.join(block_name)
            if block_name not in self.groups:
                self.groups[block_name] = []
            if table not in self.groups[block_name]:
                self.groups[block_name].append(table)
            is_last_block = (v['step_name'] == v['sequence_name'].split('+')[-1])
            if self.master_names is not None:
                is_last_block = block_name in self.master_names or is_last_block
            if not block_name in self.last_block and is_last_block:
                self.last_block.append(block_name)
            #
            for x in ['step_id', 'return', 'depends']:
                if x in v.keys():
                    v['.{}'.format(x)] = v.pop(x)
            #
            if not table in self.data:
                self.data[table] = {}
                for x in list(v.keys()) + ['step_id', 'return', 'depends']:
                    if x not in ['sequence_id', 'sequence_name', 'step_name', 'exec']:
                        self.data[table][x] = []
            else:
                keys1 = repr(sorted([x for x in v.keys() if not x in
                                     ['sequence_id', 'sequence_name', 'step_name', 'exec']]))
                keys2 = repr(sorted([x for x in self.data[table].keys() if not x in
                                     ['step_id', 'return', 'depends']]))
                if keys1 != keys2:
                    raise ResultDBError('Inconsistent keys between step '\
                                              '``{0} (value {2})`` and ``{1} (value {3})``.'.\
                                              format(idx + 1, keys1, self.data[table]['step_id'], keys2))
            self.data[table]['step_id'].append(idx + 1)
            k = k.split()
            self.data[table]['return'].append(find_namemap(k[0]))
            if len(k) > 1:
                self.data[table]['depends'].append(search_dependent_index(k[-1]))
            else:
                self.data[table]['depends'].append(np.nan)
            for k1, v1 in v.items():
                if k1 not in ['sequence_id', 'sequence_name', 'step_name', 'exec']:
                    self.data[table][k1].append(v1)

    def __find_block(self, step):
        for k in self.groups:
            if step in self.groups[k]:
                return k
        raise ResultDBError('Cannot find ``{}`` in any blocks!'.format(step))

    def __get_sequence(self, step, step_id, step_idx, res):
        '''Input are last step name, ID, and corresponding index (in its data frame)'''
        res.append((step, step_id))
        depend_id = self.data[step]['depends'][step_idx]
        if depend_id is np.nan:
            return
        else:
            idx = None
            step = None
            for k in self.data:
                # try get some idx
                if depend_id in self.data[k]['step_id']:
                    idx = self.data[k]['step_id'].index(depend_id)
                    step = k
                    break
            if idx is None or step is None:
                raise ResultDBError('Cannot find step_id ``{}`` in any tables!'.format(depend_id))
            self.__get_sequence(step, depend_id, idx, res)

    def write_master_table(self, block):
        '''
        Create a master table in DSCR flavor. Columns are:
        name, block1, block1_ID, block2, block2_ID, ...
        I'll create multiple master tables for as many as last steps.
        Also extend the master table to include information from the
        output of last step
        (step -> id -> depend_id -> step ... )_n
        '''
        res = []
        for step in self.groups[block]:
            for step_idx, step_id in enumerate(self.data[step]['step_id']):
                tmp = []
                self.__get_sequence(step, step_id, step_idx, tmp)
                res.append(list(reversed(tmp)))
        data = {}
        for item in res:
            key = tuple([self.__find_block(x[0]) for x in item])
            if key not in data:
                data[key] = [flatten_list([('{}_name'.format(x), '{}_id'.format(x)) for x in key])]
            data[key].append(flatten_list(item))
        for key in data:
            header = data[key].pop(0)
            data[key] = pd.DataFrame(data[key], columns = header)
        return pd.concat([data[key] for key in data], ignore_index = True)

    def Build(self, script = None):
        self.load_parameters()
        for block in self.last_block:
            self.master['master_{}'.format(block)] = self.write_master_table(block)
        tmp = ['step_id', 'depends', 'return']
        for table in self.data:
            cols = tmp + [x for x in self.data[table].keys() if x not in tmp]
            self.data[table] = pd.DataFrame(self.data[table], columns = cols)
        self.data.update(self.master)
        if script is not None:
            self.data['.dscsrc'] = repr(script)
        pickle.dump(self.data, open(self.dat_prefix + '.db', 'wb'))


class ResultAnnotator:
    def __init__(self, ann_files, ann_table, dsc_data):
        '''Load master table to be annotated and annotation contents'''
        self.dsc = dsc_data
        data = pickle.load(open('.sos/.dsc/{}.db'.format(os.path.basename(self.dsc.runtime.output)), 'rb'))
        self.data = {k : pd.DataFrame(v) for k, v in data.items() if k != '.dscsrc'}
        if ann_table is not None:
            self.master = ann_table if ann_table.startswith('master_') else 'master_{}'.format(ann_table)
        else:
            self.master = [k for k in self.data if k.startswith('master_')]
            if len(self.master) > 1:

                raise ValueError("Please specify the DSC block to target, via ``--target``."\
                                 "\nChoices are ``{}``".\
                                 format(repr([x[7:] for x in self.master])))
            else:
                self.master = self.master[0]
        if self.master not in data:
            raise ValueError('Cannot find target block ``{}``.'.format(self.master[7:]))
        self.ann = OrderedDict()
        for ann_file in ann_files:
            ann = yaml.load(open(ann_file))
            if ann is None:
                raise ValueError("Annotation file ``{}`` does not contain proper annotation information!".\
                                 format(ann_file))
            else:
                self.ann.update(ann)
        self.msg = []

    def ConvertAnnToQuery(self):
        '''
        Parse annotations to make pytable syntax
        1. for simple numbers / strings use '==' logic
        2. for list / tuples use "or" logic
        3. for raw queries use a special function ... maybe starting with % sign?
        4. non-master table queries: if `exec` presents use the tables specified and double check the table names; otherwise do it for all the tables in the given block.
        '''
        def get_query(obj, text):
            def to_str(p1):
                p1 = p1.strip()
                try:
                    res = re.search(r'^Asis\((.*?)\)$', p1).group(1)
                    return repr(res)
                except:
                    return repr(repr(p1))

            if isinstance(text, str) and text.startswith('%'):
                return text.lstrip('%')
            else:
                if isinstance(text, list) or isinstance(text, tuple):
                    return ' OR '.join(['{} == {}'.format(obj, x if not isinstance(x, str) else to_str(x)) for x in text])
                else:
                    return '{} == {}'.format(obj, text if not isinstance(text, str) else to_str(text))

        self.queries = {}
        for tag in self.ann:
            self.queries[tag] = []
            # for each tag we need a query
            for block in self.ann[tag]:
                # get subtables
                if 'exec' in self.ann[tag][block]:
                    # we potentially found the sub table to query from
                    subtables = [os.path.splitext(os.path.basename(x))[0] for x in self.ann[tag][block]['exec'].split(',')]
                else:
                    # we take that all sub tables in this block is involved
                    # and we look for these tables in DSC data
                    subtables = [x.name for x in self.dsc.blocks[block].steps]
                # get query
                block_query = []
                for k1 in self.ann[tag][block]:
                    if k1 == 'params':
                        for k2 in self.ann[tag][block][k1]:
                            block_query.append(get_query(k2, self.ann[tag][block][k1][k2]))
                    elif k1 == 'exec':
                        continue
                    else:
                        block_query.append(get_query(k1, self.ann[tag][block][k1]))
                # OR logic for multiple subtables
                self.queries[tag].append(['[{}] {}'.format(table, ' AND '.join(['({})'.format(x) for x in block_query]) if len(block_query) else 'ALL') for table in subtables])
        for tag in self.queries:
            self.queries[tag] = cartesian_list(*self.queries[tag])

    def ApplyAnotation(self):
        '''Run query on result table and make a tag column'''

        def get_id(query, target = None):
            name = self.master[7:] if self.master.startswith('master_') else self.master
            query = query.strip()
            if target is None:
                col_id = self.data[self.master].query(query)[name + '_id'] if query != 'ALL' else self.data[self.master][name + '_id']
                col_id = col_id.tolist()
            else:
                col_id = self.data[target[0]].query(query)['step_id'] if query != 'ALL' else self.data[target[0]]['step_id']
                col_id = [x for x, y in zip(self.data[self.master][name + '_id'].tolist(),
                                            self.data[self.master][target[1][:-5] + '_id'].\
                                            isin(col_id).tolist()) if y]
            return col_id
        #
        def get_output(name, col_id):
            # Get list of file names
            # given name of column and target col_id's
            lookup = {}
            for x, y in zip(self.data[self.master].query('{}_id == @col_id'.format(name))[name + '_name'].tolist(), col_id):
                if x not in lookup:
                    lookup[x] = []
                lookup[x].append(y)
            res = []
            for k, value in lookup.items():
                # FIXME: cannot use `.query('step_id == @value')`
                # because it cannot propagate duplicate values
                # which is what we want to do here
                # implementation below maybe inefficient
                # can be improved in the future
                step_id_list = self.data[k]['step_id'].tolist()
                rows = [step_id_list.index(x) for x in value]
                res.append(self.data[k].iloc[rows][['return']])
            res = pd.concat(res)
            for item in ['{}_id'.format(name), 'step_id']:
                if item in res.columns.values:
                    res.drop(item, axis = 1, inplace = True)
            return res
        #
        def run_query(text):
            return_id = None
            # Get ID for the last step as result of the query
            for item in text:
                pattern = re.search(r'^\[(.*)\](.*)', item)
                if pattern:
                    # query from sub-table
                    for k in self.data[self.master]:
                        if pattern.group(1) in self.data[self.master][k].tolist():
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
            if return_id is None or len(return_id) == 0:
                self.msg.append("Cannot find matching entries based on query ``{}``".format(repr(text)))
                res = {}
            else:
                res = {k: [] for k in [x[:-3] for x in self.data[self.master].keys() if x.endswith('_id')]}
                for k in res:
                    if k == self.master[7:]:
                        res[k] = get_output(k, return_id)['return'].tolist()
                    else:
                        target_id = self.data[self.master].loc[
                            self.data[self.master]['{}_id'.format(self.master[7:])].isin(return_id)]['{}_id'.format(k)].\
                            dropna()
                        if target_id.size:
                            res[k] = get_output(k, target_id)['return'].tolist()
            return res
        #
        self.result = OrderedDict()
        for tag in self.queries:
            self.result[tag] = OrderedDict()
            for queries in self.queries[tag]:
                self.result[tag] = extend_dict(self.result[tag], run_query(queries))
        metafile = os.path.join('.sos/.dsc', self.dsc.runtime.output + '.{}.tags'.format(self.master[7:]))
        open(metafile, "wb").write(msgpack.packb(self.result))
        return metafile

    def ShowQueries(self, verbosity):
        '''Make a table summary of what has been performed'''
        from prettytable import PrettyTable
        res = PrettyTable()
        res.field_names = ["Tag", "No. unique obj.", "Logic"] if verbosity > 2 else ["Tag", "No. unique obj."]
        for tag in sorted(self.queries):
            counts = ['``{}`` {}'.format(len(set(self.result[tag][block])), block) for block in sorted(self.result[tag])]
            if verbosity > 2:
                res.add_row(["``{}``".format(tag), ' & '.join(counts), '\n'.join([' & '.join(item) for item in self.queries[tag]])])
            else:
                res.add_row(["``{}``".format(tag), ' & '.join(counts)])
        res.align = "l"
        return res.get_string(padding_width = 2)

    def SaveShinyMeta(self):
        '''Save some meta info for shinydsc to load'''
        # Get available var menu
        var_menu = []
        lask_blocks = [k[7:] for k in self.data if k.startswith('master_')]
        for block in self.dsc.blocks:
            if block in lask_blocks and block != self.master[7:]:
                continue
            # FIXME: Here assuming all steps have the same output variables
            for item in set(flatten_list([list(step.rv.keys()) for step in self.dsc.blocks[block].steps])):
                var_menu.append('{}:{}'.format(block, item))
        res = {'tags': sorted(self.ann.keys()), 'variables': sorted(var_menu)}
        metafile = os.path.join('.sos/.dsc', self.dsc.runtime.output + '.{}.shinymeta.rds'.format(self.master[7:]))
        save_rds(res, metafile)
        return metafile


EXTRACT_RDS_R = '''
res = list()
keys = c(${key!r,})
for (key in keys) {
  res[[key]] = list()
}
targets = c(${target!r,})
f_counter = 1
for (item in c(${input!r,})) {
  tryCatch({
    dat = readRDS(item)
    for (idx in length(targets)) {
       res[[keys[idx]]][[f_counter]] = dat[[targets[idx]]]
       res[[paste0('DSC_TIMER_', keys[idx])]][[f_counter]] = dat$DSC_TIMER[1]
    }
  }, error = function(e) {})
  for (idx in length(targets)) {
    if (length(res[[keys[idx]]]) < f_counter) {
      res[[keys[idx]]][[f_counter]] = item
      res[[paste0('DSC_TIMER_', keys[idx])]][[f_counter]] = NA
    }
  }
  f_counter = f_counter + 1
}
saveRDS(res, ${output!r})
'''

CONCAT_RDS_R = '''
res = do.call(c, lapply(c(${input!r,}), readRDS))
saveRDS(res, ${output!r})
'''


class ResultExtractor:
    def __init__(self, tags, from_table, to_file, targets_list):
        tag_file = glob.glob('.sos/.dsc/*.tags')
        tables = [x.split('.')[-2] for x in tag_file]
        if len(tag_file) == 0:
                raise ValueError("DSC result has not been annotated. Please use ``-a`` option to annotate the results before running ``-e``.")
        if from_table is not None and not from_table in tables:
                raise ValueError("DSC result for ``{}`` has not been annotated. Please use ``-a`` option to annotate the results before running ``-e``.".format(from_table))
        if len(tag_file) == 1:
            # we have a unique table to extract from
            self.master = tag_file[0].split('.')[-2]
        else:
            if from_table:
                self.master = from_table
            else:
                raise ValueError("Please specify the DSC block to target, via ``--target``."\
                                 "\nChoices are ``{}``".\
                                 format(repr([x.split('.')[-2] for x in tag_file])))
        tag_file = tag_file[tables.index(self.master)]
        self.ann = msgpack.unpackb(open(tag_file, 'rb').read(), encoding = 'utf-8',
                                   object_pairs_hook = OrderedDict)
        valid_vars = load_rds(tag_file[:-4] + 'shinymeta.rds')['variables'].tolist()
        if tags is None:
            self.tags = {x:x for x in self.ann.keys()}
        else:
            self.tags = {}
            for tag in tags:
                tag = tag.strip().strip('=') # add this line in case shinydsc gives empty tag alias
                if "=" in tag:
                    if len(tag.split('=')) != 2:
                        raise ValueError("Invalid tag syntax ``{}``!".format(tag))
                    self.tags[tag.split('=')[0].strip()] = tag.split('=')[1].strip()
                else:
                    self.tags['_'.join([x.strip() for x in tag.split('&&')])] = tag
        self.name = os.path.split(tag_file)[1].rsplit('.', 2)[0]
        if to_file is None:
            to_file = self.name + '.{}.rds'.format(self.master)
        self.output = to_file
        self.ann_cache = []
        self.script = []
        # Organize targets
        targets = {}
        for item in targets_list:
            if not item in valid_vars:
                raise ValueError('Invalid input value: ``{}``. \nChoices are ``{}``.'.\
                                 format(item, repr(valid_vars)))
            target = item.split(":")
            if target[0] not in targets:
                targets[target[0]] = []
            targets[target[0]].append(target[1])
        # Compose executable job file
        for key, item in targets.items():
            for tag, ann in self.tags.items():
                # Handle union logic
                if not '&&' in ann:
                    input_files = sorted(self.ann[ann][key])
                else:
                    arrays = [self.ann[x.strip()][key] for x in ann.split('&&')]
                    input_files = sorted(set.intersection(*map(set, arrays)))
                input_files = flatten_list([glob.glob("{}/{}.*".format(self.name, x)) for x in input_files])
                output_prefix = ['_'.join([tag, key, x]) for x in item]
                step_name = '{}_{}'.format(tag, key)
                output_file = '{}/ann_cache/{}.rds'.format(self.name, step_name)
                # Compose execution step
                self.ann_cache.append(output_file)
                self.script.append("[{0}: provides = '{1}']".\
                                   format(step_name, output_file))
                self.script.append('parameter: target = {}'.format(repr(item)))
                self.script.append('parameter: key = {}'.format(repr(output_prefix)))
                self.script.append('input: {}'.format(','.join([repr(x) for x in input_files])))
                self.script.append('output: \'{}\''.format(output_file))
                self.script.extend(['R:', EXTRACT_RDS_R])
        self.script.append("[Extracting (concatenate RDS)]")
        self.script.append('depends: {}'.format(','.join([repr(x) for x in sorted(self.ann_cache)])))
        self.script.append('input: {}'.format(','.join([repr(x) for x in sorted(self.ann_cache)])))
        self.script.append('output: {}'.format(repr(self.output)))
        self.script.extend(['R:', CONCAT_RDS_R])
        self.script = '\n'.join(self.script)
