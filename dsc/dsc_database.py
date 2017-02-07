#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
import sys, os, msgpack, json, yaml, re, glob
from collections import OrderedDict
import pandas as pd
import numpy as np
from sos.utils import Error
from sos.__main__ import cmd_remove
from .utils import load_rds, save_rds, \
     flatten_list, no_duplicates_constructor, \
     cartesian_list, extend_dict, dotdict

yaml.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, no_duplicates_constructor)

def remove_obsolete_db(fid, additional_files = []):
    map_db = '.sos/.dsc/{}.map.mpk'.format(fid)
    if os.path.isfile(map_db):
        map_data = msgpack.unpackb(open(map_db, 'rb').read(), encoding = 'utf-8')
    else:
        map_data = {}
    # Remove file signature when files are deleted
    to_remove = []
    for k, x in map_data.items():
        x = os.path.join(fid, x)
        if not os.path.isfile(x):
            to_remove.append(x)
    # Additional files to remove
    for x in additional_files:
        if not os.path.isfile(x):
            to_remove.append(x)
    if len(to_remove):
        cmd_remove(dotdict({"tracked": False, "untracked": False,
                            "targets": to_remove, "__dryrun__": False,
                            "__confirm__": True, "signature": True, "verbosity": 0}), [])


def build_config_db(input_files, io_db, map_db, conf_db, vanilla = False):
    '''
    - collect all output file names in md5 style
    - check if map file should be loaded, and load it
    - update map file: remove irrelevant entries; add new file name mapping (starting from max index)
    - create conf file based on map file and io file
    '''
    def get_names(data):
        names = []
        lookup = {}
        # 1. collect exec names and ID
        for k in data:
            for k1 in data[k]:
                if k1 == "DSC_IO_" or k1 == "DSC_EXT_":
                    continue
                prefix = [x.split(':', 1)[0] for x in k1.split()]
                prefix.append(prefix.pop(0))
                suffix = [x.split(':', 1)[1] for x in k1.split()]
                suffix.append(suffix.pop(0))
                names.append([prefix, suffix])
                for x, y in zip(prefix, suffix):
                    if x not in lookup:
                        lookup[x] = []
                    lookup[x].append(y.split(':', 1)[0])
        # 2. append index to the [prefix, suffix] list so it becomes list of [prefix, suffix, index]
        for x, y in enumerate(names):
            names[x].append([lookup[xx].index(yy.split(':', 1)[0]) + 1 for xx, yy in zip(y[0], y[1])])
        # 3. construct names
        return sorted(set([('{}:{}'.format(x[0][-1], x[1][-1]),
                            '_'.join(['{}_{}'.format(xx, yy) for xx, yy in zip(x[0], x[2])]) + \
                            '.{}'.format(data[k]["DSC_EXT_"])) for x in names]))


    def update_map(files):
        '''Update maps and write to disk'''
        for item in files:
            if item[0] not in map_data:
                map_data[item[0]] = item[1]
        open(map_db, "wb").write(msgpack.packb(map_data))

    #
    if os.path.isfile(map_db) and not vanilla:
        map_data = msgpack.unpackb(open(map_db, 'rb').read(), encoding = 'utf-8')
    else:
        map_data = {}
    data = OrderedDict()
    for item in input_files:
        data.update(msgpack.unpackb(open(item, "rb").read(), encoding = 'utf-8',
                                    object_pairs_hook = OrderedDict))
    open(io_db, "wb").write(msgpack.packb(data))
    update_map(get_names(data))
    fid = os.path.splitext(os.path.basename(conf_db))[0]
    conf = {}
    for k in data:
        sid, name = k.split(':')
        if sid not in conf:
            conf[sid] = {}
        if name not in conf[sid]:
            conf[sid][name] = {}
        conf[sid][name]['input'] = [os.path.join(fid, map_data[item]) \
                                    for item in data[k]['DSC_IO_'][0]]
        conf[sid][name]['output'] = [os.path.join(fid, map_data[item]) \
                                     for item in data[k]['DSC_IO_'][1]]
    #
    with open(conf_db, 'w') as f:
        f.write(json.dumps(conf))


class ResultDBError(Error):
    """Raised when there is a problem building the database."""
    def __init__(self, msg):
        Error.__init__(self, msg)
        self.args = (msg, )

class ResultDB:
    def __init__(self, db_name, master_names):
        self.name = db_name
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
        self.dat_prefix = '.sos/.dsc/{}'.format(os.path.basename(db_name))
        if os.path.isfile(self.dat_prefix + '.map.mpk'):
            self.maps = msgpack.unpackb(open(self.dat_prefix + '.map.mpk', 'rb').read(),
                                        encoding = 'utf-8')
        else:
            raise ResultDBError("DSC file name database is corrupted!")

    def load_parameters(self):

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
                                              '``{1} (value {3})`` and ``{2} (value {4})``.'.\
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
        save_rds(self.data, self.name + '.rds')


class ResultAnnotator:
    def __init__(self, ann_file, ann_table, dsc_data):
        '''Load master table to be annotated and annotation contents'''
        data = load_rds(dsc_data['DSC']['output'][0] + '.rds')
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
        self.ann = yaml.load(open(ann_file))
        self.dsc = dsc_data
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
                    subtables = self.ann[tag][block]['exec'].split(',')
                else:
                    # we take that all sub tables in this block is involved
                    # and we look for these tables in DSC data
                    subtables = [x[0] for x in self.dsc[block]['meta']['exec']]
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
            if len(return_id) == 0:
                self.msg.append("Cannot find matching entries based on query ``{}``".format(repr(text)))
                res = {}
            else:
                res = {k: [] for k in [x[:-3] for x in self.data[self.master].keys() if x.endswith('_id')]}
                for k in res:
                    if k == self.master[7:]:
                        res[k] = get_output(k, return_id)['return'].tolist()
                    else:
                        target_id = self.data[self.master].loc[
                            self.data[self.master]['{}_id'.format(self.master[7:])].isin(return_id)]['{}_id'.format(k)]
                        res[k] = get_output(k, target_id)['return'].tolist()
            return res
        #
        self.result = {}
        for tag in self.queries:
            self.result[tag] = {}
            for queries in self.queries[tag]:
                self.result[tag] = extend_dict(self.result[tag], run_query(queries))
        open(os.path.join('.sos/.dsc', self.dsc['DSC']['output'][0] + '.{}.tags'.format(self.master[7:])), "wb").\
            write(msgpack.packb(self.result))

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
        for block in self.dsc:
            if block == 'DSC' or (block in lask_blocks and block != self.master[7:]):
                continue
            if isinstance(self.dsc[block]['out'], dict):
                self.dsc[block]['out'] = [y for x, y in self.dsc[block]['out'].items()]
            for item in self.dsc[block]['out']:
                var_menu.append('{}:{}'.format(block, item.split('=')[0].strip()))
        res = {'tags': sorted(self.ann.keys()), 'variables': sorted(var_menu)}
        save_rds(res, os.path.join('.sos/.dsc', self.dsc['DSC']['output'][0] + '.{}.shinymeta.rds'.format(self.master[7:])))


EXTRACT_RDS_R = '''
res = list()
idx = 1
for (item in c(${input!r,})) {
  res[[idx]] = readRDS(item)$${target}
  idx = idx + 1
}
saveRDS(list(${key} = res), ${output!r})
'''

CONCAT_RDS_R = '''
res = do.call(c, lapply(c(${input!r,}), readRDS))
saveRDS(res, ${output!r})
'''


class ResultExtractor:
    def __init__(self, tags, from_table, to_file, targets):
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
        self.ann = msgpack.unpackb(open(tag_file, 'rb').read(), encoding = 'utf-8')
        valid_vars = load_rds(tag_file[:-4] + 'shinymeta.rds')['variables'].tolist()
        if tags is None:
            self.tags = list(self.ann.keys())
        else:
            self.tags = tags
        self.name = os.path.split(tag_file)[1].rsplit('.', 2)[0]
        if to_file is None:
            to_file = self.name + '.{}.rds'.format(self.master)
        self.output = to_file
        self.ann_cache = []
        self.script = []
        # Compose executable job file
        idx = 1
        for item in targets:
            if not item in valid_vars:
                raise ValueError('Invalid input value: ``{}``. \nChoices are ``{}``.'.\
                                 format(item, repr(valid_vars)))
            target = item.split(":")
            for ann in self.tags:
                # Handle union logic
                if not '&&' in ann:
                    input_files = sorted(self.ann[ann][target[0]])
                else:
                    ann = [x.strip() for x in ann.split('&&')]
                    arrays = [self.ann[x][target[0]] for x in ann]
                    ann = '_'.join(ann)
                    input_files = sorted(set.intersection(*map(set, arrays)))
                output_prefix = '_'.join([ann, target[0], target[1]])
                # Compose execution step
                self.ann_cache.append('{}_output'.format(output_prefix))
                self.script.append("[Extracting_{0} ({1}): shared = {{'{1}_output' : 'output'}}]".format(idx, output_prefix))
                self.script.append('parameter: target = {}'.format(repr(target[1])))
                self.script.append('parameter: key = {}'.format(repr(output_prefix)))
                self.script.append('input: {}'.format(','.join([repr("{}/{}.rds".format(self.name, x)) for x in input_files])))
                self.script.append('output: \'{}/ann_cache/{}.rds\''.format(self.name, output_prefix))
                self.script.extend(['R:', EXTRACT_RDS_R])
                idx += 1
        self.script.append("[Extracting_{} (concate RDS)]".format(idx))
        self.script.append('input: {}'.format('+'.join(sorted(self.ann_cache))))
        self.script.append('output: {}'.format(repr(self.output)))
        self.script.extend(['R:', CONCAT_RDS_R])
        self.script = '\n'.join(self.script)
