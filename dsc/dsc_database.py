#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
import sys, os, msgpack, json, yaml, re, fnmatch
from collections import OrderedDict
import pandas as pd
import numpy as np
from sos.target import textMD5
from sos.utils import Error
from .utils import load_rds, save_rds, \
     flatten_list, flatten_dict, is_null, \
     no_duplicates_constructor, cartesian_list
import readline
import rpy2.robjects.vectors as RV
import rpy2.rinterface as RI

yaml.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, no_duplicates_constructor)

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


def build_config_db(input_files, io_db, map_db, conf_db, vanilla = False):
    '''
    - collect all output file names in md5 style
    - check if map file should be loaded, and load it
    - based on map file and file names in md5 style, remove irrelevant files from output folder
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

    def remove_obsolete_output(fid):
        # Remove file signature when files are deleted
        runtime_dir = os.path.expanduser('~/.sos/.runtime') \
                      if os.path.isabs(os.path.expanduser(fid)) \
                      else '.sos/.runtime'
        for k, x in map_data.items():
            x = os.path.join(fid, x)
            if not os.path.isfile(x):
                try:
                    os.remove('{}/{}.file_info'.\
                              format(runtime_dir, textMD5(os.path.abspath(os.path.expanduser(x)))))
                except:
                    sys.stderr.write('Obsolete file {} has already been purged!\n'.format(x))

    def update_map(files):
        '''Update maps and write to disk'''
        for item in files:
            if item[0] not in map_data:
                map_data[item[0]] = item[1]
        open(map_db, "wb").write(msgpack.packb(map_data))

    #
    fid = os.path.splitext(os.path.basename(conf_db))[0]
    if os.path.isfile(map_db) and not vanilla:
        map_data = msgpack.unpackb(open(map_db, 'rb').read(), encoding = 'utf-8')
    else:
        map_data = {}
    data = OrderedDict()
    for item in input_files:
        data.update(msgpack.unpackb(open(item, "rb").read(), encoding = 'utf-8',
                                    object_pairs_hook = OrderedDict))
    open(io_db, "wb").write(msgpack.packb(data))
    remove_obsolete_output(fid)
    update_map(get_names(data))
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
                raise ValueError("Please specify the master table to annotate.\nChoices are ``{}``".\
                                 format(repr(self.master)))
            else:
                self.master = self.master[0]
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
        def get_output(col_id, output = None):
            name = self.master[7:] if self.master.startswith('master_') else self.master
            # Get list of files
            lookup = {}
            for x, y in zip(self.data[self.master].query('{}_id == @col_id'.format(name))[name + '_name'].tolist(), col_id):
                if x not in lookup:
                    lookup[x] = []
                lookup[x].append(y)
            results = []
            files = []
            for k, value in lookup.items():
                # Get output columns
                if output:
                    tmp = ['{}_id'.format(name)]
                    tmp.extend(flatten_list([[x for x in fnmatch.filter(self.data[self.master].columns.values, o)]
                                             for o in output]))
                    results.append(self.data[self.master].query('{}_id == @value'.format(name))[tmp])
                else:
                    results.append(pd.DataFrame())
                # Get output files
                files.append(self.data[k].query('step_id == @value')[['step_id', 'return']])
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
        def run_query(text):
            return_id = None
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
                res = []
            else:
                res = get_output(return_id)['return']
            return res
        #
        self.result = {}
        for tag in self.queries:
            self.result[tag] = []
            for queries in self.queries[tag]:
                self.result[tag].extend(run_query(queries))
        open(os.path.join('.sos/.dsc', self.dsc['DSC']['output'][0] + '.{}.tags'.format(self.master)), "wb").\
            write(msgpack.packb(self.result))

    def ShowQueries(self):
        res = 'DSC result ``{}`` has been annotated.\n'.format(self.master[7:])
        for tag in self.queries:
            res += '\tTag ``{}`` created via:\n'.format(tag)
            for item in self.queries[tag]:
                res += "\t{}\n".format(' & '.join(item))
            res += '\n'
        return res.strip()

class ResultExtractor:
    def __init__(self, tags, from_table, from_file, to_file, force = False):
        data = load_rds(from_file + '.rds')
        if from_table is not None:
            self.master = from_table if from_table.startswith('master_') else 'master_{}'.format(from_table)
        else:
            self.master = [k for k in data if k.startswith('master_')]
            if len(self.master) > 1:
                raise ValueError("Please specify the master table to annotate.\nChoices are ``{}``".\
                                 format(repr(self.master)))
            else:
                self.master = self.master[0]
        tag_file = os.path.join('.sos/.dsc', from_file + '.{}.tags'.format(self.master))
        if not os.path.isfile(tag_file):
            raise ValueError("DSC result for ``{}`` has not been annotated. Please use '--annotation' option to annotate the results before running '--extract'.".format(self.master[7:]))
        self.ann = msgpack.unpackb(open(tag_file, 'rb').read(), encoding = 'utf-8')
        if tags is None:
            self.tags = list(self.ann.keys())
        else:
            self.tags = tags
        if to_file is None:
            to_file = from_file + '.extracted.rds'
        if os.path.isfile(to_file):
            if not force:
                raise RuntimeError('File ``{}`` already exists. Please use \'--to\' option to set another file name, or use \'-f\' to force overwrite.'.format(to_file))
        self.output = to_file
        self.name = from_file

    def _extract_rds_array(self, files, k):
        '''
        Given an array of RDS file names, extract given variable from these RDS files
        '''
        result = []
        for f in files:
            rds = '{}/{}.rds'.format(self.name, f)
            if not os.path.isfile(rds):
                continue
            rdata = flatten_dict(load_rds(rds, types = (RV.Array, RV.IntVector, RV.FactorVector,
                                                        RV.BoolVector, RV.FloatVector, RV.StrVector,
                                                        RI.RNULLType)))
            if is_null(rdata[k]) or len(rdata[k]) == 0:
                continue
            elif len(rdata[k].shape) > 2:
                # too large. will just give the name of the file
                result.append(rds)
            elif len(rdata[k]) == 1:
                result.append(rdata[k][0])
            else:
                result.append(rdata[k])
        return result

    def Extract(self, var):
        result = {}
        for ann in self.tags:
            if not "&&" in ann:
                result[ann] = self._extract_rds_array(self.ann[ann], var)
            else:
                ann = [x.strip() for x in ann.split('&&')]
                arrays = [self.ann[x] for x in ann]
                ann = '_'.join(ann)
                result[ann] = self._extract_rds_array(set.intersection(*map(set, arrays)), var)
        save_rds(result, self.output)
