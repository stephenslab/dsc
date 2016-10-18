#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
import os, msgpack, json
from collections import OrderedDict
import pandas as pd
import numpy as np
from pysos.utils import Error
from .utils import load_rds, save_rds, \
     flatten_list, flatten_dict, is_null
import readline
import rpy2.robjects.vectors as RV
import rpy2.rinterface as RI

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
                if k == 'DSC_IO_':
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


    def cbind_output(self, name, table):
        '''
        For output from the last step of sequence (the output we ultimately care),
        if output.rds exists, then try to read that RDS file and
        dump its values to parameter space if it is "simple" enough (i.e., 1D vector at the most)
        load those values to the master table directly. Keep the parameters
        of those steps separate.
        '''
        data = []
        colnames = None
        previous_rds = None
        previous_step = None
        for step, idx in zip(table['{}_name'.format(name)], table['{}_id'.format(name)]):
            rds = '{}/{}.rds'.format(self.name,
                                     self.data[step]['return'][self.data[step]['step_id'].index(idx)])
            if previous_rds is None:
                previous_rds = rds
            if previous_step is None:
                previous_step = step
            if not os.path.isfile(rds):
                continue
            rdata = flatten_dict(load_rds(rds, types = (RV.Array, RV.IntVector, RV.FactorVector,
                                                        RV.BoolVector, RV.FloatVector, RV.StrVector,
                                                        RI.RNULLType)))
            tmp_colnames = []
            values = []
            for k in sorted(rdata.keys()):
                if is_null(rdata[k]) or len(rdata[k]) == 0:
                    continue
                elif len(rdata[k].shape) > 1:
                    continue
                elif len(rdata[k]) == 1:
                    tmp_colnames.append(k)
                    values.append(rdata[k][0])
                else:
                    tmp_colnames.extend(['{}_{}'.format(k, idx + 1) for idx in range(len(rdata[k]))])
                    values.extend(rdata[k])
            if len(values) == 0:
                return table
            if colnames is None:
                colnames = tmp_colnames
            else:
                if colnames != tmp_colnames:
                    raise ResultDBError('``{0}`` from ``{1}`` (in file ``{2}``, len({0}) = {3}) and '\
                                        '``{4}`` (in file ``{5}``, len({0}) = {6}) are inconsistent!'.\
                                        format(colnames[0].split("_")[0], step, rds, len(tmp_colnames),
                                               previous_step, previous_rds, len(colnames)))
            data.append([idx] + values)
            previous_rds = rds
            previous_step = step
        # Now bind data to table, by '{}_id'.format(name)
        if data:
            return pd.merge(table, pd.DataFrame(data, columns = ['{}_id'.format(name)] + colnames),
                            on = '{}_id'.format(name), how = 'outer')
        else:
            return table

    def Build(self, script = None):
        self.load_parameters()
        for block in self.last_block:
            self.master['master_{}'.format(block)] = self.write_master_table(block)
        for item in self.master:
            self.master[item] = self.cbind_output(item.split('_', 1)[1], self.master[item])
        tmp = ['step_id', 'depends', 'return']
        for table in self.data:
            cols = tmp + [x for x in self.data[table].keys() if x not in tmp]
            self.data[table] = pd.DataFrame(self.data[table], columns = cols)
        self.data.update(self.master)
        if script is not None:
            self.data['.dscsrc'] = repr(script)
        save_rds(self.data, self.name + '.rds')

class ConfigDB:
    def __init__(self, db_name, vanilla = False):
        '''
        - collect all output file names in md5 style
        - check if map file should be loaded, and load it
        - based on map file and file names in md5 style, remove irrelevant files from output folder
        - update map file: remove irrelevant entries; add new file name mapping (starting from max index)
        - create conf file based on map file and io file
        '''
        def get_names():
            names = []
            for k in self.data:
                for k1 in self.data[k]:
                    if k1 != "DSC_IO_":
                        prefix = [x.split(':')[0] for x in k1.split()]
                        prefix.append(prefix.pop(0))
                        prefix = '_'.join(prefix)
                        names.append((prefix, k1.split()[0]))
            return sorted(set(names))
        #
        self.name = db_name
        self.dat_prefix = '.sos/.dsc/{}'.format(os.path.basename(db_name))
        if os.path.isfile(self.dat_prefix + '.map.mpk') and not vanilla:
            self.maps = msgpack.unpackb(open(self.dat_prefix + '.map.mpk', 'rb').read(),
                                        encoding = 'utf-8')
        else:
            self.maps = {}
        self.data = OrderedDict()
        for item in [x.strip() for x in open(self.dat_prefix + ".io").readlines()]:
            self.data.update(msgpack.unpackb(open("{}.{}.mpk".format(self.dat_prefix, item), "rb").read(),
                                             encoding = 'utf-8', object_pairs_hook = OrderedDict))
            os.remove("{}.{}.mpk".format(self.dat_prefix, item))
        open("{}.io.mpk".format(self.dat_prefix), "wb").write(msgpack.packb(self.data))
        self.files = get_names()

    def RemoveObsoleteOutput(self):
        # Remove file signature when files are deleted
        runtime_dir = os.path.expanduser('~/.sos/.runtime') \
                      if os.path.isabs(os.path.expanduser(self.name)) \
                      else '.sos/.runtime'
        for k, x in self.maps.items():
            if k == 'NEXT_ID':
                continue
            # # Remove obsolete output from map
            # if k not in self.files:
            #     del self.maps[k]
            x = os.path.join(self.name, x)
            if not os.path.isfile(x):
                try:
                    os.remove('{}/{}.file_info'.format(runtime_dir, x))
                except:
                    pass

    def WriteMap(self):
        '''Update maps and write to disk'''
        start_id = self.maps['NEXT_ID'] if 'NEXT_ID' in self.maps else 1
        for item in self.files:
            if item[1] not in self.maps:
                self.maps[item[1]] = '{}_{}{}'.format(item[0], start_id, os.path.splitext(item[1])[1])
                start_id += 1
        self.maps['NEXT_ID'] = start_id
        open(self.dat_prefix + ".map.mpk", "wb").write(msgpack.packb(self.maps))

    def Build(self):
        self.RemoveObsoleteOutput()
        self.WriteMap()
        data = {}
        for k in self.data:
            sid, name = k.split(':')
            if sid not in data:
                data[sid] = {}
            if name not in data[sid]:
                data[sid][name] = {}
            data[sid][name]['input'] = [os.path.join(self.name, self.maps[item]) \
                                        for item in self.data[k]['DSC_IO_'][0]]
            data[sid][name]['output'] = [os.path.join(self.name, self.maps[item]) \
                                         for item in self.data[k]['DSC_IO_'][1]]
        #
        with open('.sos/.dsc/{}.conf'.format(os.path.basename(self.name)), 'w') as f:
            f.write(json.dumps(data))
