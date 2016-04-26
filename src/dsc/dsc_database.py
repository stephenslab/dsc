#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
import os, yaml, json, gzip, glob
from collections import OrderedDict
import pandas as pd
from copy import deepcopy
from pysos.utils import Error
from .utils import load_rds, SQLiteMan, sos_pair_input, ordered_load
import readline
import rpy2.robjects.vectors as RV

class ResultDBError(Error):
    """Raised when there is a problem building the database."""
    def __init__(self, msg):
        Error.__init__(self, msg)
        self.args = (msg, )

class ResultDB:
    def __init__(self, db_name):
        self.name = db_name
        self.data = {}

    def __load_parameters(self):
        def search_dependent_index(x):
            res = None
            for ii, kk in enumerate(data.keys()):
                if kk.split('_')[1] == x:
                    res = ii + 1
                    break
            if res is None:
                raise ResultDBError('Cannot find dependency step for output ``{}``!'.format(x))
            return res
        #
        try:
            data = OrderedDict()
            for item in glob.glob('.sos/.dsc/*{}.yaml.tmp'.format(os.path.basename(self.name))):
                with open(item) as f: data.update(ordered_load(f, yaml.SafeLoader))
        except FileNotFoundError:
            raise ResultDBError('Cannot load source data to build database!')
        seen = []
        for k in list(data.keys()):
            k1 = '_'.join(k.split('_')[1:])
            if not k1 in seen:
                seen.append(k1)
            else:
                del data[k]
        for idx, (k, v) in enumerate(data.items()):
            # each v is a dict
            for x in ['step_id', 'return', 'depends']:
                if x in v.keys():
                    v['.{}'.format(x)] = v.pop(x)
            table = v['exec']
            if not table in self.data:
                self.data[table] = {}
                for x in list(v.keys()) + ['step_id', 'return', 'depends']:
                    if x not in ['sequence_id', 'sequence_name', 'step_name', 'exec']:
                        self.data[table][x] = []
            else:
                keys1 = repr(sorted([x for x in v.keys() if not x in ['sequence_id', 'sequence_name', 'step_name', 'exec']]))
                keys2 = repr(sorted([x for x in self.data[table].keys() if not x in ['step_id', 'return', 'depends']]))
                if keys1 != keys2:
                    raise ResultDBError('Inconsistent keys between step '\
                                              '``{1} (value {3})`` and ``{2} (value {4})``.'.\
                                              format(idx + 1, keys1, self.data[table]['step_id'], keys2))
            self.data[table]['step_id'].append(str(idx + 1))
            k = k.split('_')
            self.data[table]['return'].append(k[1])
            if len(k) > 2:
                depends = [str(search_dependent_index(x)) for x in k[2:]]
                self.data[table]['depends'].append(','.join(depends))
            else:
                self.data[table]['depends'].append(None)
            for k1, v1 in v.items():
                if k1 not in ['sequence_id', 'sequence_name', 'step_name', 'exec']:
                    self.data[table][k1].append(v1)

    def __load_output(self):
        '''
        For each output, if output.rds exists, then try to read that RDS file and
        dump its values to parameter space if it is "simple" enough (i.e., is int, float or str)
        '''
        for k, v in self.data.items():
            for item in v['__output__']:
                rds = '{}/{}.rds'.format(self.name, item)
                if not os.path.isfile(rds):
                    continue
                rdata = load_rds(rds, types = (RV.Array, RV.IntVector, RV.FloatVector, RV.StrVector))
                for k1, v1 in rdata.items():
                    # a "simple" object
                    if len(v1) == 1 and '{}__{}'.format(k1, v['exec']) not in v:
                        self.data[k]['{}__{}'.format(k1, v['exec'])] = v1[0]

    def __search_dependent_idxes(self, v, data, res):
        '''Input is name if an input file for a step.
        We look for its dependent steps, i.e., another step
        whose output is this file. And for that step we check its
        input and decide again the dependency,
        until no input can be tracked back to.'''
        for k in data.keys():
            if v in data[k]['__output__']:
                res.append(k)
                for vv in data[k]['__input__']:
                    self.__search_dependent_idxes(vv, data, res)

    def __expand(self):
        '''
        For entries involving __input__, for each input, find the other entry with corresponding
        output and copy the parameters (except exec, __input__ and __output__) over.
        '''
        #
        for k in list(self.data.keys()):
            del self.data[k]['exec']
        data = deepcopy(self.data)
        for k in list(self.data.keys()):
            for item in self.data[k]['__input__']:
                idxes = []
                self.__search_dependent_idxes(item, data, idxes)
                if len(idxes) == 0:
                    continue
                # all its dependent steps
                for idx in idxes:
                    for k1, v1 in list(data[idx].items()):
                        if k1 in ['__input__', '__output__']:
                            continue
                        if k1 in self.data[k] and self.data[k][k1] != v1:
                            raise ResultDBError('Conflicting key ``{0}`` between section '\
                                              '``{1} (value {3})`` and ``{2} (value {4})``.'.\
                                              format(k1, k, idx, self.data[k][k1], v1))
                        else:
                            self.data[k][k1] = v1
        for k in self.data.keys():
            self.data[k]['__input__'] = '; '.join(self.data[k]['__input__'])
            self.data[k]['__output__'] = '; '.join(self.data[k]['__output__'])
            # Convert string to list of strings so that pandas can merge and properly output
            for k1 in self.data[k]:
                self.data[k][k1] = [str(self.data[k][k1])]

    def __merge(self):
        '''
        Merge all entries into one dictionary (fill missing values in pandas)
        '''
        res = pd.DataFrame()
        for item in self.data.values():
            if res.empty:
                res = pd.DataFrame.from_dict(item)
            else:
                res = pd.merge(res, pd.DataFrame.from_dict(item), how = 'outer')
        self.data = res

    def __write(self):
        '''
        and write it to a CSV file
        '''
        cols = sorted(self.data.columns.values, reverse = True)
        self.data.reindex(columns = cols).to_csv('{}.csv.gz'.format(self.name), index = False,
                                                 compression = 'gzip')
        SQLiteMan('{}.db'.format(self.name)).\
          convert(gzip.open('{}.csv.gz'.format(self.name), mode = 'rt'), 'DSC', ',', None, True)

    def build(self):
        self.__load_parameters()
        self.__load_output()
        self.__expand()
        self.__merge()
        self.__write()

class ConfigDB:
    def __init__(self, db_name):
        self.name = db_name

    def Build(self):
        ''''''
        self.data = {}
        for f in glob.glob('.sos/.dsc/*.io.tmp'):
            fid, sid, name = os.path.basename(f).split('.')[:3]
            if fid not in self.data:
                self.data[fid] = {}
            if sid not in self.data[fid]:
                self.data[fid][sid] = {}
            if name not in self.data[fid][sid]:
                self.data[fid][sid][name] = {}
            x, y, z= open(f).read().strip().split('::')
            self.data[fid][sid][name]['input'] = [os.path.join(self.name, os.path.basename(item))
                                                   for item in x.split(',') if item]
            self.data[fid][sid][name]['output'] = [os.path.join(self.name, os.path.basename(item))
                                                   for item in y.split(',') if item]
            if int(z) != 0:
                # FIXME: need a more efficient solution
                self.data[fid][sid][name]['input'] = sos_pair_input(self.data[fid][sid][name]['input'])
        #
        with open('.sos/.dsc/{}.conf'.format(os.path.basename(self.name)), 'w') as f:
            f.write(json.dumps(self.data))
