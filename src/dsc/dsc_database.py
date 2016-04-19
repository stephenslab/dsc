#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
import os, yaml, gzip
import pandas as pd
from copy import deepcopy
from pprint import pprint
from pysos.utils import Error, env
from .utils import load_rds, SQLiteMan

class MetaDBError(Error):
    """Raised when there is a problem building the database."""
    def __init__(self, msg):
        Error.__init__(self, msg)
        self.args = (msg, )

class MetaDB:
    def __init__(self, db_name):
        self.name = db_name

    def __load_parameters(self):
        '''
        Load the parameter YAML database and format it a bit:
         * each key will be an index
         * within each key, there are __input__ and __output__ keys for file dependencies
         * rename each parameter key: key => key_exec
        '''
        try:
            with open('.sos/.dsc/{}.yaml'.format(os.path.basename(self.name))) as f:
                data = yaml.load(f)
        except FileNotFoundError:
            raise MetaDBError('Cannot load source data to build database!')
        res = {}
        for idx, (k, v) in enumerate(data.items()):
            res[idx + 1] = {}
            k = k.split('_')
            # FIXME: need to prepare for multiple output
            # make it a list for now
            res[idx + 1]['__output__'] = [k[0]]
            if len(k) > 1:
                res[idx + 1]['__input__'] = k[1:]
            else:
                res[idx + 1]['__input__'] = []
            for k1, v1 in v.items():
                if k1 == 'exec':
                    res[idx + 1][k1] = v1
                else:
                    res[idx + 1]['{}__{}'.format(k1, v['exec'])] = v1
        self.data = res

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
                rdata = load_rds(rds)
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
                            raise MetaDBError('Conflicting key ``{0}`` between section '\
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
