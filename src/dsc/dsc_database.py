#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
import os, yaml, gzip
import pandas as pd
from pysos.utils import Error, env
from .utils import dict2str, load_rds, SQLiteMan

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
            with open('.sos/.dsc/{}.yaml'.format(self.name)) as f:
                data = yaml.load(f)
        except FileNotFoundError:
            raise MetaDBError('Cannot load source data to build database!')
        res = {}
        for idx, (k, v) in enumerate(data.items()):
            res[idx + 1] = {}
            k = k.split('_')
            # FIXME: need to prepare for multiple output
            # make it a list for now
            res[idx + 1]['..output..'] = [k[0]]
            if len(k) > 1:
                res[idx + 1]['..input..'] = k[1:]
            else:
                res[idx + 1]['..input..'] = []
            for k1, v1 in v.items():
                if k1 == 'exec':
                    res[idx + 1][k1] = v1
                else:
                    res[idx + 1]['{}.{}'.format(k1, v['exec'])] = v1
        self.data = res

    def __load_output(self):
        '''
        For each output, if output.rds exists, then try to read that RDS file and
        dump its values to parameter space if it is "simple" enough (i.e., is int, float or str)
        '''
        for k, v in self.data.items():
            for item in v['..output..']:
                rds = '{}/{}.rds'.format(self.name, item)
                if not os.path.isfile(rds):
                    continue
                rdata = load_rds(rds)
                for k1, v1 in rdata.items():
                    # a "simple" object
                    if len(v1) == 1 and '{}.{}'.format(k1, v['exec']) not in v:
                        self.data[k]['{}.{}'.format(k1, v['exec'])] = v1[0]

    def __expand(self):
        '''
        For entries involving __input__, for each input, find the other entry with corresponding output and
        copy the parameters (except exec, __input__ and __output__) over.
        '''
        for k in self.data.keys():
            for item in self.data[k]['..input..']:
                idx = self.__search_output_idx(item)
                if idx is None:
                    continue
                for k1, v1 in self.data[idx].items():
                    if k1 in ['exec', '..input..', '..output..']:
                        continue
                    if k1 in self.data[k] and self.data[k][k1] != v1:
                        raise MetaDBError('Conflicting key ``{}`` between section ``{}`` and ``{}``.'.\
                                          format(k1, k, idx))
                    else:
                        self.data[k][k1] = v1
        for k in self.data.keys():
            self.data[k]['..input..'] = '; '.join(self.data[k]['..input..'])
            self.data[k]['..output..'] = '; '.join(self.data[k]['..output..'])
            # Convert string to list so that pandas merge can handle
            for k1 in self.data[k]:
                self.data[k][k1] = [self.data[k][k1]]

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
        SQLiteMan('{}.db'.format(self.name)).convert(gzip.open('{}.csv.gz'.format(self.name), mode = 'rt'), 'DSC', ',', None, True)

    def __search_output_idx(self, output):
        '''Input is output string, output is data ID'''
        for kk in self.data.keys():
            if output in self.data[kk]['..output..']:
                return kk
        return None

    def build(self):
        self.__load_parameters()
        self.__load_output()
        self.__expand()
        self.__merge()
        self.__write()
