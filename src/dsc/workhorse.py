#!/usr/bin/env python3
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import sys, os
from .pyper import R as RClass
from .pyper import Str4R
from .utils import Environment, str2num
from snakemake.io import load_configfile, expand
import deepdish as dd
import hashlib
import numpy as np
import pandas as pd

env = Environment()

class DSCController(dict):
    def __init__(self, config_file, debug = False):
        self.config_file = config_file
        self.no_expand = ['_return']
        self.required_sections = ['scenario', 'method', 'score', 'runtime']
        self.output = None
        self.debug = debug
        self.update(self.__LoadConfiguration(config_file))

    def ApplyScenario(self):
        jobs = self.__PrepareJobs(self['scenario'])
        res = {}
        for job in jobs:
            lan = self.__WhichLanguage(job['_exe'])
            job_str = 'S_' + hashlib.md5(str(job).encode()).hexdigest()
            res[job_str] = {}
            if lan == 'r':
                R = self.__RunR(job)
                for out in self['scenario']['_return']:
                    res[job_str][out] = R.get("{}".format(out))
        if self.debug:
            print(res)
        dd.io.save(self.output + '_scenario.h5', res)

    def ApplyMethod(self):
        hmap = {}
        for k, val in self['method'].items():
            if type(val) is str and val.startswith('$'):
                self['method'][k], hmap[k] = self.__LoadData(val)
        jobs = self.__PrepareJobs(self['method'])
        res = {}
        for job in jobs:
            lan = self.__WhichLanguage(job['_exe'])
            # FIXME: method using direct input name now
            # This may not work if method differ by input parameters
            method = os.path.splitext(os.path.split(job['_exe'])[-1])[0]
            if method not in res:
                res[method] = {}
            # swap data back
            scenario = []
            for key in job:
                if key in hmap:
                    scenario.append(job[key])
                    job[key] = hmap[key][job[key]]
            scenario = '_'.join(set(scenario))
            res[method][scenario] = {}
            if lan == 'r':
                R = self.__RunR(job)
                for out in self['method']['_return']:
                    res[method][scenario][out] = R.get("{}".format(out))
        if self.debug:
            print(res)
        dd.io.save(self.output + '_method.h5', res)

    def ApplyScore(self):
        method_key = ''
        scenario_key = ''
        hmap = {}
        for k, val in self['score'].items():
            if type(val) is str and val.startswith('$'):
                self['score'][k], hmap[k] = self.__LoadData(val)
                if val.startswith('$method'):
                    method_key = k
                if val.startswith('$scenario'):
                    scenario_key = k
        jobs = self.__PrepareJobs(self['score'])
        res = {}
        for job in jobs:
            lan = self.__WhichLanguage(job['_exe'])
            # method name will use direct input name
            # FIXME: should have one score function
            method = job[method_key]
            if method not in res:
                res[method] = {}
            scenario = job[scenario_key]
            for key in job:
                if key in hmap:
                    if key == method_key:
                        job[key] = hmap[key][job[key]][scenario]
                    if key == scenario_key:
                        job[key] = hmap[key][scenario]
            res[method][scenario] = {}
            if lan == 'r':
                R = self.__RunR(job)
                for out in self['score']['_return']:
                    res[method][scenario][out] = R.get("{}".format(out))
        if self.debug:
            print(res)
        dd.io.save(self.output + '_score.h5', res)

    def Run(self, verbosity):
        if verbosity > 0:
            env.log("Setup scenarios ...")
        self.ApplyScenario()
        if verbosity > 0:
            env.log("Apply methods ...")
        self.ApplyMethod()
        if verbosity > 0:
            env.log("Compute scores ...")
        self.ApplyScore()

    def __LoadConfiguration(self, config_file):
        cfg = load_configfile(config_file)
        for kw1 in self.required_sections:
            if kw1 not in cfg:
                env.error('Missing required section "{}" from [{}]'.format(kw1, self.config_file),
                          exit = True)
            for kw2 in ['_exe', '_return']:
                if kw2 not in cfg[kw1].keys() and kw1 != 'runtime':
                    env.error('Missing required entry "{}" in section "{}" from [{}]'.\
                              format(kw2, kw1, self.config_file), exit = True)
            if 'output' not in cfg['runtime']:
                env.error('Missing required entry "{}" in section "{}" from [{}]'.\
                          format('output', 'runtime', self.config_file), exit = True)
        self.output = cfg['runtime']['output']
        cfg = self.__String2List(cfg)
        return cfg

    def __PrepareJobs(self, params):
        expand_args = ', '.join(['{0}=params["{0}"]'.format(x) for x in params if x not in self.no_expand])
        expand_pattern = "|".join(["('{0}', '{{{0}}}')".format(x) for x in params if x not in self.no_expand])
        expand_cmd = 'expand("{}", {})'.format(expand_pattern, expand_args)
        jobs = []
        for item in eval(expand_cmd):
            jobs.append(dict(eval(x) for x in item.split('|')))
        return jobs

    def __String2List(self, config):
        for key, value in config.items():
            if type(value) is dict:
                self.__String2List(value)
            if type(value) is str:
                if ',' in value:
                    env.error('It looks like the following might be a vector! \n' \
                            '"{0}" \n Please double check, and if so, please format it as a vector e.g.' \
                            '[{0}]'.format(value))
                if not value.startswith('$'):
                    config[key] = [value]
        return config

    def __WhichLanguage(self, name):
        try:
            return os.path.splitext(name)[1][1:].lower()
        except:
            return 'shell'

    def __LoadData(self, tag):
        assert tag.startswith('$')
        tag = tag[1:]
        fn, obj = tag.split('.')
        data = dd.io.load(self.output + '_{}.h5'.format(fn))
        res = {}
        for k in data:
            if fn == 'scenario':
                res[k] = data[k][obj]
            if fn == 'method':
                res[k] = {}
                for s in data[k]:
                    res[k][s] = data[k][s][obj]
        return list(res.keys()), res

    def __RunR(self, job):
        R = RClass()
        init = []
        for key in job:
            if key == '_exe':
                continue
            if key == 'seed':
                init.append('set.seed({})'.format(job['seed']))
            else:
                job[key] = str2num(job[key])
                init.append('{} = {}'.format(key, Str4R(job[key])))
        init.append('\n')
        with open(job['_exe']) as f:
            codes = f.read()
        if self.debug:
            print ('\n'.join(init) + codes)
        R.run('\n'.join(init) + codes)
        return R
