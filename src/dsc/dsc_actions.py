#!/usr/bin/env python3
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import sys, yaml, re
import rpy2.robjects as RO
from utils import env, lower_keys, CheckRLibraries

class DSCFileAction:
    '''
    Base class for file actions

    Apply to data = DSCFile()
    '''
    def __init__(self):
        pass

    def apply(self, data):
        pass

class DSCEntryAction:
    '''
    Base class for entry actions

    Apply to a DSC entry which is a string or a list
    '''
    def __init__(self):
        pass

    def apply(self, value):
        return value

class DSCSetupAction:
    '''
    Base class for DSC setup

    Apply to a DSC section to expand it to job initialization data
    '''
    def __init__(self):
        pass

    def apply(self, data):
        pass

class DSCFileLoader(DSCFileAction):
    '''
    Load DSC configuration file in YAML format and check for required entries
    '''
    def __init__(self):
        DSCFileAction.__init__(self)
        # Keywords
        self.kw1 = ['scenario', 'method', 'score', 'runtime']
        self.kw2 = ['exe', 'return', 'params', 'seed']
        self.kw3 = ['__logic__', '__map__']

    def apply(self, data):
        env.logger.debug("Loading configurations from [{}].".format(data.file_name))
        with open(data.file_name) as f:
            try:
                cfg = yaml.load(f)
            except:
                cfg = None
        if not isinstance(cfg, dict):
            raise RuntimeError("DSC configuration [{}] not properly formatted!".format(data.file_name))
        data.update(lower_keys(cfg))
        # Check entries
        for kw1 in list(self.kw1):
            if kw1 not in data:
                raise RuntimeError('Missing required section "{}" from [{}].'.format(kw1, data.file_name))
            if kw1 != 'runtime':
                has_exe = has_return = False
                for kw2 in list(data[kw1]):
                    if kw2 not in self.kw2 + self.kw3:
                        env.logger.warning('Ignore unknown entry "{}" in section "{}".'.format(kw2, kw1))
                        del data[kw1][kw2]
                    if kw2 == 'exe':
                        has_exe = True
                    if kw2 == 'return':
                        has_return = True
                if not has_exe:
                    raise RuntimeError('Missing required entry "exe" in section "{}"'.format(kw1))
                if not has_return:
                    raise RuntimeError('Missing required entry "return" in section "{}"'.format(kw1))
            else:
                if 'output' not in data[kw1]:
                    raise RuntimeError('Missing required entry "output" in section "runtime".')

class DSCEntryFormatter(DSCFileAction):
    '''
    Run format transformation to all DSC entries
    '''
    def __init__(self):
        DSCFileAction.__init__(self)
        self.global_vars = None
        self.r_libs = None

    def apply(self, data):
        if 'variables' in data['runtime']:
            self.global_vars = data['runtime']['r_libs']
        if 'r_libs' in data['runtime']:
            self.r_libs = data['runtime']['r_libs']
            CheckRLibraries(self.r_libs)
        self.actions = [Str2List(),
                        ExpandVars(),
                        ExpandCodes(),
                        CastData()]
        data = self.__Transform(data)

    def __Transform(self, cfg):
        for key, value in cfg.items():
            if isinstance(value, dict):
                self.__Transform(value)
            for a in self.actions:
                value = a.apply(value)
            cfg[key] = value
        return cfg

class Str2List(DSCEntryAction):
    '''
    Convert string to list via splitting by comma outside of parenthesis
    '''
    def __init__(self):
        DSCEntryAction.__init__(self)
        self.regex = re.compile(r'(?:[^,(]|\([^)]*\))+')

    def apply(self, value):
        if isinstance(value, str):
            # This does not work for nested parenthesis
            # return [x.strip() for x in self.regex.findall(value)]
            # Have to do it the hard way ...
            return self.__split(value)
        else:
            if not isinstance(value, (dict, list, tuple)):
                return [value]
            else:
                return value

    def __split(self, value):
        counts = {'(': 0,
                  ')': 0,
                  '[': 0,
                  ']': 0,
                  '{': 0,
                  '}': 0}
        res = []
        token = ''
        for item in list(value):
            if item != ',':
                token += item
                if item in counts.keys():
                    counts[item] += 1
            else:
                if counts['('] != counts[')'] or \
                  counts['['] != counts[']'] or \
                  counts['{'] != counts['}']:
                    # comma is inside some parenthesis
                    token += item
                else:
                    # comma is outside any parenthesis, time to split
                    res.append(token.strip())
                    token = ''
        res.append(token.strip())
        return res

class ExpandVars(DSCEntryAction):
    def __init__(self):
        DSCEntryAction.__init__(self)

class ExpandCodes(DSCEntryAction):
    def __init__(self):
        DSCEntryAction.__init__(self)

class CastData(DSCEntryAction):
    def __init__(self):
        DSCEntryAction.__init__(self)

class DSCScenarioSetup(DSCSetupAction):
    def __init__(self):
        DSCSetupAction.__init__(self)

class DSCMethodSetup(DSCSetupAction):
    def __init__(self):
        DSCSetupAction.__init__(self)

class DSCScoreSetup(DSCSetupAction):
    def __init__(self):
        DSCSetupAction.__init__(self)
