#!/usr/bin/env python3
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import sys, yaml, re, subprocess, ast
import rpy2.robjects as RO
from utils import env, lower_keys, CheckRLibraries, is_null, str2num

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

    def split(self, value):
        if not isinstance(value, str):
            return value
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

    def formatVar(self, var):
        '''
        Properly format variables

          * For string var will add quotes to it: str -> "str"
          * For tuple / list will make it into a string like "[item1, item2 ...]"
        '''
        var = self.split(var)
        if isinstance(var, (list, tuple)):
            if len(var) == 1:
                return '''"{0}"'''.format(var[0])
            else:
                return '[{}]'.format(', '.join(list(map(str, var))))
        else:
            return var

    def decodeStr(self, var):
        '''
        Try to properly decode str to other data type
        '''
        if not isinstance(var, str):
            return var
        # Try to convert to number
        var = str2num(var)
        # null type
        if is_null(var):
            return None
        if isinstance(var, str):
            # see if str can be converted to a list
            if (var.startswith('(') and var.endswith(')')) or \
               (var.startswith('[') and var.endswith(']')):
               var = [str2num(x.strip()) for x in re.sub(r'^\(|^\[|\)$|\]$', "", var).split(',')]
        return var

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
        self.r_libs = None

    def apply(self, data):
        if 'r_libs' in data['runtime']:
            self.r_libs = data['runtime']['r_libs']
            CheckRLibraries(self.r_libs)
        self.actions = [Str2List(),
                        ExpandVars(data['runtime']['variables']
                                   if 'variables' in data['runtime']
                                   else None),
                        ExpandCodes(),
                        CastData()]
        data = self.__Transform(data)

    def __Transform(self, cfg):
        for key, value in cfg.items():
            if isinstance(value, dict):
                self.__Transform(value)
            else:
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
            return self.split(value)
        else:
            if not isinstance(value, (dict, list, tuple)):
                return [value]
            else:
                return value


class ExpandVars(DSCEntryAction):
    '''
    Replace DSC variable place holder with actual value

    e.g. $(filename) -> "text.txt"
    '''
    def __init__(self, global_var):
        DSCEntryAction.__init__(self)
        self.global_var = global_var

    def apply(self, value):
        if self.global_var is None:
            return value
        pattern = re.compile(r'\$\((.*?)\)')
        for idx, item in enumerate(value):
            if isinstance(item, str):
                for m in re.finditer(pattern, item):
                    item = item.replace(m.group(0), self.formatVar(self.global_var[m.group(1)]))
                value[idx] = item
        return value

class ExpandCodes(DSCEntryAction):
    '''
    Run code entries and get values.

    Code entries are R(), Python() and Shell()
    '''
    def __init__(self):
        DSCEntryAction.__init__(self)
        self.method = {
            'R': self.__R,
            'Python': self.__Python,
            'Shell': self.__Shell
            }

    def apply(self, value):
        for idx, item in enumerate(value):
            if isinstance(item, str):
                for name in list(self.method.keys()):
                    pattern = re.compile(r'^{}\((.*?)\)$'.format(name))
                    for m in re.finditer(pattern, item):
                        item = item.replace(m.group(0), self.formatVar(self.method[name](m.group(1))))
                value[idx] = item
        return value

    def __R(self, code):
        return tuple(RO.r(code))

    def __Python(self, code):
        return eval(code)

    def __Shell(self, code):
        return subprocess.check_output(code, shell = True).decode('utf8').strip()

class CastData(DSCEntryAction):
    def __init__(self):
        DSCEntryAction.__init__(self)

    def apply(self, value):
        for idx, item in enumerate(value):
            value[idx] = self.decodeStr(item)
        if len(value) == 1:
            return value[0]
        else:
            return value

class DSCScenarioSetup(DSCSetupAction):
    def __init__(self):
        DSCSetupAction.__init__(self)

class DSCMethodSetup(DSCSetupAction):
    def __init__(self):
        DSCSetupAction.__init__(self)

class DSCScoreSetup(DSCSetupAction):
    def __init__(self):
        DSCSetupAction.__init__(self)
