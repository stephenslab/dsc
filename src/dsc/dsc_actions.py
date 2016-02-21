#!/usr/bin/env python3
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import sys, yaml, re, subprocess, ast
import rpy2.robjects as RO
from utils import env, lower_keys, CheckRLibraries, is_null, str2num, \
     cartesian_dict, cartesian_list, pairwise_list, get_slice, flatten_list

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
        if var.startswith('$'):
            return var.replace('$scenario', '$1').replace('$method', '$2')
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

class DSCSetupAction(DSCEntryAction):
    '''
    Base class for DSC setup

    Apply to a DSC section to expand it to job initialization data

    In addition to simply expand attributes, this will take care of all
    (remaining) DSC jargon, including:
      * __alias__ related operations
        * RList()
        * "=" operator
      * __logic__ related operations
        * Product(), Pairwise()
        * logic on exe
      * Parameter conventions
        * by sections of exe, e.g. exe[1], exe[2]
      * $ symbol
    '''
    def __init__(self):
        DSCEntryAction.__init__(self)
        self.name = ''

    def apply(self, dsc):
        if not self.name in dsc:
            return
        self.expand_exe(dsc)
        self.expand_param(dsc)
        # clean up data
        for key in list(dsc[self.name].keys()):
            if not dsc[self.name][key]:
                del dsc[self.name][key]

    def expand_exe(self, dsc):
        '''
        Expand exe variables and apply rule (sequence) to run executables
        '''
        # expand variable names
        for idx, exe in enumerate(dsc[self.name]['meta']['exe']):
            pattern = re.search('^(.*?)\((.*?)\)$', exe)
            if pattern:
                # there is a need to expand variable names
                option = pattern.group(1)
                if not option in ['Product', 'Pairwise']:
                    raise ValueError("Invalid exe rule: {}".format(option))
                value = [self.decodeStr(x) for x in self.split(pattern.group(2))]
                value = [x if isinstance(x, list) else [x] for x in value]
                if option == 'Product':
                    value = cartesian_list(*value)
                else:
                    value = pairwise_list(*value)
                dsc[self.name]['meta']['exe'][idx] = value
        dsc[self.name]['meta']['exe'] = flatten_list(dsc[self.name]['meta']['exe'])
        # apply rules
        if 'rules' in dsc[self.name] and 'meta' in dsc[self.name]['rules']:
            # there are exe related rules
            new_exe = []
            for item in dsc[self.name]['rules']['meta']:
                item = [get_slice(x.strip()) for x in item.split('+')]
                tmp_exe = tuple(dsc[self.name]['meta'][x[0]][x[1]] for x in item)
                new_exe.append(tmp_exe if len(tmp_exe) > 1 else tmp_exe[0])
            dsc[self.name]['meta']['exe'] = new_exe
            del dsc[self.name]['rules']['meta']


    def expand_param(self, dsc):
        '''
        Rule to expand parameters.

        Situations to resolve:
          * Common / unique params to executables
          * Rules to expand
        '''
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
        self.kw3 = ['__logic__', '__alias__']

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
                data[kw1] = self.__format_section(data[kw1])
            else:
                if 'output' not in data[kw1]:
                    raise RuntimeError('Missing required entry "output" in section "runtime".')
            # change key name
            data[self.kw1.index(kw1) + 1] = data.pop(kw1)

    def __format_section(self, section_data):
        '''
        Format section data to meta / params etc for easier manipulation

          * meta: will contain exe information
          * params:
            * params[0] (for shared paramss), params[1], params[2], (corresponds to exe[1], exe[2]) ...
          * rules:
            * rules['meta'], rules[0], rules[1] ...
          * params_alias:
            * params_alias[1], params_alias[2] ...
        '''
        meta = {}
        params = {0:{}}
        rules = {}
        params_alias = {}
        # Parse meta
        meta['exe'] = section_data['exe']
        if 'seed' in section_data:
            meta['seed'] = section_data['seed']
        if '__logic__' in section_data:
            rules['meta'] = section_data['__logic__']
        # Parse params
        if 'params' in section_data:
            for key, value in section_data['params'].items():
                groups = re.search('(.*?)\[(.*?)\]', key)
                try:
                    name, idx = (groups.group(1), int(groups.group(2)))
                    if name != 'exe':
                        raise ValueError('Unknown paramseter entry with index: {}.'.format(key))
                    if idx == 0:
                        raise ValueError('Invalid entry: exe[0]. Index must start from 1.')
                    if idx in params:
                        raise ValueError('Duplicate entry: {}.'.format(key))
                    if idx > len(meta['exe']):
                        raise ValueError('Index for exe out of range: {}.'.format(key))
                    params[idx] = value
                except AttributeError:
                    params[0][key] = value
            # Parse rules and params_alias
            for key in list(params.keys()):
                if '__logic__' in params[key]:
                    rules[key] = params[key]['__logic__']
                    del params[key]['__logic__']
                if '__alias__' in params[key]:
                    params_alias[key] = params[key]['__alias__']
                    del params[key]['__alias__']
                if not params[key]:
                    del params[key]
        res = {'meta': meta, 'return': section_data['return']}
        if params:
            res['params'] = params
        if rules:
            res['rules'] = rules
        if params_alias:
            res['params_alias'] = params_alias
        return res

class DSCEntryFormatter(DSCFileAction):
    '''
    Run format transformation to all DSC entries
    '''
    def __init__(self):
        DSCFileAction.__init__(self)
        self.r_libs = None

    def apply(self, data):
        if 'r_libs' in data[4]:
            self.r_libs = data[4]['r_libs']
            CheckRLibraries(self.r_libs)
        if 'variables' in data[4]:
            variables = data[4]['variables']
            del data[4]['variables']
        else:
            variables = None
        self.actions = [Str2List(),
                        ExpandVars(variables),
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
        # Recode strings
        for idx, item in enumerate(value):
            value[idx] = self.decodeStr(item)
        # Properly convert lists and tuples
        if len(value) == 1 and isinstance(value[0], (list, tuple)):
            return list(value[0])
        else:
            return [tuple(x) if isinstance(x, list) else x for x in value]

class DSCScenarioSetup(DSCSetupAction):
    def __init__(self):
        DSCSetupAction.__init__(self)
        self.name = 1

class DSCMethodSetup(DSCSetupAction):
    def __init__(self):
        DSCSetupAction.__init__(self)
        self.name = 2

class DSCScoreSetup(DSCSetupAction):
    def __init__(self):
        DSCSetupAction.__init__(self)
        self.name = 3
