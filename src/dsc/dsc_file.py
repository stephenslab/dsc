#!/usr/bin/env python3
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import sys, yaml, re, subprocess, ast, itertools, copy
import rpy2.robjects as RO
from utils import env, lower_keys, is_null, str2num, \
     cartesian_dict, cartesian_list, pairwise_list, get_slice, flatten_list, \
     try_get_value, dict2str, update_nested_dict

class DSCFileParser:
    '''
    Base class for DSC file parsing operations

    Operators applied to DSC data object
    '''
    def __init__(self):
        pass

    def apply(self, data):
        pass

class DSCEntryParser:
    '''
    Base class for entry parsing operations

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
                is_tuple = var.startswith('(')
                var = [str2num(x.strip()) for x in re.sub(r'^\(|^\[|\)$|\]$', "", var).split(',')]
                if is_tuple:
                    var = tuple(var)
        return var

class DSCBlockParser(DSCEntryParser):
    '''
    Parser for DSC Block

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
        DSCEntryParser.__init__(self)
        self.data = []

    def apply(self, dsc):
        for name in list(dsc.keys()):
            if name != 'DSC':
                self.expand_exe(dsc, name)
                self.expand_param(dsc, name)
            # clean up data
            for key in list(dsc[name].keys()):
                if not dsc[name][key]:
                    del dsc[name][key]

    def expand_exe(self, dsc, name):
        '''
        Expand exe variables and apply rule (sequence) to run executables
        '''
        # expand variable names
        for idx, exe in enumerate(dsc[name]['meta']['exe']):
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
                dsc[name]['meta']['exe'][idx] = value
        dsc[name]['meta']['exe'] = flatten_list(dsc[name]['meta']['exe'])
        max_exe = sorted(dsc[name]['params'].keys())[-1]
        if max_exe > len(dsc[name]['meta']['exe']):
            raise ValueError('Index for exe out of range: exe[{}].'.format(max_exe))

    def expand_param(self, dsc, name):
        '''
        Rule to expand parameters.

        Situations to resolve:
          * Match common / unique params to executables
          * Rules to expand
        '''
        if 'params' not in dsc[name]:
            # just exe and possibly seed
            self.__expand_param_entry(dsc[name]['meta']['exe'],
                                      param = None,
                                      seed = try_get_value(dsc[name], ('meta', 'seed')),
                                      rule = try_get_value(dsc[name], ('rule', 0)))
        for key in list(dsc[name]['params'].keys()):
            self.__expand_param_entry(dsc[name]['meta']['exe'] if key == 0 else dsc[name]['meta']['exe'][key - 1],
                                      param = dsc[name]['params'][key],
                                      seed = try_get_value(dsc[name], ('meta', 'seed')),
                                      rule = try_get_value(dsc[name], ('rule', key)))

    # def apply_exe_rules(self, dsc):
    #     # apply rules
    #     if 'rules' in dsc[self.name] and 'meta' in dsc[self.name]['rules']:
    #         # there are exe related rules
    #         new_exe = []
    #         for item in dsc[self.name]['rules']['meta']:
    #             item = [get_slice(x.strip()) for x in item.split('+')]
    #             tmp_exe = tuple(dsc[self.name]['meta'][x[0]][x[1]] for x in item)
    #             new_exe.append(tmp_exe if len(tmp_exe) > 1 else tmp_exe[0])
    #         dsc[self.name]['meta']['exe'] = new_exe
    #         del dsc[self.name]['rules']['meta']

    def __expand_param_entry(self, exe, param, seed, rule):
        '''
        Expand parameter entries
        '''
        res = None
        self.data.append(res)

    def __str__(self):
        return dict2str(self.data)

class DSCSetupParser(DSCEntryParser):
    '''
    Parser for DSC section, the DSC setup
    '''
    def __init__(self):
        DSCEntryParser.__init__(self)


class DSCFileLoader(DSCFileParser):
    '''
    Load DSC configuration file in YAML format and perform initial sanity check
    '''
    def __init__(self):
        DSCFileParser.__init__(self)
        # Keywords
        self.reserved_kw = ['DSC']
        self.prim_kw = ['level', 'exe', 'return', 'params', 'seed']
        self.aux_kw = ['__logic__', '__alias__']

    def apply(self, data):
        env.logger.debug("Loading configurations from [{}].".format(data.file_name))
        with open(data.file_name) as f:
            try:
                cfg = yaml.load(f)
            except:
                cfg = None
        if not isinstance(cfg, dict):
            raise RuntimeError("DSC configuration [{}] not properly formatted!".format(data.file_name))
        # data.update(lower_keys(cfg))
        data.update(cfg)
        # Handle derived blocks
        has_dsc = False
        blocks = self.__sort_blocks(data)
        for block in blocks:
            if block == 'DSC':
                has_dsc = True
                if 'run' not in data[block]:
                    raise RuntimeError('Missing required entry "DSC::run".')
                if try_get_value(data, ('DSC', 'runtime', 'output')) is None:
                    raise RuntimeError('Missing required entry "DSC::runtime::output".')
            else:
                groups = re.search('(.*?)\((.*?)\)', block)
                if groups:
                    data[groups.group(1)] = update_nested_dict(copy.deepcopy(data[groups.group(2)]), data[block])
                    del data[block]
        # format / check entries
        for block in list(data):
            if block == "DSC":
                continue
            has_exe = has_return = False
            for key in list(data[block]):
                if key not in self.prim_kw:
                    env.logger.warning('Ignore unknown entry "{}" in block "{}".'.format(key, block))
                    del data[block][key]
                if key == 'exe':
                    has_exe = True
                if key == 'return':
                    has_return = True
            if not has_exe:
                raise RuntimeError('Missing required entry "exe" in section "{}"'.format(block))
            if not has_return:
                raise RuntimeError('Missing required entry "return" in section "{}"'.format(block))
            data[block] = self.__format_block(data[block])

    def __sort_blocks(self, section_data):
        '''
        Sort block names such that derived block always follows the base block

        name of derived blocks looks like: "derived(base)"
        '''
        base = []
        derived = []
        blocks = []
        for block in section_data:
            groups = re.search('(.*?)\((.*?)\)', block)
            if groups:
                derived.append([groups.group(1), groups.group(2)])
            else:
                base.append(block)
                blocks.append(block)
        # Check looped derivations: x(y) and y(x)
        tmp = [sorted(x) for x in derived]
        for item in ((i, tmp.count(i)) for i in tmp):
            if item[1] > 1:
                raise ValueError("Looped block inheritance: {0}({1}) and {1}({0})!".format(item[0][0], item[0][1]))
        # Check self-derivation and non-existing base
        tmp = base + [x[0] for x in derived]
        for item in derived:
            if item[0] == item[1]:
                raise ValueError("Looped block inheritance: {0}({0})!".format(item[0]))
            if item[1] not in tmp:
                raise ValueError("Base block does not exist: {0}({1})!".format(item[0], item[1]))
        #
        derived_cycle = itertools.cycle(derived)
        while True:
            item = next(derived_cycle)
            if item[1] in base:
                base.append(item[0])
                name = '{}({})'.format(item[0], item[1])
                if name not in blocks:
                    blocks.append(name)
            if len(blocks) == len(section_data.keys()):
                break
        return blocks


    def __format_block(self, section_data):
        '''
        Format block data to meta / params etc for easier manipulation

          * meta: will contain exe information
          * params:
            * params[0] (for shared params), params[1], params[2], (corresponds to exe[1], exe[2]) ...
          * rules:
            * rules[0] (for shared params), rules[1] ...
          * params_alias:
            * params_alias[0], params_alias[1] ...
        '''
        meta = {}
        params = {0:{}}
        rules = {}
        params_alias = {}
        # Parse meta
        meta['exe'] = section_data['exe']
        if 'seed' in section_data:
            meta['seed'] = section_data['seed']
        # Parse params
        if 'params' in section_data:
            for key, value in section_data['params'].items():
                groups = re.search('(.*?)\[(.*?)\]', key)
                try:
                    name, idxes = get_slice(key)
                    if name != 'exe':
                        raise ValueError('Unknown paramseter entry with index: {}.'.format(key))
                    for idx in idxes:
                        idx += 1
                        if idx == 0:
                            raise ValueError('Invalid entry: exe[0]. Index must start from 1.')
                        if idx in params:
                            raise ValueError('Duplicate parameter entry: {}.'.format(key))
                        params[idx] = copy.copy(value)
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
        if 'level' in section_data:
            res['level'] = section_data['level']
        return res

class DSCEntryFormatter(DSCFileParser):
    '''
    Run format transformation to all DSC entries
    '''
    def __init__(self):
        DSCFileParser.__init__(self)

    def apply(self, data):
        data['DSC_R_LIBRARIES'] = try_get_value(data, ('DSC', 'runtime', 'r_libs'))
        variables = try_get_value(data, ('DSC', 'runtime', 'variables'))
        if data['DSC_R_LIBRARIES'] is not None:
            del data['DSC']['runtime']['r_libs']
        else:
            del data['DSC_R_LIBRARIES']
        if variables is not None:
            del data['DSC']['runtime']['variables']
        self.actions = [Str2List(),
                        ExpandVars(variables),
                        ExpandCodes(),
                        CastData()]
        data = self.__Transform(data)
        for key in data:
            if 'level' in data[key]:
                data[key]['level'] = data[key]['level'][0]

    def __Transform(self, cfg):
        for key, value in list(cfg.items()):
            if isinstance(value, dict):
                self.__Transform(value)
            else:
                for a in self.actions:
                    value = a.apply(value)
                if is_null(value):
                    del cfg[key]
                else:
                    cfg[key] = value
        return cfg

class Str2List(DSCEntryParser):
    '''
    Convert string to list via splitting by comma outside of parenthesis
    '''
    def __init__(self):
        DSCEntryParser.__init__(self)
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

class ExpandVars(DSCEntryParser):
    '''
    Replace DSC variable place holder with actual value

    e.g. $(filename) -> "text.txt"
    '''
    def __init__(self, global_var):
        DSCEntryParser.__init__(self)
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

class ExpandCodes(DSCEntryParser):
    '''
    Run code entries and get values.

    Code entries are R(), Python() and Shell()
    '''
    def __init__(self):
        DSCEntryParser.__init__(self)
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
        return list(RO.r(code))

    def __Python(self, code):
        return eval(code)

    def __Shell(self, code):
        return subprocess.check_output(code, shell = True).decode('utf8').strip()

class CastData(DSCEntryParser):
    def __init__(self):
        DSCEntryParser.__init__(self)

    def apply(self, value):
        # Recode strings
        for idx, item in enumerate(value):
            value[idx] = self.decodeStr(item)
        # Properly convert lists and tuples
        if len(value) == 1 and isinstance(value[0], list):
            if not is_null(value[0]):
                return list(value[0])
            else:
                return []
        else:
            res = []
            for x in value:
                if is_null(x):
                    continue
                if isinstance(x, list):
                    # [[],[]] -> [(),()]
                    res.append(tuple(x))
                else:
                    res.append(x)
            return res

class DSCData(dict):
    '''
    Read DSC configuration file and translate it to a collection of steps to run DSC

    This class reflects the design and implementation of DSC structure and syntax

    Tasks here include:
      * Properly parse DSC file in YAML format
      * Translate DSC file text
        * Replace Operators R() / Python() / Shell() / Product() ...
        * Replace global variables
      * Parse __alias__ and __logic__ to expand all settings to units of "steps"
        * i.e., list of parameter dictionaries each will initialize a job
    '''
    def __init__(self, fname):
        self.actions = [DSCFileLoader(),
                        DSCEntryFormatter(),
                        DSCBlockParser(),
                        DSCSetupParser()]
        self.file_name = fname
        for a in self.actions:
            a.apply(self)

    def __str__(self):
        return dict2str(dict(self), replace = [('!!python/tuple', '(tuple)')])
