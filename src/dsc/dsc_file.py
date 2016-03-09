#!/usr/bin/env python3
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import sys, yaml, re, subprocess, ast, itertools, copy, sympy
import rpy2.robjects as RO
from io import StringIO
from .utils import env, Error, lower_keys, is_null, str2num, \
     cartesian_dict, cartesian_list, pairwise_list, get_slice, flatten_list, \
     try_get_value, dict2str, update_nested_dict, uniq_list

class FormatError(Error):
    """Raised when format is illegal."""
    def __init__(self, msg):
        Error.__init__(self, msg)
        self.args = (msg, )

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
            # see if str can be converted to a list or tuple
            # and apply the same procedure to their elements
            if (var.startswith('(') and var.endswith(')')) or \
               (var.startswith('[') and var.endswith(']')):
                is_tuple = var.startswith('(')
                var = [self.decodeStr(x.strip()) for x in self.split(re.sub(r'^\(|^\[|\)$|\]$', "", var))]
                if is_tuple:
                    var = tuple(var)
        return var

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
        def load_from_yaml(f, content):
            try:
                cfg = yaml.load(f)
            except:
                cfg = None
            if not isinstance(cfg, dict):
                raise FormatError("DSC configuration [{}] not properly formatted!".format(content))
            # data.update(lower_keys(cfg))
            data.update(cfg)

        if os.path.isfile(data.content):
            env.logger.debug("Loading configurations from [{}].".format(data.content))
            with open(data.content) as f:
                load_from_yaml(f, data.content)
        else:
            with StringIO(data.content) as f:
                load_from_yaml(f, '<Input String>')
        # Handle derived blocks
        has_dsc = False
        blocks = self.__sort_blocks(data)
        for block in blocks:
            if block == 'DSC':
                has_dsc = True
                if 'run' not in data[block]:
                    raise FormatError('Missing required entry "DSC::run".')
                if try_get_value(data, ('DSC', 'runtime', 'output')) is None:
                    raise FormatError('Missing required entry "DSC::runtime::output".')
            else:
                groups = re.search('(.*?)\((.*?)\)', block)
                if groups:
                    data[groups.group(1).strip()] = update_nested_dict(copy.deepcopy(data[groups.group(2).strip()]), data[block])
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
                raise FormatError('Missing required entry "exe" in section "{}"'.format(block))
            if not has_return:
                raise FormatError('Missing required entry "return" in section "{}"'.format(block))
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
                derived.append([groups.group(1).strip(), groups.group(2).strip()])
            else:
                base.append(block)
                blocks.append(block)
        if len(derived) == 0:
            return blocks
        # Check looped derivations: x(y) and y(x)
        tmp = [sorted(x) for x in derived]
        for item in ((i, tmp.count(i)) for i in tmp):
            if item[1] > 1:
                raise FormatError("Looped block inheritance: {0}({1}) and {1}({0})!".format(item[0][0], item[0][1]))
        # Check self-derivation and non-existing base
        tmp = base + [x[0] for x in derived]
        for item in derived:
            if item[0] == item[1]:
                raise FormatError("Looped block inheritance: {0}({0})!".format(item[0]))
            if item[1] not in tmp:
                raise FormatError("Base block does not exist: {0}({1})!".format(item[0], item[1]))
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
                try:
                    name, idxes = get_slice(key)
                    if name != 'exe':
                        raise FormatError('Unknown paramseter entry with index: {}.'.format(key))
                    for idx in idxes:
                        idx += 1
                        if idx == 0:
                            raise FormatError('Invalid entry: exe[0]. Index must start from 1.')
                        if idx in params:
                            raise FormatError('Duplicate parameter entry: {}.'.format(key))
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
        for idx, item in enumerate(value):
            if isinstance(item, str):
                # find pattern with slicing first
                pattern = re.compile(r'\$\((.*?)\)\[(.*?)\]')
                for m in re.finditer(pattern, item):
                    tmp = [x.strip() for x in self.global_var[m.group(1)].split(',')]
                    tmp = ', '.join([tmp[i] for i in get_slice('slice[' + m.group(2) + ']')[1]])
                    item = item.replace(m.group(0), '[' + tmp + ']')
                # then pattern without slicing
                pattern = re.compile(r'\$\((.*?)\)')
                for m in re.finditer(pattern, item):
                    item = item.replace(m.group(0), self.formatVar(self.global_var[m.group(1)]))
                if item != value[idx]:
                    value[idx] = item
        return value

class ExpandActions(DSCEntryParser):
    '''
    Run action entries and get values.

    Action entries are R(), Python(), Shell(), Product() and Pairwise()
    '''
    def __init__(self):
        DSCEntryParser.__init__(self)
        self.method = {
            'R': self.__R,
            'Python': self.__Python,
            'Shell': self.__Shell,
            'Product': self.__Product,
            'Pairwise': self.__Pairwise
            }

    def apply(self, value):
        for idx, item in enumerate(value):
            if isinstance(item, str):
                for name in list(self.method.keys()):
                    pattern = re.compile(r'^{}\((.*?)\)$'.format(name))
                    for m in re.finditer(pattern, item):
                        item = item.replace(m.group(0), self.formatVar(self.method[name](m.group(1))))
                if item != value[idx]:
                    value[idx] = item
        return value

    def __Product(self, value):
        value = [self.decodeStr(x) for x in self.split(value)]
        value = [x if isinstance(x, list) else [x] for x in value]
        return cartesian_list(*value)

    def __Pairwise(self, value):
        value = [self.decodeStr(x) for x in self.split(value)]
        value = [x if isinstance(x, list) else [x] for x in value]
        return pairwise_list(*value)

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


class OperationParser(DSCEntryParser):
    '''
    Parse DSC logic sequence variables by expanding them

    Input: a string sequence of __logic__ or 'run'
    '''
    def __init__(self):
        DSCEntryParser.__init__(self)
        self.operators = ['(', ')', ',', '+', '*']
        # FIXME: need all keywords
        self.sympy_kws = ['sqrt', 'cos', 'sin']
        self.cache = {}
        self.cache_count = 0
        self.value = ''

    def __str__(self):
        return self.value

    def reset(self):
        self.cache = {}
        self.cache_count = 0

    def apply(self, value):
        if is_null(value):
            return value
        if not isinstance(value, str):
            raise TypeError("Argument must be string but it is %s." % type(value))
        res = []
        for seq in self.split(value):
            self.reset()
            self.sequence = seq
            seq = seq.replace(' ', '')
            for a in [self.cache_symbols,
                      self.check_syntax,
                      self.reconstruct]:
                seq = a(seq)
            res.extend(seq)
        self.value = '; '.join([' * '.join(item) for item in res])
        return res

    def cache_symbols(self, value):
        '''cache slices and "." symbol'''
        # split with delimiter kept
        seq = re.split(r'(\(|\)|\+|\*|,)', value)
        new_seq = []
        # reconstruct slice wrongfully splitted e.g., sth[2,3,4]
        start_idx = 0
        for idx, item in enumerate(seq):
            if '[' in item and not ']' in item:
                # bad slice found
                new_seq.extend(seq[start_idx:idx])
                tmp = [seq[idx]]
                i = 1
                incomplete_sq = True
                while i < len(seq):
                    tmp.append(seq[idx + i])
                    if ']' in seq[idx + i]:
                        new_seq.append(''.join(tmp))
                        incomplete_sq = False
                        start_idx += idx + i + 1
                        break
                    i += 1
                if incomplete_sq:
                    raise FormatError('Incomplete "["/"]" pair near {}'.format(''.join(tmp)))
        new_seq.extend(seq[start_idx:len(seq)])
        # hide bad symbols
        for idx, item in enumerate(new_seq):
            if re.search('\[(.*?)\]', item) or '.' in item or item in self.sympy_kws:
                new_seq[idx] = self.__string_cache(item)
        return ''.join(new_seq)

    def check_syntax(self, value):
        ''' * ensure there are not other symbols than these keyword operators
            * ensure '+' is not connecting between parenthesis
        '''

        for x in value:
            if not x in self.operators and not x.isalnum() and x != '_':
                raise FormatError('Invalid symbol "{}" in sequence "{}"'.format(x, self.sequence))
        if ')+' in value or '+(' in value:
            raise FormatError('Pairwise operator "+" cannot be used to connect multiple variables ')
        return value

    def reconstruct(self, value):
        sequence_ordering = uniq_list(re.sub(r'\(|\)|\+|\*|,', ' ', value).split())
        value = value.replace('+', '__PAIRWISE__')
        value = value.replace(',', '+')
        # restore order and syntax
        res = []
        for x in str(sympy.expand(value)).split('+'):
            x = x.strip().split('*')
            if '2' in x:
                # error for '**2'
                raise FormatError("Possibly duplicated elements found in sequence {}".format(self.sequence))
            # re-order elements in x
            # complication: the __PAIRWISE__ operator
            tmp_1 = dict((y if '__PAIRWISE__' not in y else y.split('__PAIRWISE__')[0], y) for y in x)
            if len(tmp_1.keys()) < len(x):
                raise FormatError("Possibly duplicated elements found in sequence {}".format(self.sequence))
            tmp_2 = []
            for y in sequence_ordering:
                if y in tmp_1:
                    if '__PAIRWISE__' in tmp_1[y]:
                        tmp_3 = [self.cache[x] if x in self.cache else x for x in tmp_1[y].split('__PAIRWISE__')]
                        tmp_2.append('+'.join(tmp_3))
                    else:
                        tmp_2.append(self.cache[tmp_1[y]] if tmp_1[y] in self.cache else tmp_1[y])
            res.append(tuple(tmp_2) if len(tmp_2) > 1 else tmp_2[0])
        return res

    def __string_cache(self, cache):
        self.cache_count += 1
        cache_id = 'OPERATOR_CACHE_ASDFGHJKL_{}'.format(self.cache_count)
        self.cache[cache_id] = cache
        return cache_id

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
        self.actions = [OperationParser(),
                        Str2List(),
                        ExpandVars(variables),
                        ExpandActions(),
                        CastData()]
        data = self.__Transform(data, [])
        for key in data:
            if 'level' in data[key]:
                data[key]['level'] = data[key]['level'][0]

    def __Transform(self, cfg, keys):
        for key, value in list(cfg.items()):
            if isinstance(value, dict):
                keys.append(key)
                self.__Transform(value, keys)
            else:
                is_dsc = keys[-1] == 'DSC' or (keys[-1] == 'runtime' and (keys[-2] == 'DSC' if len(keys) > 1 else False))
                if keys[-1] == 'rules' or (key == 'run' and is_dsc):
                    value = self.actions[0].apply(value)
                else:
                    for a in self.actions[1:]:
                        value = a.apply(value)
                if is_null(value):
                    del cfg[key]
                else:
                    cfg[key] = value
        return cfg

class DSCBlockParser(DSCFileParser):
    '''
    Parser for DSC Block

    Apply to a DSC block to convert it to job initialization data

    In addition to simply expand attributes, this will take care of all
    (remaining) DSC jargon, including:
      * __alias__ related operations
        * RList()
        * "=" operator
      * __logic__ related operations (__logic__ entry itself has previously been parsed)
        * expand parameters based on these rules
    '''
    def __init__(self):
        DSCFileParser.__init__(self)

    def apply(self, dsc):
        for name in list(dsc.keys()):
            if name == 'DSC':
                # FIXME
                continue
            else:
                self.name = name
                self.format_exe(dsc[name])
                self.expand_params(dsc[name])
            # clean up data
            for key in list(dsc[name].keys()):
                if not dsc[name][key]:
                    del dsc[name][key]

    def format_exe(self, block):
        '''
        Split exe string to tuples and check if exe matches parameters
        '''
        block['meta']['exe'] = [tuple(x.split()) if isinstance(x, str) else x for x in block['meta']['exe']]
        max_exe = sorted(block['params'].keys())[-1]
        if max_exe > len(block['meta']['exe']):
            raise FormatError('Index for exe out of range: exe[{}].'.format(max_exe))

    def expand_params(self, block):
        if 'params' not in block:
            return
        global_alias = try_get_value(block, ('params_alias', 0))
        global_params = self.__apply_alias(try_get_value(block, ('params', 0)), global_alias)
        for key in list(block['params'].keys()):
            if key == 0:
                continue
            alias = try_get_value(block, ('params_alias', key))
            block['params'][key] = self.__apply_alias(block['params'][key], alias if alias else global_alias)
            tmp = copy.deepcopy(global_params)
            tmp.update(block['params'][key])
            block['params'][key] = tmp

    def __apply_alias(self, params, alias):
        # FIXME: to be implemented
        if not params:
            return {}
        if not alias:
            return params
        return params if params else {}

class DSCSetupParser(DSCFileParser):
    '''
    Parser for DSC section, the DSC setup
    '''
    def __init__(self):
        DSCFileParser.__init__(self)

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
    def __init__(self, content):
        self.actions = [DSCFileLoader(),
                        DSCEntryFormatter(),
                        DSCBlockParser(),
                        DSCSetupParser()]
        self.content = content
        for a in self.actions:
            a.apply(self)

    def __str__(self):
        res = ''
        for item in sorted(list(dict(self).items()), key = lambda x: (x[1]['level'] if 'level' in x[1] else sys.maxsize, x[0])):
            res += dict2str({item[0]: item[1]}, replace = [('!!python/tuple', '(tuple)')]) + '\n'
        return res.strip()
