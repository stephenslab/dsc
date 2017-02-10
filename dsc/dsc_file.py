#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
This file defines the `DSCData` class for loading DSC file
'''

import os, yaml, re, subprocess, itertools, copy, \
  collections, warnings, datetime
import readline
import rpy2.robjects as RO
from io import StringIO
from sos.utils import logger, Error
from sos.target import textMD5
from .utils import dotdict, is_null, str2num, non_commutative_symexpand, strip_dict, \
     cartesian_list, pairwise_list, get_slice, flatten_dict, \
     try_get_value, dict2str, update_nested_dict, set_nested_value, \
     no_duplicates_constructor, install_r_libs

yaml.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, no_duplicates_constructor)

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

    def __call__(self, data):
        pass

class DSCEntryParser:
    '''
    Base class for entry parsing operations

    Apply to a DSC entry which is a string or a list
    '''
    def __init__(self):
        pass

    def __call__(self, value):
        return value

    def split(self, value):
        '''Split value by comma outside (), [] and {}'''
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

    def encodeVar(self, var):
        '''
        Code multi-entry data type to string
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

    def decodeVar(self, var):
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
                var = [self.decodeVar(x.strip()) for x in self.split(re.sub(r'^\(|^\[|\)$|\]$', "", var))]
                if is_tuple:
                    var = tuple(var)
        return var

class Str2List(DSCEntryParser):
    '''
    Convert string to list via splitting by comma outside of parenthesis
    '''
    def __init__(self):
        DSCEntryParser.__init__(self)
        self.regex = re.compile(r'(?:[^,(]|\([^)]*\))+')

    def __call__(self, value):
        if isinstance(value, str):
            # This does not work for nested parenthesis
            # return [x.strip() for x in self.regex.findall(value)]
            # Have to do it the hard way ...
            return self.split(value)
        else:
            if not isinstance(value, (collections.Mapping, list, tuple)):
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

    def __call__(self, value):
        if self.global_var is None:
            return value
        for idx, item in enumerate(value):
            if isinstance(item, str):
                # find pattern with slicing first
                pattern = re.compile(r'\$\((.*?)\)\[(.*?)\]')
                for m in re.finditer(pattern, item):
                    tmp = [x.strip() for x in self.split(self.global_var[m.group(1)])]
                    tmp = ', '.join([tmp[i] for i in get_slice('slice[' + m.group(2) + ']')[1]])
                    item = item.replace(m.group(0), '[' + tmp + ']')
                # then pattern without slicing
                pattern = re.compile(r'\$\((.*?)\)')
                for m in re.finditer(pattern, item):
                    item = item.replace(m.group(0), self.encodeVar(self.global_var[m.group(1)]))
                if item != value[idx]:
                    value[idx] = item
        return value

class ExpandActions(DSCEntryParser):
    '''
    Run action entries and get values.

    Action entries are
      * R(), Python(), Shell()
      * Combo() and Pairs()
    Untouched entries are:
      * File(), Asis()
    because they'll have to be dynamically determined
    '''
    def __init__(self):
        DSCEntryParser.__init__(self)
        self.method = {
            'R': self.__R,
            'Python': self.__Python,
            'Shell': self.__Shell,
            'Combo': self.__Combo,
            'Pairs': self.__Pairs
            }

    def __call__(self, value):
        for idx, item in enumerate(value):
            if isinstance(item, str):
                for name in list(self.method.keys()):
                    pattern = re.compile(r'^{}\((.*?)\)$'.format(name))
                    for m in re.finditer(pattern, item):
                        item = item.replace(m.group(0), self.encodeVar(self.method[name](m.group(1))))
                if item != value[idx]:
                    value[idx] = item
        return value

    def __Combo(self, value):
        raw_value = value
        value = [self.decodeVar(x) for x in self.split(value)]
        if len(value) == 1:
            raise ValueError('Cannot produce Combos for single value ``{}``!'.format(raw_value))
        value = [x if isinstance(x, (list, tuple)) else [x] for x in value]
        return cartesian_list(*value)

    def __Pairs(self, value):
        value = [self.decodeVar(x) for x in self.split(value)]
        value = [x if isinstance(x, (list, tuple)) else [x] for x in value]
        return pairwise_list(*value)

    def __R(self, code):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return list(RO.r(code))

    def __Python(self, code):
        return list(eval(code))

    def __Shell(self, code):
        return subprocess.check_output(code, shell = True).decode('utf8').strip()

class CastData(DSCEntryParser):
    def __init__(self):
        DSCEntryParser.__init__(self)

    def __call__(self, value):
        # Recode strings
        for idx, item in enumerate(value):
            value[idx] = self.decodeVar(item)
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

    Input: a string sequence of .logic or 'run'
    '''
    def __init__(self):
        DSCEntryParser.__init__(self)
        self.operators = ['(', ')', ',', '+', '*']
        self.reset()

    def __str__(self):
        return self.value

    def reset(self):
        self.cache = {}
        self.cache_count = 0
        self.value = ''

    def __call__(self, value):
        if is_null(value):
            return value
        if not isinstance(value, str):
            raise TypeError("Argument must be string but it is %s." % type(value))
        value = value.strip()
        if value[-1] in ['+', '*', ',', '=', '/']:
            raise FormatError('The end of DSC sequence ``"{}"`` cannot be operator ``{}``!'.\
                              format(value, value[-1]))
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
        self.value = '; '.join([' * '.join(item) if not isinstance(item, str) else item for item in res])
        return res

    def cache_symbols(self, value):
        '''cache all symbols'''
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
                    raise FormatError('Incomplete ``[``/``]`` pair near {}'.format(''.join(tmp)))
        new_seq.extend(seq[start_idx:len(seq)])
        # cache all symbols
        for idx, item in enumerate(new_seq):
            if item and not item in self.operators:
                new_seq[idx] = self.__string_cache(item)
        return ''.join(new_seq)

    def check_syntax(self, value):
        ''' * ensure there are not other symbols than these keyword operators
            * ensure '+' is not connecting between parenthesis
        '''
        for x in value:
            if not x in self.operators and not x.isalnum() and x != '_':
                raise FormatError('Invalid symbol ``{}`` in sequence ``{}``'.format(x, self.sequence))
        if ')+' in value or '+(' in value:
            raise FormatError('Pairs operator ``+`` cannot be used to connect multiple variables ')
        return value

    def reconstruct(self, value):
        value = value.replace('+', '_')
        value = value.replace(',', '+')
        res = []
        for x in str(non_commutative_symexpand(value)).split('+'):
            x = x.strip().split('*')
            if '2' in x:
                # error for '**2'
                raise FormatError("Possibly duplicated elements found in sequence {}".\
                                  format(self.sequence))
            # re-construct elements in x
            # complication: the _ operator
            tmp_1 = collections.OrderedDict((y if '_' not in y else y.split('_')[0], y) for y in x)
            if len(tmp_1.keys()) < len(x):
                raise FormatError("Possibly duplicated elements found in sequence {}".\
                                  format(self.sequence))
            tmp_2 = []
            for y in tmp_1:
                if '_' in tmp_1[y]:
                    tmp_3 = [self.cache[x] for x in tmp_1[y].split('_')]
                    tmp_2.append('+'.join(tmp_3))
                else:
                    tmp_2.append(self.cache[tmp_1[y]])
            res.append(tuple(tmp_2) if len(tmp_2) > 1 else tmp_2[0])
        return res

    def __string_cache(self, cache):
        # return existing cache_id
        for cache_id in self.cache:
            if self.cache[cache_id] == cache:
                return cache_id
        # add a new cache_id
        self.cache_count += 1
        cache_id = 'X{}'.format(self.cache_count)
        self.cache[cache_id] = cache
        return cache_id

class DSCFileLoader(DSCFileParser):
    '''
    Load DSC configuration file in YAML format and perform initial sanity check
    '''
    def __init__(self, content, sequence = None, output = None):
        DSCFileParser.__init__(self)
        self.content = content
        self.sequence = sequence
        self.output = output
        # Keywords
        self.block_kw = ['exec', 'return', 'params', 'seed', '.logic', '.alias']
        self.params_kw = ['.logic', '.alias', '.options']
        self.op = OperationParser()

    def __call__(self, data):
        def load_from_yaml(f, content):
            try:
                cfg = yaml.load(f)
                # data.update(lower_keys(cfg))
                data.update(cfg)
            except Exception as e:
                raise FormatError("DSC script ``{}`` is ill-formatted:\n``{}``".\
                                  format(content, e))
        #
        if os.path.isfile(self.content):
            with open(self.content) as f:
                load_from_yaml(f, self.content)
        else:
            if len(self.content.split('\n')) == 1:
                raise ValueError("Cannot find file ``{}``".format(self.content))
            with StringIO(self.content) as f:
                load_from_yaml(f, '<Input String>')
                self.content = 'DSCStringIO.yml'
        has_dsc = False
        # Handle derived class
        for block in self.__get_blocks(data):
            groups = re.search('^(.*?)\((.*?)\)$', block)
            if groups:
                data[groups.group(1).strip()] = \
                  update_nested_dict(copy.deepcopy(data[groups.group(2).strip()]), data[block])
                del data[block]
        for block in list(data.keys()):
            # Check invalid block names
            if not re.match(r'^[A-Za-z0-9_]+$', block):
                raise FormatError("Block name should contain only alphanumeric letters "\
                                  "or underscore: ``{}``".format(block))
            if block.split('_')[-1].isdigit():
                raise FormatError("Block name should not end with ``_{}``: ``{}``".\
                                  format(block.split('_')[-1], block))
            # Load data
            if block == 'DSC':
                # handle DSC section
                has_dsc = True
                if 'run' not in data.DSC:
                    raise FormatError('Missing required ``DSC::run``.')
                if self.sequence is not None:
                    data.DSC['run'] = ', '.join(self.sequence)
                data.DSC['run'] = self.__expand_slice(data.DSC['run'])
                data.DSC['run'] = [(x,) if isinstance(x, str) else x for x in self.op(data.DSC['run'])]
                if self.output is not None:
                    data.DSC['output'] = self.output
                if try_get_value(data, ('DSC', 'output')) is None:
                    logger.warning('Missing output database name in ``DSC::output``. '\
                                       'Use default name ``{}``.'.\
                                       format(os.path.split(os.path.splitext(self.content)[0])[-1]))
                    set_nested_value(data, ('DSC', 'output'),
                                     os.path.split(os.path.splitext(self.content)[0])[-1])
                if try_get_value(data, ('DSC', 'work_dir')) is None:
                    set_nested_value(data, ('DSC', 'work_dir'), './')
            else:
                # handle blocks: format / check entries
                has_exec = has_return = False
                for key in list(data[block].keys()):
                    if key not in self.block_kw:
                        logger.warning('Ignore unknown entry ``{}`` in block ``{}``.'.\
                                           format(key, block))
                        del data[block][key]
                    if key == 'exec':
                        has_exec = True
                    if key == 'return':
                        has_return = True
                if not has_exec:
                    raise FormatError('Missing required entry ``exec`` in block ``{}``'.format(block))
                if not has_return:
                    raise FormatError('Missing required entry ``return`` in block ``{}``'.format(block))
                data[block] = self.__format_block(data[block])
        if not has_dsc:
            raise ValueError('Cannot find required section ``DSC``!')

    def __get_blocks(self, data):
        '''
        Return to sorted block names such that derived block always follows the base block

        name of derived blocks looks like: "derived(base)"
        '''
        base = []
        derived = []
        blocks = []
        for block in data:
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
                raise FormatError("Looped block inheritance: {0}({1}) and {1}({0})!".\
                                  format(item[0][0], item[0][1]))
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
            if len(blocks) == len(data.keys()):
                break
        return blocks

    def __format_block(self, section_data):
        '''
        Format block data to meta / params etc for easier manipulation

          * meta: will contain exec information
          * params:
            * params[0] (for shared params), params[1], params[2], (corresponds to exec[1], exec[2]) ...
          * rules:
            * rules[0] (for shared params), rules[1] ...
          * params_alias:
            * params_alias[0], params_alias[1] ...
        '''
        res = dotdict()
        res.meta = {}
        res.params = {0:{}}
        res.rules = {}
        res.params_alias = {}
        res.out = section_data['return']
        # Parse meta
        res.meta['exec'] = section_data['exec']
        if 'seed' in section_data:
            res.meta['seed'] = section_data['seed']
        if '.logic' in section_data:
            # no need to expand exec logic
            res.meta['rule'] = section_data['.logic']
        if '.alias' in section_data:
            res.meta['exec_alias'] = section_data['.alias']
        # Parse params
        if 'params' in section_data:
            for key, value in section_data['params'].items():
                try:
                    # get indexed slice
                    name, idxes = get_slice(key)
                    if name != 'exec':
                        raise FormatError('Unknown indexed parameter entry: {}.'.format(key))
                    for idx in idxes:
                        idx += 1
                        if idx == 0:
                            raise FormatError('Invalid entry: exec[0]. Index must start from 1.')
                        if idx in res.params:
                            res.params[idx].update(flatten_dict(value))
                        else:
                            res.params[idx] = flatten_dict(value)
                except AttributeError:
                    res.params[0][key] = flatten_dict(value)
            # Parse rules and params_alias
            for key in list(res.params.keys()):
                if '.logic' in res.params[key]:
                    res.rules[key] = self.op(res.params[key]['.logic'])
                    del res.params[key]['.logic']
                if '.alias' in res.params[key]:
                    res.params_alias[key] = res.params[key]['.alias']
                    del res.params[key]['.alias']
        return dotdict(strip_dict(res))

    def __expand_slice(self, line):
        '''
        input: .... xxx[1,2,3] ....
        output: .... (xxx[1], xxx[2], xxx[3]) ....
        '''
        pattern = re.compile(r'\w+\[(?P<b>.+?)\](?P<a>,|\s*|\*|\+)')
        for m in re.finditer(pattern, line):
            sliced_text = get_slice(m.group(0))
            if len(sliced_text[1]) == 1:
                continue
            text = '({})'.format(','.join(['{}[{}]'.format(sliced_text[0], x + 1) for x in sliced_text[1]]))
            line = line.replace(m.group(0), text + m.group('a'), 1)
        return line

class DSCEntryFormatter(DSCFileParser):
    '''
    Run format transformation to DSC entries
    '''
    def __init__(self):
        DSCFileParser.__init__(self)

    def __call__(self, data):
        actions = [Str2List(),
                   ExpandVars(try_get_value(data.DSC, 'parameters')),
                   ExpandActions(),
                   CastData()]
        data = self.__Transform(data, actions)

    def __Transform(self, cfg, actions):
        '''Apply actions to items'''
        for key, value in list(cfg.items()):
            if isinstance(value, collections.Mapping):
                self.__Transform(value, actions)
            else:
                for a in actions:
                    value = a(value)
                if is_null(value):
                    del cfg[key]
                else:
                    cfg[key] = value
        return cfg

class DSCData(dotdict):
    '''
    Read DSC configuration file and translate it to a collection of steps to run DSC

    This class reflects the design and implementation of DSC structure and syntax

    Tasks here include:
      * Properly parse DSC file in YAML format
      * Translate DSC file text
        * Replace Operators R() / Python() / Shell() / Combo() ...
        * Replace global variables
      * Some sanity check

    Structure of self:
      self.block_name.block_param_name = dict()
    '''
    def __init__(self, content, sequence = None, output = None, check_rlibs = True):
        actions = [DSCFileLoader(content, sequence, output), DSCEntryFormatter()]
        for a in actions:
            a(self)
        for name in list(self.keys()):
            if name == 'DSC':
                continue
            else:
                # double check if any computational routines are
                # out of index
                self[name]['meta']['exec'] = [tuple(x.split()) if isinstance(x, str) else x
                                              for x in self[name]['meta']['exec']]
                if ('exec_alias' in self[name]['meta'] and 'rule' not in self[name]['meta'] \
                    and len(self[name]['meta']['exec_alias']) != len(self[name]['meta']['exec'])) or \
                    ('exec_alias' in self[name]['meta'] and 'rule' in self[name]['meta'] \
                     and len(self[name]['meta']['exec_alias']) != len(self[name]['meta']['rule'])):
                    raise FormatError('Alias does not match the length of exec, in block ``{}``!'.\
                                      format(name))
                if 'params' in self[name]:
                    max_exec = max(self[name]['params'].keys())
                    if max_exec > len(self[name]['meta']['exec']):
                        raise FormatError('Index for exec out of range: ``exec[{}]``.'.format(max_exec))
        #
        rlibs = try_get_value(self['DSC'], ('R_libs'))
        if rlibs and check_rlibs:
            rlibs_md5 = textMD5(repr(rlibs) + str(datetime.date.today()))
            if not os.path.exists('.sos/.dsc/RLib.{}.info'.format(rlibs_md5)):
                install_r_libs(rlibs)
                os.makedirs('.sos/.dsc', exist_ok = True)
                os.system('echo "{}" > {}'.format(repr(rlibs),
                                                  '.sos/.dsc/RLib.{}.info'.format(rlibs_md5)))

    def __str__(self):
        res = ''
        for item in sorted(list(dict(self).items())):
            # res += dict2str({item[0]: dict(item[1])}, replace = [('!!python/tuple', '(tuple)')]) + '\n'
            res += dict2str({item[0]: dict(item[1])}) + '\n'
        return res.strip()
