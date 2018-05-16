#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

'''Handle one line in a DSC file, a customized YAML parser'''

import re, collections
from io import StringIO
import tokenize
from sos.utils import get_output
from .utils import FormatError, is_null, str2num, cartesian_list, pairwise_list, uniq_list, \
    get_slice, remove_parens, do_parentheses_match, find_parens, parens_aware_split, flatten_list
from .syntax import DSC_FILE_OP

class YLine:
    '''
    Apply to a YAML line: a string or a list
    '''
    def __init__(self):
        pass

    def __call__(self, value):
        return value

    @staticmethod
    def split(value):
        '''Split value by comma outside (), [] and {}'''
        return parens_aware_split(value, ',')

    def decodeVar(self, var):
        '''
        Try to properly decode str to other data type
        '''
        # Try to convert to number
        var = str2num(var)
        if isinstance(var, str):
            # see if str can be converted to a list or tuple
            # and apply the same procedure to their elements
            p1 = list(find_parens(var).items())
            p2 = list(find_parens(var, start='[', end=']').items())
            if ((len(p1) >= 1 and p1[-1][0] == 0 and p1[-1][1] == len(var) - 1) or \
               (len(p2) >= 1 and p2[-1][0] == 0 and p2[-1][1] == len(var) - 1)):
                var = [self.decodeVar(x.strip()) for x in self.split(remove_parens(var))]
                if len(var) == 1:
                    var = var[0]
                else:
                    var = tuple(var)
        return var


class Str2List(YLine):
    '''
    Convert string to list via splitting by comma outside of parenthesis
    '''
    def __init__(self):
        YLine.__init__(self)
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


class ExpandVars(YLine):
    '''
    Replace DSC variable place holder with actual value

    e.g. $(filename) -> "text.txt"
    '''
    def __init__(self, global_var):
        YLine.__init__(self)
        self.global_var = global_var if global_var else dict()

    def encodeVar(self, var, slice_idx):
        '''
        Code multi-entry data type to string
          * For tuple / list will make it into a string like "[item1, item2 ...]"
        '''
        var = self.split(var)
        if slice_idx is not None and not isinstance(var, (list, tuple)):
            var = [var]
        if isinstance(var, (list, tuple)):
            if slice_idx is not None:
                var = [var[i] for i in get_slice('slice[' + slice_idx + ']')[1]]
            if len(var) == 1:
                    return '{}'.format(var[0])
            else:
                return '[{}]'.format(','.join(list(map(str, var))))
        else:
            return var

    def __call__(self, value):
        if isinstance(value, str):
            # find pattern with slicing first
            pattern = re.compile(r'\$\{(.*?)\}\[(.*?)\]')
            for m in re.finditer(pattern, value):
                if m.group(1) not in self.global_var:
                    raise FormatError(f"Cannot find variable ``{m.group(1)}`` in DSC::global")
                value = value.replace(m.group(0), self.encodeVar(self.global_var[m.group(1)], m.group(2)))
            # then pattern without slicing
            pattern = re.compile(r'\$\{(.*?)\}')
            for m in re.finditer(pattern, value):
                if m.group(1) not in self.global_var:
                    raise FormatError(f"Cannot find variable ``{m.group(1)}`` in DSC::global")
                value = value.replace(m.group(0), self.encodeVar(self.global_var[m.group(1)], None))
        return value


class ExpandActions(YLine):
    '''
    Run action entries and get values.

    Action entries are
      * R(), Python(), Shell()
      * each() and pairs()
    Untouched entries are:
      * file(), temp(), raw()
    because they'll have to be dynamically determined
    '''
    def __init__(self):
        YLine.__init__(self)
        self.method = {
            'R': self.__R,
            'Python': self.__Python,
            'Shell': self.__Shell,
            'each': self.__ForEach,
            'pairs': self.__Pairs
            }

    def __call__(self, value):
        if isinstance(value, str):
            for name in list(self.method.keys()):
                pos = [m.end() - 1 for m in re.finditer(f'{name}(\(|\{{)', value)]
                p_end = 0
                replacements = []
                for p in pos:
                    if value[p] == '(':
                        shatter = False
                        start = '('
                        end = ')'
                    else:
                        shatter = True
                        start = '{'
                        end = '}'
                    if p < p_end:
                        # Run into nested pattern, no problem: eg R(some_function_R())
                        continue
                    try:
                        p_end = find_parens(value[p:], start = start, end = end)[0]
                    except IndexError:
                        raise FormatError(f"Invalid parentheses pattern in ``{value}``")
                    replacements.append((f'{name}{value[p:p_end+p+1]}',
                                         ('(' if shatter == False else '') + self.method[name](value[p:p_end+p+1]) + (')' if shatter == False else '')))
                for r in replacements:
                    value = value.replace(r[0], r[1], 1)
        return value

    def __ForEach(self, value):
        raw_value = value
        value = [self.decodeVar(x) for x in self.split(value)]
        if len(value) == 1:
            raise FormatError(f'Cannot produce combinations for single value ``{raw_value}``! '\
                             ' Please use "," to separate input string to multiple values.')
        value = [x if isinstance(x, (list, tuple)) else [x] for x in value]
        return cartesian_list(*value)

    def __Pairs(self, value):
        value = [self.decodeVar(x) for x in self.split(value)]
        value = [x if isinstance(x, (list, tuple)) else [x] for x in value]
        return pairwise_list(*value)

    @staticmethod
    def __R(code):
        try:
            output = get_output(f"R --slave -e \"cat(dscrutils::dscreval({repr(code[1:-1])}))\"").strip()
        except Exception:
            from .utils import install_r_lib
            from .version import __version__
            install_r_lib(f'dscrutils@stephenslab/dsc/dscrutils ({__version__}+)')
            output = get_output(f"R --slave -e \"cat(dscrutils::dscreval({repr(code[1:-1])}))\"").strip()
        return output

    @staticmethod
    def __Python(code):
        if not isinstance(code, str):
            return str(code)
        try:
            res = eval(code)
        except Exception as e:
            raise FormatError(f"Evaluation of the following Python expression failed:\n``{code}``.\nError message: ``{e}``")
        if isinstance(res, (bool, int, float, str)):
            return str(res)
        elif isinstance(res, (list, tuple)):
            return ','.join(map(str, res))
        else:
            raise FormatError(f"Evaluation of Python expression ``code`` resulted in unsupported type ``{type(res).__name__}``.")

    @staticmethod
    def __Shell(code):
        # FIXME: is this behavior any good?
        out = get_output(code[1:-1])
        return ','.join(flatten_list([x.split() for x in out.strip().split("\n")]))


class CastData(YLine):
    def __init__(self):
        YLine.__init__(self)

    def __call__(self, value):
        # Recode strings
        for idx, item in enumerate(value):
            value[idx] = self.decodeVar(item)
        # Properly convert lists and tuples
        if len(value) == 1 and isinstance(value[0], list):
            return value[0]
        else:
            res = []
            for x in value:
                if isinstance(x, list):
                    # [[],[]] -> [(),()]
                    res.append(tuple(x))
                else:
                    res.append(x)
            return uniq_list(res)

class CheckFile(YLine):
    def __init__(self):
        YLine.__init__(self)

    def __call__(self, value):
        for item in value:
            if isinstance(item, tuple):
                for ii in item:
                    if isinstance(ii, str) and DSC_FILE_OP.search(ii):
                        raise FormatError(f'File operator inside tuple ``{item}`` is not allowed!')
            else:
                if isinstance(item, str) and DSC_FILE_OP.search(item) and len(value) > 1:
                    raise FormatError(f'Cannot mix file operator ``{item}`` with other values ``{[i for i in value if i != item]}``!')
        return value

class OperationParser(YLine):
    '''
    Parse DSC sequence variables by expanding them

    Input: a string sequence of 'run'
    '''
    def __init__(self):
        YLine.__init__(self)
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
            raise FormatError("Argument must be string but it is %s." % type(value))
        value = value.strip()
        if value[-1] in ['+', '*', ',', '=', '/']:
            raise FormatError('The end of operator ``"{}"`` cannot be operator ``{}``!'.\
                              format(value, value[-1]))
        res = []
        for seq in self.split(value):
            self.reset()
            self.sequence = seq
            #seq = seq.replace(' ', '')
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
        seq = [y.strip() for y in re.split(r'({})'.format("|".join([re.escape(x) for x in self.operators])), value) if y.strip()]
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
        from .utils import non_commutative_symexpand
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
            tmp_1 = dict((y if '_' not in y else y.split('_')[0], y) for y in x)
            if len(tmp_1.keys()) < len(x):
                raise FormatError("Possibly duplicated elements found in sequence {}".\
                                  format(self.sequence))
            tmp_2 = []
            for y in tmp_1:
                if '_' in tmp_1[y]:
                    tmp_3 = [self.cache[i] for i in tmp_1[y].split('_')]
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


class LogicParser(OperationParser):
    '''
    Parse DSC sequence variables by expanding them

    Input: a string sequence of @FILTER
    '''
    def __init__(self):
        OperationParser.__init__(self)
        self.operators = ['(', ')', '|', '&', '~']

    def reconstruct(self, value):
        from .utils import bool_symexpand
        res = []
        for x in str(bool_symexpand(value)).split('|'):
            x = x.strip().strip('(').strip(')').split('&')
            tmp = [self.cache[y.strip()] if not y.strip().startswith('~') else f'not {self.cache[y.strip()[1:]]}'
                   for y in x]
            res.append(tuple(tmp) if len(tmp) > 1 else tmp[0])
        return res


class EntryFormatter:
    '''
    Run format transformation to DSC entries
    '''
    def __init__(self):
        pass

    def __call__(self, data, variables):
        actions = [ExpandVars(variables),
                   ExpandActions(),
                   Str2List(),
                   CastData(),
                   CheckFile()]
        return self.__Transform(data, actions)

    def __Transform(self, cfg, actions):
        '''Apply actions to items'''
        for key, value in list(cfg.items()):
            if isinstance(value, str):
                value = value.strip().strip(',')
            if isinstance(value, collections.Mapping):
                self.__Transform(value, actions)
            else:
                if not do_parentheses_match(str(value)):
                    raise FormatError(f"Invalid parentheses matching pattern in ``{str(value)}``")
                for a in actions:
                    value = a(value)
                # empty list
                if len(value) == 0:
                    del cfg[key]
                else:
                    cfg[key] = value
        return cfg

def parse_exe(string):
    '''
    input: eg. R(some, code) + (a.R cmd_args, b.R cmd_args) + R(some, code)
    output: (R(some, code), a.R cmd_args, R(some, code)), (R(some, code), b.R cmd_args, R(some, code))
    '''
    if not do_parentheses_match(string):
        raise FormatError(f"Invalid parentheses matching pattern in ``{string}``")
    #
    def parse_inline(inline, sigil):
        #
        def get_item(item):
            if sigil == '$':
                return f'${item}'
            elif sigil == '{}':
                return '{%s}' % item
            else:
                return item
        #
        replaced_inline = []
        # for global variable
        pattern = re.compile(r'\${(.*?)\}')
        for m in re.finditer(pattern, inline):
            inline = inline.replace(m.group(0), get_item(m.group(1)))
            replaced_inline.append((m.group(1), '${%s}' % m.group(1)))
        # for module variable
        pattern = re.compile(r'\$\((.*?)\)')
        for m in re.finditer(pattern, inline):
            inline = inline.replace(m.group(0), get_item(m.group(1)))
            replaced_inline.append((m.group(1), f'${m.group(1)}'))
        return inline, replaced_inline
    #
    ext_map = {'R': 'R', 'Python': 'PY', 'Shell': 'SH'}
    action_dict = dict()
    idx = 0
    for name in ext_map:
        pos = [m.end() - 1 for m in re.finditer(f'{name}\(', string)]
        p_end = 0
        replacements = []
        for p in pos:
            if p < p_end:
                continue
            p_end = find_parens(string[p:])[0]
            key = f'__DSC_INLINE_{idx}__'
            action_dict[key] = (ext_map[name], string[(p+1):(p_end+p)])
            replacements.append((f'{name}{string[p:p_end+p+1]}', key))
            idx += 1
        for r in replacements:
            string = string.replace(r[0], r[1], 1)
    string = string.replace('*', '__DSC_STAR__').replace('+', '*')
    res = []
    replaced = []
    try:
        string_parsed = OperationParser()(string)
    except Exception:
        raise RuntimeError(f'Invalid executable operator: {string}')
    for x in string_parsed:
        if isinstance(x, str):
            x = [x]
        else:
            x = list(x)
        exe_type = []
        for idx, item in enumerate(x):
            x[idx] = item.replace('__DSC_STAR__', '*').split(None, 1)
            # After split, the first element is script or command variable
            # the other is command arguments
            if len(x[idx]) > 1:
                x[idx][1], content = parse_inline(x[idx][1], '{}')
                replaced.extend(content)
            is_typed = False
            for key, value in action_dict.items():
                if key in x[idx][0]:
                    content = parse_inline(value[1], '$' if value[0] == 'SH' else None)
                    x[idx][0] = x[idx][0].replace(key, content[0], 1)
                    replaced.extend(content[1])
                    exe_type.append(value[0])
                    is_typed = True
                    if all([x in exe_type for x in ext_map.values()]):
                        raise FormatError(f"Cannot mix executable types ``{[x for x in exe_type if x != 'unknown']}``, near ``{x[idx][0]}``")
            if not is_typed:
                exe_type.append('unknown')
        res.append(tuple([tuple(exe_type)] + x))
    return res, dict(replaced)

def expand_logic(string):
    '''
    bool logic expander
    '''
    string = string.replace('|', '__DSC_BAR__')
    string = string.replace('~', '__DSC_TA__')
    string = string.replace('&', '__DSC_N__')
    string = string.replace(' or ', '|')
    string = string.replace(' OR ', '|')
    string = string.replace(' and ', '&')
    string = string.replace(' AND ', '&')
    string = string.replace(' not ', '~')
    string = string.replace(' NOT ', '~')
    quote_dict = dict()
    idx = 1
    for m in re.findall(r"\"[^\"]+\"|'[^']+'", string):
     # - Match either of the following options
     #     - `"[^"]+"`
     #         - `"` Match this literally
     #         - `[^"]+` Match any character except `"` one or more times
     #         - `"` Match this literally
     #     - `'[^']+'`
     #         - `'` Match this literally
     #         - `[^']+` Match any character except `'` one or more times
     #         - `'` Match this literally
        key = f'__DSC_QT_{idx}__'
        quote_dict[key] = m
        string = string.replace(m, key, 1)
        idx += 1
    res = []
    op = LogicParser()
    for x in op(string):
        if isinstance(x, str):
            x = [x]
        else:
            x = list(x)
        for idx in range(len(x)):
            x[idx] = x[idx].replace('__DSC_BAR__', '|')
            x[idx] = x[idx].replace('__DSC_TA__', '~')
            x[idx] = x[idx].replace('__DSC_N__', '&')
            for key, value in quote_dict.items():
                x[idx] = x[idx].replace(key, value, 1)
        res.append(tuple(x))
    return res

def parse_filter(condition, groups = None, dotted = True):
    '''
    parse condition statement
    After expanding, condition is a list of list
    - the outer lists are connected by OR
    - the inner lists are connected by AND
    eg: input 'x in 0 and not (y < x and y > 0)'
    output:
    [('', ['', 'x'], 'in', '0'), ('not', ['', 'y'], '<', 'x')]
    [('', ['', 'x'], 'in', '0'), ('not', ['', 'y'], '>', '0')]
    '''
    # FIXME: check legalize names
    if condition is None:
        return ([], [])
    groups = groups or dict()
    res = []
    cond_tables = []
    symbols = ['=', '==', '!=', '>', '<', '>=', '<=', 'in']
    try:
        expanded_condition = expand_logic(' and '.join(condition) if isinstance(condition, (list, tuple)) else condition)
    except Exception:
        raise FormatError(f"Condition ``{' AND '.join(condition)}`` cannot be properly expanded by logic operators. Please ensure the correct use of logic syntax.")
    for and_list in expanded_condition:
        tmp = []
        for value in and_list:
            if value.strip().lower().startswith('not '):
                value = value.strip()[4:]
                is_not = True
            else:
                value = value.strip()
                is_not = False
            tokens = [token[1] for token in tokenize.generate_tokens(StringIO(value).readline) if token[1]]
            if tokens[1] == 'in':
                tokens = [tokens[0], tokens[1], f"({remove_parens(''.join(tokens[2:]))})"]
            if not (len(tokens) == 3 and tokens[1] in symbols) and not (len(tokens) == 5 and tokens[1] == '.' and tokens[3] in symbols):
                raise FormatError(f"Condition ``{value}`` is not a supported math expression.\nSupported expressions are ``{symbols}``")
            if len(tokens) == 5:
                tokens = [[tokens[0], tokens[2]], tokens[3], tokens[4]]
            if isinstance(tokens[0], str):
                if dotted:
                    raise FormatError(f"Condition contains invalid module / parameter specification ``{'.'.join(tokens[0])}`` ")
                else:
                    tokens[0] = ['', tokens[0]]
            if not tokens[0][0] in groups:
                tmp.append(('not' if is_not else '', tokens[0], "==" if tokens[1] == "=" else tokens[1], tokens[2]))
                cond_tables.append((tokens[0][0], tokens[0][1]))
            else:
                # will be connected by OR logic
                tmp.append([('not' if is_not else '', [x, tokens[0][1]], "==" if tokens[1] == "=" else tokens[1], tokens[2])
                            for x in groups[tokens[0][0]]])
                cond_tables.extend([(x, tokens[0][1]) for x in groups[tokens[0][0]]])
        res.append(tmp)
    return res, cond_tables
