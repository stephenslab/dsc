#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import sys, os, re, itertools, collections, sympy
from itertools import cycle, chain, islice
from collections import OrderedDict
from multiprocessing import Process, Manager
from ruamel.yaml import YAML
from ruamel.yaml.compat import StringIO
from difflib import SequenceMatcher
from .constant import HTML_CSS, HTML_JS
from xxhash import xxh32 as xxh

class Logger:
    def __init__(self):
        self.__width_cache = 1
        self.verbosity = 2

    def error(self, msg = None, q = True):
        if msg is None:
            sys.stderr.write('\n')
            return
        if isinstance(msg, list):
            msg = ' '.join(map(str, msg))
        else:
            msg = str(msg)
        start = '\n' if msg.startswith('\n') else ''
        end = '\n' if msg.endswith('\n') else ''
        msg = msg.strip()
        if q:
            sys.stderr.write(start + f"\033[1;33mERROR: {self.emphasize(msg, 33)}\033[0m\n" + end)
            sys.exit(1)
        else:
            sys.stderr.write(start + f"\033[1;35mWARNING: {self.emphasize(msg, 35)}\033[0m\n" + end)

    def log(self, msg = None, flush=False, debug=False):
        if msg is None:
            sys.stderr.write('\n')
            return
        if isinstance(msg, list):
            msg = ' '.join(map(str, msg))
        else:
            msg = str(msg)
        start = "{0:{width}}".format('\r', width = self.__width_cache + 10) + "\r" if flush else ''
        end = '' if flush else '\n'
        start = '\n' + start if msg.startswith('\n') else start
        end = end + '\n' if msg.endswith('\n') else end
        msg = msg.strip()
        if debug:
            sys.stderr.write(start + f"\033[1;34mDEBUG: {self.emphasize(msg, 34)}\033[0m" + end)
        else:
            sys.stderr.write(start + f"\033[1;32mINFO: {self.emphasize(msg, 32)}\033[0m" + end)
        self.__width_cache = len(msg)

    def debug(self, msg = None, flush = False):
        if self.verbosity < 3:
            return
        self.log(msg, flush, True)

    def info(self, msg = None, flush = False):
        if self.verbosity < 2:
            return
        self.log(msg, flush, False)

    def warning(self, msg = None):
        if self.verbosity == 0:
            return
        self.error(msg, False)

    @staticmethod
    def emphasize(msg, level_color):
        if msg is None:
            return msg
        return re.sub(r'``([^`]*)``', f'\033[0m\033[1;4m\\1\033[0m\033[1;{level_color}m', str(msg))

logger = Logger()

class DYAML(YAML):
    def dump(self, data, stream=None, **kw):
        inefficient = False
        if stream is None:
            inefficient = True
            stream = StringIO()
        YAML.dump(self, data, stream, **kw)
        if inefficient:
            return stream.getvalue()

yaml = DYAML()

class FormatError(Exception):
    """Raised when format is illegal."""
    def __init__(self, msg):
        Exception.__init__(self, msg)
        self.args = (msg, )

class DBError(Exception):
    """Raised when there is a problem building the database."""
    def __init__(self, msg):
        Exception.__init__(self, msg)
        self.args = (msg, )

Expr_mul = sympy.Expr.__mul__

def mymul(a,b):
     if not a.is_commutative and not b.is_commutative:
         if isinstance(a, sympy.Symbol) and isinstance(b, sympy.Symbol):
             return(Expr_mul(a,b))
         else:
             return(Expr_mul(a,b))
     else:
         return(Expr_mul(a,b))

sympy.Expr.__mul__ = mymul

def non_commutative_symexpand(expr_string):
    from sympy.parsing.sympy_parser import parse_expr
    parsed_expr = parse_expr(expr_string, evaluate=False)
    new_locals = {sym.name:sympy.Symbol(sym.name, commutative=False)
                  for sym in parsed_expr.atoms(sympy.Symbol)}
    return sympy.expand(eval(expr_string, {}, new_locals))

def bool_symexpand(expr_string):
    from sympy.parsing.sympy_parser import parse_expr
    from sympy.logic.boolalg import to_dnf
    parsed_expr = parse_expr(expr_string, evaluate=False)
    new_locals = {sym.name:sympy.Symbol(sym.name)
                  for sym in parsed_expr.atoms(sympy.Symbol)}
    return to_dnf(eval(expr_string, {}, new_locals), simplify = True)

def lower_keys(x, level_start = 0, level_end = 2, mapping = dict):
    level_start += 1
    if level_start > level_end:
        return x
    if isinstance(x, list):
        return [lower_keys(v, level_start, level_end) for v in x]
    elif isinstance(x, collections.Mapping):
        return mapping((k.lower(), lower_keys(v, level_start, level_end)) for k, v in x.items())
    else:
        return x

def is_null(var):
    if var is None:
        return True
    if isinstance(var, str):
        if var.lower() in ['na','nan','null','none','']:
            return True
    if isinstance(var, (list, tuple, collections.Mapping)):
        return True if len(var) == 0 else False
    return False

def convert_null(var, language):
    if var is None:
        if language.lower() == 'r':
            return "NULL"
        # FIXME: more to comce
        else:
            return None
    else:
        return var

def str2num(var):
    if isinstance(var, str):
        # try to warn about boolean
        if var in ['T', 'F'] or var.lower() in ['true', 'false']:
            bmap = {'t': 1, 'true': 1, 'f': 0, 'false': 0}
            msg = 'Possible Boolean variable detected: ``{}``. \n\
            This variable will be treated as string, not Boolean data. \n\
            It may cause problems to your jobs. \n\
            Please set this variable to ``{}`` if it is indeed Boolean data.'.format(var, bmap[var.lower()])
            logger.error('\n\t'.join([x.strip() for x in msg.split('\n')]), False)
        try:
            return int(var)
        except ValueError:
            try:
                var = float(var)
                if var.is_integer():
                    return int(var)
                else:
                    return var
            except ValueError:
                return re.sub(r'''^"|^'|"$|'$''', "", var)
    else:
        try:
            if var.is_integer():
                return int(var)
            else:
                return var
        except AttributeError:
            return var

def cartesian_dict(value, mapping = dict):
    return [mapping(zip(value, x)) for x in itertools.product(*value.values())]

def cartesian_list(*args):
    return list(itertools.product(*args))

def pairwise_list(*args):
    if len(set(len(x) for x in args)) > 1:
        raise ValueError("Cannot perform pairwise operation because input vectors are not of equal lengths.")
    return list(map(tuple, zip(*args)))

def flatten_list(lst):
    return sum( ([x] if not isinstance(x, (list, tuple)) else flatten_list(x) for x in lst), [] )

def flatten_dict(d, mapping = dict):
    if not isinstance(d, collections.Mapping):
        return d
    items = []
    for k, v in d.items():
        if isinstance(v, collections.Mapping):
            items.extend(flatten_dict(v).items())
        else:
            items.append((k, v))
    return mapping(items)

def uniq_list(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (repr(x) in seen or seen_add(repr(x)))]

def merge_lists(seq1, seq2):
    '''
    >>> keys1 = ['A', 'B', 'C', 'D', 'E',           'H', 'I']
    >>> keys2 = ['A', 'B',           'E', 'F', 'G', 'H',      'J', 'K']
    >>> merge_lists(keys1, keys2)
    ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K']
    '''
    sm = SequenceMatcher(a=seq1,b=seq2)
    res = []
    for (op, start1, end1, start2, end2) in sm.get_opcodes():
        if op == 'equal' or op=='delete':
            #This range appears in both sequences, or only in the first one.
            res += seq1[start1:end1]
        elif op == 'insert':
            #This range appears in only the second sequence.
            res += seq2[start2:end2]
        elif op == 'replace':
            #There are different ranges in each sequence - add both.
            res += seq1[start1:end1]
            res += seq2[start2:end2]
    return res

def get_slice(value, all_tuple = True, mismatch_quit = True, non_negative = True):
    '''
    Input string is R index style: 1-based, end position inclusive slicing
    Output index will convert it to Python convention
    exe[1,2,4] ==> (exe, (0,1,3))
    exe[1] ==> (exe, (0))
    exe[1:4] ==> (exe, (0,1,2,3))
    exe[1:9:2] ==> (exe, (0,2,4,6,8))
    '''
    try:
         slicearg = re.search('\[(.*?)\]', value).group(1)
    except Exception as e:
         if mismatch_quit:
              raise AttributeError(f'Cannot obtain slice from input string {value}. [{e}].')
         else:
              return value, None
    name = value.split('[')[0]
    idxs = []
    for item in slicearg.split(','):
         item = item.strip()
         if not item:
              continue
         if ':' in item:
              slice_ints = [ int(n) for n in item.split(':') ]
              if len(slice_ints) == 1:
                   raise ValueError('Wrong syntax for slice {}.'.format(value))
              slice_ints[1] += 1
              slice_obj = slice(*tuple(slice_ints))
              idxs.extend([x - 1 for x in range(slice_obj.start or 0, slice_obj.stop or -1, slice_obj.step or 1)])
         else:
              idxs.append(int(item) - 1)
    if any([i < 0 for i in idxs]) and non_negative:
        raise ValueError("Invalid slice ``{}``. Indices should be positive integers.".format(value))
    if len(idxs) == 1 and not all_tuple:
         return name, idxs[0]
    else:
         return name, tuple(idxs)

def expand_slice(line):
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

def try_get_value(value, keys, default = None):
    '''
    Input: dict_data, (key1, key2, key3 ...)
    Output: dict_data[key1][key2][key3][...] or None
    '''
    if value is None:
        return default
    if not isinstance(keys, (list, tuple)):
        keys = [keys]
    try:
        if len(keys) == 0:
            return value
        else:
            return try_get_value(value[keys[0]], keys[1:])
    except KeyError:
        return default

def set_nested_value(d, keys, value, default_factory = dict):
    """
    Equivalent to `reduce(dict.get, keys, d)[newkey] = newvalue`
    if all `keys` exists and corresponding values are of correct type
    """
    for key in keys[:-1]:
        try:
            val = d[key]
        except KeyError:
            val = d[key] = default_factory()
        else:
            if not isinstance(val, collections.MutableMapping):
                val = d[key] = default_factory()
        d = val
    d[keys[-1]] = value

def find_nested_key(key, dictionary):
    '''
    example = {'simulate': {'exec1': {'key': 'value'}}}
    print(list(find('key', example)))
    # [['simulate', 'exec1', 'key']]
    '''
    for k, v in dictionary.items():
        if k == key:
            yield [k]
        elif isinstance(v, dict):
            for result in find_nested_key(key, v):
                yield [k] + result
        elif isinstance(v, list):
            for d in v:
                for result in find_nested_key(key, d):
                    yield [k] + result

def get_nested_keys(dictionary):
    for k, v in dictionary.items():
        if not isinstance(v, collections.Mapping):
            yield [k]
        else:
            for result in get_nested_keys(v):
                yield [k] + result

def dict2str(value):
    res = yaml.dump(strip_dict(value, into_list = True))
    # pattern = re.compile(r'!!python/(.*?)\s')
    # for m in re.finditer(pattern, res):
    #     res = res.replace(m.group(1), '', 1)
    # res = res.replace('!!python/', '')
    # res = '\n'.join([x[2:] for x in res.split('\n') if x.strip() != 'dictitems:' and x])
    return res

def update_nested_dict(d, u, mapping = dict):
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            r = update_nested_dict(d.get(k, mapping()), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d

def strip_dict(data, mapping = dict, into_list = False, skip_keys = None):
    if not isinstance(data, collections.Mapping):
        return data
    skip_keys = skip_keys or []
    mapping_null = [dict()]
    new_data = mapping()
    for k, v in data.items():
        if k in skip_keys:
            new_data[k] = v
            continue
        if isinstance(v, collections.Mapping):
            v = strip_dict(v, mapping, into_list)
        if isinstance(v, list) and into_list:
            v = [strip_dict(x, mapping, into_list) for x in v]
        if not is_null(v) and not v in mapping_null:
            new_data[k] = v
    return new_data

def extend_dict(dict1, dict2, unique = False):
     for key, value in dict2.items():
         if isinstance(value, list):
             dict1.setdefault(key, []).extend(value)
         else:
             dict1.setdefault(key, []).append(value)
         if unique:
             dict1[key] = uniq_list(dict1[key])
     return dict1

def sos_hash_output(values, jobs = 1):
    '''
    Parallel hash
    FIXME: parallel not implemented for now
    '''
    return [xxh(value).hexdigest() for value in values]

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    return [l[i:i + n] for i in range(0, len(l), n)]

def sos_pair_input(value):
    '''Input must be a list of two lists,
    the lists are ordered such that the length of the
    2nd list is always multiples of the previous list.
    The way lists are supposed to be combined is:
    ABCD              ABCDABCD
    ABCDEFGH -------> ABCDEFGH ------> ABCDABCDABCDEFGH

    Input can also be a flat list of 2N length in which case
    I'll pair the first N with the 2nd N
    '''
    if len(value) == 2 and isinstance(value[0], (list, tuple)):
        # Input is a pair of vectors
        multiplier = len(value[1]) / len(value[0])
        if multiplier > int(multiplier):
            # is not integer
            raise ValueError('Length of the 2nd list must be multiple of the 1st.')
        else:
            multiplier = int(multiplier)
        value[0] = flatten_list([value[0] for y in range(multiplier)])
    else:
        if not len(value):
            return []
        if isinstance(value[0], (list, tuple)):
            raise ValueError("Input must be a pair of vectors or flat vectors!")
        else:
            # cut by half, by default
            if len(value) % 2:
                raise ValueError("Invalid input to pair!")
            else:
                value = list(zip(*[x for x in chunks(value, 2)]))
    return flatten_list(value)

def sos_group_input_safe(value):
    '''
    Input is a list of lists or tuples. Lists are ordered such that
    the length of the next list is always multiples of the previous
    ABCD              ABCDABCD
    ABCDEFGH -------> ABCDEFGH ------> AABBCCDDAEBFCGDH
    '''
    for idx in reversed(range(1, len(value))):
        if not isinstance(value[idx], (list, tuple)):
            raise ValueError('Input elements must be list or tuples')
        multiplier = len(value[idx]) / len(value[idx - 1])
        if multiplier > int(multiplier):
            # is not integer
            raise ValueError('Length of the next list must be multiple of the previous.')
        else:
            multiplier = int(multiplier)
        if multiplier > 1:
            value[idx - 1] = flatten_list([value[idx - 1] for i in range(multiplier)])
    return flatten_list(list(zip(*value)))

def sos_group_input_adam(*lsts):
    '''
    https://stackoverflow.com/questions/48346169/fast-zip-list-of-lists-while-completing-shorter-lists-by-cycling
    '''
    n = len(lsts) - 1
    cyclic = [lst if i == n else itertools.cycle(lst) for i, lst in enumerate(lsts)]
    return list(itertools.chain.from_iterable(zip(*cyclic)))

def sos_group_input(*lsts):
    '''
    https://stackoverflow.com/questions/48346169/fast-zip-list-of-lists-while-completing-shorter-lists-by-cycling
    '''
    return list(chain(*islice(
        zip(*(cycle(l) for l in lsts)),
        0, len(lsts[-1]))))

def load_rds(filename, types = None):
    import pandas as pd
    import numpy as np
    import rpy2.robjects as RO
    import rpy2.robjects.vectors as RV
    import rpy2.rinterface as RI
    from rpy2.robjects import numpy2ri
    numpy2ri.activate()
    from rpy2.robjects import pandas2ri
    pandas2ri.activate()
    def load(data, types):
         if types is not None and not isinstance(data, types):
              return np.array([])
         if isinstance(data, RI.RNULLType):
              res = np.array([np.nan])
         elif isinstance(data, RV.BoolVector):
              data = RO.r['as.integer'](data)
              res = np.array(data, dtype = int)
              # Handle c(NA, NA) situation
              if np.sum(np.logical_and(res != 0, res != 1)):
                   res = res.astype(float)
                   res[res < 0] = np.nan
                   res[res > 1] = np.nan
         elif isinstance(data, RV.FactorVector):
              data = RO.r['as.character'](data)
              res = np.array(data, dtype = str)
         elif isinstance(data, RV.IntVector):
              res = np.array(data, dtype = int)
         elif isinstance(data, RV.FloatVector):
              res = np.array(data, dtype = float)
         elif isinstance(data, RV.StrVector):
              res = np.array(data, dtype = str)
         elif isinstance(data, RV.DataFrame):
              res = pd.DataFrame(data)
         elif isinstance(data, RV.Matrix):
              res = np.matrix(data)
         elif isinstance(data, RV.Array):
              res = np.array(data)
         else:
              # I do not know what to do for this
              # But I do not want to throw an error either
              res = np.array([str(data)])
         return res

    def load_dict(res, data, types):
        '''load data to res'''
        names = data.names if data.names else [i + 1 for i in range(len(data))]
        for name, value in zip(names, list(data)):
            if isinstance(value, RV.ListVector):
                res[name] = {}
                res[name] = load_dict(res[name], value, types)
            else:
                res[name] = load(value, types)
        return res
    #
    if not os.path.isfile(filename):
        raise IOError('Cannot find file ``{}``!'.format(filename))
    rds = RO.r['readRDS'](filename)
    if isinstance(rds, RV.ListVector):
        res = load_dict({}, rds, types)
    else:
        res = load(rds, types)
    return res

def save_rds(data, filename):
    import pandas as pd
    import numpy as np
    import rpy2.robjects as RO
    from rpy2.robjects import numpy2ri
    numpy2ri.activate()
    from rpy2.robjects import pandas2ri
    pandas2ri.activate()
    # Supported data types:
    # int, float, str, tuple, list, numpy array
    # numpy matrix and pandas dataframe
    def assign(name, value):
        name = re.sub(r'[^\w' + '_.' + ']', '_', name)
        if isinstance(value, (tuple, list)):
             if all(isinstance(item, int) for item in value):
                  value = np.asarray(value, dtype = int)
             elif all(isinstance(item, float) for item in value):
                  value = np.asarray(value, dtype = float)
             else:
                  value = np.asarray(value)
        if isinstance(value, np.matrix):
            value = np.asarray(value)
        if isinstance(value, (str, float, int, np.ndarray)):
            if isinstance(value, np.ndarray) and value.dtype.kind == "u":
                value = value.astype(int)
            RO.r.assign(name, value)
        elif isinstance(value, pd.DataFrame):
            # FIXME: does not always work well for pd.DataFrame
            RO.r.assign(name, value)
        else:
            raise ValueError("Saving ``{}`` to RDS file is not supported!".format(str(type(value))))
    #
    def assign_dict(name, value):
        RO.r('%s <- list()' % name)
        for k, v in value.items():
            k = re.sub(r'[^\w' + '_.' + ']', '_', k)
            if isinstance(v, collections.Mapping):
                assign_dict('%s$%s' %(name, k), v)
            else:
                assign('item', v)
                RO.r('%s$%s <- item' % (name, k))
    #
    if isinstance(data, collections.Mapping):
        assign_dict('res', data)
    else:
        assign('res', data)
    RO.r("saveRDS(res, '%s')" % filename)

def round_print(text, sep, pc = None):
    if pc is None:
        print(text)
        return
    for line in text.split('\n'):
        line = line.rstrip().split(sep)
        for i, value in enumerate(line):
            try:
                line[i] = int(value)
            except Exception:
                try:
                    line[i] = float(value)
                except Exception:
                    line[i] = value
        print(sep.join([('{0:.'+ str(pc) + 'E}').format(x) if isinstance(x, float) else str(x)
                        for x in line]).strip())

def install_r_lib(lib, dryrun = False):
    from sos.targets_r import R_library
    groups = re.search('(.*?)\((.*?)\)', lib)
    if groups is not None:
        lib = groups.group(1).strip()
        versions = [x.strip() for x in groups.group(2).split(',')]
    else:
        versions = None
    if not dryrun:
        logger.info("Checking R library {} ...".format(lib))
        return R_library(lib, versions).target_exists()
    else:
        return(lib, versions)

def install_py_module(lib):
    from sos.targets_python import Py_Module
    logger.info("Checking Python module {} ...".format(lib))
    return Py_Module(lib).target_exists()

def make_html_name(value):
    return "".join(x for x in value.replace(' ', '-') if x.isalnum() or x in ['-', '_']).lower()

def yaml2html(content, to_file, title = ''):
    if os.path.isfile(content):
        content = open(content).read()
    if not os.path.splitext(to_file)[1] == '.html':
        to_file += '.html'
    with open(to_file, 'w') as f:
        f.write('<!DOCTYPE html><html><head><title>{} | DSC2</title>\n'.format(title))
        f.write('<style type="text/css">\n')
        f.write(HTML_CSS)
        f.write('\n</style>\n<script type="text/javascript">\n')
        f.write(HTML_JS)
        f.write('</script></head><body>{}<pre><code class='\
                '"language-yaml; line-numbers; left-trim; right-trim;">\n'.\
                format('<h3>{}:</h3>'.format(os.path.basename(title)) if title else ''))
        f.write(content)
        f.write('\n</code></pre></body></html>')


def transcript2html(content, to_file, title = ''):
    if os.path.isfile(content):
        content = open(content).readlines()
    if not os.path.splitext(to_file)[1] == '.html':
        to_file += '.html'
    with open(to_file, 'w') as f:
        f.write('<!DOCTYPE html><html><head><title>{} | DSC2</title>\n'.format(title))
        f.write('<style type="text/css">\n')
        f.write(HTML_CSS)
        f.write('\n</style>\n<script type="text/javascript">\n')
        f.write(HTML_JS)
        f.write('</script></head><body>')
        idx = 1
        for line in content:
            if not re.match(r'^\s', line):
                continue
            if line.strip().startswith('##') and "script UUID:" in line and len(line.strip().split()) == 5:
                if idx > 1:
                    f.write('\n</code></pre>\n')
                lan = line.split()[1]
                f.write('{0} script {1}<pre><code class='\
                        '"language-{2}; line-numbers; left-trim; right-trim;">\n'.\
                        format(lan.capitalize(), idx, lan.lower()))
                idx += 1
            f.write(line[4:])
        if idx > 1:
            f.write('\n</code></pre>')
        f.write('</body></html>')


def md2html(content, to_file):
    import pypandoc
    if os.path.isfile(content):
        content = open(content).read()
    if not os.path.splitext(to_file)[1] == '.html':
        to_file += '.html'
    output = pypandoc.convert_text(content, 'html', format = 'md')
    with open(to_file, 'w') as f:
        f.write(output)

def dsc2html(dsc_conf, output, sequences, modules, lib_content = None, dsc_ann = None):
    '''
    section_content: ordered dictionary of lists,
    {'section 1': ['exec1.R', 'exec2.py']}
    '''
    lib_content = lib_content or []
    modules = dict(modules)
    section_content = [('->'.join(x), flatten_list([modules[i] for i in x])) for x in sequences]
    section_content = dict(lib_content + section_content)
    languages = {'py': 'python', 'sh': 'bash', 'rb': 'ruby', 'r': 'r', 'm': 'matlab', 'pl': 'perl'}
    if os.path.isfile(dsc_conf):
        dsc_conf = open(dsc_conf).read()
    if not os.path.splitext(output)[1] == '.html':
        output += '.html'
    with open(output, 'w') as f:
        # header and style/scripts
        f.write('<!DOCTYPE html><html><head><title>{} | DSC2</title>\n'.format(os.path.basename(output)[:-5]))
        f.write('<style type="text/css">\n')
        f.write(HTML_CSS)
        f.write('\n</style>\n<script type="text/javascript">\n')
        f.write(HTML_JS)
        # DSC script file
        f.write('</script></head><body><h3>DSC <a class="various" href="#dsc_conf">configuration script</a>{}</h3>\n'.\
                format('' if dsc_ann is None else ' and <a class="various" href="#dsc_ann">annotation</a>'))
        f.write('<div style="display:none"><div id="dsc_conf"><pre><code class="language-yaml; '
                'line-numbers; left-trim; right-trim;">\n')
        f.write(dsc_conf)
        f.write('\n</code></pre></div></div><div class="accordion">\n')
        if dsc_ann is not None:
          if os.path.isfile(dsc_ann):
               dsc_ann = open(dsc_ann).read()
          f.write('<div style="display:none"><div id="dsc_ann"><pre><code class="language-yaml; '
                  'line-numbers; left-trim; right-trim;">\n')
          f.write(dsc_ann)
          f.write('\n</code></pre></div></div><div class="accordion">\n')
        # DSC sections with executable scripts
        for name, commands in section_content.items():
            # get section scripts
            scripts = []
            seen = []
            for command in commands:
                command = command.split()[0]
                if command in seen:
                    continue
                else:
                    seen.append(command)
                try:
                    text = open(command).read()
                except Exception:
                    continue
                scripts.append((os.path.basename(command), os.path.splitext(command)[1][1:].lower(), text))
            if len(scripts) == 0:
                continue
            f.write('<div class="accodion-section">\n'
                    '<a class="accordion-section-title" href="#{1}">{0}</a>\n'
                    '<div id={1} class="accordion-section-content">\n'.format(name, make_html_name(name)))
            f.write('<div class="tabs">\n<ul class="tab-links">\n')
            for idx, script in enumerate(scripts):
                f.write('<li{2}><a href="#{0}">{1}</a></li>\n'.\
                        format(make_html_name(name + '_' + script[0]), script[0],
                               ' class="active"' if idx == 0 else ''))
            f.write('</ul>\n<div class="tab-content">\n')
            for idx, script in enumerate(scripts):
                f.write('<div id="{0}" class="tab{1}">\n'.\
                        format(make_html_name(name + '_' + script[0]), ' active' if idx == 0 else ''))
                f.write('<pre><code class="{}line-numbers; left-trim; right-trim;">\n'.\
                        format(("language-" + languages[script[1]] + "; ") if script[1] in languages else ''))
                f.write(script[2])
                f.write('\n</code></pre></div>\n')
            f.write('</div></div></div></div>\n')
        f.write('\n</div></body></html>')

def workflow2html(output, *multi_workflows):
    with open(output, 'w') as f:
        # header and style/scripts
        f.write('<!DOCTYPE html><html><head><title>{} | DSC2</title>\n'.format(os.path.basename(output)[:-5]))
        f.write('<style type="text/css">\n')
        f.write(HTML_CSS)
        f.write('\n</style>\n<script type="text/javascript">\n')
        f.write(HTML_JS)
        # DSC script file
        f.write('</script></head><body>\n')
        f.write('<div class="accordion">\n')
        for j, workflow_content in enumerate(multi_workflows):
            for i, modules in enumerate(workflow_content):
                if i > 0:
                    f.write('\n<hr>\n')
                f.write('<div class="accodion-section">\n'
                    '<a class="accordion-section-title" href="#{1}">{0}</a>\n'
                    '<div id={1} class="accordion-section-content">\n'.\
                    format('&'.join(modules.keys()), make_html_name('_'.join(modules.keys()) + f'_{j+1}')))
                f.write('<div class="tabs">\n<ul class="tab-links">\n')
                idx = 0
                for key, module in modules.items():
                    name = module.name if hasattr(module, 'name') else 'DSC_' + key
                    f.write('<li{2}><a href="#{0}">{1}</a></li>\n'.\
                            format(make_html_name(name + f'_{j+1}_{i+1}'),
                                   name,
                                   ' class="active"' if idx == 0 else ''))
                    idx += 1
                f.write('</ul>\n<div class="tab-content">\n')
                idx = 0
                for key, module in modules.items():
                    name = module.name if hasattr(module, 'name') else 'DSC_' + key
                    f.write('<div id="{0}" class="tab{1}">\n'.\
                            format(make_html_name(name + f'_{j+1}_{i+1}'),
                                   ' active' if idx == 0 else ''))
                    f.write('<pre><code class="{}line-numbers; left-trim; right-trim;">\n'.\
                            format("language-yaml; "))
                    f.write(str(module) if not isinstance(module, list) else '\n'.join(['- ' + str(x) for x in module]))
                    f.write('\n</code></pre></div>\n')
                    idx += 1
                f.write('</div></div></div></div>\n')
            if j + 1 != len(multi_workflows):
                f.write('<hr size="8">\n')
        f.write('\n</div></body></html>')

def locate_file(file_name, file_path):
    '''Use file_path information to try to complete the path of file'''
    if file_path is None:
        return file_name
    res = None
    for item in file_path:
        if os.path.isfile(os.path.join(item, file_name)):
            if res is not None:
                raise ValueError("File ``{}`` found in multiple directories ``{}`` and ``{}``!".\
                                format(file_name, item, os.path.join(*os.path.split(res)[:-1])))
            res = os.path.join(item, file_name)
    return res if res else file_name

def n2a(col_num, col_abs=False):
    """
    Convert a one indexed column cell reference to a string.
    Args:
       col:     The cell column. Int.
       col_abs: Optional flag to make the column absolute. Bool.
    Returns:
        Column style string.
    """
    col_str = ''
    col_abs = '$' if col_abs else ''
    while col_num:
        # Set remainder from 1 .. 26
        remainder = col_num % 26
        if remainder == 0:
            remainder = 26
        # Convert the remainder to a character.
        col_letter = chr(ord('A') + remainder - 1)
        # Accumulate the column letters, right to left.
        col_str = col_letter + col_str
        # Get the next order of magnitude.
        col_num = int((col_num - 1) / 26)
    return col_abs + col_str

def is_sublist(sub, lst):
    ln = len(sub)
    for i in range(len(lst) - ln + 1):
        if all(sub[j] == lst[i+j] for j in range(ln)):
            return True
    return False

def filter_sublist(lists, ordered = True):
    '''remove lists who are sublist of other lists'''
    lists = uniq_list(lists)
    max_lists = []
    for x in lists:
        include = True
        for y in lists:
            if x == y:
                continue
            if ordered:
                if is_sublist(x, y):
                    include = False
                    break
            else:
                if all([xx in y for xx in x]):
                    include = False
                    break
        if include:
            max_lists.append(x)
    return max_lists

def do_parentheses_match(input_string, l = '(', r = ')'):
    s = []
    balanced = True
    index = 0
    while index < len(input_string) and balanced:
        unit = input_string[index]
        if unit == l:
            s.append(unit)
        elif unit == r:
            if len(s) == 0:
                balanced = False
            else:
                s.pop()
        index += 1
    return balanced and len(s) == 0

def find_parens(s, lenient = True):
    '''
    return all pairs of matching parentheses
    '''
    toret = {}
    pstack = []
    for i, c in enumerate(s):
        if c == '(':
            pstack.append(i)
        elif c == ')':
            if len(pstack) == 0:
                if not lenient:
                    raise IndexError("No matching closing parens at: " + str(i))
                else:
                    break
            toret[pstack.pop()] = i
    if len(pstack) > 0:
        raise IndexError("No matching opening parens at: " + str(pstack.pop()))
    return toret

def remove_multiple_strings(cur_string, replace_list):
    for cur_word in sorted(set(replace_list), key=len):
        cur_string = cur_string.replace(cur_word, '')
    return cur_string

def load_mpk(mpk_files, jobs = 2):
    import msgpack
    if isinstance(mpk_files, str):
        return msgpack.unpackb(open(mpk_files, "rb").read(), encoding = 'utf-8',
                                     object_pairs_hook = OrderedDict)
    d = Manager().dict()
    def f(d, x):
        for xx in x:
            d.update(msgpack.unpackb(open(xx, "rb").read(), encoding = 'utf-8',
                                     object_pairs_hook = OrderedDict))
    #
    mpk_files = [x for x in chunks(mpk_files, int(len(mpk_files) / jobs) + 1)]
    job_pool = [Process(target = f, args = (d, x)) for x in mpk_files]
    for job in job_pool:
        job.start()
    for job in job_pool:
        job.join()
    return OrderedDict([(x, d[x]) for x in sorted(d.keys(), key = lambda x: int(x.split(':')[0]))])

def remove_quotes(value):
    if not isinstance(value, str):
        return value
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    return value
