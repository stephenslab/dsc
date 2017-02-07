#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import os, copy, re, itertools, yaml, collections, time, sympy
from sympy.parsing.sympy_parser import parse_expr
from collections import OrderedDict
from difflib import SequenceMatcher
from io import StringIO
import readline
import rpy2.robjects as RO
import rpy2.robjects.vectors as RV
import rpy2.rinterface as RI
from rpy2.robjects import numpy2ri
numpy2ri.activate()
from rpy2.robjects import pandas2ri
pandas2ri.activate()
import numpy as np
import pandas as pd
from sos.utils import env
from sos.target import textMD5
from sos.R.target import R_library

from dsc import HTML_CSS, HTML_JS

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
    parsed_expr = parse_expr(expr_string, evaluate=False)

    new_locals = {sym.name:sympy.Symbol(sym.name, commutative=False)
                  for sym in parsed_expr.atoms(sympy.Symbol)}
    return sympy.expand(eval(expr_string, {}, new_locals))

def no_duplicates_constructor(loader, node, deep=False):
    """YAML check for duplicate keys."""
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        value = loader.construct_object(value_node, deep=deep)
        if key in mapping:
            raise yaml.constructor.ConstructorError("while constructing a mapping", node.start_mark,
                                   "found duplicate key (%s)" % key, key_node.start_mark)
        mapping[key] = value
    return loader.construct_mapping(node, deep)

yaml.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, no_duplicates_constructor)
yaml.Dumper.ignore_aliases = lambda self, value: True

class Timer(object):
    def __init__(self, verbose=False):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        self.msecs = self.secs * 1000  # millisecs
        if self.verbose:
            env.logger.info('Elapsed time ``%.03f`` seconds.' % self.secs)

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
    if type(var) is str:
        if var.lower() in ['na','nan','null','none','']:
            return True
    if isinstance(var, (list, tuple, dict)):
        return True if len(var) == 0 else False
    return False

def str2num(var):
    if type(var) is str:
        # try to warn about boolean
        if var in ['T', 'F'] or var.lower() in ['true', 'false']:
            bmap = {'t': 1, 'true': 1, 'f': 0, 'false': 0}
            msg = 'Possible Boolean variable detected: ``{}``. \n\
            This variable will be treated as string, not Boolean data. \n\
            It may cause problems to your jobs. \n\
            Please set this variable to ``{}`` if it is indeed Boolean data.'.format(var, bmap[var.lower()])
            env.logger.warning('\n\t'.join([x.strip() for x in msg.split('\n')]))
        try:
            return int(var)
        except ValueError:
            try:
                return float(var)
            except ValueError:
                return re.sub(r'''^"|^'|"$|'$''', "", var)
    else:
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
    return [x for x in seq if not (x in seen or seen_add(x))]

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

def get_slice(value, all_tuple = True, mismatch_quit = True):
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
    except:
         if mismatch_quit:
              raise AttributeError('Cannot obtain slice from input string {}'.format(value))
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
    if len(idxs) == 1 and not all_tuple:
         return name, idxs[0]
    else:
         return name, tuple(idxs)

def try_get_value(value, keys):
    '''
    Input: dict_data, (key1, key2, key3 ...)
    Output: dict_data[key1][key2][key3][...] or None
    '''
    if not isinstance(keys, (list, tuple)):
        keys = [keys]
    try:
        if len(keys) == 0:
            return value
        else:
            return try_get_value(value[keys[0]], keys[1:])
    except KeyError:
        return None

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

def dict2str(value, replace = []):
    out = StringIO()
    yaml.dump(strip_dict(value, into_list = True), out, default_flow_style=False)
    res = out.getvalue()
    out.close()
    for item in replace:
        res = res.replace(item[0], item[1])
    return res

def update_nested_dict(d, u, mapping = dict):
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            r = update_nested_dict(d.get(k, mapping()), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d

def strip_dict(data, mapping = dict, into_list = False):
    if not isinstance(data, mapping):
        return data
    mapping_null = mapping()
    new_data = mapping()
    for k, v in data.items():
        if isinstance(v, collections.Mapping):
            v = strip_dict(v, mapping, into_list)
        if isinstance(v, list) and into_list:
            v = [strip_dict(x, mapping, into_list) for x in v]
        if not is_null(v) and v != mapping_null:
            new_data[k] = v
    return new_data

def extend_dict(dict1, dict2):
     for key, value in dict2.items():
          dict1.setdefault(key, []).extend(value)
     return dict1

class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __setattr__ = dict.__setitem__
    __getattr__ = dict.__getitem__
    __delattr__ = dict.__delitem__

    def __deepcopy__(self, memo):
        return dotdict(copy.deepcopy(dict(self)))

def sos_hash_output(values, db_name = '', prefix = None, suffix = None):
     if not isinstance(values, list):
          md5 = textMD5(values)
          if isinstance(prefix, str):
               md5 = '{}:{}'.format(prefix, md5)
          if isinstance(suffix, str):
               md5 = '{}:{}'.format(md5, suffix)
          res = os.path.join(db_name, md5)
          return res
     else:
          res = []
          for value in values:
               md5 = textMD5(value)
               if isinstance(prefix, str):
                    md5 = '{}:{}'.format(prefix, md5)
               if isinstance(suffix, str):
                    md5 = '{}:{}'.format(md5, suffix)
               res.append(os.path.join(db_name, md5))
          return res

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

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

def sos_group_input(value):
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

def load_rds(filename, types = None):
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
        if isinstance(value, (str, float, int, np.ndarray, pd.DataFrame)):
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
        for i in range(len(line)):
            try:
                line[i] = int(line[i])
            except:
                try:
                    line[i] = float(line[i])
                except:
                    pass
        print (sep.join([('{0:.'+ str(pc) + 'E}').format(x) if isinstance(x, float) else str(x)
                         for x in line]).strip())

def install_r_libs(libs):
    if libs is None:
        return
    for value in libs:
        groups = re.search('(.*?)\((.*?)\)', value)
        if groups is not None:
            value = groups.group(1).strip()
            versions = [x.strip() for x in groups.group(2).split(',')]
        else:
            versions = None
        env.logger.info("Checking R library ``{}`` ...".format(value))
        R_library(value, versions).exists()

def ordered_load(stream, Loader=yaml.Loader, object_pairs_hook=OrderedDict):
    # ordered_load(stream, yaml.SafeLoader)
    class OrderedLoader(Loader):
        pass
    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return object_pairs_hook(loader.construct_pairs(node))
    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_mapping)
    return yaml.load(stream, OrderedLoader)

def ordered_dump(data, stream=None, Dumper=yaml.Dumper, **kwds):
    # ordered_dump(data, Dumper=yaml.SafeDumper)
    class OrderedDumper(Dumper):
        pass
    def _dict_representer(dumper, data):
        return dumper.represent_mapping(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
            data.items())
    OrderedDumper.add_representer(OrderedDict, _dict_representer)
    return yaml.dump(data, stream, OrderedDumper, **kwds)

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

def dsc2html(dsc_conf, dsc_ann, output, section_content = {}):
    '''
    section_content: ordered dictionary of lists,
    {'section 1': ['exec1.R', 'exec2.py']}
    '''
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
        for name, section in section_content.items():
            # get section scripts
            commands = flatten_list([x['command'] for x in section])
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
                except:
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

def rq_worker(host_url):
     import redis
     from rq import Worker, Queue, Connection
     listen = ['high', 'default', 'low']
     redis_url = 'redis://' + host_url
     conn = redis.from_url(redis_url)
     with Connection(conn):
          worker = Worker(map(Queue, listen))
          worker.work()
