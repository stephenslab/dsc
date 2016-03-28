#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import sys, os, random, copy, re, itertools,\
  yaml, collections, hashlib
from io import StringIO
from pysos.utils import env

def lower_keys(x, level_start = 0, level_end = 2, mapping = dict):
    level_start += 1
    if level_start > level_end:
        return x
    if isinstance(x, list):
        return [lower_keys(v, level_start, level_end) for v in x]
    elif isinstance(x, colletions.Mapping):
        return mapping((k.lower(), lower_keys(v, level_start, level_end)) for k, v in x.items())
    else:
        return x

def is_null(var):
    if var is None:
        return True
    if type(var) is str:
        if var.lower() in ['na','nan','null','none','']:
            return True
    if isinstance(var, (list, tuple)):
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
    return sum( ([x] if not isinstance(x, list) else flatten_list(x) for x in lst), [] )

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
    if ',' in slicearg:
        return name, tuple(int(n.strip()) - 1 for n in slicearg.split(',') if n.strip())
    elif ':' in slicearg:
        slice_ints = [ int(n) for n in slicearg.split(':') ]
        if len(slice_ints) == 1:
            raise ValueError('Wrong syntax for slice {}.'.format(value))
        slice_ints[1] += 1
        slice_obj = slice(*tuple(slice_ints))
        return name, tuple(x - 1 for x in range(slice_obj.start or 0, slice_obj.stop or -1, slice_obj.step or 1))
    else:
        if all_tuple:
            return name, tuple([int(slicearg.strip()) - 1])
        else:
            return name, int(slicearg.strip()) - 1

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
    yaml.dump(value, out, default_flow_style=False)
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

def strip_dict(data, mapping = dict):
    mapping_null = mapping()
    new_data = mapping()
    for k, v in data.items():
        if isinstance(v, collections.Mapping):
            v = strip_dict(v)
        if not v in ('', None, {}, [], mapping_null):
            new_data[k] = v
    return new_data

class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __setattr__ = dict.__setitem__
    __getattr__ = dict.__getitem__
    __delattr__ = dict.__delitem__

    def __deepcopy__(self, memo):
        return dotdict(copy.deepcopy(dict(self)))

class REncoder:
    """Encoding Pyton data structures into R."""
    @classmethod
    def encode_value(cls, value):
        if isinstance(value, list):
            return cls.encode_list(value)
        elif isinstance(value, dict):
            return cls.encode_dict(value)
        elif isinstance(value, str):
            return repr(value)
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, int) or isinstance(value, float):
            return str(value)
        else:
            raise ValueError(
                "Unsupported value for conversion into R: {}".format(value))

    @classmethod
    def encode_list(cls, l):
        return "c({})".format(", ".join(map(cls.encode_value, l)))

    @classmethod
    def encode_items(cls, items):
        def encode_item(item):
            name, value = item
            return '"{}" = {}'.format(name, cls.encode_value(value))

        return ", ".join(map(encode_item, items))

    @classmethod
    def encode_dict(cls, d):
        d = "list({})".format(cls.encode_items(d.items()))
        return d

    @classmethod
    def encode_namedlist(cls, namedlist):
        positional = cls.encode_list(namedlist)
        named = cls.encode_items(namedlist.items())
        source = "list("
        if positional != "c()":
            source += positional
        if named:
            source += ", " + named
        source += ")"
        return source

def get_md5_sos(values):
    def calc(x, y):
        return '{}.{}'.format(hashlib.md5(x.encode('utf-8')).hexdigest() if sys.version_info[0] == 3 else hashlib.md5(x).hexdigest(), y)
    if isinstance(values, str):
        base, ext = values.rsplit('.', 1)
        res = calc(base, ext)
    else:
        res = []
        for value in values:
            base, ext = value.rsplit('.', 1)
            res.append(calc(base, ext))
    return res

def get_input_sos(values):
    return list(sum(list(zip(*itertools.product(*values))), ()))
