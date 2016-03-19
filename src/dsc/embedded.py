#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
This file defines functions to be embedded to SoS codes
'''
HEADER = '''
def get_params(value, is_r):
    res = []
    for item in value:
        if isinstance(item, str):
            if item.startswith('$'):
                if is_r:
                    continue
                else:
                    item = eval("step_returned_{}".format(item[1:]))
            elif re.search(r'^Asis\((.*?)\)', item):
                item = re.search(r'^Asis\((.*?)\)', item).group(1)
            else:
                item = repr(item)
        res.append(item)
    if is_r and len(res) < len(value) and len(res) > 0:
        # This means that $XX and other variables coexist
        # For an R program
        raise ValueError("Cannot use return value from an R program as input parameter in parallel to others!\\nLine: {}".format(', '.join(map(str, value))))
    if len(res) == 0:
        res = ['NULL']
    return res

def get_md5(value):
    import sys, hashlib
    base, ext = value.rsplit('.', 1)
    res = hashlib.md5(base.encode('utf-8')).hexdigest() if sys.version_info[0] == 3 else hashlib.md5(base).hexdigest()
    return '{}.{}'.format(res, ext)

[parameters]
#FIXME: should set to False by default
step_is_r = True
'''
