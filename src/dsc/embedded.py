#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
This file defines functions to be embedded to SoS codes
'''
HEADER = '''
def get_md5(value):
    import sys, hashlib
    base, ext = value.rsplit('.', 1)
    res = hashlib.md5(base.encode('utf-8')).hexdigest() if sys.version_info[0] == 3 else hashlib.md5(base).hexdigest()
    return '{}.{}'.format(res, ext)

def get_input(value):
    import itertools
    return sum(list(zip(*itertools.product(*value))), ())
'''
