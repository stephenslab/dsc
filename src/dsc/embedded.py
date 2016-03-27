#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
This file defines functions to be embedded to SoS codes
'''
HEADER = '''
import sys, hashlib, itertools
from pysos import expand_pattern
def get_md5(values):
    res = []
    for value in values:
        base, ext = value.rsplit('.', 1)
    res.append('{}.{}'.format(hashlib.md5(base.encode('utf-8')).hexdigest() if sys.version_info[0] == 3 else hashlib.md5(base).hexdigest(), ext))
    return res

def get_input(values):
    return sum(list(zip(*itertools.product(*values))), ())
'''
