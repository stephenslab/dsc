#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
This file defines DSC syntax templates
'''
import re
from sos.sos_syntax import LazyRegex

DSC_BLOCKP = ['exec', 'return', 'params', 'seed', '.logic', '.alias', '.options'] # block properties
DSC_PARAMP = ['.logic', '.alias', '.options'] # parameter properties

_TMPL = r'^(.*?)\((.*?)\)$'
DSC_DERIVED_BLOCK = LazyRegex(_TMPL, re.VERBOSE)
_TMPL = r'^[A-Za-z0-9_]+$'
DSC_BLOCK_NAME = LazyRegex(_TMPL, re.VERBOSE)
_TMPL = r'^File\((.*?)\)$'
DSC_FILE_OP = LazyRegex(_TMPL, re.VERBOSE)
_TMPL = r'^(R|Python)\((.*?)\)$'
DSC_LAN_OP = LazyRegex(_TMPL, re.VERBOSE)
