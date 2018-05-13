#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
This file defines DSC syntax templates
'''
import re
from sos.syntax import LazyRegex, SOS_DIRECTIVES

DSC_KW = ['DSC_OUTPUT', 'DSC_VARS', 'DSC_DEBUG', 'DSC_REPLICATE'] # engineering keywords, reserved
DSC_KW.extend(SOS_DIRECTIVES)
DSC_MODP = ['@EXEC', '@FILTER', '@ALIAS', '@CONF'] # module properties

DSC_DERIVED_BLOCK = LazyRegex(r'^(.*?)\((.*?)\)$', re.VERBOSE)
DSC_FILE_OP = LazyRegex(r'^file\((.*?)\)$', re.VERBOSE)
DSC_ASIS_OP = LazyRegex(r'^raw\((.*?)\)$', re.VERBOSE)
DSC_PACK_OP = LazyRegex(r'((?i)list|(?i)dict)\((.*?)\)', re.VERBOSE)
DSC_BLOCK_CONTENT = LazyRegex(r'^\s(.*?)', re.VERBOSE)
DSC_GVS = LazyRegex(r'\$\((.*?)\)\[(.*?)\]', re.VERBOSE) # global variable with slicing
DSC_GV = LazyRegex(r'\$\((.*?)\)', re.VERBOSE)
DSC_RLIB = LazyRegex(r'((^|\W|\()library|(^|\W|\()require)\((.*?)\)', re.VERBOSE)
DSC_PYMODULE = LazyRegex(r'(^from|^import)(.*?)', re.VERBOSE)
DSC_RESERVED_MODULE = LazyRegex(r'^default$|_\d+$|^pipeline_|_$', re.VERBOSE)
