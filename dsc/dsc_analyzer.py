#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
This file analyzes DSC contents to determine dependencies
and what exactly should be executed.
'''

import copy, re, os, datetime
from sos.target import executable, fileMD5, textMD5
from sos.utils import Error
from .utils import FormatError, dotdict, dict2str, try_get_value, get_slice, \
     cartesian_list, merge_lists, uniq_list, flatten_list
from .plugin import Plugin, R_LMERGE, R_SOURCE

__all__ = ['DSC_Analyzer']

class DSC_Analyzer:
    '''Putting together DSC steps to sequences and propagate computational routines for execution
    * Handle step dependencies
    * Auto-complete R, Python or Shell jobs: is_plugin
    * Handle file I/O rules: from_plugin, to_plugin
    * Merge steps: via block rule
    * Merge blocks: via block option (NOT IMPLEMENTED)
    The output will be analyzed DSC ready to be translated to pipelines for execution
    '''
    def __init__(self, script_data):
        '''
        script_data comes from DSC_Script, having members:
        blocks, runtime.sequence, runtime.rlib, runtime.pymodule
        '''

        self.install_libs(script_data.runtime.rlib, "R_library")
        self.install_libs(script_data.runtime.pymodule, "Python_Module")

    def install_libs(self, libs, lib_type):
        if lib_type not in ["R_library", "Python_Module"]:
            raise ValueError("Invalid library type ``{}``.".format(lib_type))
        if libs is None:
            return
        libs_md5 = textMD5(repr(libs) + str(datetime.date.today()))
        if not os.path.exists('.sos/.dsc/{}.{}.info'.format(lib_type, libs_md5)):
            if lib_type == 'R_library':
                install_r_libs(libs)
            if lib_type == 'Python_Module':
                install_py_modules(libs)
            os.makedirs('.sos/.dsc', exist_ok = True)
            os.system('echo "{}" > {}'.format(repr(libs), '.sos/.dsc/{}.{}.info'.format(lib_type, libs_md5)))
