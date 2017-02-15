#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
Parser for DSC script and annotation files
'''

import os, yaml, re, subprocess, itertools, copy, warnings, datetime, collections
import readline
import rpy2.robjects as RO
from io import StringIO
from sos.utils import logger
from sos.target import textMD5
from .utils import dotdict, OrderedDict, FormatError, is_null, str2num, non_commutative_symexpand, strip_dict, \
     cartesian_list, pairwise_list, get_slice, expand_slice, flatten_dict, \
     try_get_value, dict2str, update_nested_dict, set_nested_value, load_from_yaml, \
     no_duplicates_constructor, install_r_libs, install_py_modules
from .syntax import *
from .line import OperationParser, Str2List, ExpandVars, ExpandActions, CastData

__all__ = ['DSC_Script', 'DSC_Annotation']

class DSC_Script:
    '''Parse a DSC script'''
    def __init__(self, content, output = None):
        if os.path.isfile(content):
            with open(content) as f:
                self.content = load_from_yaml(f, content)
            self.output = os.path.split(os.path.splitext(content)[0])[-1] if output is not None else output
        else:
            if len(content.split('\n')) == 1:
                raise ValueError("Cannot find file ``{}``".format(content))
            with StringIO(content) as f:
                self.content = load_from_yaml(f)
            self.output = 'DSCStringIO' if output is not None else output
        self.blocks = OrderedDict()

    def propagate_derived_block(self):
        '''
        Name of derived blocks looks like: "derived(base)"
        This function first figures out sorted block names such that derived block always follows the base block
        Then it propagate self.content derived blocks
        '''
        base = []
        blocks = []
        derived = OrderedDict()
        for block in self.content:
            groups = DSC_DERIVED_BLOCK.search(block.strip())
            if groups:
                derived[block] = (groups.group(1).strip(), groups.group(2).strip())
            else:
                base.append(block)
                blocks.append(block)
        if len(derived) == 0:
            return
        # Check looped derivations: x(y) and y(x)
        tmp = [sorted(x) for x in derived.values()]
        for item in ((i, tmp.count(i)) for i in tmp):
            if item[1] > 1:
                raise FormatError("Looped block inheritance: {0}({1}) and {1}({0})!".\
                                  format(item[0][0], item[0][1]))
        # Check self-derivation and non-existing base
        tmp = base + [x[0] for x in derived.values()]
        for item in derived.values():
            if item[0] == item[1]:
                raise FormatError("Looped block inheritance: {0}({0})!".format(item[0]))
            if item[1] not in tmp:
                raise FormatError("Base block does not exist: {0}({1})!".format(item[0], item[1]))
        #
        derived_cycle = itertools.cycle(derived.values())
        while True:
            item = next(derived_cycle)
            if item[1] in base:
                base.append(item[0])
                name = '{}({})'.format(item[0], item[1])
                if name not in blocks:
                    blocks.append(name)
            if len(blocks) == len(self.content.keys()):
                break
        # propagate data
        for block in blocks:
            if block in derived:
                self.content[derived[block][0]] = update_nested_dict(
                    copy.deepcopy(self.content[derived[block][1]]), self.content[block])
                del self.content[block]
        return

    def check_block_error(self):
        if 'DSC' not in self.content:
            raise ValueError('Cannot find required section ``DSC``!')
        for block in self.content:
            if block == 'DSC':
                continue
            # Check invalid block names
            if not DSC_BLOCK_NAME.match(block):
                raise FormatError("Block name should contain only alphanumeric letters "\
                                  "or underscore: ``{}``".format(block))
            if block.split('_')[-1].isdigit():
                raise FormatError("Block name should not end with ``_{}``: ``{}``".\
                                  format(block.split('_')[-1], block))
            # Check block elements
            if not 'exec' in self.content[block]:
                raise FormatError('Missing required entry ``exec`` in block ``{}``'.format(block))
            if not 'return' in self.content[block]:
                raise FormatError('Missing required entry ``return`` in block ``{}``'.format(block))
            for key in self.content[block]:
                if key not in DSC_BLOCKP:
                    logger.warning('Ignore unknown entry ``{}`` in block ``{}``.'.\
                                   format(key, block))
                    del self.content[block][key]


    def __call__(self, sequence = None):
        self.propagate_derived_block()
        self.check_block_error()
        if sequence:
            self.content['DSC']['run'] = sequence
        self.content = DSCEntryFormatter()(self.content, try_get_value(self.content['DSC'], 'params'))
        self.blocks = OrderedDict([(x, DSC_Block(x, y)) for x, y in self.content.items() if x != 'DSC'])
        self.runtime = DSC_Section(self.content['DSC'], sequence)


class DSC_Annotation:
    def __init__(self):
        pass

class DSCEntryFormatter:
    '''
    Run format transformation to DSC entries
    '''
    def __init__(self):
        pass

    def __call__(self, data, variables):
        actions = [Str2List(),
                   ExpandVars(variables),
                   ExpandActions(),
                   CastData()]
        return self.__Transform(data, actions)

    def __Transform(self, cfg, actions):
        '''Apply actions to items'''
        for key, value in list(cfg.items()):
            if isinstance(value, collections.Mapping):
                self.__Transform(value, actions)
            else:
                for a in actions:
                    value = a(value)
                if is_null(value):
                    del cfg[key]
                else:
                    cfg[key] = value
        return cfg

class DSC_Step:
    def __init__(self, name):
        self.name = None
        # s for system seed
        self.s = None
        # p for params
        self.p = OrderedDict()
        # e for exec
        self.e = None
        # r for return
        self.r = None


class DSC_Block:
    def __init__(self, name, content):
        self.name = name
        print(content)
        #seed = content['']
        #template = DSC_Step('template', )


class DSC_Section:
    def __init__(self, content, sequence):
        self.content = content
        if 'run' not in self.content:
            raise FormatError('Missing required ``DSC::run``.')
        self.sequence = [OperationParser()(expand_slice(x)) for x in self.content['run']]
        if 'work_dir' not in self.content:
            self.workdir = None
        else:
            self.workdir = self.content['work_dir']

    def __str__(self):
        return dict2str({'workdir': self.workdir, 'sequence': self.sequence})
