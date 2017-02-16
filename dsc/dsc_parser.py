#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
Parser for DSC script and annotation files
'''

import os, re, itertools, copy, collections
from io import StringIO
from sos.utils import logger
from sos.target import textMD5
from .utils import OrderedDict, FormatError, is_null, strip_dict, \
     cartesian_list, get_slice, expand_slice, flatten_dict, merge_lists, \
     try_get_value, dict2str, update_nested_dict, load_from_yaml, \
     locate_file
from .syntax import *
from .line import OperationParser, Str2List, ExpandVars, ExpandActions, CastData
from .plugin import Plugin

__all__ = ['DSC_Script', 'DSC_Annotation']

class DSC_Script:
    '''Parse a DSC script
     * provides self.blocks, self.runtime, and self.runtime.sequence that contain all DSC information needed for a run
    '''
    def __init__(self, content, sequence = None, output = None):
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
        #
        self.propagate_derived_block()
        self.check_block_error()
        if sequence:
            self.content['DSC']['run'] = sequence
        self.content = DSCEntryFormatter()(self.content, try_get_value(self.content['DSC'], 'params'))
        self.runtime = DSC_Section(self.content['DSC'], sequence)
        # FIXME: add annotation info / filter here
        # Or, not?
        self.blocks = OrderedDict([(x, DSC_Block(x, self.content[x], self.runtime.options))
                                    for x in self.runtime.sequence_ordering.keys()])
        self.runtime.expand_sequences(self.blocks)
        # FIXME: maybe this should be allowed?
        self.runtime.check_looped_computation()
        # Finally prune blocks removing unused steps
        for name, idxes in self.runtime.sequence_ordering.items():
            self.blocks[name].extract_steps(idxes)

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


    def __str__(self):
        res = '# Blocks\n' + '\n'.join(['## {}\n```yaml\n{}\n```'.format(x, y) for x, y in self.blocks.items()]) \
              + '\n# Execution\n```yaml\n{}\n```'.format(self.runtime)
        return res


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
        # step name
        self.name = name
        # system seed
        self.seed = None
        # params: alias, value
        self.p = OrderedDict()
        # return variables (to plugin): alias, value
        self.rv = OrderedDict()
        # return files: alias, ext
        self.rf = OrderedDict()
        # exec
        self.exe = None
        self.exe_id = 0
        # script plugin object
        self.plugin = None
        # runtime variables
        self.workdir = None
        self.libpath = None
        self.path = None
        # dependencies
        self.depends = []

    def set_seed(self, seed):
        self.seed = seed

    def set_exec(self, exec_var):
        self.exe = ' '.join([locate_file(exec_var[0], self.path)] + list(exec_var[1:]))
        self.plugin = Plugin(os.path.splitext(exec_var[0])[1].lstrip('.'), textMD5(self.exe)[:10])
                        # re.sub(r'^([0-9])(.*?)', r'\2', textMD5(data.command)))

    def set_return(self, common_return, spec_return):
        return_var = common_return if common_return is not None else spec_return
        for item in return_var:
            if item[0] == item[1]:
                # no alias, so it is possible the return is a file
                param_return = try_get_value(self.p, item[0])
                if param_return:
                    # return value is found in params
                    for p in param_return:
                        if not isinstance(p, str):
                            continue
                        groups = DSC_FILE_OP.search(p)
                        if groups:
                            try:
                                self.rf[item[0]].append(groups.group(1))
                            except:
                                self.rf[item[0]] = [groups.group(1)]
                    # are there remaining values not File()?
                    if item[0] in self.rf and len(self.rf[item[0]]) < len(param_return):
                        raise FormatError("Return ``{0}`` cannot be a mixture of File() and non-File() variables".\
                                          format(item[0], repr(param_return)))
                if item[0] not in self.rf:
                    self.rv[item[0]] = item[1]
            else:
                groups = DSC_LAN_OP.search(item[1])
                if groups:
                    # have to set return alias inside plugin
                    if groups.group(1).lower() != str(self.plugin):
                        raise FormatError('Return alias cannot be created with ``{}`` for this computational routine.'.\
                                          format(groups.group(1)))
                    self.plugin.add_return(item[0], groups.group(2))
                else:
                    self.rv[item[0]] = item[1]

    def set_options(self, common_option, spec_option):
        workdir1 = try_get_value(common_option, 'work_dir')
        workdir2 = try_get_value(spec_option, 'work_dir')
        libpath1 = try_get_value(common_option, 'lib_path')
        libpath2 = try_get_value(spec_option, 'lib_path')
        path1 = try_get_value(common_option, 'exec_path')
        path2 = try_get_value(spec_option, 'exec_path')
        self.workdir = workdir2 if workdir2 is not None else workdir1
        self.libpath = libpath2 if libpath2 is not None else libpath1
        self.path = path2 if path2 is not None else path1

    def set_params(self, common, spec, alias_common, alias_spec):
        alias = {}
        if common is not None:
            self.p.update(common)
        if spec is not None:
            self.p.update(spec)
        if alias_common is not None:
            alias.update(dict([(x.strip() for x in item.split('=', 1)) for item in alias_common]))
        if alias_spec is not None:
            alias.update(dict([(x.strip() for x in item.split('=', 1)) for item in alias_spec]))
        # Swap parameter key with alias when applicable
        for k1, k2 in list(alias.items()):
            if k2 in self.p:
                self.p[k1] = self.p.pop(k2)
                del alias[k1]
        # Handle special alias
        # Currently it is List() / Dict()
        for k1, k2 in list(alias.items()):
            groups = re.search(r'(List|Dict)\((.*?)\)', k2)
            if groups:
                self.plugin.set_container(k1, groups.group(2), self.p)
                del alias[k1]
        if len(alias):
            raise FormatError('Invalid .alias for computational routine ``{}``:\n``{}``'.\
                              format(self.name, dict2str(alias)))


    def apply_params_rule(self, common_rule, spec_rule):
        # FIXME: not implemented yet
        pass

    def apply_params_operator(self):
        '''
        Do the following:
        * convert string to raw string, leave alone `$` variables
        * strip off Asis() operator
        * Handle File() parameter based on context
        '''
        for k, p in list(self.p.items()):
            values = []
            for p1 in p:
                if isinstance(p1, str):
                    if DSC_ASIS_OP.search(p1):
                        p1 = DSC_ASIS_OP.search(p1).group(1)
                    elif DSC_FILE_OP.search(p1):
                        # p1 is file extension
                        file_ext = DSC_FILE_OP.search(p1).group(1)
                        if k in self.rf:
                            # This file is to be saved as output
                            # FIXME: have to figure out what is the index of the output
                            self.plugin.add_input(k, '${_output!r}')
                            continue
                        else:
                            # This file is a temp file
                            self.plugin.add_tempfile(k, file_ext)
                            continue
                    else:
                        p1 = repr(p1)
                if isinstance(p1, tuple):
                    # FIXME format_tuple has to be defined for shell as well
                    p1 = [plugin.format_tuple(p1)]
                values.append(p1)
            if len(values) == 0:
                del self.p[k]

    def __str__(self):
        return dict2str(self.dump())

    def dump(self):
        return strip_dict({'seed': self.seed, 'name': self.name,
                           'parameters': self.p,
                           'return variables': self.rv,
                           'return files': self.rf,
                           'command': self.exe,
                           'command_index': self.exe_id,
                           'dependencies': self.depends,
                           'runtime options': {
                               'exec path': self.path,
                               'workdir': self.workdir,
                               'library path': self.libpath
                           },
                           'plugin status': self.plugin.dump()})


class DSC_Block:
    def __init__(self, name, content, global_options = {}):
        '''Populate steps in the block and keep track of block rules
        Members are:
          - self.steps, self.rule, self.name
        '''
        self.name = name
        # block executable rules
        self.rule = self.get_exec_rule(content['.logic']) if '.logic' in content else None
        exes = [tuple(x.split()) if isinstance(x, str) else x for x in content['exec']]
        exe_alias = content['.alias'] if '.alias' in content else ['_'.join([x for x in y if not x.startswith('$')])
                                                                   for y in exes]
        if len(exes) == 1 and len(exe_alias) > 1:
            exes = exes * len(exe_alias)
        # check if any exec out of index
        if (self.rule is None and len(exes) != len(exe_alias)) or \
           (self.rule is not None and len(exe_alias) != len(self.rule)):
            raise FormatError('Alias ``{}`` (length {}) does not match exec (length {}), in block ``{}``!'.\
                              format(repr(exe_alias), len(exe_alias), len(exes), name))
        # block runtime options
        options = self.get_exec_options(global_options, content['.options'] if '.options' in content else None)
        # get return values
        return_vars = self.get_return_vars(content['return'], len(exe_alias))
        # get parameters
        params, params_alias, params_rules = self.get_params(content['params'] if 'params' in content else None)
        # check if exec param index is out of range
        if len(params) and max(params.keys()) > len(exes):
            raise FormatError('``exec[{}]`` out of range, in ``params`` section of ``{}`` with {} executable routines.'.\
                              format(max(params.keys()), name, len(exes)))
        # initialize steps
        self.steps = [DSC_Step(x) for x in exe_alias]
        for i in range(len(self.steps)):
            self.steps[i].set_options(try_get_value(options, 0), try_get_value(options, i + 1))
            self.steps[i].set_exec(exes[i])
            if 'seed' in content:
                self.steps[i].set_seed(content['seed'])
            self.steps[i].set_params(try_get_value(params, 0), try_get_value(params, i + 1),
                                     try_get_value(params_alias, 0), try_get_value(params_alias, i + 1))
            self.steps[i].apply_params_rule(try_get_value(params_rules, 0), try_get_value(params_rules, i + 1))
            self.steps[i].apply_params_operator()
            self.steps[i].set_return(try_get_value(return_vars, 0), try_get_value(return_vars, i + 1))

    def get_exec_options(self, global_options, local_options):
        options = copy.deepcopy(global_options)
        if local_options:
            options.update(local_options)
        if len(options) == 0:
            return {}
        res = {0:{}}
        for key, value in options.items():
            try:
                # get indexed slice
                name, idxes = get_slice(key)
                if name != 'exec':
                    raise FormatError('Unknown indexed option entry: {}.'.format(key))
                for idx in idxes:
                    idx += 1
                    if idx == 0:
                        raise FormatError('``[{}]`` Invalid entry: ``exec[0]``. Index must start from 1.'.\
                                          format(self.name))
                    if idx in res:
                        res[idx].update(flatten_dict(value))
                    else:
                        res[idx] = flatten_dict(value)
            except AttributeError:
                res[0][key] = flatten_dict(value)
        return res

    def get_exec_rule(self, rule):
        return rule

    def get_return_vars(self, return_vars, num_exec):
        res = {}
        if isinstance(return_vars, collections.Mapping):
            # exec specific return alias involved
            for i in range(num_exec):
                try:
                    res[i + 1] = return_vars['exec[{}]'.format(i+1)]
                except KeyError:
                    pass
        else:
            res[0] = return_vars
        for k in res:
            for kk, item in enumerate(res[k]):
                if '=' in item:
                    # return alias exists
                    res[k][kk] = tuple(x.strip() for x in item.split('='))
                else:
                    res[k][kk] = (item.strip(), item.strip())
        return res

    def get_params(self, params):
        if params is None:
            return {}, {}, {}
        res = {0:{}}
        res_rules = {}
        res_alias = {}
        OP = OperationParser()
        for key, value in params.items():
            try:
                # get indexed slice
                name, idxes = get_slice(key)
                if name != 'exec':
                    raise FormatError('Unknown indexed parameter entry: {}.'.format(key))
                for idx in idxes:
                    idx += 1
                    if idx == 0:
                        raise FormatError('``[{}]`` Invalid entry: ``exec[0]``. Index must start from 1.'.\
                                          format(self.name))
                    if idx in res:
                        res[idx].update(flatten_dict(value))
                    else:
                        res[idx] = flatten_dict(value)
            except AttributeError:
                res[0][key] = flatten_dict(value)
            # Parse parameter rules and alias
            for key in res:
                if '.logic' in res[key]:
                    res_rules[key] = OP(res[key]['.logic'])
                    del res[key]['.logic']
                if '.alias' in res[key]:
                    res_alias[key] = res[key]['.alias']
                    del res[key]['.alias']
        return res, res_alias, res_rules

    def extract_steps(self, idxes):
        self.steps = [y for x, y in enumerate(self.steps) if x in idxes]

    def __str__(self):
        steps = {step.name: step.dump() for step in self.steps}
        return dict2str(strip_dict({'computational routines': steps, 'rule': self.rule}))


class DSC_Section:
    def __init__(self, content, sequence):
        self.content = content
        if 'run' not in self.content:
            raise FormatError('Missing required ``DSC::run``.')
        OP = OperationParser()
        self.sequence = sequence if sequence is not None else self.content['run']
        self.sequence = [(x,) if isinstance(x, str) else x
                         for x in sum([OP(expand_slice(y)) for y in self.sequence], [])]
        self.sequence_ordering = self.__merge_sequences(self.sequence)
        self.options = {}
        self.options['work_dir'] = self.content['work_dir'] if 'work_dir' in self.content else None
        self.options['lib_path'] = self.content['lib_path'] if 'lib_path' in self.content else None
        self.options['exec_path'] = self.content['exec_path'] if 'exec_path' in self.content else None
        self.rlib = self.content['R_libs'] if 'R_libs' in self.content else None
        self.pymodule = self.content['python_modules'] if 'python_modules' in self.content else None

    def __merge_sequences(self, input_sequences):
        '''Extract the proper ordering of elements from multiple sequences'''
        # remove slicing
        sequences = [[y.split('[')[0] for y in x] for x in input_sequences]
        values = sequences[0]
        for idx in range(len(sequences) - 1):
            values = merge_lists(values, sequences[idx + 1])
        values = OrderedDict([(x, [-9]) for x in values])
        return values

    def expand_sequences(self, blocks):
        '''expand DSC sequences by index'''
        default = {x: [i for i in range(len(blocks[x].steps))]
                   for x in self.sequence_ordering.keys()} if len(blocks) else {}
        res = []
        for value in self.__index_sequences(self.sequence):
            seq = [x[0] for x in value]
            idxes = [x[1] if x[1] is not None else default[x[0]] for x in value]
            for x, y in zip(seq, idxes):
                self.sequence_ordering[x].extend(y)
            res.append((seq, cartesian_list(*idxes)))
        for x in self.sequence_ordering.keys():
            self.sequence_ordering[x] = sorted(list(set([i for i in self.sequence_ordering[x] if i >= 0])))
        self.sequence = res

    def check_looped_computation(self):
        # check duplicate block names
        for seq, idx in self.sequence:
            # check duplicated block names
            if len(set(seq)) != len(seq):
                raise ValueError('Duplicated blocks found in DSC sequence ``{}``. '\
                                 'Iteratively executing blocks is currently disallowed. '\
                                 'If you need to execute one routine after another in the same block '\
                                 'please re-write your DSC script to make these routines in separate blocks'.\
                                 format(seq))

    def __index_sequences(self, input_sequences):
        '''Strip slicing symbol out of sequences and add them as index'''
        res = []
        for seq in input_sequences:
            res.append(tuple([get_slice(x, mismatch_quit = False) for x in seq]))
        return res

    def __str__(self):
        return dict2str(strip_dict({'sequences to execute': self.sequence,
                                    'sequence ordering': list(self.sequence_ordering.keys()),
                                    'R libraries': self.rlib, 'Python modules': self.pymodule}))
