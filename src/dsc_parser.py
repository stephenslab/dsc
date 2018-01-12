#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
Parser for DSC script and annotation files
'''

import os, re, itertools, copy, collections, subprocess
from ruamel.yaml import YAML
from functools import reduce
from sos.utils import logger
from sos.target import textMD5
from .utils import FormatError, is_null, strip_dict, flatten_list, find_nested_key, \
     cartesian_list, get_slice, expand_slice, flatten_dict, merge_lists, \
     try_get_value, dict2str, update_nested_dict, locate_file, uniq_list, filter_sublist
from .syntax import *
from .line import OperationParser, Str2List, ExpandVars, ExpandActions, CastData
from .plugin import Plugin
yaml = YAML()

__all__ = ['DSC_Script']


class DSC_Script:
    '''Parse a DSC script
     * provides self.steps, self.runtime that contain all DSC information needed for a run
    '''
    def __init__(self, content, output = None, sequence = None, seeds = None, extern = None):
        self.content = dict()
        if os.path.isfile(content):
            dsc_name = os.path.split(os.path.splitext(content)[0])[-1]
            script_path = os.path.dirname(os.path.expanduser(content))
            content = open(content).readlines()
        else:
            if len(content.split('\n')) == 1:
                raise ValueError(f"Cannot find file ``{content}``")
            dsc_name = 'DSCStringIO'
            script_path = None
        res = exe = ''
        for line in content:
            if line.strip().startswith('@'):
                line = line.replace('@', '.', 1)
            if not DSC_BLOCK_CONTENT.search(line) and not line.startswith('#'):
                if res and exe:
                    self.update(res, exe)
                    res = exe = ''
                text = line.split(':')
                if len(text) != 2 or (len(text[1].strip()) == 0 and text[0].strip() != 'DSC'):
                    raise FormatError(f'Invalid syntax ``{line}``. Should be in the format of ``module names: module executables``')
                res += f'{text[0]}:\n'
                exe = text[1].strip()
            else:
                res += line
        self.update(res, exe)
        self.propagate_derived_block()
        global_vars = try_get_value(self.content, ('DSC', 'global'))
        self.set_global_vars(global_vars)
        self.content = DSCEntryFormatter()(self.content, global_vars)
        self.extract_modules()
        self.runtime = DSC_Section(self.content['DSC'], sequence, output, extern)
        if self.runtime.output is None:
            # logger.warning("Using default output name ````.".format(dsc_name))
            self.runtime.output = dsc_name

    def update(self, text, exe):
        block = yaml.load(''.join(text))
        name = re.sub(re.compile(r'\s+'), '', list(block.keys())[0])
        block[name] = block.pop(list(block.keys())[0])
        if block[name] is None:
            block[name] = dict()
        if exe:
            block[name]['.EXEC'] = exe
        self.content.update(block)

    def set_global_vars(self, gvars):
        if gvars is None:
            return
        for v in gvars:
            for block in self.content:
                keys = list(find_nested_key(v, self.content[block]))
                for k in keys:
                    set_nested_value(self.content[block], k, gvars[v])

    def propagate_derived_block(self):
        '''
        Name of derived blocks looks like: "derived(base)"
        This function first figures out sorted block names such that derived block always follows the base block
        Then it propagate self.content derived blocks
        '''
        base = []
        blocks = []
        derived = dict()
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
                raise FormatError(f"Looped block inheritance: {item[0][0]}({item[0][1]}) and {item[0][1]}({item[0][0]})!")
        # Check self-derivation and non-existing base
        tmp = base + [x[0] for x in derived.values()]
        for item in derived.values():
            if item[0] == item[1]:
                raise FormatError(f"Looped block inheritance: {item[0]}({item[0]})!")
            if item[1] not in tmp:
                raise FormatError(f"Base block does not exist: {item[0]}({item[1]})!")
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

    def extract_modules(self):
        if 'DSC' not in self.content:
            raise ValueError('Cannot find required section ``DSC``!')
        res = dict()
        for block in self.content:
            if block == 'DSC':
                res['DSC'] = self.content['DSC']
                continue
            modules = block.split(',')
            if len(modules) != len(self.content[block]['.EXEC']) and len(self.content[block]['.EXEC']) > 1:
                raise ValueError(f"``{len(modules)}`` modules ``{modules}`` does not match ``{len(self.content[block]['.EXEC'])}`` executables ``{self.content[block]['.EXEC']}``. " \
                                 "Please ensure modules and executables match.")
            if len(modules) > 1 and len(self.content[block]['.EXEC']) == 1:
                self.content[block]['.EXEC'] = self.content[block]['.EXEC'] * len(modules)

    def dump(self):
        res = dict([('Blocks', self.blocks),
                    ('DSC', dict([("Sequence", self.runtime.sequence),
                                  ("Ordering", [(x, y) for x, y in self.runtime.sequence_ordering.items()])]))])
        return res

    def __str__(self):
        res = '# Blocks\n' + '\n'.join([f'## {x}\n```yaml\n{y}\n```' for x, y in self.blocks.items()]) \
              + f'\n# DSC\n```yaml\n{self.runtime}\n```'
        return res


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
            if isinstance(value, str):
                value = value.strip().strip(',')
            if isinstance(value, collections.Mapping):
                self.__Transform(value, actions)
            else:
                if key != '.FILTER':
                    for a in actions:
                        value = a(value)
                # empty list
                if is_null(value):
                    del cfg[key]
                else:
                    cfg[key] = value
        return cfg


class DSC_Module:
    def __init__(self, group, name):
        # block name
        self.group = group
        # module name
        self.name = name
        # system seed
        self.seed = None
        # params: alias, value
        self.p = dict()
        # parameter logic:
        self.l = None
        # return variables (to plugin): alias, value
        self.rv = dict()
        # return files: alias, ext
        self.rf = dict()
        # exec
        self.exe = None
        self.exe_id = 0
        # script plugin object
        self.plugin = None
        # runtime variables
        self.workdir = None
        self.is_extern = None
        self.libpath = None
        self.path = None
        # dependencies
        self.depends = []
        # check if it runs in shell
        self.shell_run = None

    def set_seed(self, seed):
        self.seed = seed

    def set_exec(self, exec_var):
        self.exe = ' '.join([locate_file(exec_var[0], self.path)] + list(exec_var[1:]))
        self.plugin = Plugin(os.path.splitext(exec_var[0])[1].lstrip('.'), textMD5(self.exe)[:10])
                        # re.sub(r'^([0-9])(.*?)', r'\2', textMD5(data.command)))

    def check_shell(self, exec_var):
        # FIXME: check if the exec is meant to be executed from shell
        # True only if command has $ parameters and they exist in parameter list
        # Also this conflicts with self.rv: self.shell_run == True && len(self.rv) == 0
        self.shell_run = False
        if not self.shell_run and len(self.rv) > 0:
            # make it a list in order to readily merge with other self.rf items
            self.rf['DSC_AUTO_OUTPUT_'] = ['rds']

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
                            except Exception as e:
                                self.rf[item[0]] = [groups.group(1)]
                    # are there remaining values not File()?
                    if item[0] in self.rf and len(self.rf[item[0]]) < len(param_return):
                        raise FormatError("Return ``{0}`` in ``{1}`` cannot be a mixture of File() and non-File() varables".\
                                          format(item[0], repr(param_return)))
                if item[0] not in self.rf:
                    self.rv[item[0]] = item[1]
            else:
                self.rv[item[0]] = item[1]

    def set_options(self, common_option, spec_option):
        workdir1 = try_get_value(common_option, 'work_dir')
        workdir2 = try_get_value(spec_option, 'work_dir')
        libpath1 = try_get_value(common_option, 'lib_path')
        libpath2 = try_get_value(spec_option, 'lib_path')
        path1 = try_get_value(common_option, 'exec_path')
        path2 = try_get_value(spec_option, 'exec_path')
        extern1 = try_get_value(common_option, 'submit')
        extern2 = try_get_value(spec_option, 'submit')
        self.workdir = workdir2 if workdir2 is not None else workdir1
        self.libpath = libpath2 if libpath2 is not None else libpath1
        self.path = path2 if path2 is not None else path1
        self.is_extern = extern2 if extern2 is not None else extern1

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
        # Currently it is list() / dict()
        for k1, k2 in list(alias.items()):
            groups = DSC_PACK_OP.search(k2)
            if groups:
                self.plugin.set_container(k1, groups.group(2), self.p)
                del alias[k1]
        if len(alias):
            raise FormatError('Invalid .ALIAS for computational routine ``{}``:\n``{}``'.\
                              format(self.name, dict2str(alias)))

    def apply_params_rule(self, common_rule, spec_rule):
        rule = spec_rule if spec_rule else common_rule
        if rule is None or len(rule) == 0:
            return
        raw_rule = rule
        rule = ' and '.join(['({})'.format(x.replace('.', '_')) for x in rule.split(',')])
        statement = ';'.join(["{} = {}".format(k, str(self.p[k])) for k in self.p])
        value_str = ','.join(['_{}'.format(x) for x in self.p.keys()])
        loop_str = ' '.join(["for _{0} in {0}".format(x) for x in self.p])
        statement += ';print(len([({}) {} if {}]))'.format(value_str, loop_str, rule)
        try:
            ret = subprocess.check_output("python -c '''{}'''".\
                                          format(str(statement)), shell = True).decode('utf-8').strip()
        except:
            raise FormatError("Invalid @FILTER: ``{}``!".format(raw_rule))
        if int(ret) == 0:
            raise FormatError("No parameter combination satisfies @FILTER ``{}``!".format(raw_rule))
        self.l = rule

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
                        if not p1.startswith('$'):
                            p1 = repr(p1)
                if isinstance(p1, tuple):
                    # FIXME format_tuple has to be defined for shell as well
                    p1 = self.plugin.format_tuple(p1)
                values.append(p1)
            if len(values) == 0:
                del self.p[k]
            else:
                self.p[k] = values

    def __str__(self):
        return dict2str(self.dump())

    def dump(self):
        return strip_dict(dict([ ('name', self.name), ('group', self.group),
                                        ('dependencies', self.depends), ('command', self.exe),
                                        ('command_index', self.exe_id),('use replicates', self.seed),
                                        ('parameters', self.p), ('parameter rule', self.l),
                                        ('return variables', self.rv),
                                        ('return files', self.rf),  ('shell status', self.shell_run),
                                        ('plugin status', self.plugin.dump()),
                                        ('runtime options', dict([('exec path', self.path),
                                                                         ('workdir', self.workdir),
                                                                         ('library path', self.libpath)]))]),
                          mapping = dict, skip_keys = ['parameters'])

class DSC_Block:
    def __init__(self, name, content, global_options = None, script_path = None):
        '''Populate modules in the block and keep track of block rules
        Members are:
          - self.modules, self.rule, self.name
        '''
        self.name = name
        # block executable rules
        self.rule = self.get_exec_rule(content['.FILTER']) if '.FILTER' in content else None
        exes = [tuple(x.split()) if isinstance(x, str) else x for x in content['exec']]
        if '.ALIAS' in content:
            for item in content['.ALIAS']:
                if '.' in item:
                    raise FormatError("Dot ``.`` in alias ``{}`` is not allowed. "\
                                      "Please remove or replace it with underscore ``_``".format(item))
            exe_alias = content['.ALIAS']
        else:
            exe_alias = ['_'.join([os.path.splitext(os.path.basename(x))[0] if i == 0 else x \
                                   for i, x in enumerate(y) if not x.startswith('$')]) for y in exes]
        if len(exes) == 1 and len(exe_alias) > 1:
            exes = exes * len(exe_alias)
        # check if any exec out of index
        if (self.rule is None and len(exes) != len(exe_alias)) or \
           (self.rule is not None and len(exe_alias) != len(self.rule)):
            raise FormatError('Alias ``{}`` (length {}) does not match exec (length {}), in block ``{}``!'.\
                              format(repr(exe_alias), len(exe_alias), len(exes), name))
        # block runtime options
        options = self.get_exec_options(global_options or {},
                                        content['.CONF'] if '.CONF' in content else None, script_path)
        # get return values
        return_vars = self.get_return_vars(content['return'], len(exe_alias))
        # get parameters
        params, params_alias, params_rules = self.get_params(content['params'] if 'params' in content else None)
        # check if exec param index is out of range
        if len(params) and max(params.keys()) > len(exes):
            raise FormatError('``exec[{}]`` out of range, in ``params`` section of ``{}`` with {} executable routines.'.\
                              format(max(params.keys()), name, len(exes)))
        # initialize modules
        self.modules = [DSC_Module(name, x) for x in exe_alias]
        for i in range(len(self.modules)):
            self.modules[i].exe_id = i + 1
            self.modules[i].set_options(try_get_value(options, 0), try_get_value(options, i + 1))
            self.modules[i].set_exec(exes[i])
            if 'seed' in content:
                self.modules[i].set_seed(content['seed'])
            self.modules[i].set_params(try_get_value(params, 0), try_get_value(params, i + 1),
                                     try_get_value(params_alias, 0), try_get_value(params_alias, i + 1))
            self.modules[i].set_return(try_get_value(return_vars, 0), try_get_value(return_vars, i + 1))
            self.modules[i].apply_params_operator()
            self.modules[i].apply_params_rule(try_get_value(params_rules, 0), try_get_value(params_rules, i + 1))
            self.modules[i].check_shell(exes[i])

    def get_exec_options(self, global_options, local_options, script_path):
        options = copy.deepcopy(global_options)
        if local_options:
            options.update(local_options)
        if len(options) == 0:
            return {}
        # options = self.swap_abs_paths(options, script_path)
        res = dict()
        res[0] = dict()
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
                        res[idx].update(flatten_dict(value, mapping = dict))
                    else:
                        res[idx] = flatten_dict(value, mapping = dict)
            except AttributeError:
                res[0][key] = flatten_dict(value, mapping = dict)
        return res

    @staticmethod
    def get_exec_rule(rule):
        return rule

    @staticmethod
    def get_return_vars(return_vars, num_exec):
        res = dict()
        if isinstance(return_vars, collections.Mapping):
            # exec specific return alias involved
            for i in range(num_exec):
                try:
                    res[i + 1] = return_vars[f'exec[{i+1}]']
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
        res = dict()
        res[0] = dict()
        res_rules = dict()
        res_alias = dict()
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
                        res[idx].update(flatten_dict(value, mapping = dict))
                    else:
                        res[idx] = flatten_dict(value, mapping = dict)
            except AttributeError:
                res[0][key] = flatten_dict(value, mapping = dict)
            # Parse parameter rules and alias
            for key in res:
                res[key] = flatten_dict(res[key])
                if '.FILTER' in res[key]:
                    res_rules[key] = res[key]['.FILTER']
                    del res[key]['.FILTER']
                if '.ALIAS' in res[key]:
                    res_alias[key] = res[key]['.ALIAS']
                    del res[key]['.ALIAS']
        return res, res_alias, res_rules

    def extract_modules(self, idxes):
        self.modules = [y for x, y in enumerate(self.modules) if x in idxes]

    @staticmethod
    def swap_abs_paths(data, master_path):
        if master_path is None:
            return data
        cwd = os.getcwd() + '/'
        for k in ['work_dir', 'lib_path', 'exec_path']:
            if data[k] is None:
                continue
            for kk, item in enumerate(data[k]):
                item = os.path.normpath(os.path.abspath(os.path.expanduser(item)))
                relative_path = item.replace(cwd, '')
                if relative_path != item:
                    data[k][kk] = os.path.join(master_path, relative_path)
        return data

    def __str__(self):
        modules = {module.name: module.dump() for module in self.modules}
        return dict2str(strip_dict(dict([('computational routines', modules), ('rule', self.rule)]),
                                   mapping = dict))

class DSC_Section:
    def __init__(self, content, sequence, output, extern):
        self.content = content
        self.output = self.content['output'][0] if 'output' in self.content else output
        if 'run' not in self.content:
            raise FormatError('Missing required ``DSC::run``.')
        self.OP = OperationParser()
        self.sequence = []
        if isinstance(self.content['run'], collections.Mapping):
            self.named_sequence = self.content['run']
        else:
            self.named_sequence = None
        if sequence is not None:
            for item in sequence:
                if isinstance(self.content['run'], collections.Mapping) and item in self.content['run']:
                    self.sequence.extend(self.content['run'][item])
                else:
                    self.sequence.append(item)
        else:
            if isinstance(self.content['run'], collections.Mapping):
                self.sequence = flatten_list(self.content['run'].values())
            else:
                self.sequence = self.content['run']
        self.sequence = [(x,) if isinstance(x, str) else x
                         for x in sum([self.OP(expand_slice(y)) for y in self.sequence], [])]
        self.sequence = filter_sublist(self.sequence)
        self.sequence_ordering = self.__merge_sequences(self.sequence)
        self.options = dict()
        self.options['work_dir'] = self.content['work_dir'] if 'work_dir' in self.content else './'
        self.options['lib_path'] = self.content['lib_path'] if 'lib_path' in self.content else None
        self.options['exec_path'] = self.content['exec_path'] if 'exec_path' in self.content else None
        if extern:
            self.options['submit'] = True
        else:
            self.options['submit'] = self.content['submit'] if 'submit' in self.content else None
        self.rlib = self.content['R_libs'] if 'R_libs' in self.content else None
        self.pymodule = self.content['python_modules'] if 'python_modules' in self.content else None

    @staticmethod
    def __merge_sequences(input_sequences):
        '''Extract the proper ordering of elements from multiple sequences'''
        # remove slicing
        sequences = [[y.split('[')[0] for y in x] for x in input_sequences]
        values = sequences[0]
        for idx in range(len(sequences) - 1):
            values = merge_lists(values, sequences[idx + 1])
        values = dict([(x, [-9]) for x in values])
        return values

    def expand_sequences(self, blocks):
        '''expand DSC sequences by index'''
        default = {x: [i for i in range(len(blocks[x].modules))]
                   for x in self.sequence_ordering.keys()} if len(blocks) else {}
        res = []
        for value in self.__index_sequences(self.sequence):
            seq = tuple([x[0] for x in value])
            idxes = [x[1] if x[1] is not None else default[x[0]] for x in value]
            for x, y in zip(seq, idxes):
                self.sequence_ordering[x].extend(y)
            res.append([seq, cartesian_list(*idxes)])
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

    @staticmethod
    def __index_sequences(input_sequences):
        '''Strip slicing symbol out of sequences and add them as index'''
        res = []
        for seq in input_sequences:
            res.append(tuple([get_slice(x, mismatch_quit = False) for x in seq]))
        return res


    def consolidate_sequences(self):
        '''
        For trivial multiple sequences, eg, "module1 * module2[1], module1 * module[2]", should be consolidated to one
        This cannot be done with symbolic math logic so we have to do it here
        '''
        sequences = dict()
        for sequence, idx in self.sequence:
            if sequence not in sequences:
                sequences[sequence] = idx
            else:
                for item in idx:
                    if item not in sequences[sequence]:
                        sequences[sequence].append(item)
        self.sequence = [[k, sorted(value)] for k, value in sequences.items()]

    def __str__(self):
        return dict2str(strip_dict(dict([('sequences to execute', self.sequence), (
                                    'sequence ordering', list(self.sequence_ordering.keys())), (
                                        'R libraries', self.rlib), ( 'Python modules', self.pymodule)]),
                                   mapping = dict))
