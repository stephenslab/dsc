#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
This file defines DSCJobs and DSC2SoS classes
to convert DSC configuration to SoS codes
'''
import copy, re, os
from sos.target import executable, fileMD5, textMD5
from sos.utils import Error
from .utils import dotdict, dict2str, try_get_value, get_slice, \
     cartesian_list, merge_lists, uniq_list, flatten_list
from .plugin import Plugin, R_LMERGE, R_SOURCE

class StepError(Error):
    """Raised when Step parameters are illegal."""
    def __init__(self, msg):
        Error.__init__(self, msg)
        self.args = (msg, )

class DSCJobs(dotdict):
    '''
    Sanity check and convert DSC data to SoS steps.
      * Input is DSCData object

    This includes:
      * Ensure step ordering for DSC::run are legitimate
      * Prepare environments to run R / Python scripts: libraries, alias, return alias
      * Prepare additional codes for R / Python exec
      * Prepare environments to run non-R/py exec: checking / putting together arguments
      * Figure out step dependencies; figure out whether a step is from plugins
      * Slicing by rules
      * Handle replicates if any

    The output of this will be a DSCJobs object ready to convert to SoS steps
    '''
    def __init__(self, data):
        '''
        master_data: dict
        data: dict
        output_prefix: str
          output directory
        default_workdir: str
        ordering: list
        sequences: list
        '''
        self.data = []
        self.master_data = {}
        self.output_prefix = data.DSC['output'][0]
        self.libpath = try_get_value(data.DSC, ('lib_path'))
        self.path = try_get_value(data.DSC, ('exec_path'))
        self.default_workdir = data.DSC['work_dir'][0]
        # sequences in action, logically ordered
        self.ordering = self.merge_sequences(data.DSC['run'])
        for block in self.ordering:
            self.load_master_data(data[block], name = block)
        # sequences in action, unordered but expanded by index
        self.sequences = self.expand_sequences(data.DSC['run'],
                                                {x : range(len(self.master_data[x]))
                                                 for x in self.master_data})
        for seq, idx in self.sequences:
            # check duplicated block names
            if len(set(seq)) != len(seq):
                raise ValueError('Duplicated blocks found in DSC sequence ``{}``. '\
                                 'Iteratively executing blocks is logically disallowed. '\
                                 'If you need to execute one routine after another in the same block '\
                                 'please re-write your DSC script to make these routines in separate blocks'.\
                                 format(seq))
            self.data.append(self.get_workflow(seq))

    def __initialize_block(self, name):
        '''Intermediate data to be appended to self.data'''
        data = dotdict()
        data.work_dir = self.default_workdir
        data.output_db = self.output_prefix
        # to_plugin: If the output is an RDS file: is so, if both `plugin` in ('R', 'Python')
        # and return parameter is not found in params list
        # About plugin mode:
        # The logic is a 2 by 2 table:
        #       | plugin | not plugin
        # ------|--------|-----------
        # From  |   A         B
        # ------|
        # To    |   C         D
        # Here we must make sure every step in a block have the same consistent from/to status
        # to_plugin "C" can result in only one file; others can result in multiple files
        # FIXME: currently A allows 2 files, D allows only one file and not sure about B yet.
        # How to determine A, B, C, D?
        # For C & D: if return value (after alias mapping) is not in parameter list it must be C
        # If a parameter is File() and it is in return value, it then must be D
        # If all return values are in parameter list and no File() is there I'll throw an error
        # For A & B: if the dependent step is to_plugin then A, or B
        # FIXME: should allow for mixed A & B. then from_plugin parameter should be a dictionary
        # Because it will be parameter specific.
        data.plugin = Plugin()
        data.to_plugin = None
        # Input depends: [(step_name, out_var, idx), (step_name, out_var, idx) ...]
        # Then out_var will be assigned in the current step as: out_var = step_name.output[idx]
        data.depends = []
        # For every depends if it is from some plugin,
        # via checking the corresponding to_plugin property.
        data.from_plugin = None
        data.name = name
        return data

    def __reset_block(self, data, exe, exe_name, exec_path):
        '''Intermediate data to be appended to self.data'''
        data.command = ' '.join([self.__search_exec(exe[0], exec_path)] + list(exe[1:]))
        plugin = Plugin(os.path.splitext(exe[0])[1].lstrip('.'), textMD5(data.command)[:10])
                        # re.sub(r'^([0-9])(.*?)', r'\2', textMD5(data.command)))
        if (data.plugin.name is not None and (not plugin.name) != (not data.plugin.name)):
            raise StepError("A mixture of plugin codes and other executables are not allowed " \
            "in the same block ``{}``.".format(data.name))
        else:
            data.plugin = plugin
        # if data.plugin.name:
        #     executable(data.plugin.name)
        data.plugin.reset()
        data.parameters = []
        # FIXME: currently only one output ext is set
        data.output_ext = []
        data.output_vars = []
        data.exe = ' '.join([x for x in exe if not x.startswith('$')]) if exe_name is None else exe_name

    def load_master_data(self, block, name = 'block'):
        '''Load block data to self.master_data with some preliminary processing
        '''
        def load_params():
            params = {}
            if 'params' in block:
                if 0 in list(block.params.keys()):
                    params = copy.deepcopy(block.params[0])
                if (idx + 1) in list(block.params.keys()):
                    params.update(block.params[idx + 1])
            # Handle seed here
            if 'seed' in block.meta:
                params['seed'] = block.meta['seed']
            return params

        def load_rules():
            rules = None
            if 'rules' in block:
                if 0 in list(block.rules.keys()):
                    rules = block.rules[0]
                if (idx + 1) in list(block.rules.keys()):
                    rules = block.rules[idx + 1]
            return rules

        def load_alias():
            alias = {}
            if 'params_alias' in block:
                if 0 in list(block.params_alias.keys()):
                    alias = dict([(x.strip() for x in item.split('=')) for item in block.params_alias[0]])
                if (idx + 1) in list(block.params_alias.keys()):
                    alias = dict([(x.strip() for x in item.split('=')) for item in block.params_alias[idx + 1]])
            return alias

        def process_alias():
            for k, item in list(alias.items()):
                groups = re.search(r'(.*?)\((.*?)\)', item)
                if not groups:
                    # swap key
                    params[k] = params.pop(item)
                else:
                    if groups.group(1) == 'Pack':
                        if not data.plugin.name:
                            raise StepError("Alias ``Pack`` is not applicable to executable ``{}``.".\
                                            format(exe[0]))
                        data.plugin.set_container(k, groups.group(2), params)
                    else:
                        raise StepError('Invalid .alias ``{}`` in block ``{}``.'.\
                                        format(groups.group(1), name))

        def process_rules():
            # parameter value slicing
            pass

        def process_return(out, idx):
            if isinstance(out, dict):
                # exec specific return alias involved
                # need to extract the one that matches
                # and discard others
                try:
                    out = out['exec[{}]'.format(idx)]
                except KeyError:
                    raise StepError("Invalid return alias ``{}``".format(out))
            for item in out:
                lhs = ''
                if '=' in item:
                    # return alias exists
                    lhs, rhs = (x.strip() for x in item.split('='))
                    groups = re.search(r'^(R|Python)\((.*?)\)$', rhs)
                    if groups:
                        # alias is within plugin
                        data.plugin.add_return(lhs, groups.group(2))
                        data.to_plugin = True
                    else:
                        # alias is not for value inside other return objects
                        # It may be a parameter specified by DSC2
                        # Or be name of a variable inside the plugin
                        if rhs not in params:
                            data.to_plugin = True
                        lhs = (lhs, rhs)
                else:
                    lhs = item.strip()
                if lhs not in params and not isinstance(lhs, tuple):
                    data.to_plugin = True
                data.output_vars.append(lhs)
            if data.to_plugin:
                data.output_ext = repr('rds')
        #
        # Load command parameters
        #
        self.master_data[name] = []
        data = self.__initialize_block(name)
        exec_alias = try_get_value(block.meta, ('exec_alias'))
        exec_rule = try_get_value(block.meta, ('rule'))
        for idx, exe in enumerate(block.meta['exec']):
            self.__reset_block(data, exe,
                               exec_alias[idx] if exec_alias and len(exec_alias) > idx else None,
                               self.path)
            # temporary variables
            params, rules, alias = load_params(), load_rules(), load_alias()
            # handle alias
            process_alias()
            # FIXME: handle rules, not implemented
            process_rules()
            # handle return
            # if return is not in parameter then
            # to_plugin will be True.
            # After this function, to_plugin is either True or None
            process_return(block.out, idx + 1)
            # assign parameters
            data.parameters = params
            self.master_data[name].append(dict(data))
        #
        # Consolidate commands if there are meta rules (.logic under exec in DSC script)
        # things to notice:
        # 1. The signature of the combined execs will consists of every execs
        # meaning that changes to any of them will result in rerun
        # 2. For plugin mode, the commands that follows the first one will load and save ${_output}
        # NOT ${_input}
        # 3. FIXME: For non-plugin mode ...
        if exec_rule:
            master_swap = []
            for item in exec_rule:
                # Get the sequence in which the exec are combined
                # Rules to combine:
                # 1. for 'command' and 'plugin' have to merge them to tuples
                # 2. for 'exec' have to come up with a combined name (or use alias if provided)
                # 3. for 'params' have to do concatenate together into one
                item = flatten_list([get_slice(x)[1] for x in item.split('+')])
                tmp_master = None
                for idx in item:
                    if tmp_master is None:
                        tmp_master = self.master_data[name][idx]
                        tmp_master['command'] = [tmp_master['command']]
                        tmp_master['plugin'] = [tmp_master['plugin']]
                    else:
                        tmp_master['command'].append(self.master_data[name][idx]['command'])
                        tmp_master['plugin'].append(self.master_data[name][idx]['plugin'])
                        tmp_master['parameters'].update(self.master_data[name][idx]['parameters'])
                        if exec_alias is None:
                            tmp_master['exe'] += '.{}'.format(self.master_data[name][idx]['exe'])
                tmp_master['command'] = tuple(tmp_master['command'])
                tmp_master['plugin'] = tuple(tmp_master['plugin'])
                master_swap.append(tmp_master)
            self.master_data[name] = master_swap
        else:
            for idx in range(len(self.master_data[name])):
                self.master_data[name][idx]['command'] = (self.master_data[name][idx]['command'],)
                self.master_data[name][idx]['plugin'] = (self.master_data[name][idx]['plugin'],)

    def get_workflow(self, sequence):
        '''Convert self.master_data to self.data
           * Fully expand sequences so that each sequence will be one SoS instance
           * Resolving step dependencies and some syntax conversion, most importantly the $ symbol
        '''
        def find_dependencies(value):
            curr_idx = idx
            if curr_idx == 0:
                raise StepError('Symbol ``$`` is not allowed in the first step of DSC sequence.')
            curr_idx = curr_idx - 1
            dependence = None
            to_plugin = None
            while curr_idx >= 0:
                output_vars = [x[0] if isinstance(x, tuple) else x for x in res[curr_idx][0]['output_vars']]
                try:
                    dependence = (res[curr_idx][0]['name'], value, output_vars.index(value))
                    to_plugin = res[curr_idx][0]['to_plugin']
                except ValueError:
                    pass
                if dependence is not None:
                    break
                else:
                    curr_idx = curr_idx - 1
            if dependence is None:
                raise StepError('Cannot find return value for ``${}`` in any of its previous steps.'.\
                                format(value))
            if dependence not in master_data[item][step_idx]['depends']:
                master_data[item][step_idx]['depends'].append(dependence)
                if master_data[item][step_idx]['from_plugin'] is not None \
                and master_data[item][step_idx]['from_plugin'] != to_plugin:
                    raise StepError("Mixed input from previous plugin and non-plugin steps is currently "\
                                    "not implemented!")
                else:
                    master_data[item][step_idx]['from_plugin'] = to_plugin
        #
        res = []
        master_data = copy.deepcopy(self.master_data)
        for idx, item in enumerate(sequence):
            # for each step
            for step_idx, step in enumerate(master_data[item]):
                output_vars = [x[0] if isinstance(x, tuple) else x for x in master_data[item][step_idx]['output_vars']]
                # for each exec
                for k, p in list(step['parameters'].items()):
                    values = []
                    for p1 in p:
                        if isinstance(p1, str):
                            if p1.startswith('$'):
                                find_dependencies(p1[1:])
                                for plugin in master_data[item][step_idx]['plugin']:
                                    plugin.add_input(k, p1)
                                continue
                            elif re.search(r'^Asis\((.*?)\)$', p1):
                                p1 = re.search(r'^Asis\((.*?)\)$', p1).group(1)
                            elif re.search(r'^File\((.*?)\)$', p1):
                                # p1 is file extension
                                # then will see if k is in the return list
                                # if so, this is not plugin mode, set plugin to False
                                # (this is because we'll not allow returning a file if it is already
                                # plugin mode! It is possible to use File() though but I'll not monitor
                                # the resulting product [in terms of signature])
                                file_ext = re.search(r'^File\((.*?)\)$', p1).group(1)
                                if k in output_vars:
                                    if file_ext.lower() == 'tmp':
                                        raise ValueError('Cannot return temporary file ``{}: File({})``!'.\
                                                         format(k, file_ext))
                                    if master_data[item][step_idx]['to_plugin'] is True:
                                        raise StepError("Cannot return to additional file ``{}: File({})``"\
                                                        " in plugin mode!".format(k))
                                    elif master_data[item][step_idx]['to_plugin'] is False:
                                        # FIXME: multiple file output not allowed for now
                                        raise StepError('Multiple file output not allowed in ``{}``'.\
                                                        format(item))
                                    else:
                                        master_data[item][step_idx]['to_plugin'] = False
                                        master_data[item][step_idx]['output_ext'] = repr(file_ext)
                                        # continue because we do not need to have this parameter
                                        # in the parameter list
                                        for plugin in master_data[item][step_idx]['plugin']:
                                            plugin.add_input(k, '${_output!r}')
                                        continue
                                else:
                                    # this file is a tmp file
                                    for plugin in master_data[item][step_idx]['plugin']:
                                        plugin.add_tempfile(k, file_ext)
                                    continue
                            else:
                                p1 = repr(p1)
                        if isinstance(p1, tuple):
                            # FIXME: for ambiguous tuple, will have to ask users to reformat
                            tmp = set([plugin.format_tuple(p1)
                                      for plugin in master_data[item][step_idx]['plugin']])
                            if len(tmp) > 1:
                                raise StepError('Cannot properly determine the format for ``{}`` '\
                                                'for multiple executables of different types. '\
                                                'Please explicitly format it via ``Asis()`` syntax.')
                            p1 = list(tmp)[0]
                        values.append(p1)
                    if len(values) == 0:
                        del master_data[item][step_idx]['parameters'][k]
                    elif len(values) < len(p):
                        # This means that $XX and other variables coexist
                        # For a plugin script
                        raise StepError("Cannot use return value from a script " \
                                         "as input parameter in parallel to others!\nLine: ``{}``".\
                                         format(', '.join(map(str, p))))
                    else:
                        master_data[item][step_idx]['parameters'][k] = values
                if len(master_data[item][step_idx]['parameters']) == 0:
                    del master_data[item][step_idx]['parameters']
                # sort input depends
                master_data[item][step_idx]['depends'].sort(key = lambda x: self.ordering.index(x[0]))
                if master_data[item][step_idx]['to_plugin'] is None:
                    # At this point if this is still pending
                    # then it means that every return value must be in the parameter list
                    # and that none of the value is a file to be created.
                    # FIXME: logically this should be a plugin mode
                    # because I assume the data is transformed in this computational routine.
                    # I cannot think of situations otherwise.
                    # and written back into RDS. Treat it plugin for now until I see things break.
                    # then it is there is no point to run this step because nothing new is produced!
                    master_data[item][step_idx]['to_plugin'] = True
                    master_data[item][step_idx]['output_ext'] = repr('rds')
                # add exec identifier
                master_data[item][step_idx]['exe_id'] = step_idx + 1
            res.append(master_data[item])
        return res

    def merge_sequences(self, input_sequences):
        '''Extract the proper ordering of elements from multiple sequences'''
        # remove slicing
        sequences = [[y.split('[')[0] for y in x] for x in input_sequences]
        values = sequences[0]
        for idx in range(len(sequences) - 1):
            values = merge_lists(values, sequences[idx + 1])
        return values

    def expand_sequences(self, sequences, default = {}):
        '''expand DSC sequences by index'''
        res = []
        for value in self.__index_sequences(sequences):
            seq = [x[0] for x in value]
            idxes = [x[1] if x[1] is not None else default[x[0]] for x in value]
            res.append((seq, cartesian_list(*idxes)))
        return res

    def __search_exec(self, exe, exec_path):
        '''Use exec_path information to try to complete the path of cmd'''
        if exec_path is None:
            return exe
        res = None
        for item in exec_path:
            if os.path.isfile(os.path.join(item, exe)):
                if res is not None:
                    raise StepError("File ``{}`` found in multiple directories ``{}`` and ``{}``!".\
                                    format(exe, item, os.path.join(*os.path.split(res)[:-1])))
                res = os.path.join(item, exe)
        return res if res else exe

    def __index_sequences(self, input_sequences):
        '''Strip slicing symbol out of sequences and add them as index'''
        res = []
        for seq in input_sequences:
            res.append(tuple([get_slice(x, mismatch_quit = False) for x in seq]))
        return res

    def __str__(self):
        text1 = ''
        for item in self.ordering:
            text1 += dict2str({item: self.master_data[item]}) + '\n'
        text2 = ''
        for sequence in self.data:
            for block in sequence:
                for item in block:
                    text2 += dict2str(item) + '\n'
        return text1.strip() + '\n{}'.format('#\n' * 5) + text2.strip()

class DSC2SoS:
    '''
    Initialize SoS workflows with DSC jobs
      * Input is DSC job objects
      * Output is SoS workflow codes

    Here are the ideas from DSC to SoS:
      * Each DSC computational routine `exec` is a step; step name is `block name + routine index`;
        step alias is block name
      * When there are combined routines in a block via `.logic` for `exec` then sub-steps are generated
        with name `block name + combined routine index + routine index` without alias name then
        create nested workflow
        and eventually the nested workflow name will be `block name + combined routine index` with
        alias being block name
      * Parameters utilize `for_each` (and `paired_with`??).
      * Parameters are pre-expanded such that SoS `for_each` and `paired_with` are
        support for otherwise complicated DSC `.logic`.
      * Final workflow also use nested workflow structure,
        with workflow name "DSC" for each sequence, indexed by
        the possible ways to combine exec routines. The possible ways are pre-determined and passed here.
      * Each DSC sequence is a separate SoS code piece. These pieces have to be executed sequentially
        to reuse some runtime signature although -j N is allowed in each script
    '''
    def __init__(self, data, dsc_file, rerun = False):
        def replace_right(source, target, replacement, replacements=None):
            return replacement.join(source.rsplit(target, replacements))
        #
        self.dsc_file = dsc_file
        self.output_prefix = data.output_prefix
        self.libpath = data.libpath
        self.confdb =  "'.sos/.dsc/{}.{{}}.mpk'.format('_'.join((sequence_id, step_name[8:])))".\
                       format(os.path.basename(data.output_prefix))
        conf_header = 'import msgpack\nfrom collections import OrderedDict\n' \
                      'from dsc.utils import sos_hash_output, sos_group_input, chunks\n' \
                      'from dsc.dsc_database import remove_obsolete_db, build_config_db\n\n\n'
        job_header = "import msgpack\nfrom collections import OrderedDict\n"\
                     "parameter: IO_DB =  msgpack.unpackb(open('.sos/.dsc/{}.conf.mpk'"\
                     ", 'rb').read(), encoding = 'utf-8', object_pairs_hook = OrderedDict)\n\n".\
                     format(os.path.basename(data.output_prefix))
        processed_steps = {}
        conf_str = []
        job_str = []
        self.step_map = {} # name map for steps
        # Get steps
        for idx, sequence in enumerate(data.data):
            self.step_map[idx] = {}
            for block in sequence:
                for step in block:
                    name = "{0}_{1}_{2}".\
                           format(step['name'], step['exe_id'],
                                  '_'.join([i[0] for i in step['depends']]))
                    exe_id = step['exe_id']
                    if name not in processed_steps:
                        pattern = re.compile("^{0}_{1}$|^{0}_[0-9]+_{1}$".\
                                             format(step['name'], step['exe_id']))
                        cnt = len([k for k in
                                   set(flatten_list([list(self.step_map[i].values()) for i in range(idx)]))
                                   if pattern.match(k)])
                        step['exe_id'] = "{0}_{1}".\
                                         format(cnt, step['exe_id']) if cnt > 0 else step['exe_id']
                        conf_str.append(self.__get_prepare_step(step))
                        job_str.append(self.__get_run_step(step))
                        processed_steps[name] = "{}_{}".format(step['name'], step['exe_id'])
                    self.step_map[idx]["{}_{}".format(step['name'], exe_id)] = processed_steps[name]
        # Get workflows
        i = 1
        io_info_files = []
        for idx, sequence in enumerate(data.sequences):
            seq, indices = sequence
            for index in indices:
                rsqn = ['{}_{}'.format(x, y + 1)
                        if '{}_{}'.format(x, y + 1) not in self.step_map[idx]
                        else self.step_map[idx]['{}_{}'.format(x, y + 1)]
                        for x, y in zip(seq, index)]
                sqn = [replace_right(x, '_', ':', 1) for x in rsqn]
                provides_files = ['.sos/.dsc/{}.{}.mpk'.\
                                  format(data.output_prefix, '_'.join((str(i), x))) for x in rsqn]
                conf_str.append("[INIT_{0}: provides = {3}]\nsos_run('{2}', {1})".\
                              format(i, "sequence_id = '{}', sequence_name = '{}'".\
                                     format(i, '+'.join(rsqn)),
                                     '+'.join(['prepare_{}'.format(x) for x in sqn]),
                                     repr(provides_files)))
                io_info_files.extend(provides_files)
                job_str.append("[DSC_{0} ({3})]\nsos_run('{2}', {1})".\
                              format(i, "sequence_id = '{}'".format(i), '+'.join(sqn), "DSC sequence {}".format(i)))
                i += 1
        self.conf_str = conf_header + '\n'.join(conf_str)
        self.job_str = job_header + '\n'.join(job_str)
        self.conf_str += '''
[INIT_{2}]
parameter: vanilla = {1}
input: dynamic(sorted(set({4})))
output: '.sos/.dsc/{0}.io.mpk', '.sos/.dsc/{0}.map.mpk', '.sos/.dsc/{0}.conf.mpk'
build_config_db(input, output[0], output[1], output[2], vanilla = vanilla)
[INIT_0]
remove_obsolete_db('{3}')
        '''.format(os.path.basename(data.output_prefix), rerun, i,
                   data.output_prefix, repr(io_info_files))
        with open('.sos/.dsc/utils.R', 'w') as f:
            f.write(R_SOURCE + R_LMERGE)

    def __call__(self):
        pass

    def __str__(self):
        return '{}'.format(dict2str(self.step_map)) + '\n\n' + '##\n' * 5 \
            + '\n{}'.format(self.conf_str) + '\n\n' + '##\n' * 5 + '\n{}'.format(self.job_str)

    def __get_prepare_step(self, step_data):
        '''
        This will produce source to build config and database for
        parameters and file names. The result is one binary json file (via msgpack)
        with keys "X:Y:Z" where X = DSC sequence ID, Y = DSC subsequence ID, Z = DSC step name
            (name of indexed DSC block corresponding to a computational routine).
        '''
        res = ["[prepare_{0}_{1}: shared = '{0}_output']".format(step_data['name'], step_data['exe_id'])]
        res.extend(["parameter: sequence_id = None", "parameter: sequence_name = None"])
        res.append("input: '{}'\noutput: {}".format(self.dsc_file, self.confdb))
        # Set params, make sure each time the ordering is the same
        params = sorted(step_data['parameters'].keys()) if 'parameters' in step_data else []
        for key in params:
            res.append('{} = {}'.format(key, repr(step_data['parameters'][key])))
        input_vars = None
        depend_steps = []
        cmds_md5 = ''.join([(fileMD5(item.split()[0], partial = False) if os.path.isfile(item.split()[0])
                             else item.split()[0]) + \
                            (item.split()[1] if len(item.split()) > 1 else '')
                            for item in step_data['command']])
        if params:
            loop_string = ' '.join(['for _{0} in {0}'.format(s) for s in reversed(params)])
        else:
            loop_string = ''
        format_string = '.format({})'.\
                        format(', '.join(['_{}'.format(s) for s in reversed(params)]))
        if step_data['depends']:
            # A step can depend on maximum of other 2 steps, by DSC design
            depend_steps = uniq_list([x[0] for x in step_data['depends']])
            if len(depend_steps) >= 2:
                # Generate combinations of input files
                input_vars = "input_files"
                res.append("depends: {}".\
                           format(', '.join(['sos_variable(\'{}_output\')'.format(x) for x in depend_steps])))
                res.append('input_files = sos_group_input([{}])'.\
                           format(', '.join(['{}_output'.format(x) for x in depend_steps])))
            else:
                input_vars = "{}_output".format(depend_steps[0])
                res.append("depends: sos_variable('{}')".format(input_vars))
            loop_string += ' for __i in chunks({}, {})'.format(input_vars, len(depend_steps))
            out_string = "[sos_hash_output('{0}'{1}, prefix = '{3}', suffix = '{{}}'.format({4})) {2}]".\
                         format(' '.join([step_data['exe'], step_data['name'], cmds_md5] \
                                           + ['{0}:{{}}'.format(x) for x in reversed(params)]),
                                format_string, loop_string, step_data['exe'], "':'.join(__i)")
        else:
            out_string = "[sos_hash_output('{0}'{1}, prefix = '{3}') {2}]".\
                         format(' '.join([step_data['exe'], step_data['name'], cmds_md5] \
                                           + ['{0}:{{}}'.format(x) for x in reversed(params)]),
                                format_string, loop_string, step_data['exe'])
        res.append("{}_output = {}".format(step_data['name'], out_string))
        param_string = '[([{0}], {1}) {2}]'.\
                       format(', '.join(["('exec', '{}')".format(step_data['exe'])] \
                                          + ["('{0}', _{0})".format(x) for x in reversed(params)]),
                              None if '__i' not in loop_string else "'{}'.format(' '.join(__i))",
                              loop_string)
        key = "DSC_UPDATES_[':'.join((sequence_id, step_name[8:]))]"
        run_string = "DSC_PARAMS_ = {}\nDSC_UPDATES_ = OrderedDict()\n{} = OrderedDict()\n".\
                     format(param_string, key)
        if step_data['depends']:
            run_string += "for x, y in zip(DSC_PARAMS_, %s_output):\n\t %s[' '.join((y, x[1]))]"\
                          " = OrderedDict([('sequence_id', sequence_id), "\
                          "('sequence_name', sequence_name), ('step_name', step_name[8:])] + x[0])\n" % \
                          (step_data['name'], key)
        else:
            run_string += "for x, y in zip(DSC_PARAMS_, %s_output):\n\t %s[y]"\
                          " = OrderedDict([('sequence_id', sequence_id), "\
                          "('sequence_name', sequence_name), ('step_name', step_name[8:])] + x[0])\n" % \
                          (step_data['name'], key)
        run_string += "{0}['DSC_IO_'] = ({1}, {2})\n".\
                      format(key,
                             '[]' if input_vars is None else '{0} if {0} is not None else []'.\
                             format(input_vars),
                             "{}_output".format(step_data['name']))
        run_string += "{0}['DSC_EXT_'] = {1}\n".format(key, step_data['output_ext'])
        run_string += "open(output[0], 'wb').write(msgpack.packb(DSC_UPDATES_))\n"
        res.append(run_string)
        return '\n'.join(res) + '\n'

    def __get_run_step(self, step_data):
        res = ["[{0}_{1} ({2})]".format(step_data['name'], step_data['exe_id'], step_data['exe'])]
        res.append("parameter: sequence_id = None")
        params = sorted(step_data['parameters'].keys()) if 'parameters' in step_data else []
        for key in params:
            res.append('{} = {}'.format(key, repr(step_data['parameters'][key])))
        #
        depend_steps = []
        for_each = 'for_each = %s' % repr(params) if len(params) else ''
        group_by = ''
        input_var = 'input: dynamic(IO_DB[sequence_id][step_name]["input"]), '
        if step_data['depends']:
            # A step can depend on maximum of other 2 steps, by DSC design
            depend_steps = uniq_list([x[0] for x in step_data['depends']])
            group_by = "group_by = {}".format(len(depend_steps))
        else:
            input_var = 'input:'
        res.append('output_files = IO_DB[sequence_id][step_name]["output"]')
        if not (not for_each and input_var == 'input:'):
            res.append('{} {}'.format(input_var, ', '.join([group_by, for_each]).strip().strip(',')))
        res.append('output: output_files[_index]')
        for idx, (plugin, cmd) in enumerate(zip(step_data['plugin'], step_data['command'])):
            res.append("{}{}:".format("task: workdir = {}, concurrent = True\n".\
                                      format(repr(step_data['work_dir'])) if idx == 0 else '',
                                      plugin.name if plugin.name else 'run'))
            # Add action
            if plugin.name:
                if step_data['from_plugin'] is False:
                    script_begin = ''
                else:
                    script_begin = plugin.get_input(params, input_num = len(depend_steps),
                                                    lib = self.libpath, index = idx,
                                                    cmd_args = cmd.split()[1:]
                                                    if len(cmd.split()) > 1 else None)
                if step_data['to_plugin']:
                    script_end = plugin.get_return(step_data['output_vars'])
                else:
                    script_end = ''
                if script_begin:
                    script_begin = '{1}\n{0}\n{2}'.\
                                   format(script_begin.strip(),
                                          '## BEGIN code auto-generated by DSC2',
                                          '## END code auto-generated by DSC2')
                if script_end:
                    script_end = '{1}\n{0}\n{2}'.\
                                   format(script_end.strip(),
                                          '## BEGIN code auto-generated by DSC2',
                                          '## END code auto-generated by DSC2')
                try:
                    cmd_text = [x.rstrip() for x in open(cmd.split()[0], 'r').readlines() if x.strip()]
                except IOError:
                    raise StepError("Cannot find script ``{}``!".format(cmd.split()[0]))
                if plugin.name == 'R':
                    cmd_text = ["suppressMessages({})".format(x) if re.search(r'^(library|require)\((.*?)\)$',x.strip()) else x for x in cmd_text]
                script = """{0}\n{1}\n{2}""".format(script_begin, '\n'.join(cmd_text), script_end)
                res.append(script)
            else:
                executable(cmd.split()[0])
                res.append('\n{}\n'.format(cmd))
        return '\n'.join([x for x in '\n'.join(res).split('\n') if not x.startswith('#')]) + '\n'
