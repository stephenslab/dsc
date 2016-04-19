#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
This file defines DSCJobs and DSC2SoS classes
to convert DSC configuration to SoS codes
'''
import copy, re, os, hashlib
from pysos import check_command
from pysos.utils import Error, env
from dsc import VERSION
from .utils import dotdict, dict2str, try_get_value, get_slice, \
     cartesian_list, merge_lists, install_r_libs, uniq_list
from .plugin import Plugin

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
        raw_data: dict
        data: list
        output_prefix: str
          output directory
        default_workdir: str
        ordering: list
        sequences: list
        '''
        self.data = []
        self.raw_data = {}
        self.output_prefix = data.DSC['output'][0]
        self.default_workdir = data.DSC['work_dir'][0]
        # sequences in action, logically ordered
        self.ordering = self.merge_sequences(data.DSC['run'])
        for block in self.ordering:
            self.load_raw_data(data[block], data.DSC, name = block)
        # sequences in action, unordered but expanded by index
        self.sequences = self.expand_sequences(data.DSC['run'],
                                                {x : range(len(self.raw_data[x])) for x in self.raw_data})
        for seq, idx in self.sequences:
            self.data.append(self.get_workflow(seq))

    def __initialize_block(self, name):
        '''Intermediate data to be appended to self.data'''
        data = dotdict()
        data.work_dir = self.default_workdir
        data.output_db = self.output_prefix
        # to_plugin: If the output is an RDS file: is so, if both `plugin` in ('R', 'Python')
        # and return parameter is not found in params list
        data.plugin = Plugin()
        data.to_plugin = None
        # Input depends: [(step_name, out_var, idx), (step_name, out_var, idx) ...]
        # Then out_var will be assigned in the current step as: out_var = step_name.output[idx]
        data.input_depends = []
        # For every input_depends if it is from some plugin,
        # via checking the corresponding to_plugin property.
        data.input_from_plugin = []
        data.name = name
        return data

    def __reset_block(self, data, exe, exe_name, exec_path):
        '''Intermediate data to be appended to self.data'''
        data.command = ' '.join([self.__search_exec(exe[0], exec_path)] + \
                               [x if not x.startswith('$') else '${_%}' % x[1:] for x in exe[1:]])
        # Decide if this step is a plugin
        # FIXME: will have to eventually decide this by checking whether or not return
        # value is in parameter list, not by extension
        # one need to use "Rscript file_name.R" not "file_name.R"
        # same for "python file_name.py"
        plugin = Plugin(os.path.splitext(exe[0])[1].lstrip('.'))
        if (data.plugin.name is not None and (not plugin.name) != (not data.plugin.name)):
            raise StepError("A mixture of plugin codes and other executables are not allowed " \
            "in the same block ``{}``.".format(data.name))
        else:
            data.plugin = plugin
        data.plugin.reset()
        data.parameters = []
        data.output_ext = []
        data.output_vars = []
        data.exe = ' '.join(exe) if exe_name is None else exe_name

    def load_raw_data(self, block, dsc_block, name = 'block'):
        '''Load block data to self.raw_data with some preliminary processing
        '''
        def load_params():
            params = {}
            if 'params' in block:
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
                rules = block.rules[0]
                if (idx + 1) in list(block.rules.keys()):
                    rules = block.rules[idx + 1]
            return rules

        def load_alias():
            alias = {}
            if 'params_alias' in block:
                alias = dict([(x.strip() for x in item.split('=')) for item in block.params_alias[0]])
                if (idx + 1) in list(block.params_alias.keys()):
                    alias = dict([(x.strip() for x in item.split('=')) for item in block.params_alias[0]])
            return alias

        def process_alias():
            for k, item in list(alias.items()):
                groups = re.search(r'(.*?)\((.*?)\)', item)
                if not groups:
                    # swap key
                    params[k] = params.pop(item)
                else:
                    if groups.group(1) == 'RList':
                        if not data.plugin.name == 'R':
                            raise StepError("Alias ``RList`` is not applicable to executable ``{}``.".\
                                            format(exe[0]))
                        data.plugin.set_container(k, groups.group(2), params)
                    else:
                        raise StepError('Invalid .alias ``{}`` in block ``{}``.'.\
                                        format(groups.group(1), name))

        def process_rules():
            # parameter value slicing
            pass

        def process_output(out):
            for item in out:
                lhs = ''
                if '=' in item:
                    # return alias exists
                    lhs, rhs = (x.strip() for x in item.split('='))
                    groups = re.search(r'^(R|Python)\((.*?)\)$', rhs)
                    if not groups:
                        # alias is not for value inside other return objects
                        # which implies that the alias comes from parameter list
                        # FIXME: Just copy it over here. I should instead do it
                        # at the saveRDS step to avoid copying
                        try:
                            params[lhs] = params[rhs]
                        except KeyError:
                            raise StepError("Parameter ``{}`` not found for block ``{}``.".\
                                            format(rhs, name))
                    else:
                        # alias is within plugin
                        data.plugin.add_return(lhs, groups.group(1))
                else:
                    lhs = item.strip()
                data.output_vars.append(lhs)
                if not data.plugin.name:
                    # output file pattern
                    # FIXME: have to extract extension from File()
                    # E.g. get_ext(lhs)
                    data.output_ext.append(lhs)
            if data.plugin.name:
                data.output_ext = 'rds'
        # Load command parameters
        self.raw_data[name] = []
        data = self.__initialize_block(name)
        exec_alias = try_get_value(block.meta, ('exec_alias'))
        for idx, exe in enumerate(block.meta['exec']):
            self.__reset_block(data, exe, exec_alias[idx] if exec_alias else None,
                               try_get_value(dsc_block, ('exec_path')))
            # temporary variables
            params, rules, alias = load_params(), load_rules(), load_alias()
            # handle alias
            process_alias()
            # FIXME: handle rules
            process_rules()
            # FIXME: File() not handled yet
            # to_plugin: if all return objects are not File
            # FIXME: let it be whether or not the block uses plugin for now
            # But in practice if return parameter matches a File() then
            # to_plugin will be False no matter what data.plugin is
            data.to_plugin = True if data.plugin.name else False
            # handle output
            process_output(block.out)
            # assign parameters
            data.parameters = params
            self.raw_data[name].append(dict(data))

    def get_workflow(self, sequence):
        '''Convert self.raw_data to self.data
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
                try:
                    dependence = (res[curr_idx][0]['name'], value,
                                  res[curr_idx][0]['output_vars'].index(value))
                    to_plugin = res[curr_idx][0]['to_plugin']
                except ValueError:
                    pass
                if dependence is not None:
                    break
                else:
                    curr_idx = curr_idx - 1
            if dependence is None:
                raise StepError('Cannot find return value for ``${}`` in any of its previous steps.'.format(value))
            if dependence not in raw_data[item][step_idx]['input_depends']:
                raw_data[item][step_idx]['input_depends'].append(dependence)
                raw_data[item][step_idx]['input_from_plugin'].append(to_plugin)
        #
        res = []
        raw_data = copy.deepcopy(self.raw_data)
        for idx, item in enumerate(sequence):
            # for each step
            for step_idx, step in enumerate(raw_data[item]):
                # for each exec
                for k, p in list(step['parameters'].items()):
                    values = []
                    for p1 in p:
                        if isinstance(p1, str):
                            if p1.startswith('$'):
                                find_dependencies(p1[1:])
                                if p1[1:] != k:
                                    raw_data[item][step_idx]['plugin'].add_input(k, p1[1:])
                                continue
                            elif re.search(r'^Asis\((.*?)\)$', p1):
                                p1 = re.search(r'^Asis\((.*?)\)$', p1).group(1)
                            else:
                                p1 = repr(p1)
                        if isinstance(p1, tuple) and raw_data[item][step_idx]['plugin'].name == 'R':
                            p1 = 'c({})'.\
                              format(', '.join([repr(x) if isinstance(x, str) else str(x) for x in p1]))
                        values.append(p1)
                    if len(values) == 0:
                        del raw_data[item][step_idx]['parameters'][k]
                    elif len(values) < len(p):
                        # This means that $XX and other variables coexist
                        # For an R program
                        raise ValueError("Cannot use return value from an R program as input parameter in parallel to others!\nLine: ``{}``".\
                                         format(', '.join(map(str, p))))
                    else:
                        raw_data[item][step_idx]['parameters'][k] = values
                if len(raw_data[item][step_idx]['parameters']) == 0:
                    del raw_data[item][step_idx]['parameters']
                # sort input depends
                raw_data[item][step_idx]['input_depends'].sort(key = lambda x: self.ordering.index(x[0]))
            res.append(raw_data[item])
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
        res = ''
        for item in self.ordering:
            res += dict2str({item: self.raw_data[item]}, replace = [('!!python/tuple', '(tuple)')]) + '\n'
        text = ''
        for sequence in self.data:
            for block in sequence:
                for item in block:
                    text += dict2str(item, replace = [('!!python/tuple', '(tuple)')]) + '\n'
        return res.strip() + '\n#######\n' + text.strip()

class DSC2SoS:
    '''
    Initialize SoS workflows with DSC jobs
      * Input is DSC job objects
      * Output is SoS workflow codes

    Here are the ideas from DSC to SoS:
      * Each DSC computational routine `exec` is a step; step name is `block name + routine index`; step alias is block name
      * When there are combined routines in a block via `.logic` for `exec` then sub-steps are generated
        with name `block name + combined routine index + routine index` without alias name then create nested workflow
        and eventually the nested workflow name will be `block name + combined routine index` with alias being block name
      * Parameters utilize `for_each` (and `paired_with`??).
      * Parameters are pre-expanded such that SoS `for_each` and `paired_with` are
        support for otherwise complicated DSC `.logic`.
      * Final workflow also use nested workflow structure, with workflow name "DSC" for each sequence, indexed by
        the possible ways to combine exec routines. The possible ways are pre-determined and passed here.
      * Each DSC sequence is a separate SoS code piece. These pieces have to be executed sequentially
        to reuse some runtime signature although -j N is allowed in each script
      * Replicates of the first step (assuming simulation) will be sorted out up-front and they will lead to different
        SoS code pieces. (Currently not implemented!)
    '''
    def __init__(self, data, echo = False):
        self.echo = echo
        self.data = []
        self.output_prefix = data.output_prefix
        header = 'from pysos import expand_pattern\nfrom dsc.utils import registered_output, sos_paired_input'
        for seq_idx, sequence in enumerate(data.data):
            script = []
            # Get steps
            for blk_idx, block in enumerate(sequence):
                for step_idx, step in enumerate(block):
                    script.append(self.__get_step(step_idx, step))
            # Get workflows
            seq, indices = data.sequences[seq_idx]
            for idx, index in enumerate(indices):
                script.append("[DSC_{0}]\nsos_run('{1}')".\
                              format(idx + 1, '+'.join(['{}_{}'.format(x, y + 1) for x, y in zip(seq, index)])))
            self.data.append('{}\n{}\n{}'.format('#!/usr/bin/env sos-runner\n#fileformat=SOS1.0\n# Auto-generated by DSC version {}\n'.format(VERSION), header + '\n', '\n'.join(script)))

    def __call__(self):
        pass

    def __str__(self):
        return '\n#######\n'.join(self.data)

    def __get_step(self, step_idx, step_data):
        res = ["[{0}_{1}: alias = '{0}']".format(step_data['name'], step_idx + 1)]
        # Set params, make sure each time the ordering is the same
        params = sorted(step_data['parameters'].keys()) if 'parameters' in step_data else []
        for key in params:
            res.append('{} = {}'.format(key, repr(step_data['parameters'][key])))
        res.append('output_suffix = {}'.format(repr(step_data['output_ext'])))
        input_vars = ''
        depend_steps = []
        if step_data['input_depends']:
            # A step can depend on maximum of other 2 steps, by DSC design
            depend_steps = uniq_list([x[0] for x in step_data['input_depends']])
            if len(depend_steps) > 2:
                raise ValueError("DSC block ``{}`` has too many dependencies: ``{}``. " \
                "By DSC design a block can have only depend on one or two other blocks.".\
                format(step_data['name'], repr(depend_steps)))
            elif len(depend_steps) == 2:
                # Generate combinations of input files
                res.append('input_files = sos_paired_input([{}])'.\
                           format(', '.join(['{}.output'.format(x) for x in depend_steps])))
                input_vars = "input_files, group_by = 'pairs', "
            else:
                input_vars = "{}.output, group_by = 'single', ".format(depend_steps[0])
            res.append("input: %spattern = '{path}/{base}.{ext}'%s" % \
                       (input_vars, (', for_each = %s'% repr(params)) if len(params) else ''))
            res.append("output: registered_output('{0}:%%:${{\"_\".join(_base)}}.${{output_suffix}}', "\
                       "'{1}')".format(':%:'.join(['exec={}'.format(step_data['exe'])] + \
                                      ['{0}=${{_{0}}}'.format(x) for x in params]), self.output_prefix))
        else:
            if params:
                res.append("input: %s" % 'for_each = %s'% repr(params))
            res.append("output: registered_output(expand_pattern('{0}.${{output_suffix}}'), '{1}')".\
                       format(':%:'.join(['exec={}'.format(step_data['exe'])] + \
                                         ['{0}=${{_{0}}}'.format(x) for x in params]), self.output_prefix))
        res.append("{}: workdir = {}, concurrent = {}".\
                   format(step_data['plugin'].name if (not self.echo and step_data['plugin'].name)
                          else 'run', repr(step_data['work_dir']), 'False' if self.echo else 'True'))
        # Add action
        if self.echo:
            # Debug and test
            res.append("""\necho {}\necho Input:\n{}\necho Parameters:\n{}\necho Output:\n{}\n""".\
                       format(step_data['command'],
                              'echo ${_input}', # FIXME input
                              '\n'.join(['echo "\t%s: ${_%s}"' % (key, key) for key in params]),
                              '\n'.join(['echo "\toutput: ${_output!r}"', 'touch ${_output}'])
                              ))
        else:
            if step_data['plugin'].name:
                script_begin = step_data['plugin'].get_input(params, input_num = len(depend_steps))
                script_end = step_data['plugin'].get_return(step_data['output_vars'])
                try:
                    script = """\n{3}\n{0}\n{4}\n{1}\n{3}\n{2}\n{4}\n""".\
                      format(script_begin,
                            '\n'.join([x.strip() for x in open(step_data['command'].split()[0], 'r').\
                                       readlines() if x.strip()]),
                            script_end, '## BEGIN code auto-generated by DSC',
                            '## END code auto-generated by DSC')
                except IOError:
                    raise StepError("Cannot find script ``{}``!".format(step_data['command'].split()[0]))
                res.append(script)
            else:
                check_command(step_data['command'].split()[0])
                res.append("""run('''\n{}\n''')""".format(step_data['command']))
        return '\n'.join(res) + '\n'
