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
from dsc.pysos import SoS_Script, check_command
from dsc.pysos.utils import Error, env
from dsc import VERSION
from utils import dotdict, dict2str, try_get_value, get_slice, \
     cartesian_list

class StepError(Error):
    """Raised when Step parameters are illegal."""
    def __init__(self, msg):
        Error.__init__(self, msg)
        self.args = (msg, )

class DSCJobs(dotdict):
    '''
    Convert DSC data to steps compatible with SoS format.
      * Input is DSCData object

    This includes:
      * Ensure step ordering for DSC::run are legitimate
      * Prepare environments to run R: libraries, alias, return alias
      * Prepare additional codes for R exec
      * Prepare environments to run non-R exec: checking / putting together arguments
      * Figure out step dependencies; figure out whether a step is from R
      * Slicing by rules
      * Handle replicates if any

    The output of this will be a DSCJobs object ready to convert to SoS steps
    '''
    def __init__(self, data):
        self.output_prefix = data.DSC['output'][0]
        self.default_workdir = data.DSC['work_dir'][0]
        # sequences in action, logically ordered
        self.ordering = self.__merge_sequences(data.DSC['run'])
        self.raw_data = {}
        for block in self.ordering:
            self.__load(data[block], data.DSC, name = block)
        # sequences in action, unordered but expanded by index
        self.sequences = self.__expand_sequences(self.__index_sequences(data.DSC['run']),
                                                {x : range(len(self.raw_data[x])) for x in self.raw_data})
        self.data = []
        for seq, idx in self.sequences:
            self.data.append(self.__get_workflow(seq))

    def __load(self, block, dsc_block, name = 'block'):
        '''Load block data to self.raw_data with some preliminary processing
        '''
        def load_params():
            params = {}
            if 'params' in block:
                params = copy.deepcopy(block.params[0])
                if (idx + 1) in list(block.params.keys()):
                   params.update(block.params[idx + 1])
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
                        if not data.is_r:
                            raise StepError("Alias ``RList`` is not applicable to executable ``{}``.".format(data.exe))
                        text, variables = self.__format_r_list(k, groups.group(2), params)
                        data.r_list.extend(text)
                        data.r_list_vars.extend(variables)
                    else:
                        raise StepError('Invalid .alias ``{}`` in block ``{}``.'.format(groups.group(1), name))
            # if data.is_r:
                # problem: what if the list has $? so I cannot do it here.

        def process_rules():
            # parameter value slicing
            pass

        def process_output(out):
            for item in out:
                lhs = ''
                if '=' in item:
                    # return alias exists
                    lhs, rhs = (x.strip() for x in item.split('='))
                    groups = re.search(r'^R\((.*?)\)', rhs)
                    if not groups:
                        # alias is not within R
                        # have to make it available from parameter list
                        # FIXME: not sure if this is the best thing to do
                        try:
                            params[lhs] = params[rhs]
                        except KeyError:
                            raise StepError("Parameter ``{}`` not found for block ``{}``.".format(rhs, name))
                    else:
                        # alias is within R
                        data.r_return_alias.append('{} <- {}'.format(lhs, groups.group(1)))
                else:
                    lhs = item.strip()
                data.output_vars.append(lhs)
                if not data.is_r:
                    # output file pattern
                    # FIXME: have to extract from File()
                    data.output_ext.append(lhs)
            if data.is_r:
                data.output_ext = 'rds'
        # FIXME: replicates not yet handled
        # Load command parameters
        self.raw_data[name] = []
        data = self.__initialize_block(name)
        for idx, exe in enumerate(block.meta['exec']):
            self.__reset_block(data, exe, try_get_value(dsc_block, ('exec_path')))
            # temporary variables
            params, rules, alias = load_params(), load_rules(), load_alias()
            # handle alias
            process_alias()
            # FIXME: handle rules
            process_rules()
            # FIXME: File(), handle it directly into SoS syntax
            # return_r: if all return objects are not File
            # FIXME: let it be is_r for now
            data.return_r = data.is_r
            # handle output
            process_output(block.out)
            # assign parameters
            data.parameters = params
            self.raw_data[name].append(dict(data))

    def __get_workflow(self, sequence):
        '''Convert self.raw_data to self.data
           * Fully expand sequences so that each sequence is standalone to initialize an SoS (though possibly partially duplicate)
           * Resolving step dependencies and some syntax conversion, most importantly the $ symbol
        '''
        def find_dependencies(value):
            curr_idx = idx
            if curr_idx == 0:
                raise StepError('Symbol ``$`` is not allowed in the first step of DSC sequence.')
            curr_idx = curr_idx - 1
            dependence = None
            return_r = None
            while curr_idx >= 0:
                try:
                    dependence = (res[curr_idx][0]['name'], value, res[curr_idx][0]['output_vars'].index(value))
                    return_r = res[curr_idx][0]['return_r']
                except ValueError:
                    pass
                curr_idx = curr_idx - 1
            if dependence is None:
                raise StepError('Cannot find return value for ``${}`` in any of its previous steps.'.format(value))
            if dependence not in raw_data[item][step_idx]['input_depends']:
                raw_data[item][step_idx]['input_depends'].append(dependence)
                raw_data[item][step_idx]['input_is_r'].append(return_r)
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
                                continue
                            elif re.search(r'^Asis\((.*?)\)$', p1):
                                p1 = re.search(r'^Asis\((.*?)\)$', p1).group(1)
                            else:
                                p1 = repr(p1)
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
            res.append(raw_data[item])
        return res

    def __initialize_block(self, name):
        data = dotdict()
        data.work_dir = self.default_workdir
        data.output_db = self.output_prefix
        # return_r: If the output is an RDS file: is so, if both `is_r` and return parameter is not found in params list
        data.is_r = data.return_r = None
        # Input depends: [(step_name, out_var, idx), (step_name, out_var, idx) ...]
        # Then out_var will be assigned in the current step as: out_var = step_name.output[idx]
        data.input_depends = []
        # For every input_depends if it is from R, via checking the corresponding return_r property.
        data.input_is_r = []
        data.name = name
        return data

    def __reset_block(self, data, exe, exec_path):
        data.command = ' '.join([self.__search_exec(exe[0], exec_path)] + \
                               [x if not x.startswith('$') else '${_%}' % x[1:] for x in exe[1:]])
        # Is the executable an R program
        # If the R program can be executed on its own
        # one need to use "Rscript file_name.R" not "file_name.R"
        is_r = exe[0].lower().endswith('.r')
        if data.is_r is not None and is_r != data.is_r:
            raise StepError("A mixture of R codes and other executables are not allowed in the same block ``{}``.".format(data.name))
        else:
            data.is_r = is_r
        data.r_list = []
        data.r_return_alias = []
        data.r_list_vars = []
        data.parameters = []
        data.output_ext = []
        data.output_vars = []
        data.exe = ' '.join(exe)

    def __format_r_list(self, name, value, params):
        keys = [x.strip() for x in value.split(',')] if value else list(params.keys())
        res = ['{} <- list()'.format(name)]
        for k in keys:
            res.append('%s$%s <- ${_%s}' % (name, k, k))
        return res, keys

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

    def __merge_sequences(self, input_sequences):
        '''Extract the proper ordering of elements from multiple sequences'''
        # remove slicing
        sequences = [[y.split('[')[0] for y in x] for x in input_sequences]
        values = list(set(sum(sequences, [])))
        for seq in sequences:
            values.sort(key = lambda x: seq.index(x))
        return values

    def __index_sequences(self, input_sequences):
        '''Strip slicing symbol out of sequences and add them as index'''
        res = []
        for seq in input_sequences:
            res.append(tuple([get_slice(x, mismatch_quit = False) for x in seq]))
        return res

    def __expand_sequences(self, sequences, default = {}):
        '''expand DSC sequences by index'''
        res = []
        for value in sequences:
            seq = [x[0] for x in value]
            idxes = [x[1] if x[1] is not None else default[x[0]] for x in value]
            res.append((seq, cartesian_list(*idxes)))
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
        env.logger.info('Printing fully extended data to file ``{}``'.format(self.output_prefix + '.log'))
        with open(self.output_prefix + '.log', 'w') as f:
            f.write(text)
        return res.strip()

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
        header = 'from dsc.pysos import expand_pattern\nfrom dsc.utils import get_md5_sos, get_input_sos'
        for seq_idx, sequence in enumerate(data.data):
            script = []
            # Get steps
            for blk_idx, block in enumerate(sequence):
                for step_idx, step in enumerate(block):
                    script.append(self.__get_step(step_idx, step))
            # Get workflows
            seq, indices = data.sequences[seq_idx]
            for idx, index in enumerate(indices):
                script.append('[DSC_{0}={1}]'.\
                              format(idx + 1, '+'.join(['{}_{}'.format(x, y + 1) for x, y in zip(seq, index)])))
        self.data.append('{}\n{}\n{}'.\
                         format('#!/usr/bin/env sos-runner\n#fileformat=SOS1.0\n# Auto-generated by DSC version {}\n'.format(VERSION),
                                header + '\n', '\n'.join(script)
                                )
                        )

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
        if step_data['input_depends']:
            if len(step_data['input_depends']) > 1:
                # Generate combinations of input files
                res.append('input_files = get_input_sos(({}))'.format(', '.join(['{}.output'.format(x[0]) for x in step_data['input_depends']])))
                input_vars = "input_files, group_by = 'pairs', "
            else:
                input_vars = "{}.output, group_by = 'single', ".format(step_data['input_depends'][0][0])
            res.append("input: %spattern = '{base}.{ext}'%s" % (input_vars, (', for_each = %s'% repr(params)) if len(params) else ''))
            res.append("output: get_md5_sos('{0}.${{\"_\".join(_base)}}.${{output_suffix}}')".format('::'.join(['exec={}'.format(step_data['exe'])] + ['{0}=${{_{0}}}'.format(x) for x in params])))
        else:
            if params:
                res.append("input: %s" % 'for_each = %s'% repr(params))
            res.append("output: get_md5_sos(expand_pattern('{0}.${{output_suffix}}'))".format('::'.join(['exec={}'.format(step_data['exe'])] + ['{0}=${{_{0}}}'.format(x) for x in params])))
        res.append("process: workdir = {}".format(repr(step_data['work_dir'])))
        # Add action
        if self.echo:
            # Debug and test
            res.append("""run('''\necho {}\necho Input:\n{}\necho Parameters:\n{}\necho Output:\n{}\n''')""".\
                       format(step_data['command'],
                              'echo ${_input}', # FIXME input
                              '\n'.join(['echo "\t%s: ${_%s}"' % (key, key) for key in params]),
                              '\n'.join(['echo "\toutput: ${_output!r}"', 'touch ${_output}'])
                              ))
        else:
            if step_data['is_r']:
                r_begin = step_data['r_list']
                r_begin.extend(self.__format_r_args([x for x in params if not x in step_data['r_list_vars']]))
                if step_data['input_depends']:
                    if len(step_data['input_depends']) > 1:
                        r_begin.append('input.files <- c(${_input!r,})\nfor (i in 1:length(input.files)) attach(readRDS(input.files[i]), warn.conflicts = F)')
                    else:
                        r_begin.append('attach(readRDS("${_input}"), warn.conflicts = F)')
                r_end = step_data['r_return_alias']
                if step_data['return_r']:
                    r_end.append('saveRDS(list({}), ${{_output!r}})'.format(', '.join(['{0}={0}'.format(x) for x in step_data['output_vars']])))
                try:
                    rscript = """R('''\n{3}\n{0}\n{4}\n{1}\n{3}\n{2}\n{4}\n''')""".\
                      format('\n'.join(r_begin).strip(),
                             '\n'.join([x.strip() for x in open(step_data['command'].split()[0], 'r').readlines() if x.strip()]),
                             '\n'.join(r_end).strip(),
                             '## BEGIN code auto-generated by DSC',
                             '## END code auto-generated by DSC')
                except IOError:
                    raise StepError("Cannot find R script ``{}``!".format(step_data['command'].split()[0]))
                res.append(rscript)
            else:
                check_command(step_data['command'].split()[0])
                res.append("""run('''\n{}\n''')""".format(step_data['command']))
        return '\n'.join(res) + '\n'

    def __format_r_args(self, keys):
        res = []
        for k in keys:
            res.append('%s <- ${_%s}' % (k, k))
        return res
