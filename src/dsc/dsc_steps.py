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
from pysos import SoS_Script, Error, check_command, env
from dsc import VERSION
from utils import dotdict, dict2str, try_get_value, get_slice, \
     cartesian_list
from embedded import HEADER

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
      * Prepare environments to run non-R exec: checking / putting together arguments
      * ...

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
                        # data.r_begin.append(text)
                        data.r_list_vars.extend(variables)
                    else:
                        raise StepError('Invalid .alias ``{}`` in block ``{}``.'.format(groups.group(1), name))
            # if data.is_r:
                # data.r_begin.append(self.__format_r_args([x for x in list(params.keys()) if not x in self.r_list_vars]))
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
                # data.r_begin.append('if (grepl(".rds$", "${_input}")) attach(readRDS("${_input}"), warn.conflicts = F)')
                # data.r_end.append('saveRDS(list({}), ${{_output!r}})'.format(', '.join(['{0}={0}'.format(x) for x in data.output_vars])))
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
            # handle output
            process_output(block.out)
            # assign parameters
            data.parameters = params
            # data.r_begin = '\n'.join(data.r_begin)
            # data.r_end = '\n'.join(data.r_end)
            self.raw_data[name].append(dict(data))

    def __get_workflow(self, sequence):
        '''Convert self.raw_data to self.data
           * Fully expand sequences so that each sequence is standalone to initialize an SoS (though possibly partially duplicate)
           * Resolving step dependencies and some syntax conversion, most importantly the $ symbol
        '''
        def process_params():
            # handle parameters with $ and asis
            for k, value in list(params.items()):
                res = []
                for item in value:
                    if isinstance(item, str):
                        if item.startswith('$'):
                            # It must be return var from previous block
                            # and that block must have already been processed
                            # because of the input order
                            if is_r:
                                continue
                            else:
                                item = eval("step_returned_{}".format(item[1:]))
                        elif re.search(r'^Asis\((.*?)\)', item):
                            item = re.search(r'^Asis\((.*?)\)', item).group(1)
                        else:
                            item = repr(item)
                    res.append(item)
                if is_r and len(res) < len(value) and len(res) > 0:
                    # This means that $XX and other variables coexist
                    # For an R program
                    raise ValueError("Cannot use return value from an R program as input parameter in parallel to others!\\nLine: {}".format(', '.join(map(str, value))))
                if len(res) == 0:
                    res = ['NULL']
        #
        res = []
        # is previous step R?
        is_r = None
        for idx, item in enumerate(sequence):
            print(item)
            for step in self.raw_data[item]:
                pass
            res.append(self.raw_data[item])
        return res

    def __initialize_block(self, name):
        data = dotdict()
        data.work_dir = self.default_workdir
        # return_r: If the output is an RDS file: is so, if both `is_r` and return parameter is not found in params list
        # load_r: If the input loads a previously generated RDS file
        # This need to check the sequence and see if all its potential steps are `return_r`
        data.is_r = data.return_r = data.load_r = None
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
        # Input depends: [(step_name, out_var, idx), (step_name, out_var, idx) ...]
        # Then out_var will be assigned in the current step as: out_var = step_name.output[idx]
        data.input_depends = []
        data.exe = ' '.join(exe)

    def __format_r_list(self, name, value, params):
        keys = [x.strip() for x in value.split(',')] if value else list(params.keys())
        res = ['{} <- list()'.format(name)]
        for k in keys:
            res.append('%s$%s <- ${_%s}' % (name, k, k))
        return res, keys

    def __format_r_args(self, params, keys):
        res = []
        for k in keys:
            res.append('%s <- ${_%s}' % (k, k))
        return '\n'.join(res)

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
        env.logger.info('Printing fully extended data to file ``DSCJobsVars.log``')
        with open('DSCJobsVars.log', 'w') as f:
            f.write(text)
        return res.strip()

class DSC2SoS:
    '''
    Initialize SoS workflows with DSC jobs
      * Input is DSC job objects
      * Output is SoS workflow codes

    Here are the ideas from DSC to SoS:
      * Each DSC computational routine `exec` is a step; step name is `block name + routine index`
      * When there are combined routines in a block via `.logic` for `exec` then sub-steps are generated
        with name `block name + combined routine index + routine index` index then create nested workflow
        and eventually the nested workflow name will be `block name + combined routine index`
      * Parameters utilize `for_each` and `paired_with`. Of course will have to distinguish input / output
        from parameters (input will be the ones with $ sigil; output will be the ones in return)
      * Parameters might have to be pre-expanded to some degree given limited SoS `for_each` and `paired_with`
        support vs. potentially complicated DSC `.logic`.
      * Final workflow also use nested workflow structure. The number of final workflow is the same as number of
        DSC sequences. These sequences will be executed one after the other
      * Replicates of the first step (assuming simulation) will be sorted out up-front and they will lead to different
        SoS codes.
    '''
    def __init__(self, data, echo = False):
        self.echo = echo
        steps = []
        step_added = []
        workflows = []
        workflow_id = 0
        for seq, idxes in data.sequences:
            for idx in idxes:
                # A workflow
                workflow_id += 1
                wf_name = 'DSC_{}='.format(workflow_id)
                for x, y in zip(seq, idx):
                    step_name = '{}_{}'.format(x, y + 1)
                    wf_name += '{}+'.format(step_name)
                    if step_name in step_added:
                        continue
                    step_added.append(step_name)
                    steps.append(self.__get_steps(step_name, data.data[x][y]))
                workflows.append('[{}]'.format(wf_name.rstrip('+')))
        steps = sorted(steps,
                       key = lambda x: data.ordering.index(x.split('\n')[0][1:-1].rsplit('_', 1)[0]))
        self.data = '{}\n{}\n{}\n{}'.\
          format('#!/usr/bin/env sos-runner\n#fileformat=SOS1.0\n# Auto-generated by DSC version {}\n'.format(VERSION),
                 HEADER.strip() + '\n', '\n'.join(steps), '\n'.join(workflows)
                 )

    def __call__(self):
        pass

    def __str__(self):
        return self.data

    def __get_steps(self, step_name, step_data):
        res = ['[{}]'.format(step_name)]
        # Set params
        for key, item in step_data['parameters'].items():
            # FIXME: format parameter assignment
            res.append('{} = get_params({}, step_is_r)'.format(key, repr(item)))
        res.append('output_suffix = {}'.format(repr(step_data['output_ext'])))
        # FIXME: input from external file (via "depend")
        params = sorted(step_data['parameters'].keys())
        res.append("input: pattern = '{base}.{ext}', for_each = %s" % repr(params))
        res.append("output: pattern = get_md5('{0}.${{output_suffix}}' if _base is None else '{0}.{{base}}.${{output_suffix}}')".format('::'.join(['exec={}'.format(step_data['exec'])] + ['{0}=${{_{0}}}'.format(x) for x in params])))
        # Add action
        if self.echo:
            # Debug and test
            res.append("""run('''\necho {}\necho Input:\n{}\necho Parameters:\n{}\necho Output:\n{}\n''')""".\
                       format(step_data['command'],
                              'echo ${_input}', # FIXME input
                              '\n'.join(['echo "\t%s: ${_%s}"' % (key, key) for key in step_data['parameters']]),
                              '\n'.join(['echo "\toutput: ${_output!r}"', 'touch ${_output}'])
                              ))
        else:
            if step_data['is_r']:
                try:
                    rscript = """R('''\n{3}\n{0}\n{4}\n{1}\n{3}\n{2}\n{4}\n''')""".\
                      format(step_data['r_begin'].strip(),
                             '\n'.join([x.strip() for x in open(step_data['command'].split()[0], 'r').readlines() if x.strip()]),
                             step_data['r_end'].strip(),
                             '## BEGIN code auto-generated by DSC',
                             '## END code auto-generated by DSC')
                except IOError:
                    raise StepError("Cannot find R script ``{}``!".format(step_data['command'].split()[0]))
                res.append(rscript)
            else:
                check_command(step_data['command'].split()[0])
                res.append("""run('''\n{}\n''')""".format(step_data['command']))
        # Make output available for later steps
        # For an R script the variable will be in the serialized output from previous and will be loaded
        # For command line these names will have to be exposed to global namespace
        if not step_data['is_r']:
            for lhs, rhs in enumerate(step_data['output_vars']):
                res.append("step_returned_{} = _output[{}]".format(lhs, rhs))
        res.append('step_is_r = {}'.format(step_data['is_r']))
        return '\n'.join(res) + '\n'
