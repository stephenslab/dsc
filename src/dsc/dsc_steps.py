#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
This file defines DSCJobs and DSC2SoS classes
to convert DSC configuration to SoS codes
'''
import copy, re, os, glob
from pysos import check_command
from pysos.utils import Error
from pysos.signature import fileMD5
from dsc import VERSION
from .utils import dotdict, dict2str, try_get_value, get_slice, \
     cartesian_list, merge_lists, uniq_list
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
        self.libpath = try_get_value(data.DSC, ('lib_path'))
        self.path = try_get_value(data.DSC, ('exec_path'))
        self.default_workdir = data.DSC['work_dir'][0]
        # sequences in action, logically ordered
        self.ordering = self.merge_sequences(data.DSC['run'])
        for block in self.ordering:
            self.load_raw_data(data[block], name = block)
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
        # if data.plugin.name:
        #     check_command(data.plugin.name)
        data.plugin.reset()
        data.parameters = []
        data.output_ext = []
        data.output_vars = []
        data.exe = ' '.join(exe) if exe_name is None else exe_name

    def load_raw_data(self, block, name = 'block'):
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
                        data.plugin.add_return(lhs, groups.group(2))
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
            self.__reset_block(data, exe, exec_alias[idx] if exec_alias else None, self.path)
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
                raise StepError('Cannot find return value for ``${}`` in any of its previous steps.'.\
                                format(value))
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
                        if isinstance(p1, tuple):
                            p1 = raw_data[item][step_idx]['plugin'].format_tuple(p1)
                        values.append(p1)
                    if len(values) == 0:
                        del raw_data[item][step_idx]['parameters'][k]
                    elif len(values) < len(p):
                        # This means that $XX and other variables coexist
                        # For a plugin script
                        raise ValueError("Cannot use return value from a script " \
                                         "as input parameter in parallel to others!\nLine: ``{}``".\
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
        return res.strip() + '\n{}\n'.format('#' * 20) + text.strip()

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
    def __init__(self, data):
        self.output_prefix = data.output_prefix
        self.libpath = data.libpath
        self.confstr = []
        self.jobstr = []
        self.cleanup()
        for seq_idx, sequence in enumerate(data.data):
            conf_header = 'from dsc.utils import sos_hash_output, sos_pair_input\n'
            conf_header += 'meta_file = ".sos/.dsc/{}.{}.yaml.tmp"\n'.\
                format(seq_idx + 1, os.path.basename(self.output_prefix))
            conf_header += 'file_id = "{}"'.format(seq_idx + 1)
            job_header = 'file_id = "{}"'.format(seq_idx + 1)
            confstr = []
            jobstr = []
            # Get steps
            for blk_idx, block in enumerate(sequence):
                for step_idx, step in enumerate(block):
                    confstr.append(self.__get_prepare_step(step_idx, step, '.sos/.dsc/md5'))
                    jobstr.append(self.__get_run_step(step_idx, step))
            # Get workflows
            seq, indices = data.sequences[seq_idx]
            confstr.append("[DSC_1]\nrun(\"rm -f .sos/.dsc/md5/*\")")
            for idx, index in enumerate(indices):
                item = '+'.join(['{}_{}'.format(x, y + 1)
                                 for x, y in zip(seq, index)])
                confstr.append("[DSC_{0}]\n{1}\nsos_run('{2}')".\
                              format(idx + 2,
                                     'sequence_id = "{}"\nsequence_name = "{}"'.format(idx + 1, item),
                                     item))
                jobstr.append("[DSC_{0}]\n{1}\nsos_run('{2}')".\
                              format(idx + 1, 'sequence_id = "{}"'.format(idx + 1), item))
            self.confstr.append('{}\n{}'.format(conf_header + '\n', '\n'.join(confstr)))
            self.jobstr.append('{}\n{}'.format(job_header + '\n', '\n'.join(jobstr)))

    def cleanup(self):
        for item in glob.glob('.sos/.dsc/*.tmp'):
            os.remove(item)
        if os.path.isfile(".sos/.dsc/{}.yaml".format(os.path.basename(self.output_prefix))):
            os.remove(".sos/.dsc/{}.yaml".format(os.path.basename(self.output_prefix)))

    def __call__(self):
        pass

    def __str__(self):
        return  '\n{}\n'.format('#' * 20).join(self.confstr) + '\n' * 10 + \
          '\n{}\n'.format('#' * 20).join(self.jobstr)

    def __get_prepare_step(self, step_idx, step_data, output_prefix):
        '''
        This will produce source to build config and database for
        parameters and file names
          * X.Y.Z.io.tmp: X = DSC sequence ID, Y = DSC subsequence ID, Z = DSC step name
            (name of computational routine). Contents of this file are "input_names::output_names"
          * X.NAME.yaml.tmp: X = DSC sequence ID, NAME = value of DSC::output entry. Contents are
        '''
        res = ['[{0}_{1}: alias = "{0}", sigil = "%( )"]'.format(step_data['name'], step_idx + 1)]
        # Set params, make sure each time the ordering is the same
        params = sorted(step_data['parameters'].keys()) if 'parameters' in step_data else []
        for key in params:
            res.append('{} = {}'.format(key, repr(step_data['parameters'][key])))
        res.append('output_suffix = {}'.format(repr(step_data['output_ext'])))
        input_vars = ''
        depend_steps = []
        io_ratio = 0
        if step_data['input_depends']:
            # A step can depend on maximum of other 2 steps, by DSC design
            depend_steps = uniq_list([x[0] for x in step_data['input_depends']])
            if len(depend_steps) > 2:
                raise ValueError("DSC block ``{}`` has too many dependencies: ``{}``. " \
                "By DSC design a block can have only depend on one or two other blocks.".\
                format(step_data['name'], repr(depend_steps)))
            elif len(depend_steps) == 2:
                # Generate combinations of input files
                res.append('input_files = sos_pair_input([{}])'.\
                           format(', '.join(['{}.output'.format(x) for x in depend_steps])))
                input_vars = "input_files, group_by = 'pairs', "
                io_ratio = 2
            else:
                input_vars = "{}.output, group_by = 'single', ".format(depend_steps[0])
            res.append("input: %spattern = '{path}/{base}.{ext}'%s" % \
                       (input_vars, (', for_each = %s'% repr(params)) if len(params) else ''))
            res.append("output: sos_hash_output('{0}:%%:%(\"_\".join(_base)).%(output_suffix)', "\
                       "'{1}')".format(':%:'.join(['exec={}'.format(fileMD5(step_data['command'],
                                                                            partial = False))] + \
                                      ['{0}=%(_{0})'.format(x) for x in params]), output_prefix))
        else:
            if params:
                res.append("input: %s" % 'for_each = %s'% repr(params))
            res.append("output: sos_hash_output(expand_pattern('{0}.%(output_suffix)'), '{1}')".\
                       format(':%:'.join(['exec={}'.format(fileMD5(step_data['command'], partial = False))] \
                                         + ['{0}=%(_{0})'.format(x) for x in params]), output_prefix))
        param_string = ["  exec: {}".format(step_data['exe'])]
        param_string += ['  {0}: %(_{0})'.format(x) for x in params]
        # meta data file
        res.append("run:\ns='%(_output)'\ns=${{s##*/}}\necho %(sequence_id) "\
                   "${{s%.*}}{}: | sed 's/ /_/g' >> %(meta_file)\necho -e '"\
                   "  sequence_id: %(sequence_id)\\n  sequence_name: %(sequence_name)\\n" \
                   "  step_name: %(step_name)\\n{}' >> %(meta_file)\ntouch %(_output)".\
                   format(' %(_base)' if step_data['input_depends'] else '', '\\n'.join(param_string)))
        res.append("if [ ! -f .sos/.dsc/%(file_id).%(sequence_id).%(step_name).io.tmp ]; "\
                   "then echo %(input!,)::%(output!,)::{}"\
                   " > .sos/.dsc/%(file_id).%(sequence_id).%(step_name).io.tmp; fi".format(io_ratio))
        return '\n'.join(res) + '\n'

    def __get_run_step(self, step_idx, step_data):
        res = ["[{0}_{1}]".format(step_data['name'], step_idx + 1)]
        params = sorted(step_data['parameters'].keys()) if 'parameters' in step_data else []
        for key in params:
            res.append('{} = {}'.format(key, repr(step_data['parameters'][key])))
        #
        depend_steps = []
        for_each = 'for_each = %s' % repr(params) if len(params) else ''
        group_by = ''
        input_var = 'input: CONFIG[file_id][sequence_id][step_name]["input"], dynamic = True, '
        if step_data['input_depends']:
            # A step can depend on maximum of other 2 steps, by DSC design
            depend_steps = uniq_list([x[0] for x in step_data['input_depends']])
            if len(depend_steps) > 2:
                raise ValueError("DSC block ``{}`` has too many dependencies: ``{}``. " \
                "By DSC design a block can have only depend on one or two other blocks.".\
                format(step_data['name'], repr(depend_steps)))
            elif len(depend_steps) == 2:
                group_by = "group_by = 'pairs'"
            else:
                group_by = "group_by = 'single'"
        else:
            input_var = 'input:'
        res.append('output_files = CONFIG[file_id][sequence_id][step_name]["output"]')
        if not (not for_each and input_var == 'input:'):
            res.append('{} {}'.format(input_var, ','.join([group_by, for_each]).strip(',')))
        res.append('output: output_files[_index]')
        res.append("{}: workdir = {}, concurrent = True".\
                   format(step_data['plugin'].name if step_data['plugin'].name else 'run',
                          repr(step_data['work_dir'])))
        # Add action
        if step_data['plugin'].name:
            script_begin = step_data['plugin'].get_input(params, input_num = len(depend_steps),
                                                         lib = self.libpath)
            script_end = step_data['plugin'].get_return(step_data['output_vars'])
            try:
                script = """\n{3}\n{0}\n{4}\n{1}\n{3}\n{2}\n{4}\n""".\
                  format(script_begin,
                        '\n'.join([x.strip() for x in open(step_data['command'].split()[0], 'r').\
                                   readlines() if x.strip()]),
                        script_end, '## BEGIN code auto-generated by DSC {}'.format(VERSION),
                        '## END code auto-generated by DSC {}'.format(VERSION))
            except IOError:
                raise StepError("Cannot find script ``{}``!".format(step_data['command'].split()[0]))
            res.append(script)
        else:
            check_command(step_data['command'].split()[0])
            res.append("""run('''\n{}\n''')""".format(step_data['command']))
        return '\n'.join(res) + '\n'
