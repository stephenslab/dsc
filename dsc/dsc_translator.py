#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
This file defines methods to translate DSC into pipeline in SoS language
'''

class DSC_Translator:
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
    '''
    def __init__(self, data, dsc_file, runtime, rerun = False, n_cpu = 4):
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
                    step['exe'] = re.sub('\s+', '_', step['exe'])
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
                conf_str.append("[INIT_{0}]\ninput: None\nsos_run('{2}', {1})".\
                              format(i, "sequence_id = '{}', sequence_name = '{}'".\
                                     format(i, '+'.join(rsqn)),
                                     '+'.join(['prepare_{}'.format(x) for x in sqn]),
                                     repr(provides_files)))
                io_info_files.extend(provides_files)
                job_str.append("[DSC_{0} ({3})]\ninput: None\nsos_run('{2}', {1})".\
                              format(i, "sequence_id = '{}'".format(i), '+'.join(sqn), "DSC sequence {}".format(i)))
                i += 1
        self.conf_str = conf_header + '\n'.join(conf_str)
        self.job_str = job_header + '\n'.join(job_str)
        self.conf_str += '''
[BUILD_0]
parameter: vanilla = {1}
input: {4}
output: '.sos/.dsc/{0}.io.mpk', '.sos/.dsc/{0}.map.mpk', '.sos/.dsc/{0}.conf.mpk'
build_config_db(input, output[0], output[1], output[2], vanilla = vanilla, jobs = {5})
[INIT_0]
remove_obsolete_db('{3}')
        '''.format(os.path.basename(data.output_prefix), rerun, i,
                   data.output_prefix, repr(sorted(set(io_info_files))), n_cpu)
        with open('.sos/.dsc/utils.R', 'w') as f:
            f.write(R_SOURCE + R_LMERGE)
        #
        self.install_libs(runtime.rlib, "R_library", rerun)
        self.install_libs(runtime.pymodule, "Python_Module", rerun)


    def __call__(self):
        pass

    def __str__(self):
        return '{}'.format(dict2str(self.step_map)) + '\n\n' + '##\n' * 5 \
            + '\n{}'.format(self.conf_str) + '\n\n' + '##\n' * 5 + '\n{}'.format(self.job_str)

    def install_libs(self, libs, lib_type, force = False):
        if lib_type not in ["R_library", "Python_Module"]:
            raise ValueError("Invalid library type ``{}``.".format(lib_type))
        if libs is None:
            return
        libs_md5 = textMD5(repr(libs) + str(datetime.date.today()))
        if not os.path.exists('.sos/.dsc/{}.{}.info'.format(lib_type, libs_md5)) and not force:
            if lib_type == 'R_library':
                ret = install_r_libs(libs)
            if lib_type == 'Python_Module':
                ret = install_py_modules(libs)
            # FIXME: need to check if installation is successful
            os.makedirs('.sos/.dsc', exist_ok = True)
            os.system('echo "{}" > {}'.format(repr(libs), '.sos/.dsc/{}.{}.info'.format(lib_type, libs_md5)))


    def __get_prepare_step(self, step_data):
        '''
        This will produce source to build config and database for
        parameters and file names. The result is one binary json file (via msgpack)
        with keys "X:Y:Z" where X = DSC sequence ID, Y = DSC subsequence ID, Z = DSC step name
            (name of indexed DSC block corresponding to a computational routine).
        '''
        res = ["[prepare_{0}_{1}: shared = '{0}_output']".format(step_data['name'], step_data['exe_id'])]
        res.extend(["parameter: sequence_id = None", "parameter: sequence_name = None"])
        res.append("input: None\noutput: {}".format(self.confdb))
        # Set params, make sure each time the ordering is the same
        params = sorted(step_data['parameters'].keys()) if 'parameters' in step_data else []
        for key in params:
            res.append('{} = {}'.format(key, repr(step_data['parameters'][key])))
        input_vars = None
        depend_steps = []
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
                         format(' '.join([step_data['exe'], step_data['name']] \
                                           + ['{0}:{{}}'.format(x) for x in reversed(params)]),
                                format_string, loop_string, step_data['exe'], "':'.join(__i)")
        else:
            out_string = "[sos_hash_output('{0}'{1}, prefix = '{3}') {2}]".\
                         format(' '.join([step_data['exe'], step_data['name']] \
                                           + ['{0}:{{}}'.format(x) for x in reversed(params)]),
                                format_string, loop_string, step_data['exe'])
        res.append("{}_output = {}".format(step_data['name'], out_string))
        param_string = '[([{0}], {1}) {2}]'.\
                       format(', '.join(["('exec', '{}')".format(step_data['exe'])] \
                                          + ["('{0}', _{0})".format(x) for x in reversed(params)]),
                              None if '__i' not in loop_string else "'{}'.format(' '.join(__i))",
                              loop_string)
        key = "DSC_UPDATES_[':'.join((sequence_id, step_name[8:]))]"
        run_string = "DSC_UPDATES_ = OrderedDict()\n{} = OrderedDict()\n".format(key)
        if step_data['depends']:
            run_string += "for x, y in zip(%s, %s_output):\n\t %s[' '.join((y, x[1]))]"\
                          " = OrderedDict([('sequence_id', sequence_id), "\
                          "('sequence_name', sequence_name), ('step_name', step_name[8:])] + x[0])\n" % \
                          (param_string, step_data['name'], key)
        else:
            run_string += "for x, y in zip(%s, %s_output):\n\t %s[y]"\
                          " = OrderedDict([('sequence_id', sequence_id), "\
                          "('sequence_name', sequence_name), ('step_name', step_name[8:])] + x[0])\n" % \
                          (param_string, step_data['name'], key)
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
        cmds_md5 = ''.join([(fileMD5(item.split()[0], partial = False) if os.path.isfile(item.split()[0])
                             else item.split()[0]) + \
                            (item.split()[1] if len(item.split()) > 1 else '')
                            for item in step_data['command']])
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
                                                    cmd_args = cmd.split()[1:] if len(cmd.split()) > 1 else None)
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
                script = """DSC_STEP_ID__ = '{3}'\n{0}\n{1}\n{2}""".\
                         format(script_begin, '\n'.join(cmd_text), script_end, cmds_md5)
                res.append(script)
            else:
                executable(cmd.split()[0])
                res.append('\n{}\n'.format(cmd))
        return '\n'.join([x for x in '\n'.join(res).split('\n') if not x.strip().startswith('#')]) + '\n'
