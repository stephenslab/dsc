#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
This file defines methods to translate DSC into pipeline in SoS language
'''
import re, os, datetime, msgpack
from sos.target import fileMD5, textMD5, executable
from .utils import OrderedDict, flatten_list, uniq_list, dict2str, install_r_libs, install_py_modules
from .plugin import R_LMERGE, R_SOURCE


class DSC_Translator:
    '''
    Translate workflow to SoS pipeline:
      * Each DSC computational routine `exec` is a sos step with name `group + command index`
      * When there are combined routines in a block via `.logic` for `exec` then sub-steps are generated
        with name `group + super command index + command index` without alias name then
        create nested workflow named `group + combined routine index`
      * FIXME: to above, because it still produce intermediate files which is not what we want
      * Parameters utilize `for_each`. `paired_with` is not supported
      * Final workflow also use nested workflow structure,
        with workflow name "DSC" for each sequence, indexed by
        the possible ways to combine exec routines. The possible ways are pre-determined and passed here.
    '''
    def __init__(self, workflows, runtime, rerun = False, n_cpu = 4, try_catch = False):
        def replace_right(source, target, replacement, replacements=None):
            return replacement.join(source.rsplit(target, replacements))
        #
        self.output = runtime.output
        self.db = os.path.basename(runtime.output)
        conf_header = 'import msgpack\nfrom collections import OrderedDict\n' \
                      'from dsc.utils import sos_hash_output, sos_group_input, chunks\n' \
                      'from dsc.dsc_database import remove_obsolete_db, build_config_db\n\n\n'
        job_header = "import msgpack\nfrom collections import OrderedDict\n"\
                     "parameter: IO_DB = msgpack.unpackb(open('.sos/.dsc/{}.conf.mpk'"\
                     ", 'rb').read(), encoding = 'utf-8', object_pairs_hook = OrderedDict)\n\n".\
                     format(self.db)
        processed_steps = {}
        conf_dict = {}
        conf_str = []
        job_str = []
        exe_signatures = {}
        # name map for steps
        self.step_map = {}
        # Execution steps, unfiltered
        self.job_pool = OrderedDict()
        # Get workflow steps
        for workflow_id, workflow in enumerate(workflows):
            self.step_map[workflow_id] = {}
            for block in workflow.values():
                for step in block.steps:
                    name = "{0}_{1}_{2}".\
                           format(step.group, step.exe_id, '_'.join([i[0] for i in step.depends]))
                    exe_id_tmp = step.exe_id
                    if name not in processed_steps:
                        pattern = re.compile("^{0}_{1}$|^{0}_[0-9]+_{1}$".\
                                             format(step.group, step.exe_id))
                        cnt = len([k for k in
                                   set(flatten_list([list(self.step_map[i].values()) for i in range(workflow_id)]))
                                   if pattern.match(k)])
                        step.exe_id = "{0}_{1}".format(cnt, step.exe_id) if cnt > 0 else step.exe_id
                        conf_translator = self.Step_Translator(step, self.db, 1, try_catch)
                        if conf_translator.name in conf_dict:
                            raise ValueError('[BUG] Duplicate section name ``{}``'.format(conf_translator.name))
                        conf_dict[conf_translator.name] = conf_translator.dump()
                        job_translator = self.Step_Translator(step, self.db, 0, try_catch)
                        job_str.append(job_translator.dump())
                        processed_steps[name] = "{}_{}".format(step.group, step.exe_id)
                        exe_signatures["{}_{}".format(step.group, step.exe_id)] = job_translator.exe_signature
                    self.step_map[workflow_id]["{}_{}".format(step.group, exe_id_tmp)] = processed_steps[name]
        # Get workflows executions
        i = 1
        io_info_files = []
        final_step_label = []
        for workflow_id, sequence in enumerate(runtime.sequence):
            sequence, step_ids = sequence
            for step_id in step_ids:
                rsqn = ['{}_{}'.format(x, y + 1)
                        if '{}_{}'.format(x, y + 1) not in self.step_map[workflow_id]
                        else self.step_map[workflow_id]['{}_{}'.format(x, y + 1)]
                        for x, y in zip(sequence, step_id)]
                sqn = [replace_right(x, '_', ':', 1) for x in rsqn]
                # Configuration
                conf_str.append("[INIT_{0}]\nparameter: sequence_id = '{0}'\nparameter: sequence_name = '{1}'".\
                                format(i, '+'.join(rsqn)))
                conf_str.append("input: None\noutput: '.sos/.dsc/{1}_{0}.mpk'".format(i, self.db))
                conf_str.append("DSC_UPDATES_ = OrderedDict()")
                conf_str.extend([conf_dict[x] for x in rsqn])
                conf_str.append("open(output[0], 'wb').write(msgpack.packb(DSC_UPDATES_))")
                io_info_files.append('.sos/.dsc/{1}_{0}.mpk'.format(i, self.db))
                # Execution pool
                ii = 1
                for x, y in zip(rsqn, sqn):
                    tmp_str = []
                    tmp_str.append("[{0}{1}: provides = IO_DB['{1}']['{0}']['output_repr']]".format(x, i))
                    tmp_str.append("parameter: script_signature = {}".format(repr(exe_signatures[x])))
                    if ii > 1:
                        tmp_str.append("depends: IO_DB['{1}']['{0}']['input_repr']".format(x, i))
                    tmp_str.append("output:IO_DB['{1}']['{0}']['output']".format(x, i))
                    tmp_str.append("sos_run('core_{2}', output_files = IO_DB['{1}']['{0}']['output']"\
                                   ", input_files = IO_DB['{1}']['{0}']['input'], "\
                                   "DSC_STEP_ID_ = script_signature)".format(x, i, y))
                    self.job_pool[(str(i), x)] = '\n'.join(tmp_str)
                    ii += 1
                final_step_label.append((str(i), x))
                i += 1
        self.conf_str = conf_header + '\n'.join(conf_str)
        self.job_str = job_header + '\n'.join(job_str)
        self.conf_str += "\n[INIT_0]\nremove_obsolete_db('{0}')\n[BUILD_0]\nparameter: vanilla = {1}\n" \
                         "input: {2}\noutput: '.sos/.dsc/{0}.io.mpk', '.sos/.dsc/{0}.map.mpk', '.sos/.dsc/{0}.conf.mpk'"\
                         "\nbuild_config_db(input, output[0], output[1], "\
                         "output[2], vanilla = vanilla, jobs = {3})".\
                         format(self.db, rerun, repr(sorted(set(io_info_files))), n_cpu)
        self.job_str += "\n[DSC]\ndepends: sum([IO_DB[x[0]][x[1]]['output_repr'] for x in {}], [])".\
                        format(repr(final_step_label))
        with open('.sos/.dsc/utils.R', 'w') as f:
            f.write(R_SOURCE + R_LMERGE)
        #
        self.install_libs(runtime.rlib, "R_library", rerun)
        self.install_libs(runtime.pymodule, "Python_Module", rerun)

    def write_pipeline(self, pipeline_id, dest = None):
        import tempfile
        res = []
        if pipeline_id == 1:
            res.extend(['## {}'.format(x) for x in dict2str(self.step_map).split('\n')])
            res.append(self.conf_str)
        else:
            res.append(self.job_str)
        output = dest if dest is not None else (tempfile.NamedTemporaryFile().name + '.sos')
        with open(output, 'w') as f:
            f.write('\n'.join(res))
        return output

    def filter_execution(self):
        '''Filter steps removing the ones having common input and output'''
        IO_DB = msgpack.unpackb(open('.sos/.dsc/{}.conf.mpk'.format(self.db), 'rb').\
                                 read(), encoding = 'utf-8', object_pairs_hook = OrderedDict)
        for x in self.job_pool:
            if x[0] in IO_DB and x[1] in IO_DB[x[0]]:
                self.job_str += '\n{}'.format(self.job_pool[x])

    def install_libs(self, libs, lib_type, force = False):
        if lib_type not in ["R_library", "Python_Module"]:
            raise ValueError("Invalid library type ``{}``.".format(lib_type))
        if libs is None:
            return
        libs_md5 = textMD5(repr(libs) + str(datetime.date.today()))
        if os.path.exists('.sos/.dsc/{}.{}.info'.format(lib_type, libs_md5)) and not force:
            return
        if lib_type == 'R_library':
            ret = install_r_libs(libs)
        if lib_type == 'Python_Module':
            ret = install_py_modules(libs)
        # FIXME: need to check if installation is successful
        os.makedirs('.sos/.dsc', exist_ok = True)
        os.system('echo "{}" > {}'.format(repr(libs), '.sos/.dsc/{}.{}.info'.format(lib_type, libs_md5)))

    class Step_Translator:
        def __init__(self, step, db, prepare, try_catch):
            '''
            prepare step:
             - will produce source to build config and database for
            parameters and file names. The result is one binary json file (via msgpack)
            with keys "X:Y:Z" where X = DSC sequence ID, Y = DSC subsequence ID, Z = DSC step name
                (name of indexed DSC block corresponding to a computational routine).
            run step:
             - will construct the actual script to run
            '''
            # FIXME
            if len(flatten_list(list(step.rf.values()))) > 1:
                raise ValueError('Multiple output files not implemented')
            self.try_catch = try_catch
            self.exe_signature = []
            self.prepare = prepare
            self.step = step
            self.db = db
            self.input_vars = None
            self.header = ''
            self.loop_string = ''
            self.filter_string = ''
            self.param_string = ''
            self.input_string = ''
            self.output_string = ''
            self.input_option = []
            self.step_option = ''
            self.action = ''
            self.name = '{}_{}'.format(self.step.group, self.step.exe_id)
            self.get_header()
            self.get_parameters()
            self.get_input()
            self.get_output()
            self.get_step_option()
            self.get_action()

        def get_header(self):
            if self.prepare:
                self.header = "## [prepare_{1}: shared = '{0}_output']".format(self.step.group, self.name)
            else:
                self.header = "[core_{0} ({1})]\n".\
                               format(self.name, self.step.name)
                self.header += "parameter: DSC_STEP_ID_ = None\nparameter: output_files = list"
                # FIXME: using [step.exe] for now as super step has not yet been ported over

        def get_parameters(self):
            # Set params, make sure each time the ordering is the same
            self.params = list(self.step.p.keys())
            for key in self.params:
                self.param_string += '{}{} = {}\n'.\
                                     format('' if self.prepare else "parameter: ", key, repr(self.step.p[key]))
            if self.step.seed:
                self.params.append('seed')
                self.param_string += '{}seed = {}'.format('' if self.prepare else "parameter: ", repr(self.step.seed))
            if self.params:
                self.loop_string = ' '.join(['for _{0} in {0}'.format(s) for s in reversed(self.params)])
            if self.step.l:
                self.filter_string = ' if ' + self.step.l

        def get_input(self):
            depend_steps = uniq_list([x[0] for x in self.step.depends]) if self.step.depends else []
            if self.prepare:
                if len(depend_steps) >= 2:
                    self.input_vars = 'input_files'
                    self.input_string += "## depends: {}\n".\
                       format(', '.join(['sos_variable(\'{}_output\')'.format(x) for x in depend_steps]))
                    self.input_string += 'input_files = sos_group_input([{}])'.\
                       format(', '.join(['{}_output'.format(x) for x in depend_steps]))
                elif len(depend_steps) == 1:
                    self.input_vars = "{}_output".format(depend_steps[0])
                    self.input_string += "## depends: sos_variable('{}')".format(self.input_vars)
                else:
                    pass
                if len(depend_steps):
                    self.loop_string += ' for __i in chunks({}, {})'.format(self.input_vars, len(depend_steps))
            else:
                if len(depend_steps):
                    self.input_string += "parameter: input_files = list\ninput: dynamic(input_files)".format(self.name)
                    self.input_option.append('group_by = {}'.format(len(depend_steps)))
                else:
                    self.input_string += "input:"
                if len(self.params):
                    if self.filter_string:
                        self.input_option.append("for_each = {{'{0}':[({0}) {1}{2}]}}".\
                                                 format(','.join(['_{}'.format(x) for x in self.params]),
                                                        ' '.join(['for _{0} in {0}'.format(s)
                                                                  for s in reversed(self.params)]),
                                                        self.filter_string))
                    else:
                        self.input_option.append('for_each = %s' % repr(self.params))

        def get_output(self):
            if self.prepare:
                format_string = '.format({})'.format(', '.join(['_{}'.format(s) for s in reversed(self.params)]))
                self.output_string += "{}_output = ".format(self.step.group)
                if self.step.depends:
                    self.output_string += "[sos_hash_output('{0}'{1}, prefix = '{3}', "\
                                          "suffix = '{{}}'.format({4})) {2}]".\
                                          format(' '.join([self.step.name, str(self.step.exe), self.step.group] \
                                                          + ['{0}:{{}}'.format(x) for x in reversed(self.params)]),
                                                 format_string, self.loop_string + self.filter_string, self.step.name, "':'.join(__i)")
                else:
                    self.output_string += "[sos_hash_output('{0}'{1}, prefix = '{3}') {2}]".\
                                      format(' '.join([self.step.name, str(self.step.exe), self.step.group] \
                                                      + ['{0}:{{}}'.format(x) for x in reversed(self.params)]),
                                             format_string, self.loop_string + self.filter_string, self.step.name)
            else:
                # FIXME
                output_group_by = 1
                self.output_string += "output: output_files, group_by = {}".format(output_group_by)

        def get_step_option(self):
            if not self.prepare:
                self.step_option += "task: workdir = {}".format(repr(self.step.workdir))

        def get_action(self):
            if self.prepare:
                combined_params = '[([{0}], {1}) {2}]'.\
                                  format(', '.join(["('exec', '{}')".format(self.step.name)] \
                                                   + ["('{0}', _{0})".format(x) for x in reversed(self.params)]),
                                         None if '__i' not in self.loop_string else "'{}'.format(' '.join(__i))",
                                         self.loop_string + self.filter_string)
                key = "DSC_UPDATES_['{{}}:{}'.format(sequence_id)]".format(self.name)
                self.action += "{} = OrderedDict()\n".format(key)
                if self.step.depends:
                    self.action += "for x, y in zip({}, {}_output):\n\t{}[' '.join((y, x[1]))]"\
                                  " = OrderedDict([('sequence_id', {}), "\
                                  "('sequence_name', {}), ('step_name', '{}')] + x[0])\n".\
                                  format(combined_params, self.step.group, key, 'sequence_id',
                                         'sequence_name', self.name)
                else:
                    self.action += "for x, y in zip({}, {}_output):\n\t{}[y]"\
                                   " = OrderedDict([('sequence_id', {}), "\
                                   "('sequence_name', {}), ('step_name', '{}')] + x[0])\n".\
                                   format(combined_params, self.step.group, key, 'sequence_id',
                                          'sequence_name', self.name)
                self.action += "{0}['DSC_IO_'] = ({1}, {2})\n".\
                               format(key, '[]' if self.input_vars is None else '{0} if {0} is not None else []'.\
                                      format(self.input_vars), "{}_output".format(self.step.group))
                # FIXME: multiple output to be implemented
                self.action += "{0}['DSC_EXT_'] = \'{1}\'\n".\
                               format(key, flatten_list(self.step.rf.values())[0])
            else:
                # FIXME: have not considered super-step yet
                # Create fake plugin and command list for now
                for idx, (plugin, cmd) in enumerate(zip([self.step.plugin], [self.step.exe])):
                    self.action += '{}:\n'.format(plugin.name)
                    # Add action
                    if not self.step.shell_run:
                        script_begin = plugin.get_input(self.params, input_num = len(self.step.depends),
                                                        lib = self.step.libpath, index = idx,
                                                        cmd_args = cmd.split()[1:] if len(cmd.split()) > 1 else None,
                                                        autoload = True if len([x for x in self.step.depends if x[2] == 'var']) else False)
                        script_begin = '{1}\n{0}\n{2}'.\
                                       format(script_begin.strip(),
                                              '## BEGIN code by DSC2',
                                              '## END code by DSC2')
                        if len(self.step.rv):
                            script_end = plugin.get_return(self.step.rv)
                            script_end = '{1}\n{0}\n{2}'.\
                                         format(script_end.strip(),
                                                '## BEGIN code by DSC2',
                                                '## END code by DSC2')
                        else:
                            script_end = ''
                        try:
                            cmd_text = [x.rstrip() for x in open(cmd.split()[0], 'r').readlines()
                                        if x.strip() and not x.strip().startswith('#')]
                        except IOError:
                            raise IOError("Cannot find script ``{}``!".format(cmd.split()[0]))
                        if plugin.name == 'R':
                            cmd_text = ["suppressMessages({})".format(x.strip())
                                        if re.search(r'^(library|require)\((.*?)\)$', x.strip())
                                        else x for x in cmd_text]
                        script = '\n'.join([script_begin, '\n'.join(cmd_text), script_end])
                        if self.try_catch:
                            script = plugin.add_try(script, len(flatten_list([self.step.rf.values()])))
                        script = """## {0} script UUID: ${{DSC_STEP_ID_}}\n{1}""".\
                                 format(str(plugin), script)
                        self.action += script
                        self.exe_signature.append(fileMD5(self.step.exe.split()[0], partial = False)
                                                  if os.path.isfile(self.step.exe.split()[0])
                                                  else self.step.exe.split()[0] + \
                                                  (self.step.exe.split()[1]
                                                   if len(self.step.exe.split()) > 1 else ''))
                    else:
                        executable(cmd.split()[0])
                        self.action += cmd

        def dump(self):
            return '\n'.join([x for x in
                              [self.header,
                               self.param_string,
                               ' '.join([self.input_string,
                                         (', ' if self.input_string != 'input:' else '') + ', '.join(self.input_option)])
                               if not self.prepare else self.input_string,
                               self.output_string,
                               self.step_option,
                               self.action]
                              if x])
