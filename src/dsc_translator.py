#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
This file defines methods to translate DSC into pipeline in SoS language
'''
import os, sys, msgpack, glob, inspect
from xxhash import xxh32 as xxh
from collections import OrderedDict
from sos.targets import path
from .utils import uniq_list, dict2str, n2a, \
    install_r_lib, install_py_module
__all__ = ['DSC_Translator']

class DSC_Translator:
    '''
    Translate preprocessed DSC to SoS pipelines:
      * Each DSC module's name translates to SoS step name
      * Pipelines are executed via nested SoS workflows
    '''
    def __init__(self, workflows, runtime, rerun = False, n_cpu = 4, try_catch = False,
                 host_conf = None, use_logfile = True):
        # FIXME: to be replaced by the R utils package
        self.output = runtime.output
        self.db = os.path.basename(runtime.output)
        if host_conf is not None:
            for k in list(host_conf.keys()):
                if k in runtime.groups:
                    for kk in runtime.groups[k]:
                        host_conf[kk] = host_conf[k]
                    del host_conf[k]
        conf_header = 'from dsc.dsc_database import build_config_db\n'
        job_header = "import msgpack\nfrom collections import OrderedDict\n"\
                     f"IO_DB = msgpack.unpackb(open('{self.output}/{self.db}.conf.mpk'"\
                     ", 'rb').read(), encoding = 'utf-8', object_pairs_hook = OrderedDict)\n\n"\
                     f"{inspect.getsource(n2a)}\n"
        processed_steps = dict()
        conf_dict = dict()
        conf_str = []
        job_str = []
        exe_signatures = dict()
        # name map for steps, very important
        # to be used to expand IO_DB after load
        self.step_map = dict()
        self.exe_check = []
        # Get workflow steps
        for workflow_id, workflow in enumerate(workflows):
            self.step_map[workflow_id + 1] = dict()
            keys = list(workflow.keys())
            for step in workflow.values():
                flow = "_".join(['_'.join(keys[:keys.index(step.name)]), step.name]).strip('_')
                depend = '_'.join(uniq_list([i[0] for i in step.depends]))
                # beginning of pipeline := module that does not have any dependency
                if len(step.depends) == 0:
                    step.p['DSC_REPLICATE'] = runtime.replicate
                # either different flow or different dependency will create a new entry
                if (step.name, flow, depend) not in processed_steps:
                    name = (step.name, workflow_id + 1)
                    self.step_map[workflow_id + 1][step.name] = name
                    # Has the core been processed?
                    if len([x for x in [k[0] for k in processed_steps.keys()] if x == step.name]) == 0:
                        job_translator = self.Step_Translator(step, self.db, None,
                                                              try_catch, host_conf,
                                                              use_logfile)
                        job_str.append(job_translator.dump())
                        job_translator.clean()
                        exe_signatures[step.name] = job_translator.exe_signature
                        self.exe_check.extend(job_translator.exe_check)
                    processed_steps[(step.name, flow, depend)] = name
                    conf_translator = self.Step_Translator(step, self.db,
                                                           self.step_map[workflow_id + 1],
                                                           try_catch,
                                                           None)
                    conf_dict[name] = conf_translator.dump()
                else:
                    self.step_map[workflow_id + 1][step.name] = processed_steps[(step.name, flow, depend)]
        # Get workflows executions
        io_info_files = []
        self.last_steps = []
        # Execution steps, unfiltered
        self.job_pool = OrderedDict()
        # Do not document steps that has been configured already in its unique context
        configured_steps = set()
        for workflow_id, sequence in enumerate(runtime.sequence):
            sqn = [self.step_map[workflow_id + 1][x] for x in sequence]
            new_steps = [conf_dict[x] for x in sqn if x not in configured_steps]
            configured_steps.update(sqn)
            # Configuration
            if len(new_steps):
                conf_str.append(f"###\n# [{n2a(workflow_id + 1)}]\n###\n" \
                                f"__pipeline_id__ = '{workflow_id + 1}'\n"\
                                f'''__pipeline_name__ = '{"+".join([n2a(x[1]).lower()+"_"+x[0] for x in sqn])}'\n''' \
                                f"# output: '.sos/.dsc/{self.db}_{workflow_id + 1}.mpk'\n")
                conf_str.extend(new_steps)
                io_info_files.append(f'.sos/.dsc/{self.db}_{workflow_id + 1}.mpk')
            # Execution pool
            ii = 1
            for y in sequence:
                tmp_str = [f"\n[{n2a(workflow_id + 1).lower()}_{y} ({y})]"]
                if ii > 1:
                    tmp_str.append(f"depends: [sos_step('%s_%s' % (n2a(x[1]).lower(), x[0])) for x in IO_DB['{workflow_id + 1}']['{y}']['depends']]")
                tmp_str.append(f"output: IO_DB['{workflow_id + 1}']['{y}']['output']")
                tmp_str.append(f"sos_run('{y}', {y}_output_files = IO_DB['{workflow_id + 1}']['{y}']['output']"\
                               f", {y}_input_files = IO_DB['{workflow_id + 1}']['{y}']['input'], "\
                               f"DSC_STEP_ID_ = {abs(int(xxh(repr(exe_signatures[y])).hexdigest(), 16)) % (10**8)})")
                if ii == len(sequence):
                    self.last_steps.append((y, workflow_id + 1))
                self.job_pool[(y, workflow_id + 1)] = '\n'.join(tmp_str)
                ii += 1
        conf_str_py = 'import msgpack\nfrom collections import OrderedDict\n' + \
                      'from dsc.utils import sos_hash_output, sos_group_input, chunks\n' + \
                      '\n'.join([f'## {x}' for x in dict2str(self.step_map).split('\n')]) + \
                      '@profile #via "kernprof -l" and "python -m line_profiler"\ndef prepare_io():\n\t'+ \
                      f'\n\t__io_db__ = OrderedDict()\n\t' + \
                      '\n\t'.join('\n'.join(conf_str).split('\n')) + \
                      f"\n\topen('.sos/.dsc/{self.db}.io.mpk', 'wb').write(msgpack.packb(__io_db__))\n\n" + \
                      "if __name__ == '__main__':\n\tprepare_io()"
        self.job_str = job_header + "\n{}".format('\n'.join(job_str))
        # tmp_dep = ", ".join([f"sos_step('{n2a(x+1)}')" for x, y in enumerate(set(io_info_files))])
        self.conf_str_sos = conf_header + \
                            "\n[deploy_1 (Hashing output files)]" + \
                            (f'\ndepends: {", ".join(uniq_list(self.exe_check))}' if len(self.exe_check) and host_conf is None else '') + \
                            f"\noutput: '.sos/.dsc/{self.db}.io.mpk'" + \
                            "\nscript: interpreter={}, suffix='.py'\n{}\n".\
                            format(f'{path(sys.executable):er}',
                                   '\n'.join(['\t' + x for x in conf_str_py.split('\n')])) + \
                            "\n[deploy_2 (Configuring output filenames)]\n" \
                            f"parameter: vanilla = {rerun}\n"\
                            f"input: '.sos/.dsc/{self.db}.io.mpk'\n"\
                            f"output: '{self.output}/{self.db}.map.mpk', "\
                            f"'{self.output}/{self.db}.conf.mpk'"\
                            "\nbuild_config_db(str(_input[0]), str(_output[0]), "\
                            f"str(_output[1]), vanilla = vanilla, jobs = {n_cpu})"
        #
        self.lib_depends = []
        if host_conf is None:
            # run on local computer
            # have to check and ensure local library work
            self.install_libs(runtime.rlib, "R_library")
            self.install_libs([x for x in runtime.pymodule if x != 'dsc'], "Python_Module")
        else:
            self.lib_depends.extend([f'if not R_library({repr(l[0])}, {repr(l[1]) if l[1] is not None else "None"}).target_exists():\n   raise RuntimeError("Automatic installation failed. Please login and manually install {repr(l[0])}.")'
                                     for l in [install_r_lib(x, dryrun = True) for x in runtime.rlib]])
            self.lib_depends.extend([f'if not Py_Module("{l}").target_exists():\n   raise RuntimeError("Automatic installation failed. Please login and manually install {repr(l)}.")'
                                     for l in (runtime.pymodule + ['msgpack'])])

    def write_pipeline(self, arg):
        if arg == 1:
            res = self.conf_str_sos
            open(f'.sos/.dsc/{self.db}.io.meta.mpk', 'wb').write(msgpack.packb(self.step_map))
        elif arg == 2:
            res = self.job_str
        else:
            # args is a list of files to send to host
            chk = (', ' + ', '.join(uniq_list(self.exe_check))) if len(self.exe_check) else ''
            res = '\n'.join([f'[1]\ndepends: executable("rsync"), executable("scp"), executable("ssh"){chk}',
                             f'output: ".sos/{self.db}.remote_lib-info"',
                             f'task: to_host = {repr(arg)}' if len(arg) else '',
                             f'python:\n from sos.targets_r import R_library\n from sos.targets_python import Py_Module\n from sos_pbs.tasks import *'])
            for item in self.lib_depends:
                res += "\n %s" % item
            res += '\n print({}, file=open(".sos/{}.remote_lib-info", "w"))'.format(repr(arg), self.db)
        output = os.path.join('.sos', f'{xxh(res).hexdigest()}.sos')
        with open(output, 'w') as f:
            f.write(res)
        return output

    def filter_execution(self):
        '''Filter steps removing the ones having common input and output'''
        included_steps = []
        for x in self.job_pool:
            if self.step_map[x[1]][x[0]] == x:
                self.job_str += f'\n{self.job_pool[x]}'
                included_steps.append(x)
        #
        self.last_steps = [x for x in self.last_steps if x in included_steps]
        self.job_str += "\n[DSC]\ndepends: {}\noutput: {}".\
                        format(', '.join([f"sos_step('{n2a(x[1]).lower()}_{x[0]}')" for x in self.last_steps]),
                               ', '.join([f"IO_DB['{x[1]}']['{x[0]}']['output']" for x in self.last_steps]))

    def install_libs(self, libs, lib_type):
        if lib_type not in ["R_library", "Python_Module"]:
            raise ValueError("Invalid library type ``{}``.".format(lib_type))
        if len(libs) == 0:
            return
        libs = uniq_list(libs)
        installed_libs = []
        fn = f'.sos/.dsc/{self.db}.{xxh("".join(libs)).hexdigest()}.{lib_type.lower()}-info'
        for item in glob.glob(f'.sos/.dsc/{self.db}.*.{lib_type.lower()}-info'):
            if item == fn:
                installed_libs = [x.strip() for x in open(fn).readlines() if x.strip().split(' ', 1)[1] in libs]
            else:
                os.remove(item)
        new_libs = []
        for lib in libs:
            if f'{lib_type} {lib}' in installed_libs:
                continue
            else:
                if lib_type == 'R_library':
                    ret = install_r_lib(lib)
                if lib_type == 'Python_Module':
                    ret = install_py_module(lib)
                if ret:
                    new_libs.append(f'{lib_type} {lib}')
                else:
                    raise ValueError(f"DSC aborted! Please manually install {lib_type.replace('_', ' ')} ``{lib}``.")
        with open(fn, 'w') as f:
            f.write('\n'.join(installed_libs + new_libs))

    class Step_Translator:
        def __init__(self, step, db, step_map, try_catch, host_conf = None, use_logfile = True):
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
            if step_map is not None and len(step.rf.values()) > 1:
                sys.stderr.write(f'INTERNAL WARNING: "{step.name}" has multiple output files, but only meta-file signature is tracked. '\
                                 'Rigorous support of multiple output files is not yet implemented in current version of DSC.\n')
            self.step_map = step_map
            self.try_catch = try_catch
            self.exe_signature = []
            self.exe_check = []
            self.prepare = 0 if step_map is None else 1
            self.step = step
            self.current_depends = uniq_list([x[0] for x in step.depends]) if step.depends else []
            self.db = db
            self.conf = host_conf
            self.input_vars = None
            self.use_log = use_logfile
            self.header = ''
            self.loop_string = ['', '']
            self.filter_string = ''
            self.param_string = ''
            self.input_string = ''
            self.output_string = ''
            self.input_option = []
            self.step_option = ''
            self.action = ''
            self.get_header()
            self.get_parameters()
            self.get_input()
            self.get_output()
            self.get_step_option()
            self.get_action()

        def clean(self):
            # Remove temp scripts
            for item in glob.glob(f'.sos/{self.step.name}_*'):
                os.remove(item)

        def get_header(self):
            if self.prepare:
                self.header = f"## Codes for {self.step.name}\n"
                self.header += f"__out_vars__ = {repr([x for x in list(self.step.rv.keys()) + list(self.step.rf.keys())])}"
            else:
                self.header = f"\n[{self.step.name}]\n"
                self.header += f"parameter: DSC_STEP_ID_ = None\nparameter: {self.step.name}_output_files = list"

        def get_parameters(self):
            # Set params, make sure each time the ordering is the same
            self.params = list(self.step.p.keys())
            for key in self.params:
                self.param_string += '{}{} = {}\n'.\
                                     format('' if self.prepare else "parameter: ", key, repr(self.step.p[key]))
            if self.params:
                self.loop_string[0] = ' '.join([f'for _{s} in {s}' for s in reversed(self.params)])
            if self.step.ft:
                self.filter_string = ' if ' + self.step.ft

        def get_input(self):
            if self.prepare:
                if self.current_depends:
                    self.input_string += f"## With variables from: {', '.join(self.current_depends)}"
                if len(self.current_depends) >= 2:
                    self.input_vars = f'__{n2a(int(self.step_map[self.step.name][1])).lower()}_{self.step.name}_input__'
                    self.input_string += '\n{} = sos_group_input({})'.\
                       format(self.input_vars,
                              ', '.join([f'__{n2a(int(self.step_map[x][1])).lower()}_{x}_output__' for x in self.current_depends]))
                elif len(self.current_depends) == 1:
                    self.input_vars = f"__{n2a(int(self.step_map[self.current_depends[0]][1])).lower()}_{self.current_depends[0]}_output__"
                else:
                    pass
                if len(self.current_depends):
                    if len(self.current_depends) > 1:
                        self.loop_string[1] = f'for __i__ in chunks({self.input_vars}, {len(self.current_depends)})'
                    else:
                        self.loop_string[1] = f'for __i__ in {self.input_vars}'
            else:
                if len(self.current_depends):
                    self.input_string += "parameter: {0}_input_files = list\ninput: dynamic({0}_input_files)".\
                                         format(self.step.name)
                    self.input_option.append(f'group_by = {len(self.current_depends)}')
                else:
                    self.input_string += "input:"
                if len(self.params):
                    if self.filter_string:
                        self.input_option.append("for_each = {{'{0}':[({0}) {1}{2}]}}".\
                                                 format(','.join([f'_{x}' for x in self.params]),
                                                        ' '.join([f'for _{s} in {s}' for s in reversed(self.params)]),
                                                        self.filter_string))
                    else:
                        self.input_option.append(f'for_each = {repr(self.params)}')
                if self.conf is None:
                    self.input_option.append('concurrent = True')

        def get_output(self):
            if self.prepare:
                format_string = '.format({})'.format(', '.join([f'_{s}' for s in reversed(self.params)])) if len(self.params) else ''
                output_lhs = f"__{n2a(int(self.step_map[self.step.name][1])).lower()}_{self.step.name}_output__"
                self.output_string += "{3} = sos_hash_output(['{0}'{1} {2}])".\
                                      format(' '.join([self.step.name,
                                                       ' '.join([x.replace('{', '{{').replace('}', '}}') for x in self.step.exe['args']]) if self.step.exe['args'] else ''] \
                                                      + self.step.exe['file'] + [f'{k}:{xxh(str(self.step.rv[k])).hexdigest()}' for k in sorted(self.step.rv)] \
                                                      + [f'{k}:{self.step.rf[k]}' for k in sorted(self.step.rf)] \
                                                      + [f'{x}:{{}}' for x in reversed(self.params)]),
                                             format_string, self.loop_string[0] + self.filter_string, output_lhs)
                if len(self.current_depends):
                    self.output_string += "\n{0} = ['{1}:{{}}:{{}}'.format(item, {3}) " \
                                          "for item in {0} {2}]".format(output_lhs, self.step.name, self.loop_string[1],
                                                                        "':'.join(__i__)" if len(self.current_depends) > 1 else "__i__")
                else:
                    self.output_string += "\n{0} = ['{1}:{{}}'.format(item) for item in {0}]".\
                                          format(output_lhs, self.step.name)
            else:
                self.output_string += f"output: {self.step.name}_output_files[_index]"

        def get_step_option(self):
            if not self.prepare and self.conf is not None:
                self.step_option += f"task: {', '.join([str(k) + ' = ' + (repr(v) if isinstance(v, str) else str(v)) for k, v in self.conf[self.step.name if self.step.name in self.conf else 'default'].items()])}, tags = f'{self.step.name}_{{_output:bn}}', workdir = {repr(self.step.workdir)}, concurrent = True\n"

        def get_action(self):
            if self.prepare:
                combined_params = '[([{0}], {1}) {2}]'.\
                                  format(', '.join([f"('{x}', _{x})" for x in reversed(self.params)]),
                                         None if self.loop_string[1] is '' else ("f\"{' '.join(__i__)}\"" if len(self.current_depends) > 1 else "f'{__i__}'"),
                                         ' '.join(self.loop_string) + self.filter_string)
                input_str = '[]' if self.input_vars is None else '{0} if {0} is not None else []'.format(self.input_vars)
                output_str = f"__{n2a(int(self.step_map[self.step.name][1])).lower()}_{self.step.name}_output__"
                # FIXME: multiple output to be implemented
                ext_str = self.step.plugin.output_ext if (len(self.step.exe['path']) == 0 and len(self.step.rv) > 0) else 'yml'
                if len(self.current_depends):
                    self.action += f"__io_db__['{self.step.name}:' + str(__pipeline_id__)] = dict([(' '.join((y, x[1])), dict([('__pipeline_id__', __pipeline_id__), ('__pipeline_name__', __pipeline_name__), ('__module__', '{self.step.name}'), ('__out_vars__', __out_vars__)] + x[0])) for x, y in zip({combined_params}, {output_str})] + [('__input_output___', ({input_str}, {output_str})), ('__ext__', '{ext_str}')])\n"
                else:
                    self.action += f"__io_db__['{self.step.name}:' + str(__pipeline_id__)] = dict([(y, dict([('__pipeline_id__', __pipeline_id__), ('__pipeline_name__', __pipeline_name__), ('__module__', '{self.step.name}'), ('__out_vars__', __out_vars__)] + x[0])) for x, y in zip({combined_params}, {output_str})] + [('__input_output___', ({input_str}, {output_str})), ('__ext__', '{ext_str}')])\n"
            else:
                # FIXME: have not considered multi-action module (or compound module) yet
                # Create fake loop for now with idx going around
                if self.conf is None and self.use_log:
                    self.action += "if _output.with_suffix('.stderr').exists():\n\topen(_output.with_suffix('.stderr'), 'w').close()\n" \
                                   "if _output.with_suffix('.stdout').exists():\n\topen(_output.with_suffix('.stdout'), 'w').close()\n"
                for idx, (plugin, cmd) in enumerate(zip([self.step.plugin], [self.step.exe])):
                    sigil = '$[ ]' if plugin.name == 'bash' else '${ }'
                    if self.conf is None:
                        self.action += f'{plugin.name}: expand = "{sigil}", workdir = {repr(self.step.workdir)}'
                        if self.use_log:
                            self.action += f', stderr = f"{{_output:n}}.stderr", stdout = f"{{_output:n}}.stdout"'
                    else:
                        self.action += f'{plugin.name}: expand = "{sigil}"'
                    self.action += plugin.get_cmd_args(cmd['args'], self.params)
                    # Add action
                    if len(cmd['path']) == 0:
                        script_begin = plugin.load_env(self.step.depends,
                                                       idx > 0 and len(self.step.rv))
                        script_begin += '\n' + plugin.get_input(self.params,
                                                                self.step.libpath if self.step.libpath else [])
                        if len(self.step.rf):
                            script_begin += '\n' + plugin.get_output(self.step.rf)
                        script_begin = '\n'.join([x for x in script_begin.split('\n') if x])
                        script_begin = f"{cmd['header']}\n{script_begin.strip()}\n\n## BEGIN DSC CORE"
                        script_end = plugin.get_return(self.step.rv) if len(self.step.rv) else ''
                        script_end = f'## END DSC CORE\n\n{script_end.strip()}'.strip()
                        script = '\n'.join([script_begin, cmd['content'], script_end])
                        if self.try_catch:
                            script = plugin.add_try(script, len([self.step.rf.values()]))
                        script = f"""## {str(plugin)} script UUID: ${{DSC_STEP_ID_}}\n{script}\n"""
                        script = '\n'.join([f'  {x}' for x in script.split('\n')])
                        self.action += script
                        self.exe_signature.append(cmd['signature'])
                    else:
                        self.exe_check.append(f"executable({repr(cmd['path'])})")
                        self.action += f"\t{cmd['path']} {'$*' if cmd['args'] else ''}\n"
                if self.conf is None and self.use_log:
                    self.action += "\ntry:\n\tif _output.with_suffix('.stderr').stat().st_size == 0:\n\t\tos.remove(_output.with_suffix('.stderr'))\nexcept Exception:\n\tpass\n" \
                                   "try:\n\tif _output.with_suffix('.stdout').stat().st_size == 0:\n\t\tos.remove(_output.with_suffix('.stdout'))\nexcept Exception:\n\tpass"

        def dump(self):
            return '\n'.join([x for x in
                              [self.header,
                               self.param_string.strip(),
                               ' '.join([self.input_string,
                                         (', ' if self.input_string != 'input:' else '') + ', '.join(self.input_option)])
                               if not self.prepare else self.input_string,
                               self.output_string,
                               self.step_option,
                               self.action]
                              if x])
