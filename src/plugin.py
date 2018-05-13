#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
Process R and Python plugin codes to DSC
'''
import yaml, re
from collections import OrderedDict
from copy import deepcopy
from .syntax import DSC_FILE_OP
from .utils import flatten_list

def dict2yaml(value):
    return yaml.dump(value, default_flow_style=False).strip()

R_SOURCE = '''
source.file <- source
source <- function(x) {
 if (is.null(DSC_LIBPATH)) {
  source.file(x)
 } else {
 found <- F
 files <- paste(DSC_LIBPATH, x, sep="/")
 for (i in 1:length(files))
   if (file.exists(files[i])) {
   source.file(files[i])
   found <- T
   break
   }
 if (!found) source.file(x)
 }
}
'''

BASH_UTILS = '''
expandPath() {
  case $1 in
    ~[+-]*)
      local content content_q
      printf -v content_q '%q' "${1:2}"
      eval "content=${1:0:2}${content_q}"
      printf '%s\n' "$content"
      ;;
    ~*)
      local content content_q
      printf -v content_q '%q' "${1:1}"
      eval "content=~${content_q}"
      printf '%s\n' "$content"
      ;;
    *)
      printf '%s\n' "$1"
      ;;
  esac
}
'''

class BasePlug:
    def __init__(self, name = 'run', identifier = ''):
        self.name = name
        self.identifier = 'DSC_{}'.format(identifier.upper())
        self.reset()

    def reset(self):
        self.container = []
        self.container_vars = dict()
        self.module_input = []
        self.alias_map = dict()
        self.tempfile = []

    def add_input(self, lhs, rhs):
        pass

    def add_tempfile(self, lhs, rhs):
        pass

    def add_return(self, lhs, rhs):
        pass

    def get_return(self, output_vars):
        return ''

    def set_container(self, name, value, params):
        pass

    def load_env(self, depends_other, depends_self):
        return ''

    def get_input(self, params, lib):
        return ''

    def get_output(self, params):
        return ''

    def get_var(self, varname):
        if varname in self.alias_map:
            return self.alias_map[varname]
        else:
            return varname

    def get_cmd_args(self, args, params):
        '''
        Use plain variable name with underscore prefix
        Note that cmd arguments can therefore not contain { } eg for awk
        '''
        # FIXME: does not yet address to the case of input / output in shell
        pattern = re.compile(r'\{(.*?)\}')
        if args:
            res = ' '.join(args)
            for m in re.finditer(pattern, res):
                if m.group(1) not in params:
                    raise ValueError('Cannot find ``{}`` in parameter list'.format(m.group(1)))
                else:
                    res = res.replace(m.group(0), '{_%s}' % m.group(1))
            return ', args = "{{filename:q}}" + f" {}"\n'.format(res)
        else:
            return '\n'

    @staticmethod
    def format_tuple(value):
        return flatten_list(value)

    def dump(self):
        return dict([
            ('ID', self.identifier),
            ('container', self.container),
                ('container_variables', self.container_vars),
                ('module_input', self.module_input),
                ('variable_alias', self.alias_map),
                ('temp_file', self.tempfile)])
    @staticmethod
    def add_try(content, n_output):
        return ''


class Shell(BasePlug):
    def __init__(self, identifier = ''):
        super().__init__(name = 'bash', identifier = identifier)
        self.output_ext = 'yml'

    def get_input(self, params, lib):
        res = 'rm -f $[_output]\n'
        if len(lib):
            res += '\n'.join([f'for i in `ls {item}/*.sh`; do source $i; done' for item in lib])
        # load parameters
        for k in sorted(params):
            # FIXME: better idea?
            res += '\n{0}=$(expandPath $[repr(_{1}) if isinstance(_{1}, list) else _{1}])'.format(self.get_var(k), k)
        # FIXME: may need a timer
        # seed
        res += '\nRANDOM=$(($DSC_REPLICATE))'
        return res

    def get_output(self, params):
        '''
        FIXME: assume for now that shell output produces one single file
        accessible as `${_output}`.
        '''
        res = dict([('DSC_OUTPUT', dict())])
        res['DSC_OUTPUT'] = dict([(k,  f'$[_output:n].{params[k]}') for k in params])
        return '\n'.join([f'{k}=$[_output:n].{params[k]}' for k in params]) + \
            f"\ncat >> $[_output:n].yml << EOF\n{dict2yaml(res)}\nEOF"

    def add_input(self, lhs, rhs):
        if isinstance(lhs, str):
            # single value input add
            self.module_input.append('{}={}'.format(self.get_var(lhs),
                                                     rhs if (not rhs.startswith('$'))
                                                     or rhs in ('$[_output:r]', '$[_input:r]')
                                                     else '{}_{}'.format(self.identifier, repr(rhs[1:]))))
        elif isinstance(lhs, (list, tuple)):
            # multiple value input add
            for x in lhs:
                if rhs.startswith("$") and not rhs.startswith("$["):
                    self.module_input.append('{}=${}_{}'.format(self.get_var(x), self.identifier, repr(rhs[1:])))
                elif not rhs.startswith("$"):
                    self.module_input.append('{}={}'.format(self.get_var(x), rhs))
                else:
                    self.module_input.append('{}={}'.format(self.get_var(x[2]), rhs.replace(':r', '[{}].with_suffix(\'.{}\'):r'.format(x[0], x[1][-1]))))

    def add_tempfile(self, lhs, rhs):
        if rhs == '':
            self.tempfile.append(f'TMP_{self.identifier[4:]}=`mktemp -d`')
            self.tempfile.append(f'{self.get_var(lhs)}="""$TMP_{self.identifier[4:]}/$[_output[0]:bn].{lhs}"""')
        else:
            self.tempfile.append('{}="""{}"""'.format(self.get_var(lhs), f'$[_output[0]:n].{lhs}.{rhs}'))

    def set_container(self, name, value, params):
        value = [v.strip() for v in value.split(',') if v.strip()]
        excluded = [v[1:] for v in value if v.startswith('!')]
        if len(value) == len(excluded):
            # empty or all ! input
            keys = sorted([x for x in params.keys() if x not in excluded])
        else:
            keys = sorted([x for x in value if x not in excluded])
        if len(keys) == 0:
            return
        res = OrderedDict([(name, OrderedDict())])
        for k in keys:
            if '=' in k:
                j, k = (x.strip() for x in k.split('='))
            else:
                j = None
            if not (isinstance(params[k][0], str) and params[k][0].startswith('$')) \
               and not (isinstance(params[k][0], str) and DSC_FILE_OP.search(params[k][0])):
                res[name][str(j if j is not None else k)] = '$[_%s]' % k
            else:
                res[name][str(j if j is not None else k)] = k
            if k not in self.container_vars:
                self.container_vars[k] = [j]
            else:
                self.container_vars[k].append(j)
        self.container.append(res)

    def load_env(self, depends_other, depends_self):
        '''
        depends: [(name, var, ext), ...]
        '''
        # and assign the parameters to flat bash variables
        res = f'set -e{BASH_UTILS}'
        # FIXME: need to make it work for loading at least "meta" yaml file
        # Now just list all the names here
        # including meta file
        res += '\n'.join(['\n{}={}'.format(f"{self.identifier}_{item[1]}", "$[_output]") if item[2] is None else (item[1], "$[_output:n].%s" % item[2]) for item in depends_other])
        if len(depends_other):
            res += '\nDSC_REPLICATE=0'
        if self.module_input:
            res += '\n' + '\n'.join(sorted(self.module_input))
        if self.tempfile:
            res += '\n' + '\n'.join(sorted(self.tempfile))
        return res

    def get_return(self, output_vars):
        if len(output_vars) == 0:
            return ''
        res = deepcopy(output_vars)
        container = dict(pair for d in self.container for pair in d.items())
        # FIXME: need more variables here
        for key, val in res.items():
            if val in container:
                res[key] = container[val]
        res['DSC_DEBUG'] = dict()
        res['DSC_DEBUG']['replicate'] = 0
        return f"\ncat >> $[_output] << EOF\n{dict2yaml(res)}\nEOF"

    @staticmethod
    def add_try(content, n_output):
        return ''

    def __str__(self):
        return 'bash'


class RPlug(BasePlug):
    def __init__(self, identifier = ''):
        super().__init__(name = 'R', identifier = identifier)
        self.output_ext = 'rds'

    def add_input(self, lhs, rhs):
        if isinstance(lhs, str):
            # single value input add
            self.module_input.append('{} <- {}'.format(self.get_var(lhs),
                                                      rhs if (not rhs.startswith('$'))
                                                      or rhs in ('${_output:r}', '${_input:r}')
                                                      else '{}{}'.format(self.identifier, rhs)))
        elif isinstance(lhs, (list, tuple)):
            # multiple value input add
            for x in lhs:
                if rhs.startswith("$") and not rhs.startswith("${"):
                    self.module_input.append('{} <- {}{}'.format(self.get_var(x), self.identifier, rhs))
                elif not rhs.startswith("$"):
                    self.module_input.append('{} <- {}'.format(self.get_var(x), rhs))
                else:
                    self.module_input.append('{} <- {}'.format(self.get_var(x[2]), rhs.replace(':r', '[{}].with_suffix(\'.{}\'):r'.format(x[0], x[1][-1]))))

    def add_tempfile(self, lhs, rhs):
        if rhs == '':
            self.tempfile.append(f'TMP_{self.identifier[4:]} <- tempdir()')
            self.tempfile.append(f'{self.get_var(lhs)} <- paste0(TMP_{self.identifier[4:]}, "/", ${{_output[0]:bnr}}, ".{lhs}")')
        else:
            self.tempfile.append('{} <- {}'.format(self.get_var(lhs),f'paste0(${{_output[0]:nr}}, ".{lhs}.{rhs}")'))

    def load_env(self, depends_other, depends_self):
        '''
        depends_other: [(name, var, ext), (name, var, ext), ...]
        depends: {name: [(var, ext), (var, ext)], ...}
        '''
        depends = OrderedDict()
        for x in depends_other:
            if x[0] not in depends:
                depends[x[0]] = [(x[1], x[2])]
            else:
                depends[x[0]].append((x[1], x[2]))
        res = f'{self.identifier} <- list()' if len(depends) else ''
        load_idx = [i for i, k in enumerate(depends.keys()) if any([x[1] is None for x in depends[k]])]
        assign_idx = [(i, k) for i, k in enumerate(depends.keys()) if any([x[1].split('.')[-1] in ['rds', 'pkl', 'yml'] for x in depends[k] if x[1] is not None])]
        loader = 'dscrutils::read_dsc'
        # load files
        load_in = f'\n{self.identifier} <- dscrutils::load_inputs(c(${{paths([_input[i] for i in {load_idx}]):r,}}), {loader})'
        assign_in = ['\n']
        for i, k in assign_idx:
            for j in depends[k]:
                if j[1] is not None and j[1].split('.')[-1] in ['rds', 'pkl', 'yml']:
                    assign_in.append(f'{self.identifier}${j[0]} <- {loader}("${{_input[{i}]:n}}.{j[1]}")')
        assign_in = '\n'.join(assign_in)
        load_out = f'\nif (file.exists("${{_output}}")) attach({loader}("${{_output}}"), warn.conflicts = F)'
        if len(load_idx):
            res += load_in
            res += f'\nDSC_REPLICATE <- {self.identifier}$DSC_DEBUG$replicate'
        else:
            if len(depends):
                # FIXME: have to find another way to pass this down
                res += '\nDSC_REPLICATE <- 0'
        if len(assign_idx):
            res += assign_in
        if depends_self:
            res += load_out
        if self.module_input:
            res += '\n' + '\n'.join(sorted(self.module_input))
        if self.tempfile:
            res += '\n' + '\n'.join(sorted(self.tempfile))
        return res

    def get_input(self, params, lib):
        res = ('DSC_LIBPATH <- c({})\n'.format(','.join([repr(x) for x in lib])) + R_SOURCE) if len(lib) else ''
        # load parameters
        keys = sorted([x for x in params if not x in self.container_vars])
        res += '\n' + '\n'.join(self.container)
        for k in keys:
            res += '\n%s <- ${_%s}' % (self.get_var(k), k)
        # timer
        res += f'\nTIC_{self.identifier[4:]} <- proc.time()'
        # seed
        res += '\nset.seed(DSC_REPLICATE)'
        return res

    def get_output(self, params):
        res = dict([('DSC_OUTPUT', dict())])
        res['DSC_OUTPUT'] = dict([(k,  f'${{_output:n}}.{params[k]}') for k in params])
        return '\n'.join([f'{k} <- paste0(${{_output:nr}}, ".{params[k]}")' for k in params]) + \
            f"\nwrite({repr(dict2yaml(res))}, paste0(${{_output:nr}}, '.yml'))"

        return '\n'.join(res)

    def get_return(self, output_vars):
        if len(output_vars) == 0:
            return ''
        res = '\nsaveRDS(list({}), ${{_output:r}})'.\
          format(', '.join(['{}={}'.format(x, output_vars[x]) for x in output_vars] + \
                           [f"DSC_DEBUG=dscrutils::save_session(TIC_{self.identifier[4:]}, DSC_REPLICATE)"]))
        return res.strip()

    def set_container(self, name, value, params):
        value = [v.strip() for v in value.split(',') if v.strip()]
        excluded = [v[1:] for v in value if v.startswith('!')]
        if len(value) == len(excluded):
            # empty or all ! input
            keys = sorted([x for x in params.keys() if x not in excluded])
        else:
            keys = sorted([x for x in value if x not in excluded])
        if len(keys) == 0:
            return
        res = ['{} <- list()'.format(name)]
        for k in keys:
            if '=' in k:
                j, k = (x.strip() for x in k.split('='))
            else:
                j = None
            if not (isinstance(params[k][0], str) and params[k][0].startswith('$')) \
               and not (isinstance(params[k][0], str) and DSC_FILE_OP.search(params[k][0])):
                res.append('%s$%s <- ${_%s}' % (name, j if j is not None else k, k))
            else:
                res.append('%s$%s <- %s' % (name, j if j is not None else k, k))
            if k not in self.container_vars:
                self.container_vars[k] = [j]
            else:
                self.container_vars[k].append(j)
        self.container.extend(res)

    @staticmethod
    def add_try(content, n_output):
        content = "tryCatch({\n" + '\n'.join([' ' * 4 + x for x in content.split('\n')]) + \
                  "\n}, error = function(e) {\n"
        content += '    script <- sub(".*=", "", commandArgs()[4])\n'
        content += '    script <- readChar(script, file.info(script)$size)\n'
        content += '    script <- paste0(e, "\\n-----------\\n", script)\n'
        for i in range(n_output):
            content += '    cat(script, file = "${_output[%s]}.failed")\n    saveRDS(NULL, ${_output[%s]:r})\n' % (i, i)
        content += '})'
        return content

    @staticmethod
    def format_tuple(value):
        # this is the best I'd like to do for R ...
        has_tuple = any([re.match(r'(.*?)\((.*?)\)(.*?)', v) for v in value])
        if has_tuple:
            return 'list({})'.format(','.join([(f'c({",".join([vv for vv in v])})' if len(v) > 1 else v[0]) if isinstance(v, tuple) else v for v in value]))
        else:
            return 'c({})'.format(','.join(value))

    def __str__(self):
        return 'r'


class PyPlug(BasePlug):
    def __init__(self, identifier = ''):
        super().__init__(name = 'python', identifier = identifier)
        self.output_ext = 'pkl'

    def add_input(self, lhs, rhs):
        if isinstance(lhs, str):
            # single value input add
            self.module_input.append('{} = {}'.format(self.get_var(lhs),
                                                     rhs if (not rhs.startswith('$'))
                                                     or rhs in ('${_output:r}', '${_input:r}')
                                                     else '{}[{}]'.format(self.identifier, repr(rhs[1:]))))
        elif isinstance(lhs, (list, tuple)):
            # multiple value input add
            for x in lhs:
                if rhs.startswith("$") and not rhs.startswith("${"):
                    self.module_input.append('{} = {}[{}]'.format(self.get_var(x), self.identifier, repr(rhs[1:])))
                elif not rhs.startswith("$"):
                    self.module_input.append('{} = {}'.format(self.get_var(x), rhs))
                else:
                    self.module_input.append('{} = {}'.format(self.get_var(x[2]), rhs.replace(':r', '[{}].with_suffix(\'.{}\'):r'.format(x[0], x[1][-1]))))

    def add_tempfile(self, lhs, rhs):
        if rhs == '':
            self.tempfile.append(f'TMP_{self.identifier[4:]} = tempfile.gettempdir()')
            self.tempfile.append(f'{self.get_var(lhs)} = os.path.join(TMP_{self.identifier[4:]}, ${{_output[0]:bnr}} + ".{lhs}")')
        else:
            self.tempfile.append('{} = {}'.format(self.get_var(lhs), f'${{_output[0]:nr}} + ".{lhs}.{rhs}"'))

    def load_env(self, depends_other, depends_self):
        '''
        depends: [(name, var, ext), ...]
        '''
        res = 'import sys, os, tempfile, timeit, pickle\n'
        depends = OrderedDict()
        for x in depends_other:
            if x[0] not in depends:
                depends[x[0]] = [(x[1], x[2])]
            else:
                depends[x[0]].append((x[1], x[2]))
        if len(depends):
            res += f'{self.identifier} = dict()'
        load_idx = [i for i, k in enumerate(depends.keys()) if any([x[1] is None for x in depends[k]])]
        assign_idx = [(i, k) for i, k in enumerate(depends.keys()) if any([x[1].split('.')[-1] in ['rds', 'pkl', 'yml'] for x in depends[k] if x[1] is not None])]
        # load files
        res += '\nfrom dsc.dsc_io import load_dsc as __load_dsc__'
        load_in = f'\n{self.identifier} = __load_dsc__([${{paths([_input[i] for i in {load_idx}]):r,}}])'
        assign_in = ['\n']
        for i, k in assign_idx:
            for j in depends[k]:
                if j[1] is not None and j[1].split('.')[-1] in ['rds', 'pkl', 'yml']:
                    assign_in.append(f'{self.identifier}[{repr(j[0])}] = __load_dsc__("${{_input[{i}]:n}}.{j[1]}")')
        assign_in = '\n'.join(assign_in)
        load_out = '\nif os.path.isfile("${_output}"): globals().update(__load_dsc__("${_output}"))'
        if len(load_idx):
            res += load_in
            res += f'\nDSC_REPLICATE = {self.identifier}["DSC_DEBUG"]["replicate"]'
        else:
            if len(depends):
                # FIXME: have to find another way to pass this down
                res += '\nDSC_REPLICATE = 0'
        if len(assign_idx):
            res += assign_in
        if depends_self:
            res += load_out
        if self.module_input:
            res += '\n' + '\n'.join(sorted(self.module_input))
        if self.tempfile:
            res += '\n' + '\n'.join(sorted(self.tempfile))
        return res

    def get_input(self, params, lib):
        res = '\n'.join([f'sys.path.append(os.path.expanduser("{item}"))' for item in lib])
        # load parameters
        keys = sorted([x for x in params if not x in self.container_vars])
        res += '\n' + '\n'.join(self.container)
        for k in keys:
            res += '\n%s = ${_%s}' % (self.get_var(k), k)
        res += f'\nTIC_{self.identifier[4:]} = timeit.default_timer()'
        res += '\nimport random\nrandom.seed(DSC_REPLICATE)\ntry:\n\timport numpy; numpy.random.seed(DSC_REPLICATE)\nexcept Exception:\n\tpass'
        return res

    def get_output(self, params):
        res = dict([('DSC_OUTPUT', dict())])
        res['DSC_OUTPUT'] = dict([(k,  f'${{_output:n}}.{params[k]}') for k in params])
        return '\n'.join([f'{k} = ${{_output:nr}} + ".{params[k]}"' for k in params]) + \
            f"\nwith open(${{_output:nr}} + '.yml', 'w') as f:\n\tf.write({repr(dict2yaml(res))})"

    def get_return(self, output_vars):
        if len(output_vars) == 0:
            return ''
        res = '\npickle.dump({{{}}}, open(${{_output:r}}, "wb"))'.\
          format(', '.join(['"{0}": {1}'.format(x, output_vars[x]) for x in output_vars] + \
                           [f"'DSC_DEBUG': dict([('time', timeit.default_timer() - TIC_{self.identifier[4:]}), " \
                            "('script', open(__file__).read()), ('replicate', DSC_REPLICATE)])"]))
        # res += '\nfrom os import _exit; _exit(0)'
        return res.strip()

    def set_container(self, name, value, params):
        value = [v.strip() for v in value.split(',') if v.strip()]
        excluded = [v[1:] for v in value if v.startswith('!')]
        if len(value) == len(excluded):
            # empty or all ! input
            keys = sorted([x for x in params.keys() if x not in excluded])
        else:
            keys = sorted([x for x in value if x not in excluded])
        if len(keys) == 0:
            return
        res = [f'{name} = dict()']
        for k in keys:
            if '=' in k:
                j, k = (x.strip() for x in k.split('='))
            else:
                j = None
            if not (isinstance(params[k][0], str) and params[k][0].startswith('$')) \
               and not (isinstance(params[k][0], str) and DSC_FILE_OP.search(params[k][0])):
                res.append('%s[%s] = ${_%s}' % (name, repr(str(j if j is not None else k)), k))
            else:
                res.append('%s[%s] = %s' % (name, repr(str(j if j is not None else k)), k))
            if k not in self.container_vars:
                self.container_vars[k] = [j]
            else:
                self.container_vars[k].append(j)
        self.container.extend(res)

    @staticmethod
    def add_try(content, n_output):
        content = "try:\n" + '\n'.join([' ' * 4 + x for x in content.split('\n')]) + \
                  "\nexcept Exception as e:\n"
        content += '    import sys\nscript = open(sys.argv[len(sys.argv)-1]).read()\n'
        for i in range(n_output):
            content += '    open(${_output[%s]:r}).write("")\n    open("${_output[%s]}.failed").write(str(e) + "\\n-----------\\n" + script)\n' % (i, i)
        return content

    @staticmethod
    def format_tuple(value):
        return '({})'.format(','.join(value))

    def __str__(self):
        return 'python'

def Plugin(key = None, identifier = ''):
    if key is None:
        return BasePlug(identifier = identifier)
    elif key.upper() == 'R':
        return RPlug(identifier = identifier)
    elif key.upper() == 'PY':
        return PyPlug(identifier = identifier)
    else:
        return Shell(identifier = identifier)
