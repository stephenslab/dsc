#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
Process R and Python plugin codes to DSC
'''
import re
from .syntax import DSC_FILE_OP
from .utils import flatten_list

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

    def load_env(self, depends, depends_self):
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
        return ' '.join(flatten_list(value))

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

    def get_input(self, params, lib):
        res = 'set -e\n'
        if len(lib):
            res += '\n'.join([f'for i in `ls {item}/*.sh`; do source $i; done' for item in lib])
        # load parameters
        for k in sorted(params):
            # FIXME: better idea?
            res += '\n{0}=${{_{1}[1:-1] if isinstance(_{1}, str) and (_{1}.startswith("\'") or _{1}.startswith(\'"\')) else _{1}}}'.format(self.get_var(k), k)
        # FIXME: may need a timer
        # seed
        res += '\nRANDOM=$(($DSC_REPLICATE + ${DSC_STEP_ID_}))'
        return res

    def get_output(self, params):
        '''
        FIXME: assume for now that shell output produces one single file
        accessible as `${_output}`.
        '''
        res = '\n'.join([f'{k}=${{_output:n}}.{params[k]}' for k in params])
        res += f"\necho '''{res}''' > ${{_output}}"
        return res

    def add_input(self, lhs, rhs):
        self.add_tempfile(lhs, rhs)

    def add_tempfile(self, lhs, rhs):
        if rhs == '':
            self.tempfile.append(f'TMP_{self.identifier[4:]}=`mktemp -d`')
            self.tempfile.append(f'{self.get_var(lhs)}="""$TMP_{self.identifier[4:]}/${{_output[0]:bn}}.{lhs}"""')
        else:
            temp_var = [f'${{_output[0]:n}}.{lhs}.{item.strip()}' for item in rhs.split(',')]
            self.tempfile.append('{}="""{}"""'.format(self.get_var(lhs), ' '.join(temp_var)))

    @staticmethod
    def add_try(content, n_output):
        return ''

    def __str__(self):
        return 'bash'


class RPlug(BasePlug):
    def __init__(self, identifier = ''):
        super().__init__(name = 'R', identifier = identifier)

    def add_input(self, lhs, rhs):
        if isinstance(lhs, str):
            # single value input add
            self.module_input.append('{} <- {}'.format(self.get_var(lhs),
                                                      rhs if (not rhs.startswith('$'))
                                                      or rhs in ('${_output:r}', '${_input:r}')
                                                      else '{}{}'.format(self.identifier, rhs)))
        elif isinstance(lhs, (list, tuple)):
            # multiple value input add
            for idx, x in enumerate(lhs):
                if rhs.startswith("$") and not rhs.startswith("${"):
                    self.module_input.append('{} <- {}{}'.format(self.get_var(x), self.identifier, rhs))
                elif not rhs.startswith("$"):
                    self.module_input.append('{} <- {}'.format(self.get_var(x), rhs))
                else:
                    self.module_input.append('{} <- {}'.format(self.get_var(x), rhs.replace(':r', '[{}]:r'.format(idx))))

    def add_tempfile(self, lhs, rhs):
        if rhs == '':
            self.tempfile.append(f'TMP_{self.identifier[4:]} <- tempdir()')
            self.tempfile.append(f'{self.get_var(lhs)} <- paste0(TMP_{self.identifier[4:]}, "/", ${{_output[0]:bnr}}, ".{lhs}")')
        else:
            temp_var = [f'paste0(${{_output[0]:nr}}, ".{lhs}.{item.strip()}")' for item in rhs.split(',')]
            self.tempfile.append('{} <- c({})'.format(self.get_var(lhs), ', '.join(temp_var)))

    def load_env(self, depends, depends_self):
        '''
        depends: [(name, var, ext), ...]
        '''
        res = f'{self.identifier} <- list()' if len(depends) else ''
        load_idx = [i for i, item in enumerate(depends) if item[2] is None]
        assign_idx = [i for i, item in enumerate(depends) if i not in load_idx and item[2].split('.')[-1] in ['rds', 'pkl']]
        loader = 'dscrutils::read_dsc'
        # load files
        load_in = f'\n{self.identifier} <- dscrutils::load_inputs(c(${{paths([_input[i] for i in {load_idx}]):r,}}), {loader})'
        assign_in = '\n' + '\n'.join([f'{self.identifier}${depends[i][1]} <- {loader}("${{_input[{i}]:n}}.{depends[i][2]}")' for i in assign_idx])
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
        res += '\nset.seed(DSC_REPLICATE + ${DSC_STEP_ID_})'
        return res

    def get_output(self, params):
        res = []
        for k in params:
            if len(params[k]) > 1:
                res.append(f'{k} <- rep(NA, {len(params[k])})')
                for idx, item in enumerate(params[k]):
                    res.append(f'{k}[{idx + 1}] <- paste0(${{_output:nr}}, ".{item}")')
            else:
                res.append(f'{k} <- paste0(${{_output:nr}}, ".{params[k][0]}")')
        return '\n'.join(res)

    def get_return(self, output_vars):
        if len(output_vars) == 0:
            return ''
        res = '\nsaveRDS(list({}), ${{_output:r}})'.\
          format(', '.join(['{}={}'.format(x, output_vars[x]) for x in output_vars] + \
                           [f"DSC_DEBUG=dscrutils::save_session(TIC_{self.identifier[4:]}, DSC_REPLICATE)"]))
        return res.strip()

    def set_container(self, name, value, params):
        keys = sorted([x.strip() for x in value.split(',')] if value else list(params.keys()))
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
        has_tuple = any([isinstance(v, tuple) or re.match(r'(.*?)\((.*?)\)(.*?)', v) for v in value])
        if has_tuple:
            return 'list({})'.format(','.join([(f'c({",".join([vv for vv in v])})' if len(v) > 1 else v[0]) if isinstance(v, tuple) else v for v in value]))
        else:
            return 'c({})'.format(','.join(value))

    def __str__(self):
        return 'r'


class PyPlug(BasePlug):
    def __init__(self, identifier = ''):
        super().__init__(name = 'python', identifier = identifier)

    def add_input(self, lhs, rhs):
        if isinstance(lhs, str):
            # single value input add
            self.module_input.append('{} = {}'.format(self.get_var(lhs),
                                                     rhs if (not rhs.startswith('$'))
                                                     or rhs in ('${_output:r}', '${_input:r}')
                                                     else '{}[{}]'.format(self.identifier, repr(rhs[1:]))))
        elif isinstance(lhs, (list, tuple)):
            # multiple value input add
            for idx, x in enumerate(lhs):
                if rhs.startswith("$") and not rhs.startswith("${"):
                    self.module_input.append('{} = {}[{}]'.format(self.get_var(x), self.identifier, repr(rhs[1:])))
                elif not rhs.startswith("$"):
                    self.module_input.append('{} = {}'.format(self.get_var(x), rhs))
                else:
                    self.module_input.append('{} = {}'.format(self.get_var(x), rhs.replace(':r', '[{}]:r'.format(idx))))

    def add_tempfile(self, lhs, rhs):
        if rhs == '':
            self.tempfile.append(f'TMP_{self.identifier[4:]} = tempfile.gettempdir()')
            self.tempfile.append(f'{self.get_var(lhs)} = os.path.join(TMP_{self.identifier[4:]}, ${{_output[0]:bnr}} + ".{lhs}")')
        else:
            temp_var = [f'${{_output[0]:nr}} + ".{lhs}.{item.strip()}"' for item in rhs.split(',')]
            self.tempfile.append('{} = ({})'.format(self.get_var(lhs), ', '.join(temp_var)))

    def load_env(self, depends, depends_self):
        '''
        depends: [(name, var, ext), ...]
        '''
        res = 'import sys, os, tempfile, timeit, pickle\n'
        if len(depends):
            res += f'{self.identifier} = dict()'
        load_idx = [i for i, item in enumerate(depends) if item[2] is None]
        assign_idx = [i for i, item in enumerate(depends) if i not in load_idx and item[2].split('.')[-1] in ['rds', 'pkl']]
        # load files
        res += '\nfrom dsc.dsc_io import load_dsc as __load_dsc__'
        load_in = f'\n{self.identifier} = __load_dsc__([${{paths([_input[i] for i in {load_idx}]):r,}}])'
        assign_in = '\n' + '\n'.join([f'{self.identifier}["{depends[i][1]}"] = __load_dsc__("${{_input[{i}]:n}}.{depends[i][2]}")' for i in assign_idx])
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
        res += '\nimport random\nrandom.seed(DSC_REPLICATE + ${DSC_STEP_ID_})\ntry:\n\timport numpy; numpy.random.seed(DSC_REPLICATE + ${DSC_STEP_ID_})\nexcept Exception:\n\tpass'
        return res

    def get_output(self, params):
        res = []
        for k in params:
            if len(params[k]) > 1:
                res.append(f'{k} = [None for x in range({len(params[k])})]')
                for idx, item in enumerate(params[k]):
                    res.append(f'{k}[{idx}] = ${{_output:nr}} + ".{item}"')
            else:
                res.append(f'{k} = ${{_output:nr}} + ".{params[k][0]}"')
        return '\n'.join(res)

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
        keys = sorted([x.strip() for x in value.split(',')] if value else list(params.keys()))
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
        return '({})'.format(','.join([f'({",".join([vv for vv in v])})' if isinstance(v, tuple) else v for v in value]))

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
