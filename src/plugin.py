#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
Process R and Python plugin codes to DSC
'''
from .syntax import DSC_FILE_OP, DSC_ASIS_OP

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
def format_tuple(value):
    res = []
    # has_raw = False
    for v in value:
        if isinstance(v, str):
            groups = DSC_ASIS_OP.search(v)
            if groups:
                res.append(groups.group(1))
                # has_raw = True
            else:
                res.append(repr(v))
        else:
            res.append(str(v))
    return res


class BasePlug:
    def __init__(self, name = 'run', identifier = ''):
        self.name = name
        self.identifier = 'DSC_{}'.format(identifier.upper())
        self.reset()

    def reset(self):
        self.container = []
        self.container_vars = dict()
        self.input_alias = []
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

    def load_env(self, input_num, index, autoload, customized):
        return ''

    def get_input(self, params, lib, cmd_args):
        return ''

    def get_output(self, params):
        return ''

    @staticmethod
    def format_tuple(value):
        return ' '.join(format_tuple(value))

    def dump(self):
        return dict([
            ('ID', self.identifier),
            ('container', self.container),
                ('container_variables', self.container_vars),
                ('input_alias', self.input_alias),
                ('temp_file', self.tempfile)])
    @staticmethod
    def add_try(content, n_output):
        return ''


class Shell(BasePlug):
    def __init__(self, identifier = ''):
        super().__init__(name = 'run', identifier = identifier)

    def add_input(self, lhs, rhs):
        self.add_tempfile(lhs, rhs)

    def add_tempfile(self, lhs, rhs):
        if rhs == '':
            self.tempfile.append(f'TMP_{self.identifier}=`mktemp -d`')
            self.tempfile.append(f'{lhs}="""$TMP_{self.identifier}/${{_output[0]:bn}}.{lhs}"""')
        else:
            temp_var = [f'${{_output[0]:n}}.{lhs}.{item.strip()}' for item in rhs.split(',')]
            self.tempfile.append('{}="""{}"""'.format(lhs, ' '.join(temp_var)))

    @staticmethod
    def add_try(content, n_output):
        return ''


class RPlug(BasePlug):
    def __init__(self, identifier = ''):
        super().__init__(name = 'R', identifier = identifier)

    def add_input(self, lhs, rhs):
        if isinstance(lhs, str):
            # single value input add
            self.input_alias.append('{} <- {}'.format(lhs,
                                                      rhs if (not rhs.startswith('$'))
                                                      or rhs in ('${_output:r}', '${_input:r}')
                                                      else '{}{}'.format(self.identifier, rhs)))
        elif isinstance(lhs, (list, tuple)):
            # multiple value input add
            for idx, x in enumerate(lhs):
                if rhs.startswith("$") and not rhs.startswith("${"):
                    self.input_alias.append('{} <- {}{}'.format(x, self.identifier, rhs))
                elif not rhs.startswith("$"):
                    self.input_alias.append('{} <- {}'.format(x, rhs))
                else:
                    self.input_alias.append('{} <- {}'.format(x, rhs.replace(':r', '[{}]:r'.format(idx))))

    def add_tempfile(self, lhs, rhs):
        if rhs == '':
            self.tempfile.append(f'TMP_{self.identifier} <- tempdir()')
            self.tempfile.append(f'{lhs} <- paste0(TMP_{self.identifier}, "/", ${{_output[0]:bnr}}, ".{lhs}")')
        else:
            temp_var = [f'paste0(${{_output[0]:nr}}, ".{lhs}.{item.strip()}")' for item in rhs.split(',')]
            self.tempfile.append('{} <- c({})'.format(lhs, ', '.join(temp_var)))

    def load_env(self, input_num, index, autoload, customized):
        loader = 'readRDS' if not customized else 'dscrutils:::read_dsc'
        res = ''
        # load files
        load_multi_in = f'\n{self.identifier} <- list()' + \
          '\ninput.files <- c(${_input:r,})\nfor (i in 1:length(input.files)) ' + \
          f'{self.identifier} <- dscrutils:::merge_lists({self.identifier}, {loader}(input.files[i]))'
        load_single_in = f'\n{self.identifier} <- {loader}("${{_input}}")'
        load_out = f'\nattach({loader}("${{_output}}"), warn.conflicts = F)'
        flag = False
        if input_num > 1 and autoload:
            res += load_multi_in
            if index > 0:
                flag = True
        elif input_num == 1 and autoload:
            res += load_single_in
            if index > 0:
                flag = True
        else:
            pass
        if flag:
            res += load_out
        if self.input_alias:
            res += '\n' + '\n'.join(sorted(self.input_alias))
        if self.tempfile:
            res += '\n' + '\n'.join(sorted(self.tempfile))
        return res

    def get_input(self, params, lib, cmd_args):
        res = ('DSC_LIBPATH <- c({})\n'.format(','.join([repr(x) for x in lib])) + R_SOURCE) if len(lib) else ''
        # load parameters
        keys = sorted([x for x in params if not x in self.container_vars])
        res += '\n' + '\n'.join(self.container)
        if cmd_args:
            for item in cmd_args:
                if "=" in item:
                    lhs, rhs = item.split('=')
                    if rhs.startswith('$'):
                        if rhs[1:] not in params:
                            raise ValueError('Cannot find ``{}`` in parameter list'.format(rhs))
                        else:
                            res += '\n%s <- ${_%s}' % (lhs, rhs[1:])
                            params.remove(rhs[1:])
                    else:
                        res += '\n%s <- %s' % (lhs, rhs)
                else:
                    pass
                    # FIXME: will eventually allow for parameter input for plugins (at SoS level)
        for k in keys:
            res += '\n%s <- ${_%s}' % (k, k)
        # timer
        res += '\n{}_tic_pt <- proc.time()'.format(self.identifier)
        return res

    def get_output(self, params):
        res = []
        for k in params:
            if k == 'DSC_AUTO_OUTPUT_':
                continue
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
                           ['DSC_TIMER = proc.time() - {}_tic_pt'.format(self.identifier)]))
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
        value = format_tuple(value)
        return 'list({})'.format(', '.join(value))

    def __str__(self):
        return 'r'


class PyPlug(BasePlug):
    def __init__(self, identifier = ''):
        super().__init__(name = 'python', identifier = identifier)

    def add_input(self, lhs, rhs):
        if isinstance(lhs, str):
            # single value input add
            self.input_alias.append('{} = {}'.format(lhs,
                                                     rhs if (not rhs.startswith('$'))
                                                     or rhs in ('${_output:r}', '${_input:r}')
                                                     else '{}[{}]'.format(self.identifier, repr(rhs[1:]))))
        elif isinstance(lhs, (list, tuple)):
            # multiple value input add
            for idx, x in enumerate(lhs):
                if rhs.startswith("$") and not rhs.startswith("${"):
                    self.input_alias.append('{} = {}[{}]'.format(x, self.identifier, repr(rhs[1:])))
                elif not rhs.startswith("$"):
                    self.input_alias.append('{} = {}'.format(x, rhs))
                else:
                    self.input_alias.append('{} = {}'.format(x, rhs.replace(':r', '[{}]:r'.format(idx))))

    def add_tempfile(self, lhs, rhs):
        if rhs == '':
            self.tempfile.append(f'TMP_{self.identifier} = tempfile.gettempdir()')
            self.tempfile.append(f'{lhs} = os.path.join(TMP_{self.identifier}, ${{_output[0]:bnr}} + ".{lhs}")')
        else:
            temp_var = [f'${{_output[0]:nr}} + ".{lhs}.{item.strip()}"' for item in rhs.split(',')]
            self.tempfile.append('{} = ({})'.format(lhs, ', '.join(temp_var)))

    def load_env(self, input_num, index, autoload, customized):
        res = 'import sys, os, tempfile, timeit, pickle'
        # load files
        if customized:
            res += '\nfrom dsc.dsc_io import load_dsc as __load_dsc__'
            load_multi_in = f'\n{self.identifier} = __load_dsc__([${{_input:r,}}])'
            load_single_in = f'\n{self.identifier} = __load_dsc__("${{_input}}")'
            load_out = '\nglobals().update(__load_dsc__("${_output}"))'
        else:
            load_multi_in = f'\n{self.identifier} = {{}}' + \
                            f'\nfor item in [${{_input:r,}}]:\n\t{self.identifier}.update(pickle.load(open(item, "rb")))'
            load_single_in = f'\n{self.identifier} = pickle.load(open("${{_input}}", "rb"))'
            load_out = '\nglobals().update(pickle.load(open("${_output}", "rb")))'
        flag = False
        if input_num > 1 and autoload:
            res += load_multi_in
            if index > 0:
                flag = True
        elif input_num == 1 and autoload:
            res += load_single_in
            if index > 0:
                flag = True
        else:
            pass
        if flag:
            res += load_out
        if self.input_alias:
            res += '\n' + '\n'.join(sorted(self.input_alias))
        if self.tempfile:
            res += '\n' + '\n'.join(sorted(self.tempfile))
        return res

    def get_input(self, params, lib, cmd_args):
        res = '\n'.join([f'sys.path.append(os.path.expanduser("{item}"))' for item in lib])
        # load parameters
        keys = sorted([x for x in params if not x in self.container_vars])
        res += '\n' + '\n'.join(self.container)
        # FIXME: will eventually allow for parameter input for plugins (at SoS level)
        if cmd_args:
            cmd_list = []
            for item in cmd_args:
                if item.startswith('$'):
                    if item[1:] not in params:
                        raise ValueError(f'Cannot find ``{item}`` in parameter list')
                    else:
                        cmd_list.append('${_%s}' % item[1:])
                        params.remove(item[1:])
                else:
                    cmd_list.append(repr(item))
            res += '\nsys.argv.extend([{}])'.format(', '.join(cmd_list))
        for k in keys:
            res += '\n%s = ${_%s}' % (k, k)
        res += '\n{}_tic_pt = timeit.default_timer()'.format(self.identifier)
        return res

    def get_output(self, params):
        res = []
        for k in params:
            if k == 'DSC_AUTO_OUTPUT_':
                continue
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
                           ['"DSC_TIMER" : timeit.default_timer() - {}_tic_pt'.format(self.identifier)]))
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
                res.append('%s[%s] <- ${_%s}' % (name, repr(str(j if j is not None else k)), k))
            else:
                res.append('%s[%s] <- %s' % (name, repr(str(j if j is not None else k)), k))
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
        return '({})'.format(','.join(format_tuple(value)))

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
