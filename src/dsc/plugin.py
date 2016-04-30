#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
Process R and Python plugin codes to DSC
'''

R_SOURCE = '''
source.file <- source
source <- function(x) {
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
'''

class BasePlug:
    def __init__(self, name = None):
        self.name = name
        self.reset()

    def reset(self):
        self.container = []
        self.container_vars = []
        self.return_alias = []
        self.input_alias = []

    def add_input(self, lhs, rhs):
        pass

    def add_return(self, lhs, rhs):
        pass

    def get_return(self, output_vars):
        return ''

    def set_container(self, name, value, params):
        pass

    def get_input(self, params, input_num, lib = None):
        return ''

    def format_tuple(self, value):
        return ' '.join([repr(x) if isinstance(x, str) else str(x) for x in value])

class RPlug(BasePlug):
    def __init__(self):
        super().__init__('R')

    def add_input(self, lhs, rhs):
        self.input_alias.append('{} <- {}'.format(lhs, rhs))

    def add_return(self, lhs, rhs):
        self.return_alias.append('{} <- {}'.format(lhs, rhs))

    def get_input(self, params, input_num, lib):
        if lib is not None:
            res = '\nDSC_LIBPATH <- c({})'.format(','.join([repr(x) for x in lib])) + R_SOURCE
        else:
            res = ''
        res += '\n'.join(self.container)
        keys = [x for x in params if not x in self.container_vars]
        if 'seed' in keys:
            res += '\nset.seed(${_seed})'
            keys.remove('seed')
        for k in keys:
            res += '\n%s <- ${_%s}' % (k, k)
        if input_num > 1:
            # FIXME: using attach might be dangerous. better create some namespace.
            res += '\ninput.files <- c(${_input!r,})\nfor (i in 1:length(input.files)) ' \
              'attach(readRDS(input.files[i]), warn.conflicts = F)'
        elif input_num == 1:
            res += '\nattach(readRDS("${_input}"), warn.conflicts = F)'
        else:
            pass
        res += '\n' + '\n'.join(self.input_alias)
        return res

    def get_return(self, output_vars):
        res = '\n'.join(self.return_alias)
        res += '\nsaveRDS(list({}), ${{_output!r}})'.\
          format(', '.join(['{0}={0}'.format(x) for x in output_vars]))
        return res.strip()

    def set_container(self, name, value, params):
        keys = [x.strip() for x in value.split(',')] if value else list(params.keys())
        keys = [x for x in keys if x != 'seed']
        res = ['{} <- list()'.format(name)]
        for k in keys:
            res.append('%s$%s <- ${_%s}' % (name, k, k))
        self.container.extend(res)
        self.container_vars.extend(keys)

    def format_tuple(self, value):
        return 'c({})'.format(', '.join([repr(x) if isinstance(x, str) else str(x) for x in value]))

class PyPlug(BasePlug):
    def __init__(self):
        super().__init__('python')

    def add_input(self, lhs, rhs):
        self.input_alias.append('{} = {}'.format(lhs, rhs))

    def add_return(self, lhs, rhs):
        self.return_alias.append('{} = {}'.format(lhs, rhs))

    def get_input(self, params, input_num, lib):
        if lib is not None:
            res = '\nimport sys, os'
            for item in lib:
                res += '\nsys.path.append(os.path.abspath("{}"))'.format(item)
        else:
            res = ''
        res += '\nfrom dsc.utils import save_rds, load_rds'
        res += '\n'.join(self.container)
        keys = [x for x in params if not x in self.container_vars]
        if 'seed' in keys:
            res += '\nimport random, numpy\nrandom.seed(${_seed})\nnumpy.random.seed(${_seed})'
            keys.remove('seed')
        for k in keys:
            res += '\n%s = ${_%s}' % (k, k)
        if input_num > 1:
            res += '\nfor item in [${_input!r,}]:\n\tglobals().update(load_rds(item))'
        if input_num == 1:
            res += '\nglobals().update(load_rds("${_input}"))'
        res += '\n' + '\n'.join(self.input_alias)
        return res

    def get_return(self, output_vars):
        res = '\n'.join(self.return_alias)
        res += '\nsave_rds({{{}}}, ${{_output!r}})'.\
          format(', '.join(['"{0}": {0}'.format(x) for x in output_vars]))
        # res += '\nfrom os import _exit; _exit(0)'
        return res.strip()

    def set_container(self, name, value, params):
        keys = [x.strip() for x in value.split(',')] if value else list(params.keys())
        keys = [x for x in keys if x != 'seed']
        res = ['{} = {{}}'.format(name)]
        for k in keys:
            res.append('%s[%s] = ${_%s}' % (name, k, k))
        self.container.extend(res)
        self.container_vars.extend(keys)

    def format_tuple(self, value):
        return '({})'.format(', '.join([repr(x) if isinstance(x, str) else str(x) for x in value]))


def Plugin(key = None):
    if key is None:
        return BasePlug()
    elif key.upper() == 'R':
        return RPlug()
    elif key.upper() == 'PY':
        return PyPlug()
    else:
        return BasePlug('')
