#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
'''
Process R and Python plugin codes to DSC
'''
import os

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

R_LMERGE = '''
DSC_LMERGE <-
function(x, y, ...)
{
  if(length(x) == 0)
    return(y)
  if(length(y) == 0)
    return(x)
  for (i in 1:length(names(y)))
    x[names(y)[i]] = y[i]
  return(x)
}
'''

class BasePlug:
    def __init__(self, name = None, identifier = ''):
        self.name = name
        self.identifier = 'DSC_{}'.format(identifier.upper())
        self.reset()

    def reset(self):
        self.container = []
        self.container_vars = []
        self.return_alias = []
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

    def get_input(self, params, input_num, lib = None, index = 0, cmd_args = None):
        return ''

    def format_tuple(self, value):
        return ' '.join([repr(x) if isinstance(x, str) else str(x) for x in value])

class RPlug(BasePlug):
    def __init__(self, identifier = ''):
        super().__init__(name = 'R', identifier = identifier)

    def add_input(self, lhs, rhs):
        self.input_alias.append('{} <- {}'.format(lhs,
                                                  rhs if (not rhs.startswith('$')) or rhs == '${_output!r}'
                                                  else '{}{}'.format(self.identifier, rhs)))
    def add_tempfile(self, lhs, rhs):
        self.tempfile.append('TMP_{} <- tempdir()'.format(self.identifier))
        temp_var = ['paste0(TMP_{0}, "/", basename("${{_output}}.{1}.{2}"))'.\
                    format(self.identifier, lhs, item.strip()) for item in rhs.split(',')]
        self.tempfile.append('{} <- c({})'.format(lhs, ', '.join(temp_var)))


    def add_return(self, lhs, rhs):
        self.return_alias.append('{} <- {}'.format(lhs, rhs))

    def get_input(self, params, input_num, lib, index, cmd_args):
        if lib is not None:
            res = 'DSC_LIBPATH <- c({})'.format(','.join([repr(x) for x in lib]))
        else:
            res = 'DSC_LIBPATH <- NULL'
        res += '\nsource("{}")'.format(os.path.abspath(".sos/.dsc/utils.R"))
        # load files
        load_multi_in = '\n{} <- list()'.format(self.identifier) + \
          '\ninput.files <- c(${{_input!r,}})\nfor (i in 1:length(input.files)) ' \
          '{0} <- DSC_LMERGE({0}, readRDS(input.files[i]))'.format(self.identifier)
        load_single_in = '\n{} <- readRDS("${{_input}}")'.format(self.identifier)
        load_out = '\nattach(readRDS("${_output}"), warn.conflicts = F)'
        flag = False
        if input_num > 1:
            res += load_multi_in
            if index > 0:
                flag = True
        elif input_num == 1:
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
        # load parameters
        keys = sorted([x for x in params if not x in self.container_vars])
        if 'seed' in keys:
            res += '\nset.seed(${_seed})'
            keys.remove('seed')
        res += '\n' + '\n'.join(self.container)
        if cmd_args:
            for item in cmd_args:
                # FIXME: will eventually allow for parameter input for plugins (at SoS level)
                lhs, rhs = item.split('=')
                if rhs.startswith('$'):
                    if rhs[1:] not in params:
                        raise ValueError('Cannot find ``{}`` in parameter list'.format(rhs))
                    else:
                        res += '\n%s <- ${_%s}' % (lhs, rhs[1:])
                        params.remove(rhs[1:])
                else:
                    res += '\n%s <- %s' % (lhs, rhs)
        for k in keys:
            res += '\n%s <- ${_%s}' % (k, k)
        return res

    def get_return(self, output_vars):
        res = '\n'.join(self.return_alias)
        res += '\nsaveRDS(list({}), ${{_output!r}})'.\
          format(', '.join(['{0}={0}'.format(x) if not isinstance(x, tuple) else '{0}={1}'.format(x[0], x[1]) for x in output_vars]))
        return res.strip()

    def set_container(self, name, value, params):
        keys = [x.strip() for x in value.split(',')] if value else list(params.keys())
        keys = sorted([x for x in keys if x != 'seed'])
        res = ['{} <- list()'.format(name)]
        for k in keys:
            if not (isinstance(params[k][0], str) and params[k][0].startswith('$')):
                res.append('%s$%s <- ${_%s}' % (name, k, k))
            else:
                res.append('%s$%s <- %s' % (name, k, k))
        self.container.extend(res)
        self.container_vars.extend(keys)

    def format_tuple(self, value):
        return 'c({})'.format(', '.join([repr(x) if isinstance(x, str) else str(x) for x in value]))

class PyPlug(BasePlug):
    def __init__(self, identifier = ''):
        super().__init__(name = 'python', identifier = identifier)

    def add_input(self, lhs, rhs):
        self.input_alias.append('{} = {}'.format(lhs,
                                                 rhs if (not rhs.startswith('$')) or rhs == '${_output!r}'
                                                 else '{}[{}]'.format(self.identifier, repr(rhs[1:]))))

    def add_tempfile(self, lhs, rhs):
        self.tempfile.append('TMP_{} = tempfile.gettempdir()'.format(self.identifier))
        temp_var = ['os.path.join(TMP_{0}, os.path.basename("${{_output}}.{1}.{2}"))'.\
                    format(self.identifier, lhs, item.strip()) for item in rhs.split(',')]
        self.tempfile.append('{} = ({})'.format(lhs, ', '.join(temp_var)))

    def add_return(self, lhs, rhs):
        self.return_alias.append('{} = {}'.format(lhs, rhs))

    def get_input(self, params, input_num, lib, index, cmd_args):
        res = 'import sys, os, tempfile'
        if lib is not None:
            for item in lib:
                res += '\nsys.path.append(os.path.abspath("{}"))'.format(item)
        # load files
        res += '\nfrom dsc.utils import save_rds, load_rds'
        load_multi_in = '\n{} = {{}}'.format(self.identifier) + \
          '\nfor item in [${{_input!r,}}]:\n\t{}.update(load_rds(item))'.format(self.identifier)
        load_single_in = '\n{} = load_rds("${{_input}}")'.format(self.identifier)
        load_out = '\nglobals().update(load_rds("${_output}"))'
        flag = False
        if input_num > 1:
            res += load_multi_in
            if index > 0:
                flag = True
        elif input_num == 1:
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
        # load parameters
        keys = sorted([x for x in params if not x in self.container_vars])
        if 'seed' in keys:
            res += '\nimport random, numpy\nrandom.seed(${_seed})\nnumpy.random.seed(${_seed})'
            keys.remove('seed')
        res += '\n' + '\n'.join(self.container)
        # FIXME: will eventually allow for parameter input for plugins (at SoS level)
        if cmd_args:
            if not res:
                res = '\nimport sys'
            cmd_list = []
            for item in cmd_args:
                if item.startswith('$'):
                    if item[1:] not in params:
                        raise ValueError('Cannot find ``{}`` in parameter list'.format(item))
                    else:
                        cmd_list.append('${_%s}' % item[1:])
                        params.remove(item[1:])
                else:
                    cmd_list.append(repr(item))
            res += '\nsys.argv.extend([{}])'.format(', '.join(cmd_list))
        for k in keys:
            res += '\n%s = ${_%s}' % (k, k)
        return res

    def get_return(self, output_vars):
        res = '\n'.join(self.return_alias)
        res += '\nsave_rds({{{}}}, ${{_output!r}})'.\
          format(', '.join(['"{0}": {0}'.format(x) if not isinstance(x, tuple) else '"{0}": {1}'.format(x[0], x[1]) for x in output_vars]))
        # res += '\nfrom os import _exit; _exit(0)'
        return res.strip()

    def set_container(self, name, value, params):
        keys = [x.strip() for x in value.split(',')] if value else list(params.keys())
        keys = sorted([x for x in keys if x != 'seed'])
        res = ['{} = {{}}'.format(name)]
        for k in keys:
            if not (isinstance(params[k][0], str) and params[k][0].startswith('$')):
                res.append('%s[%s] = ${_%s}' % (name, k, k))
            else:
                res.append('%s[%s] = %s' % (name, k, k))
        self.container.extend(res)
        self.container_vars.extend(keys)

    def format_tuple(self, value):
        return '({})'.format(', '.join([repr(x) if isinstance(x, str) else str(x) for x in value]))


def Plugin(key = None, identifier = ''):
    if key is None:
        return BasePlug(identifier = identifier)
    elif key.upper() == 'R':
        return RPlug(identifier = identifier)
    elif key.upper() == 'PY':
        return PyPlug(identifier = identifier)
    else:
        return BasePlug(name = '', identifier = identifier)
