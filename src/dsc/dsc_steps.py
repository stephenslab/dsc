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
from pysos import SoS_Script, Error
from utils import dotdict, dict2str, try_get_value

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
        # Stores all DSC jobs
        self.data = {}
        # Store meta info
        self.meta = {'output_name': data.DSC['output'][0],
                     'work_dir': data.DSC['work_dir'][0],
                     'sequences': data.DSC['run'],
                     'ordering': self.__merge_sequences(data.DSC['run'])
                     }
        for block in data:
            if block == 'DSC':
                # FIXME
                continue
            self.__update(data[block], data.DSC, name = block)

    def __update(self, block, dsc_block, name = 'block'):
        '''Process block data and update self.data / self.meta
           Each DSC step is a dictionary with keys:
           * command: str
           * is_r: bool
           * r_begin: list of str
           * r_end: list of str
           * parameters: list of repr() of python style assignment
           * output: list of str
        '''
        # FIXME: replicates not yet handled
        # Load command parameters
        for idx, exe in enumerate(block.meta['exec']):
            step_name = '{}.{}'.format(name, idx + 1)
            res = dict([('command', ''), ('is_r', False), ('r_begin', []), ('r_end', []),
                        ('parameters', []), ('output_base', []), ('output_vars', []),
                        ('options', {})])
            res['options']['work_dir'] = self.meta['work_dir']
            res['command'] = ' '.join([self.__search_exec(exe[0], try_get_value(dsc_block, ('exec_path')))] + \
                                      [x if not x.startswith('$') else '${_%}' % x[1:] for x in exe[1:]])
            res['is_r'] = exe[0].lower().endswith('.r')
            # temporary variables
            params = {}
            alias = {}
            rules = None
            # load parameters, rules and alias
            if 'params' in block:
                params = copy.deepcopy(block.params[0])
                if (idx + 1) in list(block.params.keys()):
                    params.update(block.params[idx + 1])
            if 'rules' in block:
                rules = block.rules[0]
                if (idx + 1) in list(block.rules.keys()):
                    rules = block.rules[idx + 1]
            if 'params_alias' in block:
                alias = dict([(x.strip() for x in item.split('=')) for item in block.params_alias[0]])
                if (idx + 1) in list(block.params_alias.keys()):
                    alias = dict([(x.strip() for x in item.split('=')) for item in block.params_alias[0]])
            # apply rules and alias to parameters
            # handle alias
            vars_RList = []
            for k, item in list(alias.items()):
                groups = re.search(r'(.*?)\((.*?)\)', item)
                if not groups:
                    # swap key
                    params[k] = params.pop(item)
                else:
                    if groups.group(1) == 'RList':
                        text, variables = self.__format_RList(k, groups.group(2), params)
                        res['r_begin'].append(text)
                        vars_RList.extend(variables)
                    else:
                        raise StepError('Invalid .alias ``{}`` in block ``{}``.'.format(groups.group(1), name))
            if res['is_r']:
                res['r_begin'].append(self.__format_RParams(params, vars_RList))
            # FIXME: handle rules: slicing
            # FIXME: Asis() and File(), handle it directly into SoS syntax
            # handle output
            for item in block.out:
                lhs = ''
                if '=' in item:
                    # return alias exists
                    lhs, rhs = (x.strip() for x in item.split('='))
                    groups = re.search(r'^R\((.*?)\)', rhs)
                    if groups:
                        # alias is within R
                        res['r_end'].append('{} <- {}'.format(lhs, groups.group(1)))
                    else:
                        # alias is not within R
                        params[lhs] = params[rhs]
                else:
                    lhs = item.strip()
                res['output_vars'].append(lhs)
                if not res['is_r']:
                    # output file pattern
                    res['output_base'].append(lhs)
            if res['is_r']:
                res['r_end'].append('saveRDS(${_output_names}, %s)' % ', '.join(res['output_vars']))
                res['output_base'].append('{}.{}.rds'.format(self.meta['output_name'], step_name))
            # assign parameters
            res['parameters'] = params
            res['r_begin'] = '\n'.join(res['r_begin'])
            res['r_end'] = '\n'.join(res['r_end'])
            self.data[step_name] = res

    def __format_RList(self, name, value, params):
        keys = [x.strip() for x in value.split(',')] if value else list(params.keys())
        res = ['{} <- list()'.format(name)]
        for k in keys:
            res.append('%s$%s <- ${_%s}' % (name, k, k))
        return '\n'.join(res), keys

    def __format_RParams(self, params, keys):
        res = []
        for k in list(params.keys()):
            if k not in keys:
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

    def __str__(self):
        res = ''
        for item in sorted(list(dict(self).items())):
            res += dict2str({item[0]: item[1]}, replace = [('!!python/tuple', '(tuple)')]) + '\n'
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
    def __init__(self, data):
        keys = sorted(data.data.keys(),
                      key = lambda x: (data.meta["ordering"].index(x.split('.', -1)[0]), int(x.split('.', -1)[1])))
        steps = []
        last_idx = 0
        last_key = 0
        for idx, key in enumerate(keys):
            if data.meta['ordering'].index(key.split('.', -1)[0]) != last_key:
                last_key = data.meta['ordering'].index(key.split('.', -1)[0])
                last_idx = idx
            steps.append(self.__get_step(key, idx + 1 - last_idx, data.data[key]))
        self.data = '\n'.join(steps)

    def __call__(self):
        pass

    def __str__(self):
        return self.data

    def __get_step(self, step_name, step_id, step_data):
        res = ['[{}_{}]'.format(step_name, step_id)]
        for key, item in step_data['parameters'].items():
            # FIXME: format parameter assignment
            res.append('{} = {}'.format(key, repr(item)))
        if len(step_data['output_base']):
            res.append('output_base = {}'.format(repr(step_data['output_base'][0])))
        # FIXME: input and output
        res.append('input: for_each = {}'.format(repr(list(step_data['parameters'].keys()))))
        if step_data['is_r']:
            rscript = """R('''\n{3}\n{0}\n{4}\n{1}\n{3}\n{2}\n{4}\n''')""".\
              format(step_data['r_begin'].strip(),
                     '\n'.join([x.strip() for x in open(step_data['command'].split()[0], 'r').readlines() if x.strip()]),
                     step_data['r_end'].strip(),
                     '## BEGIN code auto-generated by DSC',
                     '## END code auto-generated by DSC')
            res.append(rscript)
        else:
            res.append("""run('''\n{}\n''')""".format(step_data['command']))
        return '\n'.join(res)
