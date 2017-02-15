
class DSCFileLoader(DSCFileParser):
    '''
    Load DSC configuration file in YAML format and perform initial sanity check
    '''
    def __init__(self, content, sequence = None, output = None):

    def __call__(self, data):
                data[block] = self.__format_block(data[block])

 
    def __format_block(self, section_data):
        '''
        Format block data to meta / params etc for easier manipulation

          * meta: will contain exec information
          * params:
            * params[0] (for shared params), params[1], params[2], (corresponds to exec[1], exec[2]) ...
          * rules:
            * rules[0] (for shared params), rules[1] ...
          * params_alias:
            * params_alias[0], params_alias[1] ...
        '''
        res = dotdict()
        res.meta = {}
        res.params = {0:{}}
        res.rules = {}
        res.params_alias = {}
        res.out = section_data['return']
        # Parse meta
        res.meta['exec'] = section_data['exec']
        if 'seed' in section_data:
            res.meta['seed'] = section_data['seed']
        if '.logic' in section_data:
            # no need to expand exec logic
            res.meta['rule'] = section_data['.logic']
        if '.alias' in section_data:
            res.meta['exec_alias'] = section_data['.alias']
        # Parse params
        if 'params' in section_data:
            for key, value in section_data['params'].items():
                try:
                    # get indexed slice
                    name, idxes = get_slice(key)
                    if name != 'exec':
                        raise FormatError('Unknown indexed parameter entry: {}.'.format(key))
                    for idx in idxes:
                        idx += 1
                        if idx == 0:
                            raise FormatError('Invalid entry: exec[0]. Index must start from 1.')
                        if idx in res.params:
                            res.params[idx].update(flatten_dict(value))
                        else:
                            res.params[idx] = flatten_dict(value)
                except AttributeError:
                    res.params[0][key] = flatten_dict(value)
            # Parse rules and params_alias
            for key in list(res.params.keys()):
                if '.logic' in res.params[key]:
                    res.rules[key] = self.op(res.params[key]['.logic'])
                    del res.params[key]['.logic']
                if '.alias' in res.params[key]:
                    res.params_alias[key] = res.params[key]['.alias']
                    del res.params[key]['.alias']
        return dotdict(strip_dict(res))

class DSCEntryFormatter(DSCFileParser):
    '''
    Run format transformation to DSC entries
    '''
    def __init__(self):
        DSCFileParser.__init__(self)

    def __call__(self, data):
        actions = [Str2List(),
                   ExpandVars(try_get_value(data.DSC, 'params')),
                   ExpandActions(),
                   CastData()]
        data = self.__Transform(data, actions)

    def __Transform(self, cfg, actions):
        '''Apply actions to items'''
        for key, value in list(cfg.items()):
            if isinstance(value, collections.Mapping):
                self.__Transform(value, actions)
            else:
                for a in actions:
                    value = a(value)
                if is_null(value):
                    del cfg[key]
                else:
                    cfg[key] = value
        return cfg

class DSCData(dotdict):
    '''
    Read DSC configuration file and translate it to a collection of steps to run DSC

    This class reflects the design and implementation of DSC structure and syntax

    Tasks here include:
      * Properly parse DSC file in YAML format
      * Translate DSC file text
        * Replace Operators R() / Python() / Shell() / Combo() ...
        * Replace global variables
      * Some sanity check

    Structure of self:
      self.block_name.block_param_name = dict()
    '''
    def __init__(self, content, sequence = None, output = None, check_rlibs = True, check_pymodules = True):
        actions = [DSCFileLoader(content, sequence, output), DSCEntryFormatter()]
        for a in actions:
            a(self)
        for name in list(self.keys()):
            if name == 'DSC':
                continue
            else:
                # double check if any computational routines are
                # out of index
                self[name]['meta']['exec'] = [tuple(x.split()) if isinstance(x, str) else x
                                              for x in self[name]['meta']['exec']]
                if ('exec_alias' in self[name]['meta'] and 'rule' not in self[name]['meta'] \
                    and len(self[name]['meta']['exec_alias']) != len(self[name]['meta']['exec'])) or \
                    ('exec_alias' in self[name]['meta'] and 'rule' in self[name]['meta'] \
                     and len(self[name]['meta']['exec_alias']) != len(self[name]['meta']['rule'])):
                    raise FormatError('Alias does not match the length of exec, in block ``{}``!'.\
                                      format(name))
                if 'params' in self[name]:
                    max_exec = max(self[name]['params'].keys())
                    if max_exec > len(self[name]['meta']['exec']):
                        raise FormatError('Index for exec out of range: ``exec[{}]``.'.format(max_exec))
        #
        rlibs = try_get_value(self['DSC'], ('R_libs'))
        if rlibs and check_rlibs:
            rlibs_md5 = textMD5(repr(rlibs) + str(datetime.date.today()))
            if not os.path.exists('.sos/.dsc/RLib.{}.info'.format(rlibs_md5)):
                install_r_libs(rlibs)
                os.makedirs('.sos/.dsc', exist_ok = True)
                os.system('echo "{}" > {}'.format(repr(rlibs),
                                                  '.sos/.dsc/RLib.{}.info'.format(rlibs_md5)))
        #
        pymodules = try_get_value(self['DSC'], ('python_modules'))
        if pymodules and check_pymodules:
            pymodules_md5 = textMD5(repr(pymodules) + str(datetime.date.today()))
            if not os.path.exists('.sos/.dsc/pymodules.{}.info'.format(pymodules_md5)):
                install_py_modules(pymodules)
                os.makedirs('.sos/.dsc', exist_ok = True)
                os.system('echo "{}" > {}'.format(repr(pymodules),
                                                  '.sos/.dsc/pymodules.{}.info'.format(pymodules_md5)))


    def __str__(self):
        res = ''
        for item in sorted(list(dict(self).items())):
            # res += dict2str({item[0]: dict(item[1])}, replace = [('!!python/tuple', '(tuple)')]) + '\n'
            res += dict2str({item[0]: dict(item[1])}) + '\n'
        return res.strip()
