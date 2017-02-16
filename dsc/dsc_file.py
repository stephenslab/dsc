
class DSCFileLoader(DSCFileParser):
    '''
    Load DSC configuration file in YAML format and perform initial sanity check
    '''
    def __init__(self, content, sequence = None, output = None):

    def __call__(self, data):
                data[block] = self.__format_block(data[block])

 
    def __format_block(self, section_data):
    
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
