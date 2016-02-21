#!/usr/bin/env python3
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import sys, yaml
from dsc_actions import DSCFileLoader, DSCEntryFormatter, DSCScenarioSetup, \
     DSCMethodSetup, DSCScoreSetup
from io import StringIO

class DSCData(dict):
    '''
    Read DSC configuration file and translate it to a list of job initializers

    This class reflects the design and implementation of DSC user interface

    Tasks here include:
      * Properly parse YAML
      * Setup runtime environment
        * Check availability of libraries / files / commands
      * Translate DSC file text
        * Replace R() / Python() / Shell() actions
        * Replace global variables
      * Expand all settings to a list of parameter dictionaries each will initialize a job
    '''
    def __init__(self, fname):
        self.actions = [DSCFileLoader(),
                        DSCEntryFormatter(),
                        DSCScenarioSetup(),
                        DSCMethodSetup(),
                        DSCScoreSetup()]
        self.file_name = fname
        for a in self.actions:
            a.apply(self)

    def __str__(self):
        out = StringIO()
        yaml.dump(dict(self), out, default_flow_style=False)
        res = out.getvalue()
        out.close()
        res = res.replace('!!python/tuple', '(tuple)')
        return res
