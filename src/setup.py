#! /usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
from setuptools import setup
from dsc import VERSION

setup(name        = "dsc",
      description = "Implementation of Dynamic Statistical Comparisons",
      author      = "Gao Wang",
      url         = 'https://github.com/stephenslab/dsc2',
      version     = VERSION,
      packages    = ["dsc"],
      scripts     = ["dsc/dsc", "dsc/dsc-run"],
      package_dir = {"dsc": "dsc"},
      install_requires = ['sos', 'pyyaml', 'rpy2', 'sympy']
      )
