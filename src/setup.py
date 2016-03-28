#! /usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
from setuptools import setup
try:
   from distutils.command.build_py import build_py_2to3 as build_py
except ImportError:
   from distutils.command.build_py import build_py

from dsc import VERSION

setup(name        = "dsc",
      description = "Implementation of Dynamic Statistical Comparisons",
      author      = "Gao Wang",
      url         = 'https://github.com/stephenslab/dsc2',
      version     = VERSION,
      packages    = ["dsc", "dsc.pysos"],
      scripts     = ["dsc/dsc", "dsc/dsc-run"],
      cmdclass    = {'build_py': build_py},
      package_dir = {"dsc": "dsc"}
      )
