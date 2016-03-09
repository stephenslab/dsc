#! /usr/bin/env python3
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
from setuptools import setup
try:
   from distutils.command.build_py import build_py_2to3 as build_py
except ImportError:
   from distutils.command.build_py import build_py

from dsc import PACKAGE, VERSION

setup(name        = PACKAGE,
      description = "Implementation of Dynamic Statistical Comparisons",
      author      = "Gao Wang",
      version     = VERSION,
      packages    = [PACKAGE],
      scripts     = ["dsc/dsc"],
      cmdclass    = {'build_py': build_py},
      package_dir = {PACKAGE: 'dsc'}
      )
