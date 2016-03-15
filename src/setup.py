#! /usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
import sys
from setuptools import setup
try:
   from distutils.command.build_py import build_py_2to3 as build_py
except ImportError:
   from distutils.command.build_py import build_py

from dsc import PACKAGE, VERSION

setup(name        = PACKAGE,
      description = "Implementation of Dynamic Statistical Comparisons",
      author      = "Gao Wang",
      url         = 'https://github.com/stephenslab/dsc2',
      version     = VERSION,
      packages    = [PACKAGE],
      scripts     = ["dsc/dsc", "dsc/dsc-run"],
      cmdclass    = {'build_py': build_py},
      package_dir = {PACKAGE: 'dsc'}
      )

setup(name        = "pysos",
      description = "Python library for Script of Scripts (SoS): a workflow system for the execution of scripts in different languages",
      author      = 'Bo Peng',
      url         = 'https://github.com/bpeng2000/SOS',
      version     = 'github.master',
      packages    = ['pysos'],
      cmdclass    = {'build_py': build_py },
      package_dir = {'pysos': 'pysos'}
    )
