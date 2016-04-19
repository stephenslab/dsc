#! /usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
import sys
_py_ver = sys.version_info
if _py_ver.major == 2 or (_py_ver.major == 3 and (_py_ver.minor, _py_ver.micro) < (5, 0)):
    raise SystemError('Python 3.5 or higher is required. Please upgrade your Python {}.{}.{}.'
         .format(_py_ver.major, _py_ver.minor, _py_ver.micro))
from setuptools import setup
from dsc import VERSION

setup(name        = "dsc",
      description = "Implementation of Dynamic Statistical Comparisons",
      author      = "Gao Wang",
      author_email = 'gaow@uchicago.edu',
      url         = 'https://github.com/stephenslab/dsc2',
      version     = VERSION,
      packages    = ["dsc"],
      scripts     = ["dsc/dsc", "dsc/dsc-run"],
      package_dir = {"dsc": "dsc"},
      license     = 'MIT',
      classifiers = [
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Operating System :: MacOS :: MacOS X',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 3 :: Only',
        ],
      install_requires = ['sos>=0.6.0', 'pyyaml', 'pandas>=0.18.0',
                          'rpy2>=2.7.8', 'sympy', 'numexpr>=2.5.1',
                          'numpy', 'pprint']
      )
