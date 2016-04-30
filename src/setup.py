#! /usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
__version__ = "0.1.0"
import sys
_py_ver = sys.version_info
if _py_ver.major == 2 or (_py_ver.major == 3 and (_py_ver.minor, _py_ver.micro) < (5, 0)):
    raise SystemError('Python 3.5 or higher is required. Please upgrade your Python {}.{}.{}.'
         .format(_py_ver.major, _py_ver.minor, _py_ver.micro))
from setuptools import setup

# init
css = open('asset/prism.css').read()
js = open('asset/prism.js').read()

with open('dsc/__init__.py', 'w') as f:
    f.write('#!/usr/bin/env python3\n')
    f.write('__author__ = "{}"\n'.format(__author__))
    f.write('__copyright__ = "{}"\n'.format(__copyright__))
    f.write('__email__ = "{}"\n'.format(__email__))
    f.write('__license__ = "{}"\n'.format(__license__))
    f.write('__version__ = "{}"\n'.format(__version__))
    f.write('PACKAGE = "dsc"\n')
    f.write('VERSION = __version__\n')
    f.write('HTML_CSS = {}\n'.format(repr(css)))
    f.write('HTML_JS = {}\n'.format(repr(js)))

setup(name        = "dsc",
      description = "Implementation of Dynamic Statistical Comparisons",
      author      = "Gao Wang",
      author_email = 'gaow@uchicago.edu',
      url         = 'https://github.com/stephenslab/dsc2',
      version     = __version__,
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
      install_requires = ['sos>=0.5.9', 'pyyaml', 'pandas>=0.18.0',
                          'rpy2>=2.7.8', 'sympy', 'numexpr',
                          'numpy']
      )
