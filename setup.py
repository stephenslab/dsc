#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
import os, sys
sys.path.append('src')
_py_ver = sys.version_info
if _py_ver.major == 2 or (_py_ver.major == 3 and (_py_ver.minor, _py_ver.micro) < (6, 0)):
    raise SystemError('Python 3.6 or higher is required. Please upgrade your Python {}.{}.{}.'
         .format(_py_ver.major, _py_ver.minor, _py_ver.micro))
from setuptools import setup
from setuptools.command.bdist_egg import bdist_egg
from version import __version__

try:
    with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'README.rst'), encoding='utf-8') as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = ''

class bdist_egg_disabled(bdist_egg):
    """Disabled version of bdist_egg
    Prevents setup.py install performing setuptools' default easy_install,
    which it should never ever do.
    """
    def run(self):
        sys.exit("ERROR: aborting implicit building of eggs. Use \"pip install -U --upgrade-strategy only-if-needed .\" to install from source.")

cmdclass = {'bdist_egg':  bdist_egg if 'bdist_egg' in sys.argv else bdist_egg_disabled }

setup(name        = "dsc",
      description = "Implementation of Dynamic Statistical Comparisons",
      long_description = long_description,
      author      = __author__,
      author_email = __email__,
      url         = 'https://github.com/stephenslab/dsc',
      download_url= f'https://pypi.python.org/pypi/dsc/{__version__}#downloads',
      version     = __version__,
      entry_points = {'console_scripts': ['dsc = dsc.__main__:main', 'dsc-query = dsc.__query__:main', 'dsc-io = dsc.dsc_io:main']},
      license     = __license__,
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
      packages    = ['dsc', 'dsc.parser'],
      cmdclass    = cmdclass,
      package_dir = {'dsc': 'src'},
      install_requires = ['numpy', 'pandas>=0.22.0', 'sympy', 'numexpr',
                          'sos>=0.9.14.1', 'h5py', 'PTable',
                          'pyarrow>=0.5.0', 'sqlalchemy',
                          'msgpack-python']
      )
