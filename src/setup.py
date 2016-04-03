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
      install_requires = ['pyyaml', 'numexpr>=2.5.1',
                          'pandas>=0.18.0', 'rpy2>=2.7.8',
                          'sympy', 'celery>=3.1.23', 'sos>=0.5.5']
      )
