#!/usr/bin/env python3
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

import os
import re
import unittest

def importTests():
    tests = unittest.TestSuite()
    for file in os.listdir('.'):
        match = re.match("^(test_(.*))\\.py$", file)
        if match:
            m = match.group(1)
            print("Adding test cases in %s" % m)
            module = __import__(m)
            tests.addTest(unittest.defaultTestLoader.loadTestsFromModule( module ))
    return tests

if __name__ == '__main__':
    test_runner = unittest.TextTestRunner(descriptions = False, verbosity=2)
    test_runner.run(importTests())
