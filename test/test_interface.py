#!/usr/bin/env python3
#
# Copyright (c) Gao Wang, Stephens Lab at The Univeristy of Chicago
# Distributed under the terms of the MIT License.

import subprocess
import unittest

def run_cmd(cmd):
    return subprocess.call(cmd, stderr=subprocess.DEVNULL,
                           stdout=subprocess.DEVNULL, shell=True)

class TestParser(unittest.TestCase):
    def setUp(self):
        run_cmd('sos remove -s -v0')

    def testBasicInterfacePass(self):
        self.assertEqual(run_cmd('sos run test_interface.sos'),0)

if __name__ == '__main__':
    #suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestParser)
    # unittest.TextTestRunner(, suite).run()
    unittest.main()