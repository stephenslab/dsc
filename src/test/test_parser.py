#!/usr/bin/env python3
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

from unittest import main
from .utils import DSCTestCase, load_strings
from DSC2.dsc_file import DSCData, OperationParser

class ParserTest(DSCTestCase):
    def TestSequenceParser(self):
        '''Test DSC Operation Parser '''
        op = OperationParser()
        for x, y in load_strings('file/OperationParserTest.txt', group_by = 2):
            op.apply(x)
            self.assertEqual(op.value, y)

if __name__ == '__main__':
    main()
