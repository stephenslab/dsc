#!/usr/bin/env python3
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

from unittest import main
from utils import DSCTestCase, load_strings
from DSC2.dsc_file import DSCData, OperationParser

class ParserTest(DSCTestCase):
    def testSequenceParser(self):
        '''Test DSC Operation Parser '''
        op = OperationParser()
        for x, y in load_strings('files/OperationParserTest.txt', group_by = 2):
            op.apply(x)
            self.assertEqual(op.value, y)

    def __testBlockParser(self):
        pass

    def testBasicBlock(self):
        '''Basic block parser test'''
        pass

    def testAlias(self):
        '''Test __alias__ and return alias'''
        pass

    def testProductPairwiseOperator(self):
        '''Test "Product" and "Pairwise" operators'''
        pass

    def testExeParams(self):
        '''Test executable specific parameters'''
        pass

    def testInlineOperators(self):
        '''Test R()/Python()/Shell()'''
        pass

    def testAsisOperator(self):
        '''Test Asis() operator'''
        pass

    def testGlobalVars(self):
        '''Test DSC global variables'''
        pass

    def testRLibraries(self):
        '''Test R library parser'''
        pass

    def testTupleOperator(self):
        '''Test () parser'''
        pass

    def testLogicOperator(self):
        '''Test __logic__ operator'''
        pass

    def testInheritance(self):
        '''Test block inheritance'''
        pass

if __name__ == '__main__':
    main()
