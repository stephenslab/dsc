#!/usr/bin/env python3
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

from unittest import main
from utils import DSCTestCase, load_strings, load_as_string
from DSC2.dsc_file import DSCData, OperationParser

class ParserTest(DSCTestCase):
    def testSequenceParser(self):
        '''Test DSC Operation Parser '''
        op = OperationParser()
        for x, y in load_strings('files/OperationParserTest.txt', group_by = 2):
            op.apply(x)
            self.assertEqual(op.value, y)

    def __testBlockParser(self, file_prefix):
        f_input = 'files/{}.yaml'.format(file_prefix)
        f_expected = 'files/{}.res'.format(file_prefix)
        self.assertEqual(str(DSCData(f_input, verbosity = '-1')).strip(),
                         load_as_string(f_expected).strip())

    def testBasicBlock(self):
        '''Basic block parser test'''
        self.__testBlockParser(1)

    def testAlias(self):
        '''Test __alias__ and return alias'''
        self.__testBlockParser(2)

    def testProductPairwiseOperator(self):
        '''Test "Product" and "Pairwise" operators'''
        self.__testBlockParser(3)

    def testExeParams(self):
        '''Test executable specific parameters'''
        self.__testBlockParser(4)

    def testInlineOperators(self):
        '''Test R()/Python()/Shell()'''
        self.__testBlockParser(5)

    def testAsisOperator(self):
        '''Test Asis() operator'''
        self.__testBlockParser(6)

    def testGlobalVars(self):
        '''Test DSC global variables'''
        self.__testBlockParser(7)

    def testTupleOperator(self):
        '''Test () parser'''
        self.__testBlockParser(8)

    def testLogicOperator(self):
        '''Test __logic__ operator'''
        self.__testBlockParser(9)

    def testInheritance(self):
        '''Test block inheritance'''
        self.__testBlockParser(10)

    # def testRLibraries(self):
    #     '''Test R library parser'''
    #     self.__testBlockParser(11)

if __name__ == '__main__':
    main()
