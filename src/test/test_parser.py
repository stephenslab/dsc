#!/usr/bin/env python3
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"

from unittest import main
from utils import DSCTestCase, load_strings, load_as_string, load_dsc
from dsc.dsc_file import OperationParser, FormatError

class ParserTest(DSCTestCase):
    def testSequenceParser(self):
        '''Test DSC Operation Parser '''
        def sort_seq(value):
            return '; '.join(sorted([x.strip() for x in value.split(';')]))
        op = OperationParser()
        for x, y in load_strings('files/OperationParserTest.txt', group_by = 2):
            op(x)
            self.assertEqual(sort_seq(op.value), sort_seq(y))

    def __testBlockParser(self, file_prefix):
        self.assertEqual(str(load_dsc(file_prefix)).strip(),
                         load_as_string('files/{}.res'.format(file_prefix)).strip())

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

    def testDuplication(self):
        '''Test duplicated keys in input'''
        self.assertRaises(FormatError, load_dsc, 11)

if __name__ == '__main__':
    main()
