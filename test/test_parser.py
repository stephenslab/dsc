#!/usr/bin/env python3
#
# Copyright (c) Gao Wang, Stephens Lab at The Univeristy of Chicago
# Distributed under the terms of the MIT License.

import subprocess
import unittest

from dsc.dsc_parser import DSC_Script
from dsc.utils import FormatError

text0 = '''
DSC:
    run: simulate
'''
text1 = text0 + '''
simulate: R(x=x), R(x=y)
    x: 1
    y: 2
    $out: x
'''
text2 = text0 + '''
simulate: R()
    group_1:
        x: 1
        y: 2
    $out: x
'''
text3 = text0 + '''
simulate: R()
    @simulate:
        x: 1
        y: 2
    $out: x
'''
text4 = text0 + '''
simulate: R()
    @simulate:
        group_1:
            x: 1
            y: 2
    $x: x
'''
text5 = text0 + '''
simulate: R()
    @simulate:
        x: 1
        y: 2
    @ALIAS:
        x_1: x
    $out: x
'''
text6 = text0 + '''
simulate: R()
    @simulate:
        x: 1
        y: 2
    @ALIAS: 
        simulate: x_1 = x
    $out: x
'''
text7 = text0 + '''
simulate: R()
    @simulate:
        x: 1
        y: 2
        @ALIAS: x_1 = x
    $out: x
'''
text8 = text0 + '''
simulate: R()
    @simulate:
        x: 1
        y: 2
        @ALIAS: 
            *: x_1 = x
    $out: x
'''
text9 = text0 + '''
simulate: R()
    @simulate:
        x: 1
        y: 2
    @test:
        x: 3
    $out: x
'''
text10 = text0 + '''
simulate: R()
    @simulate, test:
        x: 1
        y: 2
    $out: x
'''
text11 = text0 + '''
simulate: R()
    x: 1
    y: ${x}
    $out: x
DSC:
    run: simulate
    global:
        x: 2
'''
text12 = '''
simulate: R()
    x: 1
    y: ${x}[3]
    $out: x
DSC:
    run: simulate
    global:
        x: 4,3,2,1
'''
text13 = text0 + '''
simulate: R()
  x: 1
  y: 2
  @ALIAS: args = List(), arg1 = List(xvar = x, yy = y)
  $out: x
'''
text14 = text0 + '''
simulate: R()
    x: R{1:5}
    @FILTER:
        *: x < 3
    $out: x
'''
class TestParser(unittest.TestCase):
    def setUp(self):
        subprocess.call('sos remove -s -v0', shell=True)

    def testBasicSyntaxPass(self):
        '''basic syntax parser success'''
        # exec decoration
        res = DSC_Script(text3)
        self.assertEqual(list(res.modules['simulate'].dump()['input'].items()), [('x', [1]), ('y', [2])])
        # alias for specific module, both syntax supported
        res = DSC_Script(text6)
        self.assertEqual(list(res.modules['simulate'].dump()['input'].items()), [('x', [1]), ('y', [2])])
        self.assertEqual(list(res.modules['simulate'].plugin.alias_map.items()), [('x', 'x_1')])
        res = DSC_Script(text7)
        self.assertEqual(list(res.modules['simulate'].dump()['input'].items()), [('x', [1]), ('y', [2])])
        self.assertEqual(list(res.modules['simulate'].plugin.alias_map.items()), [('x', 'x_1')])
        res = DSC_Script(text8)
        self.assertEqual(list(res.modules['simulate'].dump()['input'].items()), [('x', [1]), ('y', [2])])
        self.assertEqual(list(res.modules['simulate'].plugin.alias_map.items()), [('x', 'x_1')])
        # use global variable
        res = DSC_Script(text11)
        self.assertEqual(list(res.modules['simulate'].dump()['input'].items()), [('x', [2]), ('y', [2])])
        res = DSC_Script(text12)
        self.assertEqual(list(res.modules['simulate'].dump()['input'].items()), [('x', [4,3,2,1]), ('y', [2])])
        # alias partial list / dict
        res = DSC_Script(text13)
        self.assertEqual(list(res.modules['simulate'].plugin.dump()['container_variables'].items()), [('x', [None, 'xvar']), ('y', [None, 'yy'])])
        # filter decorator
        res = DSC_Script(text14)
        self.assertEqual(res.modules['simulate'].dump()['input_filter'], '(_x < 3)')

    def testBasicSyntaxFail(self):
        '''basic syntax parser fail'''
        # multiple exec output
        # FIXME: test below needs fix
        self.assertRaises(FormatError, DSC_Script, text1)
        # grouped parameters is not allowed
        self.assertRaises(FormatError, DSC_Script, text2)
        # grouped parameters is not allowed in exec decoration
        self.assertRaises(FormatError, DSC_Script, text4)
        # alias is not a list
        self.assertRaises(FormatError, DSC_Script, text5)
        # invalid decoration / module
        self.assertRaises(FormatError, DSC_Script, text9)
        # invalid decoration / module
        self.assertRaises(FormatError, DSC_Script, text10)

    def testLegalNamesFail(self):
        '''illegal variable / module names'''
        text = '''
@@simulate: R()
    x: 1
'''
        self.assertRaises(FormatError, DSC_Script, text)
        text = '''
simulate: R()
    $x: 1
'''
        self.assertRaises(FormatError, DSC_Script, text)
        text = '''
simulate: R()
    .x: 1
DSC: 
    run: simulate
'''
        self.assertRaises(FormatError, DSC_Script, text)
        text = '''
simulate: R()
    _x: 1
'''
        self.assertRaises(FormatError, DSC_Script, text)
        text = '''
simulate: R()
    x$: 1
'''
        self.assertRaises(FormatError, DSC_Script, text)
        text = '''
simulate: R()
    x.y: 1
'''
        self.assertRaises(FormatError, DSC_Script, text)
        text = '''
simulate: R()
    x_y: 1
'''
        self.assertRaises(FormatError, DSC_Script, text)
        text = '''
simulate: R()
    x.1: 1
'''
        self.assertRaises(FormatError, DSC_Script, text)
        text = '''
simulate: R()
    1: 1
'''
        self.assertRaises(FormatError, DSC_Script, text)
        text = '''
simulate: R()
    _: 1
'''
        self.assertRaises(FormatError, DSC_Script, text)
        text = '''
simulate: R()
    **: 1
'''
        self.assertRaises(FormatError, DSC_Script, text)
        text = '''
simulate: R()
    .: 1
'''
        self.assertRaises(FormatError, DSC_Script, text)

    def testModuleDerivationPass(self):
        # missing exec in derived is okay
        text = text0 + '''
base: R(base=1)
    x: 2
    $out: x
simulate(base): 
    x: R(1:5)
'''
        res = DSC_Script(text)
        self.assertEqual(res.modules['simulate'].dump()['command'], 'base=1')
        text = text0 + '''
base: R(base=1)
    x: 2
    $out: x
simulate(base): R(base=2)
    x: R(1:5)
'''
        res = DSC_Script(text)
        self.assertEqual(res.modules['simulate'].dump()['command'], 'base=2')
        # Derive from one of compact modules
        text = text0 + '''
normal, t: R(), R()
    n: 1000
    @normal:
        y: 5
        n: 6
    $x: x
    
simulate(normal):
    mu: 1
'''
        res = DSC_Script(text)
        self.assertEqual(list(res.modules['simulate'].dump()['input'].items()), [('n', [6]), ('y', [5]), ('mu', [1])])


    def testModuleDerivationFail(self):
        # missing executable
        text = text0 + '''
simulate: 
    x: R{1:5}
    $out: x
'''
        self.assertRaises(FormatError, DSC_Script, text)
        # cannot derive from two modules
        text = '''
normal, t: R(), R()
    n: 1000
    @normal:
        y: 5
        n: 6
    $x: x
    
simulate(normal, t): 
    mu: 1
DSC:
    run: test
'''
        self.assertRaises(FormatError, DSC_Script, text)
        # looped derivation
        text = text0 + '''
normal, t (shifted_normal): R(), R()
    n: 1000
    @normal:
        y: 5
        n: 6
    $x: x
    
shifted_normal(normal):
    mu: 1
'''        
        self.assertRaises(FormatError, DSC_Script, text)
        # non-existing base
        text = text0 + '''
base: R()
    x: 2
    $out: x
simulate(base1): 
    x: R(1:5)
'''
        self.assertRaises(FormatError, DSC_Script, text)

    def testGroupedParametersPass(self):
        # grouped parameters
        text = text0 + '''
simulate: R()
    (n,p): (1,2), (5,6)
    a,b: (3,4)
    t: 5
    $x: x
'''
        res = DSC_Script(text)
        self.assertEqual(list(res.modules['simulate'].dump()['input'].items()), [('n', [1, 5]), ('p', [2, 6]), ('a', [3]), ('b', [4]), ('t', [5])])

    def testParenthesisOperatorPass(self):
        # () operator
        # FIXME: likely wrong here?
        text = text0 + '''
simulate: R()
    (n,p): (1,2), (5,6)
    $x: x
'''
        res = DSC_Script(text)
        self.assertEqual(list(res.modules['simulate'].dump()['input'].items()), [('n', [1, 5]), ('p', [2, 6])])
        # R{} operator
        text = text0 + '''
simulate: R()
    (n,p): R{list(c(1,2), c(5,6))}
    $x: x
'''
        res = DSC_Script(text)
        self.assertEqual(list(res.modules['simulate'].dump()['input'].items()), [('n', [1, 5]), ('p', [2, 6])])

    def testConfOperatorPass(self):
        text = text0 + '''
simulate: R()
    $x: x
    @CONF: work_dir = /tmp, exec_path = /tmp
'''
        res = DSC_Script(text)
        self.assertEqual(res.modules['simulate'].path, ['/tmp'])
        text = text0 + '''
simulate: R()
    $x: x
    @CONF: work_dir = /tmp, exec_path = (/tmp, ~/tmp), lib_path = ./, R_libs = (ashr@stephenslab/ashr (>=2.2.7), psych)
'''
        res = DSC_Script(text)
        self.assertEqual(res.modules['simulate'].path, ['/tmp', '~/tmp'])
        self.assertEqual(res.modules['simulate'].exe['header'], 'library(ashr)\nlibrary(psych)')

    def testModuleVariablesFail(self):
        # multiple input / output
        text = text0 + '''
simulate: R()
    $x: x, 7
'''
        self.assertRaises(FormatError, DSC_Script, text)
        text = text0 + '''
simulate: R()
    x: $x, y
    $x: x
'''
        self.assertRaises(FormatError, DSC_Script, text)

    def testModuleVariablesPass(self):
        # module input and output can have same name
        text = text0 + '''
simulate: R()
    n: $n
    $n: n
'''
        res = DSC_Script(text)
        self.assertEqual(list(res.modules['simulate'].dump()['input'].items()), [('n', ['$n'])])
        self.assertEqual(res.modules['simulate'].dump()['output_variables'], {'n':'n'})

    def testDuplicatesFail(self):
        # various duplicates
        text = text0 + '''
simulate: R()
    $x: x

simulate, simulate: R()
    $x: 7
'''
        self.assertRaises(FormatError, DSC_Script, text)
        text = text0 + '''
simulate: R()
    $x: x

simulate: R()
    $x: 7
''' 
        self.assertWarns(UserWarning, DSC_Script, text)
        text = text0 + '''
simulate: R()
    $x: x

t,t: R()
    $x: 7
'''
        self.assertRaises(FormatError, DSC_Script, text)
        text = text0 + '''
simulate: R()
    $x: x

t,simulate: R()
    $x: 7
'''
        self.assertRaises(FormatError, DSC_Script, text)
        text = text0 + '''
simulate: R()
    $x: x
    $x: y

t: R()
    $x: 7
'''
        self.assertRaises(FormatError, DSC_Script, text)

    def testFilterFail(self):
        # bad condition
        text = text0 + '''
simulate: R()
    n: $y
    $x: x
    @FILTER: n < 3
'''
        self.assertRaises(FormatError, DSC_Script, text)

    def testFilterPass(self):
        text = text0 + '''
simulate, t: R(), R()
    n: 100, 200, 300, 400, 500
    k: 0, 1
    @FILTER:
        simulate: (n <= 300 and k = 0) or (n > 300 and k = 1)
        t: n = 500
    $x: x
'''
        res = DSC_Script(text)
        self.assertEqual(res.modules['simulate'].dump()['input_filter'], '(_n <= 300 and _k == 0) or (_n > 300 and _k == 1)')
        text = text0 + '''
simulate, t: R(), R()
    n: 100, 200, 300, 400, 500
    k: 0, 1
    @FILTER: (n in [100,200,300] and k = 0)
    $x: x
'''
        res = DSC_Script(text)
        self.assertEqual(res.modules['simulate'].dump()['input_filter'], '(_n in (100,200,300) and _k == 0)')
        text = text0 + '''
simulate, t: R(), R()
    n: 100, 200, 300, 400, 500
    k: 0, 1
    @FILTER: 
        *: n in [100,200,300]
        t: n = 300
    $x: x
'''
        res = DSC_Script(text)
        self.assertEqual(res.modules['simulate'].dump()['input_filter'], '(_n in (100,200,300))')

    def testParameterModuleConflict(self):
        # parameter name conflict with output
        text = text0 + '''
simulate: R()
    n: 100
    $n: n
'''
        self.assertRaises(FormatError, DSC_Script, text)
        # parameter name conflict with output
        text = text0 + '''
simulate: R()
    n: 100
    a: 5
    @ALIAS: m = n, m = a
    $out: n
'''
        self.assertRaises(FormatError, DSC_Script, text)

    def testFileOperatorPass(self):
        # file() operator
        text = text0 + '''
simulate: R()
    data: file(.txt)
    $out: n
'''
        res = DSC_Script(text)
        self.assertEqual(res.modules['simulate'].dump()['plugin_status']['temp_file'], ['data <- paste0(${_output[0]:nr}, ".data.txt")'])


    def testFileOperatorFail(self):
        # file() operator misuage
        text = text0 + '''
simulate: R()
    data: file(.txt), 1
    $out: n
'''
        self.assertRaises(FormatError, DSC_Script, text)
        # file() operator misuage
        text = text0 + '''
simulate: R()
    data: (file(.txt), 1)
    $out: n
'''
        self.assertRaises(FormatError, DSC_Script, text)
        # bad output
        text = text0 + '''
simulate: R()
    data: file(.txt)
    $out: data, 1
'''
        self.assertRaises(FormatError, DSC_Script, text)
        # bad output
        text = text0 + '''
simulate: R()
    data: file(.txt)
    $out: (data, 1)
'''
        self.assertRaises(FormatError, DSC_Script, text)

    def testQuotedKeywords(self):
        text = text0 + '''
simulate: R()
    K: TRUE, FALSE, NULL
    $out: K
'''
        res = DSC_Script(text)
        self.assertEqual(res.modules['simulate'].dump()['input']['K'], ['TRUE', 'FALSE', 'NULL'])
        text = text0 + '''
simulate: Python()
    K: 'TRUE', 'FALSE', 'NULL'
    $out: K
'''
        res = DSC_Script(text)
        self.assertEqual(res.modules['simulate'].dump()['input']['K'], ["'TRUE'", "'FALSE'", "'NULL'"])


if __name__ == '__main__':
    #suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestParser)
    # unittest.TextTestRunner(, suite).run()
    unittest.main()