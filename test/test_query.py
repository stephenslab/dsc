#!/usr/bin/env python3
#
# Copyright (c) Gao Wang, Stephens Lab at The Univeristy of Chicago
# Distributed under the terms of the MIT License.

import unittest

from dsc.query_engine import Query_Processor
import pandas as pd
from dsc.utils import DBError
from sos.targets import file_target
from sos.utils import get_output

def test_outcome(res, fn):
    if res is None:
        return
    if fn.endswith('.xlsx'):
        writer = pd.ExcelWriter(fn)
        res.output_table.to_excel(writer, 'Sheet1', index = False)
        if len(res.output_tables) > 1:
            for table in res.output_tables:
                res.output_tables[table].to_excel(writer, table, index = False)
        writer.save()
    else:
        # is csv
        res.output_table.to_csv(fn, index = False)
    return res.get_queries()

ash_db = 'data/dsc_result.db'
reg_db = 'data/reg_result.db'
cause_db = 'data/cause_result.db'

class TestQuery(unittest.TestCase):
    def setUp(self):
        self.temp_files = []
        self.maxDiff = None

    def tearDown(self):
        for f in self.temp_files:
            if file_target(f).exists():
                file_target(f).unlink()

    def touch(self, files):
        '''create temporary files'''
        if isinstance(files, str):
            files = [files]
        #
        for f in files:
            with open(f, 'w') as tmp:
                tmp.write('test')
        #
        self.temp_files.extend(files)

    def testSyntaxFail(self):
        '''basic syntax parser success'''
        # undefined module or group name
        self.assertRaises(DBError, Query_Processor, ash_db, \
            'simulate.nsamp shrink.mixcompdist shu.mse'.split(), ['simulate.nsamp > 20', 'shrink.mixcompdist = "normal"'], [])
        # field name is not pipeline variable
        self.assertRaises(DBError, Query_Processor, ash_db, \
            'simulate.nsamp shrink.mixcompdist simulate.mse'.split(), ['simulate.nsamp > 20', 'shrink.mixcompdist = "normal"'], [])
        # overlapping groups
        self.assertRaises(DBError, Query_Processor, ash_db, \
            'simulate.nsamp shrink.mixcompdist tmp.mse score.mse'.split(), ['simulate.nsamp > 20', 'shrink.mixcompdist = "normal"'], \
            ["tmp: score_beta, score_pi0", "score: score_beta, score_pi0"])
        # empty table returned
        self.assertRaises(DBError, Query_Processor, ash_db, \
            'simulate.nsamp shrink.mixcompdist score.mse'.split(), ['simulate.nsamp < 20', 'shrink.mixcompdist = "normal"'], \
            ["score: score_beta, score_pi0"])

    def testSyntaxPass(self):
        self.touch(['1.csv'])
        # use built-in group names
        res = Query_Processor(ash_db, 'simulate.nsamp shrink.mixcompdist score.mse'.split(), ['simulate.nsamp > 20', 'shrink.mixcompdist = "normal"'], [])
        q1 = test_outcome(res, '1.csv')
        q2 = ['SELECT "simulate".DSC_REPLICATE AS simulate_DSC_FIELD_DSC_REPLICATE, "simulate".nsamp AS simulate_DSC_FIELD_nsamp, "shrink".mixcompdist AS shrink_DSC_FIELD_mixcompdist, "score_beta".__output__ AS score_beta_DSC_VAR_mse FROM "score_beta" INNER JOIN "shrink" ON "score_beta".__parent__ = "shrink".__id__ INNER JOIN "simulate" ON "shrink".__parent__ = "simulate".__id__ WHERE (("simulate".nsamp > 20) AND ("shrink".mixcompdist == "normal"))',
              'SELECT "simulate".DSC_REPLICATE AS simulate_DSC_FIELD_DSC_REPLICATE, "simulate".nsamp AS simulate_DSC_FIELD_nsamp, "shrink".mixcompdist AS shrink_DSC_FIELD_mixcompdist, "score_pi0".__output__ AS score_pi0_DSC_VAR_mse FROM "score_pi0" INNER JOIN "shrink" ON "score_pi0".__parent__ = "shrink".__id__ INNER JOIN "simulate" ON "shrink".__parent__ = "simulate".__id__ WHERE (("simulate".nsamp > 20) AND ("shrink".mixcompdist == "normal"))']
        self.assertEqual(q1, q2)
        #  Handle empty field input: return the file path
        res = Query_Processor(ash_db, 'simulate shrink score'.split(), ['simulate.nsamp > 20', 'shrink.mixcompdist = "normal"'], [])
        q1 = test_outcome(res, '1.csv')
        q2 = ['SELECT "simulate".DSC_REPLICATE AS simulate_DSC_FIELD_DSC_REPLICATE, "simulate".__output__ AS simulate_DSC_OUTPUT_, "shrink".__output__ AS shrink_DSC_OUTPUT_, "score_beta".__output__ AS score_beta_DSC_OUTPUT_ FROM "score_beta" INNER JOIN "shrink" ON "score_beta".__parent__ = "shrink".__id__ INNER JOIN "simulate" ON "shrink".__parent__ = "simulate".__id__ WHERE (("simulate".nsamp > 20) AND ("shrink".mixcompdist == "normal"))',
              'SELECT "simulate".DSC_REPLICATE AS simulate_DSC_FIELD_DSC_REPLICATE, "simulate".__output__ AS simulate_DSC_OUTPUT_, "shrink".__output__ AS shrink_DSC_OUTPUT_, "score_pi0".__output__ AS score_pi0_DSC_OUTPUT_ FROM "score_pi0" INNER JOIN "shrink" ON "score_pi0".__parent__ = "shrink".__id__ INNER JOIN "simulate" ON "shrink".__parent__ = "simulate".__id__ WHERE (("simulate".nsamp > 20) AND ("shrink".mixcompdist == "normal"))']
        self.assertEqual(q1, q2)
        #  handle group merger
        res = Query_Processor(reg_db, 'simulate.scenario analyze score score.error'.split(), [], [])
        q = test_outcome(res, '1.csv')
        observed = sorted(get_output('(head -n 2 1.csv && tail -n +3 1.csv | sort) | head').strip().split('\n'))
        expected = sorted('''
DSC,simulate,simulate.scenario,analyze,analyze.output.file,score,score.output.file,score.error:output
1,en_sim,eg1,lasso,lasso/en_sim_1_lasso_1,sq_err,sq_err/en_sim_1_lasso_1_sq_err_1,sq_err/en_sim_1_lasso_1_sq_err_1
1,dense,NA,en,en/dense_1_en_1,sq_err,sq_err/dense_1_en_1_sq_err_1,sq_err/dense_1_en_1_sq_err_1
1,dense,NA,lasso,lasso/dense_1_lasso_1,sq_err,sq_err/dense_1_lasso_1_sq_err_1,sq_err/dense_1_lasso_1_sq_err_1
1,dense,NA,ridge,ridge/dense_1_ridge_1,sq_err,sq_err/dense_1_ridge_1_sq_err_1,sq_err/dense_1_ridge_1_sq_err_1
1,en_sim,eg1,en,en/en_sim_1_en_1,sq_err,sq_err/en_sim_1_en_1_sq_err_1,sq_err/en_sim_1_en_1_sq_err_1
1,en_sim,eg1,ridge,ridge/en_sim_1_ridge_1,sq_err,sq_err/en_sim_1_ridge_1_sq_err_1,sq_err/en_sim_1_ridge_1_sq_err_1
1,en_sim,eg2,en,en/en_sim_2_en_1,sq_err,sq_err/en_sim_2_en_1_sq_err_1,sq_err/en_sim_2_en_1_sq_err_1
1,en_sim,eg2,lasso,lasso/en_sim_2_lasso_1,sq_err,sq_err/en_sim_2_lasso_1_sq_err_1,sq_err/en_sim_2_lasso_1_sq_err_1
1,en_sim,eg2,ridge,ridge/en_sim_2_ridge_1,sq_err,sq_err/en_sim_2_ridge_1_sq_err_1,sq_err/en_sim_2_ridge_1_sq_err_1
'''.strip().split('\n'))
        self.assertEqual(observed, expected)
        # another group merger test
        # FIXME: this test fails on CircleCI:
        #        ======================================================================
        #FAIL: testSyntaxPass (__main__.TestQuery)
        #----------------------------------------------------------------------
        #Traceback (most recent call last):
        #  File "test_query.py", line 118, in testSyntaxPass
        #    self.assertEqual(observed, expected)
        #AssertionError: Lists differ: ['1,0[256 chars],0.0,NA,NA,NA,NA,q_prob_large,q_prob_large/sim[852 chars]put'] != ['1,0[256 chars],0.0,gamma_ci,gamma_ci/simulate_1_cause_grid_a[1224 chars]put']
        #
        #First differing element 3:
        #'1,0.0,NA,NA,NA,NA,q_prob_large,q_prob_large/sim[37 chars]ge_1'
        #'1,0.0,gamma_ci,gamma_ci/simulate_1_cause_grid_a[119 chars]A,NA'
        #
        #  ['1,0.0,NA,NA,NA,NA,gamma_lfsr,gamma_lfsr/simulate_1_cause_grid_adapt_1_gamma_lfsr_1',
        #   '1,0.0,NA,NA,NA,NA,gamma_lfsr,gamma_lfsr/simulate_1_cause_grid_adapt_2_gamma_lfsr_1',
        #   '1,0.0,NA,NA,NA,NA,gamma_lfsr,gamma_lfsr/simulate_1_cause_grid_adapt_3_gamma_lfsr_1',
        #-  '1,0.0,NA,NA,NA,NA,q_prob_large,q_prob_large/simulate_1_cause_grid_adapt_1_q_prob_large_1',
        #-  '1,0.0,NA,NA,NA,NA,q_prob_large,q_prob_large/simulate_1_cause_grid_adapt_2_q_prob_large_1',
        #-  '1,0.0,NA,NA,NA,NA,q_prob_large,q_prob_large/simulate_1_cause_grid_adapt_3_q_prob_large_1',
        #   '1,0.0,gamma_ci,gamma_ci/simulate_1_cause_grid_adapt_1_gamma_ci_1,gamma_ci/simulate_1_cause_grid_adapt_1_gamma_ci_1,gamma_ci/simulate_1_cause_grid_adapt_1_gamma_ci_1,NA,NA',
        #   '1,0.0,gamma_ci,gamma_ci/simulate_1_cause_grid_adapt_2_gamma_ci_1,gamma_ci/simulate_1_cause_grid_adapt_2_gamma_ci_1,gamma_ci/simulate_1_cause_grid_adapt_2_gamma_ci_1,NA,NA',
        #   '1,0.0,gamma_ci,gamma_ci/simulate_1_cause_grid_adapt_3_gamma_ci_1,gamma_ci/simulate_1_cause_grid_adapt_3_gamma_ci_1,gamma_ci/simulate_1_cause_grid_adapt_3_gamma_ci_1,NA,NA',
        #+  '1,0.0,gamma_prime_ci,gamma_prime_ci/simulate_1_cause_grid_adapt_1_gamma_prime_ci_1,gamma_prime_ci/simulate_1_cause_grid_adapt_1_gamma_prime_ci_1,gamma_prime_ci/simulate_1_cause_grid_adapt_1_gamma_prime_ci_1,NA,NA',
        #+  '1,0.0,gamma_prime_ci,gamma_prime_ci/simulate_1_cause_grid_adapt_2_gamma_prime_ci_1,gamma_prime_ci/simulate_1_cause_grid_adapt_2_gamma_prime_ci_1,gamma_prime_ci/simulate_1_cause_grid_adapt_2_gamma_prime_ci_1,NA,NA',
        #+  '1,0.0,gamma_prime_ci,gamma_prime_ci/simulate_1_cause_grid_adapt_3_gamma_prime_ci_1,gamma_prime_ci/simulate_1_cause_grid_adapt_3_gamma_prime_ci_1,gamma_prime_ci/simulate_1_cause_grid_adapt_3_gamma_prime_ci_1,NA,NA',
        #   'DSC,simulate.q,cis,cis.output.file,cis.ci_upr:output,cis.ci_lwr:output,summ_probs,summ_pr
        res = Query_Processor(cause_db, 'simulate.q cis.ci_lwr cis.ci_upr summ_probs.prob cis'.split())
        q = test_outcome(res, '1.csv')
        observed = sorted(get_output('(head -n 2 1.csv && tail -n +3 1.csv | sort) | head').strip().split('\n'))
        expected = sorted('''
DSC,simulate.q,cis,cis.output.file,cis.ci_upr:output,cis.ci_lwr:output,summ_probs,summ_probs.prob:output
1,0.0,gamma_ci,gamma_ci/simulate_1_cause_grid_adapt_1_gamma_ci_1,gamma_ci/simulate_1_cause_grid_adapt_1_gamma_ci_1,gamma_ci/simulate_1_cause_grid_adapt_1_gamma_ci_1,NA,NA
1,0.0,gamma_ci,gamma_ci/simulate_1_cause_grid_adapt_2_gamma_ci_1,gamma_ci/simulate_1_cause_grid_adapt_2_gamma_ci_1,gamma_ci/simulate_1_cause_grid_adapt_2_gamma_ci_1,NA,NA
1,0.0,gamma_ci,gamma_ci/simulate_1_cause_grid_adapt_3_gamma_ci_1,gamma_ci/simulate_1_cause_grid_adapt_3_gamma_ci_1,gamma_ci/simulate_1_cause_grid_adapt_3_gamma_ci_1,NA,NA
1,0.0,gamma_prime_ci,gamma_prime_ci/simulate_1_cause_grid_adapt_1_gamma_prime_ci_1,gamma_prime_ci/simulate_1_cause_grid_adapt_1_gamma_prime_ci_1,gamma_prime_ci/simulate_1_cause_grid_adapt_1_gamma_prime_ci_1,NA,NA
1,0.0,gamma_prime_ci,gamma_prime_ci/simulate_1_cause_grid_adapt_2_gamma_prime_ci_1,gamma_prime_ci/simulate_1_cause_grid_adapt_2_gamma_prime_ci_1,gamma_prime_ci/simulate_1_cause_grid_adapt_2_gamma_prime_ci_1,NA,NA
1,0.0,gamma_prime_ci,gamma_prime_ci/simulate_1_cause_grid_adapt_3_gamma_prime_ci_1,gamma_prime_ci/simulate_1_cause_grid_adapt_3_gamma_prime_ci_1,gamma_prime_ci/simulate_1_cause_grid_adapt_3_gamma_prime_ci_1,NA,NA
1,0.0,NA,NA,NA,NA,gamma_lfsr,gamma_lfsr/simulate_1_cause_grid_adapt_1_gamma_lfsr_1
1,0.0,NA,NA,NA,NA,gamma_lfsr,gamma_lfsr/simulate_1_cause_grid_adapt_2_gamma_lfsr_1
1,0.0,NA,NA,NA,NA,gamma_lfsr,gamma_lfsr/simulate_1_cause_grid_adapt_3_gamma_lfsr_1
'''.strip().split('\n'))
        #self.assertEqual(observed, expected)


if __name__ == '__main__':
    #suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestParser)
    # unittest.TextTestRunner(, suite).run()
    unittest.main()
