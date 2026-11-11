import unittest
from datahoover.research.edition_guard import assess
class GuardTests(unittest.TestCase):
 def setUp(self):
  self.r={'source_url':'https://www.bls.gov/example','period':'2020-04','scope':'headline_unemployment_rate','value_identity':'14.7 percent, SA, age16+','reissue_date':'2020-09-23'}
  self.e={**self.r,'basis':'independently_demonstrated_historical_edition','review_status':'accepted','edition_id':'synthetic-example-not-actual-proof','provenance_reference':'synthetic-test-only','available_utc':'2020-05-08T12:30:00Z'}
 def ok(self,e=None,origin='2020-05-08T12:30:00Z',r=None):return assess(r or self.r,'historical_asof',origin,e)['evidence_contract_satisfied']
 def test_current_descriptive(self):self.assertTrue(assess(self.r,'descriptive_current')['evidence_contract_satisfied'])
 def test_no_original_edition_proof(self):self.assertFalse(self.ok())
 def test_whole_document_backdate(self):self.assertFalse(self.ok(r={**self.r,'scope':'whole_document'}))
 def test_unaffected_is_not_edition(self):self.assertFalse(self.ok({**self.e,'basis':'publisher_correction_unaffected'}))
 def test_date_only_reissue(self):self.assertFalse(self.ok({**self.e,'available_utc':'2020-09-23'},'2020-09-23T23:59:59Z'))
 def test_naive_clock(self):self.assertFalse(self.ok({**self.e,'available_utc':'2020-05-08T08:30:00'}))
 def test_cutoff_before_release(self):self.assertFalse(self.ok(self.e,'2020-05-08T12:29:59Z'))
 def test_field_scope_mismatch(self):self.assertFalse(self.ok({**self.e,'scope':'payroll'}))
 def test_value_mismatch(self):self.assertFalse(self.ok({**self.e,'value_identity':'14.8'}))
 def test_unreviewed(self):self.assertFalse(self.ok({**self.e,'review_status':'pending'}))
 def test_missing_provenance(self):self.assertFalse(self.ok({**self.e,'provenance_reference':None}))
 def test_possible_separate_edition_route(self):self.assertTrue(self.ok(self.e))
 def test_no_action_authorization(self):self.assertFalse(assess(self.r,'historical_asof','2020-05-08T12:30:00Z',self.e)['action_permission'])
 def test_capture_does_not_backdate(self):self.assertFalse(self.ok({**self.e,'available_utc':'2026-11-11T09:00:00Z'}))
 def test_unknown_mode(self):self.assertFalse(assess(self.r,'production')['evidence_contract_satisfied'])
if __name__=='__main__':unittest.main(verbosity=2)
