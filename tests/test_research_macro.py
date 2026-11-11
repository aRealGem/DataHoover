"""Small fabricated records: no retained histories or private receipts required."""
import calendar
import copy
import hashlib
import json
import unittest
from urllib.parse import urlencode
from datahoover.research import bls_features, bls_normalize, monthly_panel, treasury_debt, treasury_interest

STAMP='2001-03-01T00:00:00+00:00'

def bls(sid='CUUR0000SA0', period='2001-01', value='100'):
    missing=value=='-'
    raw={'year':period[:4], 'period':'M'+period[5:], 'periodName':calendar.month_name[int(period[5:])], 'value':value, 'footnotes':[{'text':'synthetic missing'}] if missing else []}
    return {'series_id':sid, 'period':period, **bls_normalize.SERIES_METADATA[sid], 'source_record':raw, 'raw_value':value, 'value_decimal':None if missing else value, 'status':'source_missing_placeholder' if missing else 'observed', 'footnotes':raw['footnotes'], 'release_timestamp':None, 'historical_asof_available':False, 'capture_started_utc':STAMP, 'capture_finished_utc':STAMP, 'retrieved_utc':STAMP, **{k:'0'*64 for k in ('source_response_sha256','source_capture_sha256','source_request_sha256')}}

def interest(period='2001-01', value='5.00'):
    y,m=map(int,period.split('-'));day=f'{period}-{calendar.monthrange(y,m)[1]:02d}'
    return {'period':period, 'observation_date':day, 'security_desc':treasury_interest.CATEGORY, 'unit':'percent', 'raw_value':value, 'value_decimal':value, 'first_publication_at_utc':None, 'historical_asof_eligible':False, 'capture_started_utc':STAMP, 'capture_finished_utc':STAMP, 'source_record':{'record_date':day,'security_desc':treasury_interest.CATEGORY,'security_type_desc':'Interest-bearing Debt','avg_interest_rate_amt':value}}

class MacroTests(unittest.TestCase):
    def test_cpi_percent(self):
        out=bls_features.transform([bls(),bls(period='2001-02',value='110')]);self.assertEqual(out[2]['exact_rational'],{'numerator':'10','denominator':'1'})
    def test_unemployment_points(self):
        out=bls_features.transform([bls('LNS14000000',value='5'),bls('LNS14000000','2001-02','6')]);self.assertEqual(out[2]['feature_unit'],'percentage_points');self.assertEqual(out[2]['exact_rational']['numerator'],'1')
    def test_gap_not_previous_surviving_row(self):
        self.assertIn('absent_calendar_period',bls_features.transform([bls(),bls(period='2001-03')])[2]['reasons'])
    def test_missing_endpoint(self):
        self.assertIn('comparison_source_missing',bls_features.transform([bls(value='-'),bls(period='2001-02')])[2]['reasons'])
    def test_zero_denominator(self):
        self.assertIn('zero_denominator',bls_features.transform([bls(value='0'),bls(period='2001-02')])[2]['reasons'])
    def test_duplicate(self):
        with self.assertRaises(ValueError):bls_features.transform([bls(),bls()])
    def test_unit_rejected(self):
        with self.assertRaises(ValueError):bls_features.transform([{**bls(),'unit':'percent'}])
    def test_capture_not_release(self):
        with self.assertRaises(ValueError):bls_features.transform([{**bls(),'release_timestamp':STAMP}])
    def test_input_unchanged(self):
        r=[bls()];before=copy.deepcopy(r);bls_features.transform(r);self.assertEqual(r,before)
    def test_parser_missing_vs_absent(self):
        payload={'status':'REQUEST_SUCCEEDED','Results':{'series':[{'seriesID':sid,'data':[bls(sid,value='-')['source_record']]} for sid in bls_normalize.SERIES]}}
        rows,annual,report=bls_normalize.normalize(payload,2001,2001,'0'*64,STAMP)
        self.assertEqual(len(rows),24);self.assertEqual(sum(r['status']=='source_missing_placeholder' for r in rows),2);self.assertEqual(sum(r['status']=='absent_from_response' for r in rows),22)
    def test_capture_to_features_and_panel(self):
        req=json.dumps({'seriesid':list(bls_normalize.SERIES),'startyear':'2001','endyear':'2001'}).encode()
        raw=json.dumps({'status':'REQUEST_SUCCEEDED','Results':{'series':[{'seriesID':sid,'data':[bls(sid)['source_record'],bls(sid,'2001-02','110')['source_record']]} for sid in bls_normalize.SERIES]}}).encode()
        cap={'http_status':200,'complete_body':True,'url':'https://api.bls.gov/publicAPI/v1/timeseries/data/','method':'POST','response_sha256':hashlib.sha256(raw).hexdigest(),'request_sha256':hashlib.sha256(req).hexdigest(),'response_bytes':len(raw),'started_utc':STAMP,'finished_utc':STAMP}
        rows,absent,annual,report=bls_normalize.captured_rows(raw,cap,req)
        self.assertEqual(len(absent),20);self.assertEqual(len(bls_features.transform(rows)),8)
        self.assertEqual(monthly_panel.build(rows,[interest()],'2001-01','2001-01')[0]['numeric_cell_count'],3)
        with self.assertRaises(ValueError):bls_normalize.captured_rows(raw+b' ',cap,req)
    def test_parser_reject_nonfinite(self):
        with self.assertRaises(ValueError):bls_normalize.decimal_value('NaN')
    def test_json_duplicate(self):
        with self.assertRaises(ValueError):bls_normalize.load_json('{"x":1,"x":2}')
    def test_panel_no_listwise_deletion(self):
        r=monthly_panel.build([bls(value='-'),bls('LNS14000000',value='-')],[interest()],'2001-01','2001-02')
        self.assertEqual(r[0]['numeric_cell_count'],1);self.assertEqual(r[1]['numeric_cell_count'],0);self.assertEqual(r[0]['cells']['cpi_u']['source_row']['footnotes'],[{'text':'synthetic missing'}])
    def test_panel_duplicate(self):
        with self.assertRaises(ValueError):monthly_panel.build([bls(),bls()],[],'2001-01','2001-01')
    def test_panel_reversed(self):
        with self.assertRaises(ValueError):monthly_panel.build([],[],'2001-02','2001-01')
    def test_interest_points(self):
        self.assertEqual(treasury_interest.features([interest(),interest('2001-02','6.00')])[2]['value_decimal'],'1.00')
    def test_interest_gap(self):
        self.assertEqual(treasury_interest.features([interest(),interest('2001-03')])[2]['status'],'absent_calendar_period')

class DebtTests(unittest.TestCase):
    def fixture(self, value='3.00'):
        params={'fields':','.join(treasury_debt.FIELDS),'format':'json','sort':'record_date','page[number]':'1','page[size]':'31','filter':'record_date:gte:2001-01-01,record_date:lte:2001-01-31'}
        url=treasury_debt.BASE+'?'+urlencode(params)
        req={'method':'GET','credentials':False,'url':url,'parameters':params,'interval_inclusive':['2001-01-01','2001-01-31']}
        raw=json.dumps({'data':[{'record_date':'2001-01-02','debt_held_public_amt':'1.00','intragov_hold_amt':'2.00','tot_pub_debt_out_amt':value}], 'meta':{'count':1,'total-count':1,'total-pages':1,'dataTypes':{'record_date':'DATE',**{k:'CURRENCY' for k in treasury_debt.AMOUNTS}},'dataFormats':{'record_date':'YYYY-MM-DD',**{k:'$10.20' for k in treasury_debt.AMOUNTS}}},'links':{'prev':None,'next':None,**{k:'?page[number]=1&page[size]=31' for k in ('self','first','last')}}}).encode()
        cap={'http_status':200,'complete_body':True,'network_attempts':1,'retries':0,'redirects_enabled':False,'response_bytes':len(raw),'response_sha256':hashlib.sha256(raw).hexdigest(),'elapsed_seconds':1,'finished_utc':STAMP,'url':url,'final_url':url}
        return raw,cap,req
    def test_exact(self):
        n,a,q,r=treasury_debt.normalize(*self.fixture());self.assertEqual(len(a),1);self.assertFalse(q);self.assertFalse(n[0]['historical_as_of_eligible'])
    def test_quarantine_not_repair(self):
        n,a,q,r=treasury_debt.normalize(*self.fixture('4.00'));self.assertFalse(a);self.assertEqual(q[0]['source_values']['tot_pub_debt_out_amt'],'4.00')
    def test_null(self):self.assertEqual(len(treasury_debt.normalize(*self.fixture(None))[2]),1)
    def test_float_rejected(self):
        with self.assertRaises(ValueError):treasury_debt.normalize(*self.fixture(3.0))
    def test_hash(self):
        raw,c,r=self.fixture()
        with self.assertRaises(ValueError):treasury_debt.normalize(raw+b' ',c,r)
    def test_timezone(self):
        raw,c,r=self.fixture();c['finished_utc']='2001-03-01'
        with self.assertRaises(ValueError):treasury_debt.normalize(raw,c,r)

if __name__=='__main__':unittest.main()
