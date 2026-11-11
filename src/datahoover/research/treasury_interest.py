"""Offline Treasury monthly average-rate normalization. No network operations."""
import argparse,calendar,hashlib,json,re
from pathlib import Path
from datetime import date,datetime
from decimal import Decimal
from urllib.parse import urlencode
from collections import Counter
ENDPOINT='https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates'
CATEGORY='Total Interest-bearing Debt'
def require(ok,msg):
 if not ok:raise ValueError(msg)
def sha(b):return hashlib.sha256(b).hexdigest()
def decode(raw):
 def pairs(items):
  out={}
  for k,v in items:require(k not in out,'duplicate JSON field');out[k]=v
  return out
 return json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda x:(_ for _ in ()).throw(ValueError('nonfinite JSON')))
def month_id(period):return int(period[:4])*12+int(period[5:7])-1
def month_name(n):return f'{n//12:04d}-{n%12+1:02d}'
def monthend(p):
 y,m=map(int,p.split('-'));return f'{p}-{calendar.monthrange(y,m)[1]:02d}'
def encoded(rows):return ''.join(json.dumps(r,sort_keys=True,allow_nan=False)+'\n' for r in rows)
def normalize(payload,capture,request,capture_hash,request_hash):
 require(capture.get('http_status')==200 and capture.get('complete_body') is True and not capture.get('error'),'incomplete capture')
 require(capture['final_url']==capture['url']==request['url'] and capture['url'].startswith(ENDPOINT+'?'),'capture destination mismatch')
 require(capture['method']==request['method']=='GET' and capture['request_sha256']==request_hash,'request mismatch')
 lower,upper=map(date.fromisoformat,request['interval_inclusive'])
 require(lower<=upper,'reversed date interval')
 expected={'filter':f'security_desc:eq:Total Interest-bearing Debt,record_date:gte:{lower},record_date:lte:{upper}','sort':'record_date','format':'json','page[number]':'1','page[size]':'1000'}
 require(request['url']==ENDPOINT+'?'+urlencode(expected),'request URL/scope mismatch')
 require(capture['network_attempts']==1 and capture['retries']==0 and capture['redirects_enabled'] is False and capture['max_response_bytes']==1000000 and capture['total_timeout_seconds']==30,'acquisition bounds mismatch')
 require(request['parameters']==expected,'request scope mismatch')
 clocks=[datetime.fromisoformat(capture[k]) for k in ('started_utc','finished_utc')];require(all(x.utcoffset() is not None for x in clocks) and clocks[0]<=clocks[1],'capture clock mismatch')
 rows=payload['data'];meta=payload['meta'];links=payload['links']
 require(meta['count']==meta['total-count']==len(rows) and meta['total-pages']==1 and links['next'] is None and links['prev'] is None,'incomplete pagination')
 require(meta['dataTypes']['avg_interest_rate_amt']=='PERCENTAGE' and meta['dataFormats']['avg_interest_rate_amt']=='10.2%','percentage units mismatch')
 require(meta['labels']['avg_interest_rate_amt']=='Average Interest Rate Amount','measure label mismatch')
 out=[];seen=set()
 for i,r in enumerate(rows):
  d=date.fromisoformat(r['record_date']);p=d.strftime('%Y-%m');require(r['record_date']==monthend(p),'not month end')
  require(lower<=d<=upper,'outside request interval');require(p not in seen,'duplicate month');seen.add(p)
  require(r['security_desc']==CATEGORY and r['security_type_desc']=='Interest-bearing Debt','category mismatch')
  require(isinstance(r['avg_interest_rate_amt'],str) and re.fullmatch(r'-?\d+(\.\d+)?',r['avg_interest_rate_amt']) is not None,'null/invalid rate')
  value=Decimal(r['avg_interest_rate_amt']);require(value.is_finite(),'nonfinite rate')
  require(r['record_calendar_year']==str(d.year) and int(r['record_calendar_month'])==d.month and int(r['record_calendar_day'])==d.day and int(r['record_calendar_quarter'])==(d.month-1)//3+1,'calendar fields mismatch')
  require(int(r['record_fiscal_year'])==d.year+(d.month>=10) and int(r['record_fiscal_quarter'])==((d.month-10)%12)//3+1,'fiscal fields mismatch')
  require(r['src_line_nbr'].isdigit(),'source line invalid')
  out.append({'period':p,'observation_date':r['record_date'],'security_desc':CATEGORY,'value_decimal':str(value),'raw_value':r['avg_interest_rate_amt'],'unit':'percent','measure':'average interest rate on total interest-bearing Treasury debt','source_record':r,'raw_row_index':i,'raw_response_sha256':capture['response_sha256'],'request_sha256':request_hash,'capture_metadata_sha256':capture_hash,'capture_started_utc':capture['started_utc'],'capture_finished_utc':capture['finished_utc'],'first_publication_at_utc':None,'historical_asof_eligible':False,'vintage_basis':'latest_at_fresh_retrieval'})
 require([r['period'] for r in out]==sorted(seen),'source dates not ascending')
 expected_months=[month_name(n) for n in range(month_id(str(lower)[:7]),month_id(str(upper)[:7])+1) if lower<=date.fromisoformat(monthend(month_name(n)))<=upper]
 absent=[{'period':p,'expected_month_end':monthend(p),'status':'not_returned_in_requested_interval','source_record':None,'reason_not_inferred':True} for p in expected_months if p not in seen]
 return out,absent

def features(rows):
 keyed={r['period']:r for r in rows};require(len(keyed)==len(rows),'duplicate feature input month');first=min(month_id(p) for p in keyed);out=[]
 for r in rows:
  require(r['unit']=='percent' and r['historical_asof_eligible'] is False and r['first_publication_at_utc'] is None,'feature unit/clock mismatch')
  for lag in (1,12):
   p=month_name(month_id(r['period'])-lag);old=keyed.get(p);status='available' if old else ('initial_warmup' if month_id(p)<first else 'absent_calendar_period')
   value=str(Decimal(r['value_decimal'])-Decimal(old['value_decimal'])) if old else None
   out.append({'period':r['period'],'lag_months':lag,'comparison_period':p,'status':status,'value_decimal':value,'unit':'percentage_points','current_endpoint':r,'comparison_endpoint':old,'latest_endpoint_capture_utc':max([x['capture_finished_utc'] for x in (r,old) if x],key=datetime.fromisoformat),'first_publication_at_utc':None,'historical_asof_eligible':False})
 return out


def captured_rows(raw, capture_raw, request_raw):
 """Verify retained bytes before normalization; does not acquire or write data."""
 cap=decode(capture_raw);req=decode(request_raw)
 require(len(raw)==cap.get('response_bytes') and sha(raw)==cap.get('response_sha256'),'raw bytes/hash mismatch')
 require(len(raw)<1000000,'response cap exceeded')
 return normalize(decode(raw),cap,req,sha(capture_raw),sha(request_raw))
