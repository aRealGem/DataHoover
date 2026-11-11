"""Offline exact-calendar features; no source acquisition or historical replay claims."""
import argparse, hashlib, importlib.util, json, re, tempfile, zipfile
from collections import Counter
from datetime import datetime
from decimal import Decimal, localcontext, ROUND_HALF_EVEN
from fractions import Fraction
from pathlib import Path
META={'CUUR0000SA0':('index','not_seasonally_adjusted','percent_change_NSA_index'), 'LNS14000000':('percent','seasonally_adjusted','percentage_points')}
def require(ok,reason):
 if not ok: raise ValueError(reason)
def digest(b):return hashlib.sha256(b).hexdigest()
def encode(rows):return ''.join(json.dumps(r,sort_keys=True,ensure_ascii=False,allow_nan=False)+'\n' for r in rows)
def month(s):
 require(isinstance(s,str) and re.fullmatch(r'\d{4}-(0[1-9]|1[0-2])',s),'invalid month')
 return int(s[:4])*12+int(s[5:])-1
def month_text(n):return f'{n//12:04d}-{n%12+1:02d}'
def validate(r):
 sid=r['series_id'];require(sid in META,'unexpected series')
 unit,sa,_=META[sid];require(r['unit']==unit and r['seasonal_adjustment']==sa,'unit or adjustment mismatch')
 if sid=='CUUR0000SA0':require(r['base_period']=='1982-84=100','wrong CPI base')
 month(r['period']);require(r['release_timestamp'] is None and r['historical_asof_available'] is False,'historical clock unavailable')
 c=datetime.fromisoformat(r['capture_finished_utc']);require(c.utcoffset() is not None,'capture timezone missing')
 require(r['retrieved_utc']==r['capture_finished_utc'],'capture mismatch')
 require(r['source_record'] is not None and r['source_record']['value']==r['raw_value'],'raw source value mismatch')
 require(r['footnotes']==r['source_record'].get('footnotes',[]),'footnotes mismatch')
 require(r['source_record']['year']==r['period'][:4] and r['source_record']['period']=='M'+r['period'][5:],'source month mismatch')
 for field in ('source_response_sha256','source_capture_sha256','source_request_sha256'):require(re.fullmatch('[0-9a-f]{64}',r[field]) is not None,'provenance hash invalid')
 if r['value_decimal'] is None:
  require(r['status']=='source_missing_placeholder' and r['raw_value'].strip() in {'','-','--','.','N/A','NA','(NA)'},'missing status mismatch')
 else:
  v=Decimal(r['value_decimal']);require(v.is_finite() and v==Decimal(r['raw_value']) and r['status']=='observed','numeric status mismatch')

def transform(rows):
 keyed={};first={}
 for r in rows:
  validate(r);k=(r['series_id'],month(r['period']));require(k not in keyed,'duplicate source period');keyed[k]=r;first[k[0]]=min(first.get(k[0],k[1]),k[1])
 output=[]
 for (sid,n),r in sorted(keyed.items()):
  for lag in (1,12):
   prior=keyed.get((sid,n-lag)); reasons=[]
   if r['value_decimal'] is None:reasons.append('current_source_missing')
   if prior is None:reasons.append('initial_warmup' if n-lag<first[sid] else 'absent_calendar_period')
   elif prior['value_decimal'] is None:reasons.append('comparison_source_missing')
   if prior is not None and prior['value_decimal'] is not None and sid=='CUUR0000SA0' and Decimal(prior['value_decimal'])==0:reasons.append('zero_denominator')
   result=None;decimal=None
   if not reasons:
    cur=Fraction(Decimal(r['value_decimal']));old=Fraction(Decimal(prior['value_decimal']));result=(cur-old)*100/old if sid=='CUUR0000SA0' else cur-old
    with localcontext() as ctx:
     ctx.prec=34;ctx.rounding=ROUND_HALF_EVEN;decimal=str(Decimal(result.numerator)/Decimal(result.denominator))
   captures=[x['capture_finished_utc'] for x in (r,prior) if x is not None]
   output.append({'series_id':sid,'period':r['period'],'lag_months':lag,'comparison_period':month_text(n-lag),'feature_unit':META[sid][2], 'status':'available' if not reasons else 'unavailable','reasons':reasons,'value_decimal_34_digits':decimal,'exact_rational':None if result is None else {'numerator':str(result.numerator),'denominator':str(result.denominator)},'current_endpoint':r,'comparison_endpoint':prior,'latest_endpoint_capture_utc':max(captures,key=datetime.fromisoformat),'release_timestamp':None,'historical_asof_eligible':False})
 return output
