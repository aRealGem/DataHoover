"""Offline exact-calendar join; no acquisition, imputation, model or action authority."""
from decimal import Decimal, InvalidOperation
from datetime import datetime
import calendar, re
SPECS = {'cpi_u': {'source': 'bls', 'identity': 'CUUR0000SA0', 'unit': 'index', 'seasonal_adjustment': 'not_seasonally_adjusted', 'base_period': '1982-84=100'}, 'unemployment_rate': {'source': 'bls', 'identity': 'LNS14000000', 'unit': 'percent', 'seasonal_adjustment': 'seasonally_adjusted', 'base_period': None}, 'treasury_avg_interest_rate': {'source': 'treasury', 'identity': 'Total Interest-bearing Debt', 'unit': 'percent', 'seasonal_adjustment': 'not_reported_in_held_source', 'base_period': None}}

def month_id(s):
    if not isinstance(s, str) or not re.fullmatch('\\d{4}-(0[1-9]|1[0-2])', s):
        raise ValueError('invalid monthly period')
    y, m = map(int, s.split('-'))
    return y * 12 + m - 1

def months(start, end):
    a, b = (month_id(start), month_id(end))
    if a > b:
        raise ValueError('reversed range')
    return [f'{i // 12:04d}-{i % 12 + 1:02d}' for i in range(a, b + 1)]

def validate_row(r, key):
    spec = SPECS[key]
    month_id(r['period'])
    if r['unit'] != spec['unit']:
        raise ValueError('unit mismatch')
    if spec['source'] == 'bls':
        if r['series_id'] != spec['identity'] or r['seasonal_adjustment'] != spec['seasonal_adjustment'] or r['base_period'] != spec['base_period']:
            raise ValueError('BLS metadata mismatch')
        raw = r['source_record']
        period = raw['year'] + '-' + raw['period'][1:]
        if not re.fullmatch('M(0[1-9]|1[0-2])', raw['period']) or period != r['period'] or raw['value'] != r['raw_value']:
            raise ValueError('BLS source mismatch')
        if r['release_timestamp'] is not None or r['historical_asof_available'] is not False:
            raise ValueError('unexpected historical evidence')
        status = r['status']
        if status == 'source_missing_placeholder':
            if r['value_decimal'] is not None or r['raw_value'].strip() not in {'', '-', '--', '.', 'N/A', 'NA', '(NA)'}:
                raise ValueError('invalid missing placeholder')
        elif status != 'observed':
            raise ValueError('unknown BLS status')
    else:
        raw = r['source_record']
        if r['security_desc'] != spec['identity'] or raw['security_desc'] != spec['identity'] or raw['security_type_desc'] != 'Interest-bearing Debt':
            raise ValueError('Treasury category mismatch')
        y, m = map(int, r['period'].split('-'))
        end = f'{y:04d}-{m:02d}-{calendar.monthrange(y, m)[1]:02d}'
        if r['observation_date'] != end or raw['record_date'] != end or raw['avg_interest_rate_amt'] != r['raw_value']:
            raise ValueError('Treasury source mismatch')
        if r['first_publication_at_utc'] is not None or r['historical_asof_eligible'] is not False:
            raise ValueError('unexpected historical evidence')
        status = 'observed'
    if status == 'observed':
        if not isinstance(r['value_decimal'], str) or not isinstance(r['raw_value'], str):
            raise ValueError('observed decimals must be strings, never floats or integers')
        try:
            d = Decimal(r['value_decimal'])
            raw = Decimal(r['raw_value'])
        except (InvalidOperation, TypeError):
            raise ValueError('invalid number')
        if not d.is_finite() or not raw.is_finite() or d != raw:
            raise ValueError('nonfinite or inconsistent number')
    clocks = []
    for field in ['capture_started_utc', 'capture_finished_utc']:
        d = datetime.fromisoformat(r[field].replace('Z', '+00:00'))
        clocks.append(d)
        if d.tzinfo is None or d.utcoffset() is None:
            raise ValueError('capture time must be aware')
    if clocks[0] > clocks[1]:
        raise ValueError('capture order')
    return status

def build(bls, treasury, start='2001-01', end='2026-10'):
    lookup = {key: {} for key in SPECS}
    statuses = {key: {} for key in SPECS}
    for source, rows in [('bls', bls), ('treasury', treasury)]:
        for r in rows:
            if source == 'bls':
                matches = [k for k, s in SPECS.items() if s['source'] == source and s['identity'] == r['series_id']]
                if len(matches) != 1:
                    raise ValueError('unexpected BLS identity')
                key = matches[0]
            else:
                key = 'treasury_avg_interest_rate'
            status = validate_row(r, key)
            if r['period'] in lookup[key]:
                raise ValueError('duplicate source-period')
            lookup[key][r['period']] = r
            statuses[key][r['period']] = status
    out = []
    for period in months(start, end):
        cells = {}
        for key, spec in SPECS.items():
            r = lookup[key].get(period)
            cells[key] = {'value_decimal': r['value_decimal'] if r else None, 'status': statuses[key][period] if r else 'absent_source_record', 'unit': spec['unit'], 'seasonal_adjustment': spec['seasonal_adjustment'], 'base_period': spec['base_period'], 'source_row': r}
        numeric = sum((c['status'] == 'observed' for c in cells.values()))
        out.append({'period': period, 'cells': cells, 'numeric_cell_count': numeric, 'complete_numeric_row': numeric == len(SPECS), 'historical_asof_eligible': False})
    return out
