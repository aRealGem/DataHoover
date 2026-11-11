"""Offline evidence contract. Does not grant action, production or holdout permission."""
from datetime import datetime

def instant(value):
    if not isinstance(value,str) or 'T' not in value:
        raise ValueError('exact timezone-aware timestamp required')
    d=datetime.fromisoformat(value.replace('Z','+00:00'))
    if d.tzinfo is None or d.utcoffset() is None:
        raise ValueError('timezone required')
    return d

def assess(record, mode, origin=None, evidence=None):
    """Evidence is reviewer-supplied, never derived from URL, header or notice alone.

    For a historical field, proof must bind source, period, field and exact value
    to a demonstrated edition and its actual availability. A document proof must
    bind that full edition. Fingerprinting can help, but cryptographic immutability
    is not a universal prerequisite. This function checks contract completeness,
    not whether outside testimony is true; independent review remains necessary.
    """
    deny=lambda why: {'evidence_contract_satisfied':False,'reason':why,'action_permission':False}
    if mode=='descriptive_current':
        return {'evidence_contract_satisfied':True,'reason':'latest descriptive use only','action_permission':False}
    if mode!='historical_asof':return deny('unknown usage mode')
    try: cutoff=instant(origin)
    except (ValueError,TypeError):return deny('origin requires exact aware timestamp')
    if not evidence:return deny('no independently reviewed edition evidence; header/reissue/capture are not interchangeable')
    keys=('source_url','period','scope','value_identity')
    if any(not record.get(k) or evidence.get(k)!=record[k] for k in keys):
        return deny('evidence must bind exact source, period, scope and value or full-document identity')
    if evidence.get('basis')!='independently_demonstrated_historical_edition' or evidence.get('review_status')!='accepted':
        return deny('requires reviewed historical edition, not unchanged headline or dated URL')
    if not evidence.get('edition_id') or not evidence.get('provenance_reference'):
        return deny('missing edition identity or inspectable provenance')
    try: available=instant(evidence.get('available_utc'))
    except (ValueError,TypeError):return deny('edition availability requires exact aware timestamp, not reissue date')
    if available>cutoff:return deny('edition not available at origin')
    return {'evidence_contract_satisfied':True,'reason':'reviewed scoped edition contract satisfied; not an action authorization','action_permission':False}
