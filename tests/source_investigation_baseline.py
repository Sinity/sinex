#!/usr/bin/env python3
"""Read supplied timestamped acquisition journal and the exact provider, without Sinex."""
import datetime as dt
import json
from pathlib import Path
import sqlite3
import sys
import urllib.parse

journal=json.loads(Path(sys.argv[1]).read_text())
provider=Path(sys.argv[2])
def read_provider(path):
    try:
        with sqlite3.connect('file:'+urllib.parse.quote(str(path))+'?mode=ro',uri=True) as con:
            ids=[r[0] for r in con.execute('SELECT id FROM history ORDER BY id')]
        return {'readable':True,'native_ids':ids}
    except sqlite3.Error:
        return {'readable':False,'native_ids':None}
direct=read_provider(provider)
retained=read_provider(Path(sys.argv[3])) if len(sys.argv)>3 else None

# Independent journal/provider route. It receives observed outcomes, clocks and
# retained membership, not a native acquisition-state answer or product modules.
journey={}
for fact in journal:
    if fact['kind']!='snapshot':continue
    status=fact['status'];schedule=fact['schedule']
    state=status if fact['current_scope'] else 'unknown'
    if state=='completed' and schedule:
        now=dt.datetime.fromisoformat(fact['snapshot_at'].replace('Z','+00:00'))
        due=dt.datetime.fromisoformat(schedule['next_start_at'].replace('Z','+00:00'))
        if now>due:state='overdue'
    if state=='overdue':since={'kind':'planned_next_start','value':schedule['next_start_at']}
    elif schedule:since={'kind':'outcome_observed_at','value':schedule['settled_at']}
    else:since={'kind':'attempt_started_at','value':fact['attempt_started_at']}
    reason=fact['reason'] if state in ('unavailable','interrupted') else ('unknown' if state=='overdue' else None)
    journey[fact['phase']]={'working':state,'why':reason,'since':since,
        'recovered_native_ids':fact['retained_receipt_native_ids'],'historical_coverage':'unknown'}
recoveries=[n for n in journal if n['kind']=='catchup_retained']
answer=dict(list(journey.values())[-1]) if journey else {'working':'unknown','historical_coverage':'unknown'}
answer['recovered_native_ids']=[n['native_id'] for n in recoveries]
answer['recovered_interpretations']=[i for n in recoveries for i in n['new_interpretations']]
print(json.dumps({'provider':direct,'retained_fixture_provider':retained,'answers':answer,'journey':journey,'journal_records':len(journal),
    'limits':'Answers describe the supplied original-collector snapshots. Current provider readability alone proves no acquisition outcome, present collector applicability or historical completeness; recovered membership comes from supplied retained receipts.'}))
