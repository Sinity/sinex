#!/usr/bin/env python3
"""Focused whole-loop measurement boundaries and front-door refusals."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from recovery_costs import proc_fields, reuse_accounting, usage_delta
from recovery_context_cases import fixture, fixture_row, independent_tide_group, valid_tide_cwd
from session_cases import independent


def refuses(fn, *args):
    try:fn(*args)
    except AssertionError:return
    raise AssertionError('negative control accepted')


root=Path(__file__).resolve().parents[1]
for args in [['--recovery-prior-accounting','/synthetic/never-opened.json'],
        ['--recovery-context','--recovery-prior-accounting','/synthetic/never-opened.json'],
        ['--recovery-context','--packaged','--package-pin','/synthetic/never-opened.json','--recovery-prior-accounting','/synthetic/never-opened.json','--atuin']]:
    r=subprocess.run([sys.executable,str(root/'tests/foundation.py'),*args],capture_output=True,text=True)
    assert r.returncode==2 and '"owned_state"' not in r.stdout
# A command name containing parentheses must not shift /proc stat field offsets.
fields=['S']+['0']*21;fields[11]='10';fields[12]='20';fields[19]='1234'
p=proc_fields('1 (synthetic ) command) '+' '.join(fields),'VmRSS: 12 kB\nVmHWM: 34 kB\n')
assert p['start_ticks']==1234 and p['VmRSS']=='12 kB' and p['VmHWM']=='34 kB'
before={'monotonic':1,'self':{'user_seconds':1,'system_seconds':2,'max_rss_kib':80},'reaped_children':{'user_seconds':3,'system_seconds':4,'max_rss_kib':100}}
after={'monotonic':3,'self':{'user_seconds':2,'system_seconds':4,'max_rss_kib':90},'reaped_children':{'user_seconds':6,'system_seconds':8,'max_rss_kib':200}}
d=usage_delta(before,after)
assert d['elapsed_seconds']==2 and d['cpu_seconds']['reaped_children']=={'user_seconds':3,'system_seconds':4}
assert d['raw_after']['reaped_children']['max_rss_kib']==200 and d['simultaneous_aggregate_peak_rss']['value'] is None
# The late bridge changes the selected session from the initial day boundary.
facts=fixture();before_groups=independent(facts);facts.append(fixture_row('late-bridge','03:54:00','/work/tide',12*60*1_000_000_000));after_groups=independent(facts)
assert any(g['parents']==['old'] for g in before_groups)
assert any('old' in g['parents'] and 'positive' in g['parents'] and 'late-bridge' in g['parents'] for g in after_groups)
# Held-out newer sessions distinguish invalid lexical paths and prefix neighbors
# from a real positive own-context session. Shared agreement alone is insufficient.
held_out=[fixture_row('held-out-newer-invalid','07:30:00','/work/tide/../other'),
    fixture_row('held-out-prefix-neighbor','07:45:00','/work/tide-neighbor')]
assert not valid_tide_cwd(held_out[0]['cwd']) and not valid_tide_cwd(held_out[1]['cwd'])
assert all(valid_tide_cwd(p) for p in ['/work/tide','/work/tide/src','/work/tide/..notes','/work/tide/.notes'])
assert all(not valid_tide_cwd(p) for p in ['/work/tide/../other','/work/tide/./src','work/tide/src','/work/tide/\0src'])
original=fixture()
for additions in [[],[fixture_row('late-bridge','03:54:00','/work/tide',12*60*1_000_000_000)],
        [fixture_row('late-bridge','03:54:00','/work/tide',12*60*1_000_000_000),fixture_row('later-positive','08:00:00','/work/tide')]]:
    assert independent_tide_group(original+additions+held_out)==independent_tide_group(original+additions)
assert independent_tide_group(original+held_out)['parents']==['ambiguous-context','invalid-context','missing-context','other-context','positive','unconfigured-context']
with tempfile.TemporaryDirectory(prefix='sinex-whole-loop-check-',dir='/realm/tmp/work') as temporary:
    path=Path(temporary)/'result.json';pin={'package':'/synthetic/immutable','binaries':{'sinexd':{'sha256':'daemon'},'sinexctl':{'sha256':'client'}}}
    report={'success':True,'postgres_stop_exit':0,'candidate_tracked_diff_sha256':hashlib.sha256(b'').hexdigest(),
        'prebuilt_package':{'pin':{'package':pin['package']}},'binary_sha256':{'sinexd':'daemon','sinexctl':'client'},
        'scenarios':[{'name':'three held completed synthetic','outcome':'passed'}],'candidate_head':'synthetic','candidate_index_tree':'synthetic-tree'}
    account={'same_state_restarts':0,'snapshots':[{'source':{'last_run':{'status':'completed'}}} for _ in range(3)]}
    path.write_text(json.dumps(report));(path.parent/'retention-accounting.json').write_text(json.dumps(account))
    assert reuse_accounting(path,pin)['tree']=='synthetic-tree'
    report['binary_sha256']['sinexd']='different';path.write_text(json.dumps(report));refuses(reuse_accounting,path,pin)
    report['binary_sha256']['sinexd']='daemon';report['success']=False;path.write_text(json.dumps(report));refuses(reuse_accounting,path,pin)
print('front-door refusals, CPU/RSS boundaries, late-bridge oracle and prior receipt mismatch controls passed')
