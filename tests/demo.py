#!/usr/bin/env python3
"""Run packaged Kestrel paths, optionally qualifying both reporting failure controls."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('spec_repository', type=Path, help='read-only Git repository containing the pinned independent oracle')
parser.add_argument('--controls', action='store_true', help='require launch and answer-corruption failures before the normal path')
args = parser.parse_args()
args.spec_repository = args.spec_repository.resolve()
report = Path(tempfile.mkdtemp(prefix='sinex-kestrel-result-', dir='/realm/tmp/work'))
print(json.dumps({'driver_report': str(report/'result.json')}), flush=True)
runs = []
expected_hash = None
success = False
try:
    for control in (['launch-failure', 'corrupt-answer', None] if args.controls else [None]):
        name = control or 'normal'
        command = ['python3', str(ROOT/'tests/foundation.py'), '--packaged', '--demo-spec', str(args.spec_repository)]
        if control: command += ['--demo-control', control]
        env = os.environ.copy(); env['SINEX_FOUNDATION_REPORT_DIR'] = str(report/name)
        with (report/(name+'.stdout')).open('w') as out, (report/(name+'.stderr')).open('w') as err:
            result = subprocess.run(command, cwd=ROOT, env=env, stdout=out, stderr=err)
        entry = {'name': name, 'argv': command, 'exit': result.returncode, 'report': str(report/name/'result.json')}
        runs.append(entry)
        actual = json.loads((report/name/'result.json').read_text())
        import hashlib
        current_hash = hashlib.sha256((report/name/'prepared/expected.json').read_bytes()).hexdigest()
        if expected_hash is None: expected_hash = current_hash
        assert current_hash == expected_hash, 'independent expectations changed between controls and actual run'
        if control:
            assert result.returncode != 0 and actual['success'] is False, f'{name}: driver falsely succeeded'
            assert any(s['outcome'] == 'unrun' for s in actual['scenarios']), f'{name}: dependent unrun cases absent'
            assert (report/name/'diagnostic.txt').is_file(), f'{name}: readable expected/actual diagnostic absent'
            if control == 'launch-failure':
                assert actual['owned_processes'] and actual['owned_processes'][0]['exit'] != 0
                assert not list((report/name).glob('*.actual.json')), 'launch failure fabricated an answer'
            else:
                comparisons = json.loads((report/name/'comparisons.json').read_text())
                assert comparisons[0]['comparison'] == 'mismatch'
                original = json.loads(Path(comparisons[0]['actual']).read_text())
                corrupted = json.loads(Path(comparisons[0]['compared']).read_text())
                assert corrupted['total'] == original['total']+1
                assert any(c['exit'] == 1 and c['label'].startswith('independent oracle') for c in actual['commands'])
        else:
            assert result.returncode == 0 and actual['success'], actual.get('failure')
    success = True
except Exception as error:
    failure = str(error)
finally:
    summary = {'success': success, 'failure': None if success else failure, 'runs': runs,
               'scope': 'actual packaged Kestrel raw-event paths and reporting controls; not formal M1 acceptance',
               'open': ['observed retention/admission/index kill boundaries', 'normal client per-record malformed reason', 'general read model/index', 'physical SQLite snapshots (1c)', 'all other M1 and task-routing gates']}
    (report/'result.json').write_text(json.dumps(summary, indent=2)+'\n')
    (report/'report.txt').write_text('\n'.join([summary['scope'], f'Success: {success}', f'Failure: {summary["failure"]}']+[f'{r["name"]}: exit {r["exit"]}; {r["report"]}' for r in runs])+ '\n')
    print(json.dumps({'success': success, 'report': str(report/'result.json')}))
    sys.exit(0 if success else 1)
