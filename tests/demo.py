#!/usr/bin/env python3
"""Run packaged Kestrel paths, optionally qualifying both reporting failure controls."""
import argparse
import hashlib
import json
import os
import shutil
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('spec_repository', type=Path, help='read-only Git repository containing the pinned independent oracle')
parser.add_argument('--boundaries', action='store_true', help='also qualify observed writer-boundary SIGKILL and same-state recovery')
parser.add_argument('--controls', action='store_true', help='require launch and answer-corruption failures before the normal path')
args = parser.parse_args()
args.spec_repository = args.spec_repository.resolve()
report = Path(tempfile.mkdtemp(prefix='sinex-kestrel-result-', dir='/realm/tmp/work'))
print(json.dumps({'driver_report': str(report/'result.json')}), flush=True)

def verify_portable_links(tree):
    def reference(owner, value):
        relative = Path(value)
        assert not relative.is_absolute(), f'absolute artifact reference: {value}'
        target = (owner/relative).resolve()
        assert target.is_relative_to(tree.resolve()) and target.is_file(), f'broken artifact reference: {value}'
        return target
    summary = json.loads((tree/'result.json').read_text())
    for child in summary['runs']:
        owner = reference(tree, child['report']).parent
        basis = json.loads((owner/'oracle-basis.json').read_text())
        for path, digest in basis['files'].items():
            target = reference(owner/'oracle', path)
            assert hashlib.sha256(target.read_bytes()).hexdigest() == digest
        prepared = json.loads(reference(owner, 'prepared/expected.json').read_text())
        for project in ['polylogue', 'sinex']:
            for case in prepared[project]['cases'].values(): reference(owner/'prepared', case['input'])
        digests = owner/'input-digests.json'
        if digests.exists():
            for path, digest in json.loads(digests.read_text()).items():
                assert hashlib.sha256(reference(owner, path).read_bytes()).hexdigest() == digest
        boundaries = owner/'boundaries.json'
        if boundaries.exists():
            for boundary in json.loads(boundaries.read_text()):
                for key in ['retained_artifact', 'recovered']:
                    if key in boundary: reference(owner, boundary[key])
        comparisons = owner/'comparisons.json'
        if comparisons.exists():
            for comparison in json.loads(comparisons.read_text()):
                for key in ['actual', 'compared']: reference(owner, comparison[key])
        diagnostic = owner/'diagnostic.txt'
        if diagnostic.exists():
            for line in diagnostic.read_text().splitlines():
                if line.startswith(('Actual captured answer: ', 'Compared answer: ')):
                    reference(owner, line.split(': ',1)[1])
                elif line.startswith('Expected: '):
                    reference(owner, line.removeprefix('Expected: ').split(',',1)[0])
    for line in (tree/'report.txt').read_text().splitlines():
        if ': exit ' in line: reference(tree, line.split('; ',1)[1])

runs = []
expected_hash = None
success = False
try:
    for control in ((['launch-failure', 'corrupt-answer', None] if args.controls else [None]) + (['boundaries'] if args.boundaries else [])):
        name = control or 'normal'
        command = ['python3', str(ROOT/'tests/foundation.py'), '--packaged', '--demo-spec', str(args.spec_repository)]
        if control == 'boundaries': command += ['--demo-boundaries']
        elif control: command += ['--demo-control', control]
        env = os.environ.copy(); env['SINEX_FOUNDATION_REPORT_DIR'] = str(report/name)
        with (report/(name+'.stdout')).open('w') as out, (report/(name+'.stderr')).open('w') as err:
            result = subprocess.run(command, cwd=ROOT, env=env, stdout=out, stderr=err)
        entry = {'name': name, 'argv': command, 'exit': result.returncode, 'report': str(Path(name)/'result.json')}
        runs.append(entry)
        actual = json.loads((report/name/'result.json').read_text())
        current_hash = hashlib.sha256((report/name/'prepared/expected.json').read_bytes()).hexdigest()
        if expected_hash is None: expected_hash = current_hash
        assert current_hash == expected_hash, 'independent expectations changed between controls and actual run'
        if control and control != 'boundaries':
            assert result.returncode != 0 and actual['success'] is False, f'{name}: driver falsely succeeded'
            assert any(s['outcome'] == 'unrun' for s in actual['scenarios']), f'{name}: dependent unrun cases absent'
            assert (report/name/'diagnostic.txt').is_file(), f'{name}: readable expected/actual diagnostic absent'
            if control == 'launch-failure':
                assert actual['owned_processes'] and actual['owned_processes'][0]['exit'] != 0
                assert not list((report/name).glob('*.actual.json')), 'launch failure fabricated an answer'
            else:
                comparisons = json.loads((report/name/'comparisons.json').read_text())
                assert comparisons[0]['comparison'] == 'mismatch'
                original = json.loads((report/name/comparisons[0]['actual']).read_text())
                corrupted = json.loads((report/name/comparisons[0]['compared']).read_text())
                assert corrupted['total'] == original['total']+1
                assert any(c['exit'] == 1 and c['label'].startswith('independent oracle') for c in actual['commands'])
        else:
            assert result.returncode == 0 and actual['success'], actual.get('failure')
    success = True
except Exception as error:
    failure = str(error)
finally:
    summary = {'success': success, 'failure': None if success else failure, 'runs': runs,
               'scope': 'actual packaged Kestrel raw-event paths and reporting controls'+(' plus observed retention/admission boundary recovery' if args.boundaries else '')+'; not formal M1 acceptance',
               'open': ['read-model index kill boundary', 'general read model/index', 'physical SQLite snapshots (1c)', 'all other M1 and task-routing gates']}
    (report/'result.json').write_text(json.dumps(summary, indent=2)+'\n')
    (report/'report.txt').write_text('\n'.join([summary['scope'], f'Success: {success}', f'Failure: {summary["failure"]}']+[f'{r["name"]}: exit {r["exit"]}; {r["report"]}' for r in runs])+ '\n')
    try:
        # Check a separate copy, and reject absolute links even while the original
        # scratch still exists. Raw argv/config/stdout remain exact execution evidence.
        with tempfile.TemporaryDirectory(prefix='sinex-report-relocation-', dir='/realm/tmp/work') as scratch:
            moved = Path(scratch)/'report'
            shutil.copytree(report, moved)
            verify_portable_links(moved)
            broken = json.loads((moved/'result.json').read_text())
            broken['runs'][0]['report'] = str(report/broken['runs'][0]['report'])
            (moved/'result.json').write_text(json.dumps(broken))
            try: verify_portable_links(moved)
            except AssertionError as error:
                assert str(error).startswith('absolute artifact reference:')
            else: raise AssertionError('absolute original-scratch link was accepted')
        summary['portable_artifact_links'] = 'passed on copied tree; absolute original-scratch link refused'
    except Exception as error:
        success = False
        summary['success'] = False
        summary['failure'] = 'report relocation check: '+str(error)
    (report/'result.json').write_text(json.dumps(summary, indent=2)+'\n')
    (report/'report.txt').write_text('\n'.join([summary['scope'], f'Success: {success}', f'Failure: {summary["failure"]}']+[f'{r["name"]}: exit {r["exit"]}; {r["report"]}' for r in runs])+ '\n')
    print(json.dumps({'success': success, 'report': str(report/'result.json')}))
    sys.exit(0 if success else 1)
