"""Kestrel driver using the foundation harness's one owned runtime and real clients."""
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import time

ORACLE_REVISION = 'ba29767740bc70fbb8eaffcfd99127ceb1e7e4ea'
DEMO = 'polylogue-sinex-joint/realization/demos'
INPUTS = [f'{DEMO}/prepare.py', f'{DEMO}/check_answer.py',
          'polylogue-spec/examples/archive/claude-code/projects/-home-op-morrow/7f3a2c10-0001-4000-8000-000000000001.jsonl',
          'sinex-spec/examples/kestrel/raw/home/iris/.local/share/atuin/history.db']


def prepare_fixture(h):
    report, options, run, scenario = (h[k] for k in ['REPORT', 'OPTIONS', 'run', 'scenario'])
    basis = report/'oracle'; prepared = report/'prepared'
    def prepare():
        manifest = {'revision': ORACLE_REVISION, 'files': {}}
        for name in INPUTS:
            # Read immutable Git objects rather than the concurrently edited spec checkout.
            content = subprocess.check_output(['git', '-C', str(options.demo_spec), 'show', f'{ORACLE_REVISION}:{name}'])
            path = basis/name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(content)
            manifest['files'][name] = hashlib.sha256(content).hexdigest()
        (report/'oracle-basis.json').write_text(json.dumps(manifest, indent=2)+'\n')
        run(['python3', basis/DEMO/'prepare.py', prepared], label='pinned independent preparer')
    scenario('pinned independent fixture preparation', prepare)
    return prepared


def exercise(h):
    work, report, env, options = (h[k] for k in ['WORK', 'REPORT', 'ENV', 'OPTIONS'])
    run, scenario, start, stop = (h[k] for k in ['run', 'scenario', 'start_daemon', 'stop'])
    binaries = h['BIN_DIR']
    prepared = prepare_fixture(h)
    basis = report/'oracle'
    expected_path = prepared/'expected.json'
    expected = json.loads(expected_path.read_text())['sinex']['cases']
    expected_digest = hashlib.sha256(expected_path.read_bytes()).hexdigest()
    results = []
    observations = (report/'source-observations.jsonl').open('w')
    source = work/'configured-history.db'
    endpoint = f"http://127.0.0.1:{h['free_port']()}/rpc"
    proc = None
    def cli(command, *args):
        deadline = time.monotonic()+30
        while True:
            actual = run([binaries/'sinexctl', '--endpoint', endpoint, '--token-file', work/'token', command, *args, '--json'], expected=(0, 5) if command == 'query' else 0, label='Kestrel actual CLI '+command)
            if command != 'query': return actual
            decoded = json.loads(actual.stdout)
            if actual.returncode == 0:
                assert decoded['complete'] is True
                return actual
            # The source may begin another poll between the completed observation and
            # a query. Retain that honest behind answer and wait for a complete read.
            assert decoded['complete'] is False and decoded['source_status']
            assert all(s['status'] in ('reading', 'completed') for s in decoded['source_status']), decoded['source_status']
            if time.monotonic() >= deadline:
                raise AssertionError('query remained behind for 30 seconds; incomplete answers retained')
            time.sleep(.05)
    def wait_cycle(case, after=None):
        deadline = time.monotonic()+30
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise AssertionError(f'candidate exited {proc.returncode} before acquisition; actual answer absent; see daemon stderr')
            try:
                reply = h['rpc'](endpoint, 'synthetic-foundation-token', 'sources.list')
                observations.write(json.dumps(reply)+'\n'); observations.flush()
                sources = reply.get('result', {}).get('sources', [])
                if sources:
                    observed = sources[0]['last_run']
                    if observed and observed['id'] != after and observed['status'] == 'completed' and observed['rows_read'] == expected[case]['rows_read']:
                        assert observed['record_errors'] == expected[case]['record_errors'], observed
                        return observed
            except (OSError, ValueError):
                pass
            time.sleep(.05)
        raise AssertionError('no completed committed acquisition at requested fixture; see source observations')
    def install(case):
        # Replace rows by SQLite backup at the one configured path; the input binding stays stable.
        with sqlite3.connect(prepared/expected[case]['input']) as original, sqlite3.connect(source) as destination:
            original.backup(destination)
        target = report/f'exercised-{case}.db'
        with sqlite3.connect(source) as original, sqlite3.connect(target) as destination:
            original.backup(destination)
        return hashlib.sha256(target.read_bytes()).hexdigest()
    def launch(case, fresh=False):
        nonlocal proc
        if fresh:
            database = 'demo_'+case
            run(['createdb', database], label='fresh '+case+' database')
            for role in ['admin', 'writer', 'reader']:
                (work/('demo-'+role)).write_text((work/role).read_text().replace('foundation?', database+'?'))
            run([binaries/'sinexd', 'admin', 'schema', '--connection-file', work/'demo-admin'], label='fresh '+case+' canonical schema')
        else:
            for role in ['writer', 'reader']:
                (work/('demo-'+role)).write_text((work/role).read_text())
        config = work/'demo.toml'
        config.write_text(f'''listen = "{endpoint.removeprefix('http://').removesuffix('/rpc')}"
state_dir = "{work/('state-'+case)}"
store_dir = "{work/('store-'+case)}"
writer_connection_file = "{work/'demo-writer'}"
reader_connection_file = "{work/'demo-reader'}"
token_file = "{work/'token'}"
sources = ["terminal.atuin-history"]
automata = []
[atuin]
path = "{source}"
poll_interval_ms = 1000
[[privacy_rules]]
pattern = "tok_[a-z0-9_]+"
replacement = "[withheld]"
created_by = "operator"
''')
        if options.demo_control == 'launch-failure':
            # Force this actual candidate's configuration parser to refuse startup.
            config.write_text(config.read_text()+'\n[deliberately_invalid_launch_control]\n')
        proc = start(config, 'kestrel-'+case)
    def answers(case):
        all_answer = None
        for index, query in enumerate(expected[case]['query_checks']):
            actual = cli('query', query['query'])
            path = report/f'{case}-query-{index:02}-capture-{len(results):03}.actual.json'; path.write_text(actual.stdout)
            decoded = json.loads(actual.stdout)
            if index == 0: all_answer = decoded
            compared = path
            if options.demo_control == 'corrupt-answer':
                compared = report/f'{case}-query-{index:02}-capture-{len(results):03}.corrupted.json'
                decoded['total'] += 1
                compared.write_text(json.dumps(decoded, indent=2)+'\n')
            check = expected[case]['query_checks'][index]
            diagnostic = {'case': case, 'query_index': index, 'query': query['query'], 'expected': check,
                          'actual': path.name, 'compared': compared.name, 'control': options.demo_control,
                          'expected_sha256': expected_digest, 'comparison': 'pending'}
            results.append(diagnostic)
            (report/'comparisons.json').write_text(json.dumps(results, indent=2)+'\n')
            (report/'diagnostic.txt').write_text(f"Case: {case}, query {index}: {check['name']}\nQuery: {query['query']}\nExpected total: {check['total']}\nExpected rows: {json.dumps(check['expected_rows'], indent=2)}\nActual captured answer: {path.name}\nCompared answer: {compared.name}\nCaptured actual total: {json.loads(actual.stdout).get('total')}\nCompared total: {decoded.get('total')}\n")
            try:
                run(['python3', basis/DEMO/'check_answer.py', 'sinex', case, expected_path, compared, '--query-index', str(index)], label=f'independent oracle {case}/{index}')
            except AssertionError:
                diagnostic['comparison'] = 'mismatch'
                with (report/'diagnostic.txt').open('a') as explanation:
                    explanation.write((report/f"{len(h['COMMANDS'])-1:03}.stderr").read_text())
                raise
            finally:
                (report/'comparisons.json').write_text(json.dumps(results, indent=2)+'\n')
            diagnostic['comparison'] = 'passed'
        (report/'comparisons.json').write_text(json.dumps(results, indent=2)+'\n')
        return all_answer
    previous = None; base_ids = None
    def base():
        nonlocal previous, base_ids
        install('base'); launch('base'); previous = wait_cycle('base')
        answer = answers('base'); base_ids = {r['id'] for r in answer['rows']}
        assert json.loads(cli('sources').stdout)['sources'][0]['first_record_error'] is None
        for row in answer['rows']:
            trace = json.loads(cli('trace', row['id'], '--bytes').stdout)['row']
            assert trace['record']['id'] == row['payload']['native_id']
            assert trace['record']['_from']['table'] == 'history'
            assert trace['record']['command'] == row['payload']['command']
            assert trace['record']['timestamp'] == row['payload']['timestamp_ns']
            assert trace['end'] > trace['anchor'] >= 0
            assert row['when'] and row['seen'] and row['written']
    def repeat():
        nonlocal previous
        previous = wait_cycle('base', after=previous['id'])
        assert previous['admitted'] == 0
        assert {r['id'] for r in answers('base')['rows']} == base_ids
    def append():
        install('append'); wait_cycle('append', after=previous['id'])
        answer = answers('append'); assert base_ids.issubset({r['id'] for r in answer['rows']})
        stop(proc)
    try:
        scenario('Kestrel base actual answers', base)
        scenario('Kestrel repeated snapshot', repeat)
        scenario('Kestrel append at the same source path', append)
        for case in ['empty', 'malformed']:
            def isolated(case=case):
                install(case); launch(case, fresh=True); wait_cycle(case); answers(case)
                diagnostic = json.loads(cli('sources').stdout)['sources'][0]['first_record_error']
                if case == 'empty': assert diagnostic is None
                else:
                    assert diagnostic['record']['timestamp'] == 'not-a-time' and diagnostic['evidence_status'] == 'available'
                    assert diagnostic['reason'] and diagnostic['end'] > diagnostic['anchor_byte']
                stop(proc)
            scenario('Kestrel '+case+' actual answers', isolated)
        assert hashlib.sha256(expected_path.read_bytes()).hexdigest() == expected_digest
    finally:
        observations.close()
        # A launch failure has no answer file: retain an explicit absence and dependency reason.
        if not results:
            (report/'diagnostic.txt').write_text('Actual answer absent: candidate did not reach a completed acquisition.\nExpected: prepared/expected.json, sinex/base.\nSee kestrel-base.stderr and result.json for failure and dependent unrun cases.\n')
        (report/'input-digests.json').write_text(json.dumps({str(p.relative_to(report)): hashlib.sha256(p.read_bytes()).hexdigest() for p in report.glob('exercised-*.db')}, indent=2)+'\n')
