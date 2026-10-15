"""Observe real writer rendezvous, kill only the recorded child, then reuse its state."""
from collections import Counter
import hashlib
import json
from pathlib import Path
import signal
import socket
import sqlite3
import time
import uuid

from demo_cases import DEMO, prepare_fixture

POINTS = ['retained-fsynced', 'transaction-staged', 'transaction-committed']


def exercise(h):
    work, report, binaries = (h[k] for k in ['WORK', 'REPORT', 'BIN_DIR'])
    run, scenario, start, stop = (h[k] for k in ['run', 'scenario', 'start_daemon', 'stop'])
    prepared = prepare_fixture(h)
    expected_path = prepared/'expected.json'
    expected = json.loads(expected_path.read_text())['sinex']['cases']['malformed']
    evidence = []
    baseline = None

    def case(name, point=None):
        source = work/(name+'.db')
        with sqlite3.connect(prepared/expected['input']) as original, sqlite3.connect(source) as target:
            original.backup(target)
        with sqlite3.connect(source) as original, sqlite3.connect(report/(name+'-input.db')) as target:
            original.backup(target)
        database = 'boundary_'+name.replace('-', '_')
        run(['createdb', database], label=name+' isolated database')
        for role in ['admin', 'writer', 'reader']:
            (work/(name+'-'+role)).write_text((work/role).read_text().replace('foundation?', database+'?'))
        run([binaries/'sinexd', 'admin', 'schema', '--connection-file', work/(name+'-admin')], label=name+' actual schema')
        endpoint = f"http://127.0.0.1:{h['free_port']()}/rpc"
        store = work/(name+'-store')
        config = work/(name+'.toml')
        config.write_text(f'''listen = "{endpoint.removeprefix('http://').removesuffix('/rpc')}"
state_dir = "{work/(name+'-state')}"
store_dir = "{store}"
writer_connection_file = "{work/(name+'-writer')}"
reader_connection_file = "{work/(name+'-reader')}"
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
        input_digest = hashlib.sha256(source.read_bytes()).hexdigest()
        command_index = 0
        proc = None
        def cli(command, *args):
            nonlocal command_index
            raw = run([binaries/'sinexctl', '--endpoint', endpoint, '--token-file', work/'token', command, *args, '--json'], expected=(0,5) if command == 'query' else 0, label=name+' actual '+command)
            captured = report/f'{name}-{command_index:03}-{command}.json'; captured.write_text(raw.stdout)
            command_index += 1
            return json.loads(raw.stdout), captured
        def sources():
            return cli('sources')[0]['sources'][0]
        def wait_complete(after=None):
            deadline = time.monotonic()+30
            while time.monotonic() < deadline:
                assert proc.poll() is None, f'{name} daemon exited; see retained stderr'
                try:
                    reply = h['rpc'](endpoint, 'synthetic-foundation-token', 'sources.list')
                    runs = reply.get('result',{}).get('sources', [])
                    if runs:
                        observed = runs[0]['last_run']
                        if observed and observed['id'] != after and observed['status'] == 'completed':
                            current = sources()
                            if current['last_run']['status'] == 'completed' and current['last_run']['id'] != after:
                                return current
                except (OSError, ValueError): pass
                time.sleep(.025)
            raise AssertionError(name+' never completed a committed observation')
        def query_complete():
            deadline = time.monotonic()+30
            while time.monotonic() < deadline:
                answer, path = cli('query', expected['query_checks'][0]['query'])
                if answer['complete']:
                    run(['python3', report/'oracle'/DEMO/'check_answer.py', 'sinex', 'malformed', expected_path, path, '--query-index', '0'], label=name+' independent fact oracle')
                    return answer
                assert answer['source_status'] and all(s['status'] in ('reading','completed') for s in answer['source_status'])
                time.sleep(.025)
            raise AssertionError(name+' query stayed incomplete')
        def custody_audit(material):
            material = str(uuid.UUID(material))
            statement = ("SELECT jsonb_build_object('material',m.id,'status',m.status,'durable_bytes',m.durable_bytes,'blob_bytes',b.size_bytes,'observation',m.observation_id,'observation_status',o.status,'observation_rows_read',o.rows_read,'record_errors',o.record_errors) FROM raw.source_material_registry m LEFT JOIN core.blobs b ON b.id=m.blob_id JOIN raw.observations o ON o.id=m.observation_id WHERE m.id='"+material+"'::uuid")
            return json.loads(run(['psql', (work/(name+'-reader')).read_text(), '--no-psqlrc', '--tuples-only', '--no-align', '-v', 'ON_ERROR_STOP=1', '--command', statement], label=name+' read-only custody audit').stdout)
        def capture(label):
            source_status = wait_complete()
            assert source_status['last_run']['rows_read'] == expected['rows_read']
            assert source_status['last_run']['record_errors'] == expected['record_errors']
            assert source_status['cursor']['observation'] == source_status['last_run']['id']
            assert source_status['cursor']['rows_read'] == expected['rows_read']
            failure = source_status['first_record_error']
            assert failure['evidence_status'] == 'available' and failure['record']['timestamp'] == 'not-a-time'
            assert failure['record']['command'] == 'M1 invalid timestamp'
            assert failure['reason'] == 'invalid Atuin record field type or missing field'
            assert failure['record']['_from']['observation'] == source_status['last_run']['id']
            assert failure['end'] > failure['anchor_byte'] >= 0
            answer = query_complete()
            traces = {}
            for row in answer['rows']:
                trace = cli('trace', row['id'], '--bytes')[0]['row']
                assert trace['record']['id'] == row['payload']['native_id']
                assert trace['record']['timestamp'] == row['payload']['timestamp_ns']
                assert trace['record']['command'] == row['payload']['command']
                assert trace['material'] == row['material'] and trace['anchor'] == row['anchor']
                assert trace['representation'] == 'canonical_sqlite_row'
                assert trace['record']['_from']['table'] == 'history'
                traces[row['payload']['native_id']] = trace
            captured = {'source':source_status, 'answer':answer, 'traces':traces}
            (report/(name+'-'+label+'.json')).write_text(json.dumps(captured, indent=2)+'\n')
            return captured
        def normalized(captured):
            facts = Counter(json.dumps({k:row[k] for k in ['source','kind','payload','when','time_quality']}, sort_keys=True) for row in captured['answer']['rows'])
            rows = {}
            for native, trace in captured['traces'].items():
                row = dict(trace['record'])
                row['_from'] = {k:v for k,v in row['_from'].items() if k != 'observation'}
                rows[native] = row
            return facts, rows
        def identities(captured):
            return {r['payload']['native_id']: (r['id'],r['material'],r['anchor'],captured['traces'][r['payload']['native_id']]['record']['_from']) for r in captured['answer']['rows']}
        if point:
            listener = socket.socket(socket.AF_UNIX)
            # Unix-domain path length is bounded: use the owned short scratch path.
            socket_path = work/'boundary.sock'
            listener.bind(str(socket_path)); listener.listen(1); listener.settimeout(30)
            try:
                proc = start(config, name+'-before-kill', '--qualification-boundary', point, '--qualification-socket', socket_path)
                connection, _ = listener.accept(); connection.settimeout(30)
                with connection:
                    frame = json.loads(connection.makefile('rb').readline(65536))
                    assert frame['pid'] == proc.pid and frame['qualification_boundary'] == point
                    custody = frame['evidence'] if point == 'retained-fsynced' else frame['evidence']['custody']
                    assert custody['acknowledged'] is False and custody['source_eof'] is True
                    assert custody['step_rows'] == expected['rows_read']
                    path = store/'open'/custody['material']
                    retained = path.read_bytes()
                    assert len(retained) == custody['end'] and custody['canonical_bytes'] > 0
                    (report/(name+'-fsynced-canonical-rows.jsonl')).write_bytes(retained)
                    before, _ = cli('query', expected['query_checks'][0]['query'])
                    status = sources()
                    committed = point == 'transaction-committed'
                    assert before['total'] == (len(expected['live_commands']) if committed else 0)
                    assert status['last_run']['rows_read'] == (expected['rows_read'] if committed else 0)
                    assert (status['cursor'] is not None) == committed
                    if committed:
                        assert status['cursor'] == frame['evidence']['progress']
                        assert status['first_record_error']['record']['timestamp'] == 'not-a-time'
                        # Exercise inaccessible evidence while the writer is held: counts
                        # remain honest and the same error is explicitly unavailable.
                        hidden = path.with_suffix('.qualification-hidden')
                        path.rename(hidden)
                        try:
                            unavailable = sources()
                            assert unavailable['last_run']['record_errors'] == 1
                            assert unavailable['first_record_error']['evidence_status'] == 'unavailable'
                            assert unavailable['first_record_error']['record'] is None
                        finally:
                            hidden.rename(path)
                    else:
                        assert status['first_record_error'] is None
                    audit_before = custody_audit(custody['material'])
                    assert audit_before['durable_bytes'] == (custody['end'] if committed else 0)
                    assert audit_before['status'] == 'capturing'
                    entry = {'boundary':point, 'observed':frame, 'before_query':before, 'before_source':status,
                             'retained_bytes_sha256':hashlib.sha256(retained).hexdigest(), 'retained_artifact':name+'-fsynced-canonical-rows.jsonl', 'custody_before':audit_before, 'kill_pid':proc.pid}
                    evidence.append(entry)
                    (report/'boundaries.json').write_text(json.dumps(evidence, indent=2)+'\n')
                    # No clock-based kill: this exact recorded child is held at the observed instruction.
                    proc.send_signal(signal.SIGKILL); proc.wait(timeout=15)
                    assert proc.returncode == -signal.SIGKILL
                    entry['exit'] = proc.returncode
                    assert connection.recv(1) == b''
                proc = start(config, name+'-restart')
                wait_complete(after=custody['observation'])
                recovered = capture('recovered')
                assert recovered['source']['last_run']['id'] != custody['observation']
                account = recovered['source']['last_run']
                if committed:
                    assert account['admitted'] == 0 and account['repeated'] == expected['rows_read']-1
                else:
                    assert (account['admitted'], account['repeated']) in [(expected['rows_read']-1,0), (0,expected['rows_read']-1)]
                audit_after = custody_audit(custody['material'])
                assert audit_after['status'] == 'recovered_partial'
                assert audit_after['durable_bytes'] == audit_after['blob_bytes'] == (custody['end'] if committed else 0)
                assert audit_after['observation_status'] == ('completed' if committed else 'interrupted')
                assert audit_after['observation_rows_read'] == (expected['rows_read'] if committed else 0)
                entry['custody_after'] = audit_after
                assert normalized(recovered) == normalized(baseline)
                if committed:
                    assert {r['id'] for r in before['rows']} == {r['id'] for r in recovered['answer']['rows']}
                    assert all(t['material'] == custody['material'] and t['record']['_from']['observation'] == custody['observation'] for t in recovered['traces'].values())
                else:
                    assert all(t['material'] != custody['material'] for t in recovered['traces'].values())
                assert not path.exists(), 'recovery failed to close the interrupted open part'
                entry['recovered'] = name+'-recovered.json'
            finally:
                listener.close(); socket_path.unlink(missing_ok=True)
        else:
            for extra, expected_exit in [(['--qualification-boundary', 'retained-fsynced'], 2),
                                         (['--qualification-boundary', 'unknown', '--qualification-socket', work/'absent.sock'], 1)]:
                run([binaries/'sinexd', 'serve', '--config', config, *extra], expected=expected_exit, label='qualification control refuses missing or invalid boundary')
                assert not store.exists() and not (work/(name+'-state')).exists()
            proc = start(config, name)
            recovered = capture('uninterrupted')
        first = recovered['source']['last_run']
        wait_complete(after=first['id'])
        repeated = capture('repeated')
        assert normalized(repeated) == normalized(recovered)
        assert identities(repeated) == identities(recovered)
        assert repeated['source']['last_run']['admitted'] == 0
        assert repeated['source']['last_run']['repeated'] == expected['rows_read']-expected['record_errors']
        assert repeated['source']['last_run']['record_errors'] == 1
        stop(proc)
        assert hashlib.sha256(source.read_bytes()).hexdigest() == input_digest
        (report/'boundaries.json').write_text(json.dumps(evidence, indent=2)+'\n')
        return recovered

    def uninterrupted():
        nonlocal baseline
        baseline = case('baseline')
    try:
        scenario('uninterrupted boundary baseline', uninterrupted)
        for point in POINTS:
            scenario('observed '+point+' kill and restart', lambda point=point: case(point, point))
    finally:
        (report/'input-digests.json').write_text(json.dumps({p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in report.glob('*-input.db')}, indent=2)+'\n')
