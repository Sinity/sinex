"""Excluding one of two identical Atuin commands: what queries and the index show, undo,
replay and restart, kills before and after commit, retries and refusals."""
import concurrent.futures
import copy
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import sqlite3
import subprocess
import threading
import uuid

from _atuin_daemon import (OPERATOR_TOKEN, Daemon, access_config, append_history, blob_path, create_history, kill,
                           pause_at, paused, relaunch, sql)

EPOCH = 1_700_000_000_000_000_000
COMMAND = 'cargo test exclusion-fixture'
RAW_QUERY = 'events where source = terminal.atuin-history and kind = command.executed'
INDEXED_QUERY = '"cargo test"'


def new_action():
    return str(uuid.uuid4())


def mismatched_replies(h):
    """The CLI rejects a delete reply whose action, method or target differs from the request."""
    action, target, other = ('01800000-0000-7000-8000-0000000000' + n for n in ('11', '22', '33'))
    valid = {'operation_id': '01800000-0000-7000-8000-000000000044', 'action_id': action, 'method': 'delete.run',
             'state': 'completed', 'target': target,
             'occurrence': {'source': 'terminal.atuin-history', 'event_type': 'command.executed',
                            'equivalence_key': '["01800000-0000-7000-8000-000000000055","synthetic-a"]'},
             'exclusion_operation': '01800000-0000-7000-8000-000000000044', 'removed_exclusion_operation': None,
             'archived': [target], 'reason': None, 'raw_ref': None}
    cases = [('valid', valid)]
    for field, value in [('action_id', other), ('method', 'delete.undo'), ('target', other)]:
        wrong = copy.deepcopy(valid)
        wrong[field] = value
        missing = copy.deepcopy(valid)
        missing.pop(field)
        cases += [('wrong ' + field, wrong), ('missing ' + field, missing)]
    cases += [('empty', {}), ('null', None)]
    reply = {}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            self.rfile.read(int(self.headers['Content-Length']))
            encoded = json.dumps({'jsonrpc': '2.0', 'id': 1, 'result': reply['value']}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, *_):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        for name, value in cases:
            reply['value'] = value
            result = subprocess.run([str(h['BIN_DIR']/'sinexctl'), '--endpoint', f'http://127.0.0.1:{server.server_port}/rpc',
                                     '--token-file', str(h['WORK']/'token'), 'delete', 'sinex:ev/'+target,
                                     '--action-id', action, '--json'], capture_output=True, text=True, timeout=10)
            if name == 'valid':
                assert result.returncode == 0, result.stderr
                continue
            assert result.returncode == 5 and not result.stdout, (name, result.returncode, result.stderr)
            error = json.loads(result.stderr)
            assert error['kind'] == 'partial' and error['details']['action_id'] == action, (name, error)
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def exercise(h):
    work, scenario = h['WORK'], h['scenario']
    d = Daemon(h, 'exclusion')
    def cli(*args, operator=False, **kw):
        return d.cli(*args, token_file=work/'operator' if operator else None, **kw)

    def operator_api(method, params):
        return h['rpc'](d.endpoint, OPERATOR_TOKEN, method, params, timeout=30)
    source = work/'history.db'

    def fixture_row(key):
        return (key, EPOCH, 1_000_000_000, 0, COMMAND, '/synthetic/exclusion', 'same-session', 'synthetic-host', None)
    create_history(source, [fixture_row('occurrence-a')])

    def start(label, collect=False, *flags):
        d.configure(source if collect else None, poll_ms=3_600_000, top=access_config(h))
        relaunch(d, label, *flags)

    def action_audit(action):
        return sql(h, 
            "SELECT jsonb_build_object('operations',(SELECT count(*) FROM core.operations_log WHERE action_id='"+action+"'::uuid),"
            "'rows',(SELECT coalesce(jsonb_agg(jsonb_build_object('id',o.id,'actor',o.actor,'request_format',o.request_format,"
            "'request_ref',o.request_ref,'outcome_format',o.outcome_format,'outcome_ref',o.outcome_ref,'progress',o.progress,"
            "'effects',(SELECT coalesce(jsonb_agg(to_jsonb(f)),'[]') FROM core.operation_effects f WHERE f.operation_id=o.id),"
            "'archives',(SELECT coalesce(jsonb_agg(a.id ORDER BY a.id),'[]') FROM audit.archived a WHERE a.operation_id=o.id),"
            "'exclusions',(SELECT coalesce(jsonb_agg(to_jsonb(x)),'[]') FROM core.deleted_occurrences x WHERE x.operation_id=o.id))),'[]')"
            " FROM core.operations_log o WHERE action_id='"+action+"'::uuid))")

    def settled(source_state=None):
        """Wait until the index has caught up (and, if given, the source's last read has this status)."""
        def caught_up():
            status = d.api('status')['result']
            index = status['indexed_retrieval']
            if not (index.get('serving') and index['position'] == status['commit_seq'] and not index['pending_canonical']):
                return False
            return source_state is None or (len(d.sources()) == 1 and d.last_run()['status'] == source_state)
        d.wait('index and source to catch up', caught_up)

    def population(members, complete=True):
        """Raw and indexed queries, over the API and the CLI, return exactly these native IDs."""
        settled()
        raw = d.api('query.run', {'query': RAW_QUERY})['result']
        indexed = d.api('query.run', {'query': INDEXED_QUERY, 'page': 100})['result']
        for answer in [raw, indexed]:
            assert sorted(row['payload']['native_id'] for row in answer['rows']) == members, answer
            assert answer['total'] == len(members) and answer['complete'] == complete, answer
        exit_code = 0 if complete else 5
        assert cli('query', RAW_QUERY, expected=exit_code)['rows'] == raw['rows']
        assert cli('query', INDEXED_QUERY, '--page', '100', expected=exit_code)['rows'] == indexed['rows']
        assert all(row['kind'] == 'command.canonical' and len(row['parents']) == 1 for row in indexed['rows'])
        for row in indexed['rows']:
            assert row['snippet']['text'] and row['snippet']['spans'], row
        return ({row['payload']['native_id']: row for row in raw['rows']},
                {row['payload']['native_id']: row for row in indexed['rows']})

    def deletion(ref, action=None, undo=False, expected=0, expected_key=None, operator=True):
        args = [ref, '--action-id', action or new_action()]
        if undo:
            args.append('--undo')
        if expected_key is not None:
            args += ['--expected-key', expected_key]
        return cli('delete', *args, operator=operator, expected=expected)

    def trace(ref):
        return cli('trace', ref, '--bytes')['row']

    def same_bytes(before, after):
        for field in ['material', 'anchor', 'end', 'record']:
            assert before[field] == after[field], field

    def replay(expected=0, basis=None):
        args = [f'materials where id = {material}', '--action-id', new_action()]
        if basis is not None:
            args += ['--basis', basis]
        return cli('replay', *args, operator=True, expected=expected)

    scenario('the CLI rejects a delete reply for a different action, method or target', lambda: mismatched_replies(h))

    originals = canonicals = old_trace = material = a_ref = b_ref = initial_action = initial_outcome = None

    def two_occurrences():
        nonlocal originals, canonicals, old_trace, material, a_ref, b_ref
        start('a-only', True)
        settled('completed')
        a = d.api('query.run', {'query': RAW_QUERY})['result']
        assert a['total'] == 1
        a_ref = a['rows'][0]['id']
        old_trace = trace(a_ref)
        material = old_trace['material']
        d.stop()
        # B goes into a second material, so replaying the first one cannot create B again.
        append_history(source, [fixture_row('occurrence-b')])
        start('a-and-b', True)
        settled('completed')
        start('a-and-b-not-collecting')
        originals, canonicals = population(['occurrence-a', 'occurrence-b'])
        b_ref = originals['occurrence-b']['id']
        assert originals['occurrence-a']['id'] == a_ref and a_ref != b_ref
        keys = sql(h, "SELECT jsonb_object_agg(payload->>'native_id',equivalence_key) FROM core.live_events WHERE event_type='command.executed'")
        assert keys['occurrence-a'] != keys['occurrence-b']
        denied_action = new_action()
        denied = deletion(a_ref, denied_action, operator=False, expected=4)
        assert denied['kind'] == 'refused' and action_audit(denied_action)['operations'] == 0
    scenario('two identical commands are separate occurrences; the read token cannot delete', two_occurrences)

    def exclude():
        nonlocal initial_action, initial_outcome
        initial_action = new_action()
        initial_outcome = deletion(a_ref, initial_action)
        assert initial_outcome['state'] == 'completed'
        assert set(initial_outcome['archived']) == {a_ref, canonicals['occurrence-a']['id']}
        raw, indexed = population(['occurrence-b'])
        assert raw['occurrence-b']['id'] == b_ref and indexed['occurrence-b']['id'] == canonicals['occurrence-b']['id']
        archived = trace(a_ref)
        same_bytes(old_trace, archived)
        assert archived['archive']['operation_id'] == initial_outcome['operation_id'] and archived['archive']['reason'] == 'delete'
        shown = d.api('object.show', {'ref': 'sinex:ev/'+a_ref})['result']
        assert shown == cli('show', a_ref) and shown['resolved'] == 'archived'
        audit = action_audit(initial_action)
        assert audit['operations'] == 1 and len(audit['rows'][0]['effects']) == 1
        assert len(audit['rows'][0]['exclusions']) == 1 and set(audit['rows'][0]['archives']) == set(initial_outcome['archived'])
        assert deletion(a_ref, initial_action) == initial_outcome and action_audit(initial_action) == audit
    scenario('excluding A archives its raw and canonical rows and keeps them traceable', exclude)

    def changed_source():
        d.stop()
        with sqlite3.connect(source) as con:
            con.execute("UPDATE history SET exit=7 WHERE id='occurrence-a'")
        start('changed-source', True)
        settled('completed')
        run = d.last_run()
        assert run['excluded'] == 1 and run['admitted'] == 0 and run['rows_read'] == 2, run
        population(['occurrence-b'])
        start('changed-source-restart', True)
        settled('completed')
        population(['occurrence-b'])
        blocked = replay()
        assert blocked['state'] == 'completed' and blocked['processed'] == 1 and blocked['admitted'] == 0 and blocked['archived'] == 0
        population(['occurrence-b'])
    scenario('a changed source row, a restart and a replay do not bring A back', changed_source)

    def undo_without_source():
        d.stop()
        source.rename(work/'source-removed.db')
        start('source-missing', True)
        settled('unavailable')
        population(['occurrence-b'], False)
        preview = cli('replay', f'materials where id = {material}', '--dry-run')
        assert preview['selected'] == preview['excluded'] == 1 and preview['would_admit'] == preview['would_archive'] == 0
        undone = deletion(a_ref, undo=True)
        assert undone['state'] == 'completed' and undone['removed_exclusion_operation'] == initial_outcome['operation_id']
        population(['occurrence-b'], False)
        stale = replay(expected=4, basis=preview['basis'])
        assert stale['kind'] == 'refused' and stale['details']['admitted'] == 0
        population(['occurrence-b'], False)
        restored = replay()
        assert restored['state'] == 'completed' and restored['admitted'] == 1 and restored['archived'] == 0
        raw, indexed = population(['occurrence-a', 'occurrence-b'], False)
        assert raw['occurrence-a']['id'] != a_ref and raw['occurrence-a']['when'] == originals['occurrence-a']['when']
        assert raw['occurrence-b']['id'] == b_ref and indexed['occurrence-b']['id'] == canonicals['occurrence-b']['id']
        same_bytes(old_trace, trace(raw['occurrence-a']['id']))
        assert raw['occurrence-a']['operation'] == restored['operation_id']
        assert deletion(a_ref, initial_action) == initial_outcome
        population(['occurrence-a', 'occurrence-b'], False)
    scenario('undo with the source gone; a replay planned before undo is refused; a new replay restores A', undo_without_source)

    def collect_after_undo():
        deletion(a_ref)
        population(['occurrence-b'], False)
        deletion(a_ref, undo=True)
        population(['occurrence-b'], False)
        d.stop()
        (work/'source-removed.db').rename(source)
        with sqlite3.connect(source) as con:
            con.execute("UPDATE history SET exit=9 WHERE id='occurrence-a'")
        start('collect-after-undo', True)
        settled('completed')
        raw, _ = population(['occurrence-a', 'occurrence-b'])
        assert raw['occurrence-a']['payload']['exit_code'] == 9
        assert raw['occurrence-a']['operation'] is None and raw['occurrence-b']['id'] == b_ref
        start('not-collecting')
        settled()
    scenario('after undo, ordinary collection admits A again', collect_after_undo)

    def kill_during(point, committed):
        population(['occurrence-a', 'occurrence-b'])
        action = new_action()
        d.configure(top=access_config(h))
        listener = pause_at(d, point)
        settled()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(operator_api, 'delete.run', {'ref': 'sinex:ev/'+a_ref, 'action_id': action})
            connection, frame = paused(d, listener, point)
            before = action_audit(action)
            assert before['operations'] == (1 if committed else 0), before
            if committed:
                assert before['rows'][0]['progress'] == frame['evidence']['outcome']
                assert len(before['rows'][0]['effects']) == len(before['rows'][0]['exclusions']) == 1
                # The writer is paused after commit, so the index has work pending.
                partial = d.api('query.run', {'query': INDEXED_QUERY, 'page': 100, 'timeout_ms': 250})
                assert (('error' in partial and partial['error']['kind'] in ['budget', 'partial'])
                        or partial.get('result', {}).get('complete') is False), partial
            kill(d)
            connection.close()
            try:
                future.result(timeout=10)
            except (OSError, ValueError, http.client.HTTPException):
                pass
        relaunch(d, point + '-restart')
        settled()
        if committed:
            population(['occurrence-b'])
        else:
            assert action_audit(action)['operations'] == 0
            population(['occurrence-a', 'occurrence-b'])
        recovered = deletion(a_ref, action)
        assert recovered['state'] == 'completed'
        if committed:
            assert recovered == frame['evidence']['outcome']
        audit = action_audit(action)
        assert audit['operations'] == 1 and len(audit['rows'][0]['effects']) == len(audit['rows'][0]['exclusions']) == 1
        assert set(audit['rows'][0]['archives']) == set(recovered['archived']) and len(recovered['archived']) == 2
        assert deletion(a_ref, action) == recovered and action_audit(action) == audit
        population(['occurrence-b'])
        # Put A back for the next step; collection is off, so only replay restores it.
        deletion(a_ref, undo=True)
        population(['occurrence-b'])
        assert replay()['admitted'] == 1
        population(['occurrence-a', 'occurrence-b'])
    scenario('a kill before commit leaves nothing; the retry deletes once', lambda: kill_during('delete-staged', False))
    scenario('a kill after commit keeps the deletion; the index reports pending work meanwhile', lambda: kill_during('delete-committed', True))

    def lost_reply():
        population(['occurrence-a', 'occurrence-b'])
        action = new_action()
        d.configure(top=access_config(h))
        listener = pause_at(d, 'delete-committed', 'lost-reply')
        settled()
        out, err = (h['REPORT']/'lost-reply.stdout').open('w'), (h['REPORT']/'lost-reply.stderr').open('w')
        client = subprocess.Popen([str(h['BIN_DIR']/'sinexctl'), '--endpoint', d.endpoint, '--token-file', str(work/'operator'),
                                   'delete', a_ref, '--action-id', action, '--json'],
                                  env=h['ENV'], stdout=out, stderr=err)
        h['CHILDREN'].append((client, out, err))
        connection, frame = paused(d, listener, 'delete-committed')
        before = action_audit(action)
        assert before['operations'] == 1
        client.kill()
        client.wait(timeout=10)
        connection.sendall(b'c')
        connection.close()
        settled()
        assert not (h['REPORT']/'lost-reply.stdout').read_text()
        assert deletion(a_ref, action) == frame['evidence']['outcome']
        assert action_audit(action) == before
        population(['occurrence-b'])
    scenario('a killed CLI loses only the reply; the retry returns the committed result', lost_reply)

    def retries_and_refusals():
        counts = ("SELECT jsonb_build_object('archives',(SELECT count(*) FROM audit.archived),"
                  "'deleted',(SELECT count(*) FROM core.deleted_occurrences),'events',(SELECT count(*) FROM core.events))")
        before = sql(h, counts)
        noop = deletion(a_ref)
        assert noop['state'] == 'noop' and noop['archived'] == []
        assert deletion(a_ref, initial_action) == initial_outcome
        for target, undo, key in [(a_ref, True, None), (b_ref, False, None), (a_ref, False, 'different')]:
            assert deletion(target, initial_action, undo=undo, expected_key=key, expected=4)['kind'] == 'refused'
        wrong_action = new_action()
        wrong = deletion(a_ref, wrong_action, expected_key='wrong stable occurrence key', expected=4)
        assert wrong['details']['state'] == 'refused' and wrong['details']['archived'] == []
        assert deletion(a_ref, wrong_action, expected_key='wrong stable occurrence key', expected=4) == wrong
        for absent_ref in [new_action(), '01800000-0000-7000-8000-000000000099']:
            absent_action = new_action()
            absent = deletion(absent_ref, absent_action, expected=3)
            assert absent['details']['state'] == 'not_found'
            assert deletion(absent_ref, absent_action, expected=3) == absent
            wire = operator_api('delete.run', {'ref': absent_ref, 'action_id': absent_action})
            assert wire['error']['kind'] == 'not_found' and wire['error']['data'] == absent['details']
            audit = action_audit(absent_action)
            assert audit['operations'] == 1 and len(audit['rows'][0]['effects']) == 1
            assert audit['rows'][0]['archives'] == audit['rows'][0]['exclusions'] == []
        canonical = deletion(canonicals['occurrence-b']['id'], expected=4)
        assert canonical['details']['raw_ref'] == 'sinex:ev/'+b_ref and canonical['details']['archived'] == []
        assert operator_api('delete.run', {'ref': 'events where kind = command.executed', 'action_id': new_action()})['error']['kind'] == 'bad_query'
        # With the stored request missing, a retry of a finished action is refused.
        request_ref = sql(h, f"SELECT to_json(request_ref) FROM core.operations_log WHERE id='{initial_outcome['operation_id']}'::uuid")
        path = blob_path(d, request_ref)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        hidden = path.with_suffix('.hidden')
        path.rename(hidden)
        try:
            assert deletion(a_ref, initial_action, expected=4)['kind'] == 'refused'
        finally:
            hidden.rename(path)
        assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
        assert deletion(a_ref, initial_action) == initial_outcome
        assert sql(h, counts) == before
        population(['occurrence-b'])
        d.stop()
    scenario('repeat deletes, conflicting retries, unknown targets and canonical refs', retries_and_refusals)
