"""Replay of a stored Atuin material after its source file is gone: chunked correction of a
bad reading, recovery after kills at each pause point, cancellation and refusals."""
import concurrent.futures
import json
import subprocess
import uuid

from _atuin_daemon import Daemon, access_config, blob_path, create_history, kill, pause_at, paused, relaunch, sql

EPOCH = 1_700_000_000_123_456_789
QUERY = 'events where source = terminal.atuin-history and kind = command.executed'


def exercise(h):
    work, scenario = h['WORK'], h['scenario']
    d = Daemon(h, 'replay')
    def cli(*args, operator=False, **kw):
        return d.cli(*args, token_file=work/'operator' if operator else None, **kw)
    source = work/'replay.db'
    # 130 rows: 0..63 present, 64..127 deleted at the source, 128 and 129 present.
    create_history(source, [(f'fixture-{i:03}', EPOCH+i*1_000_000_000, 1_000_000_000, 0, f'echo fixture {i}',
                            '/synthetic/replay', 'fixture', 'fixture-host:fixture-user', EPOCH if 64 <= i < 128 else None)
                           for i in range(130)])

    def configure(collect=False, operator=True):
        d.configure(source if collect else None, poll_ms=3_600_000, top=access_config(h, operator, read_model=False))

    def audit():
        return sql(h, "SELECT json_build_object('seq',(SELECT seq FROM core.commit_counter WHERE one),"
                     "'events',(SELECT count(*) FROM core.events),'archives',(SELECT count(*) FROM audit.archived),"
                     "'effects',(SELECT count(*) FROM core.operation_effects),'operations',(SELECT count(*) FROM core.operations_log))")

    def live_rows():
        return sql(h, "SELECT coalesce(json_agg(to_jsonb(e) ORDER BY e.anchor_byte),'[]') FROM core.events e "
                     f"WHERE source_material_id='{material}'::uuid AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=e.id)")

    def ids(rows):
        return {r['anchor_byte']: r['id'] for r in rows}

    def changed(before, after):
        return sum(ids(before)[a] != ids(after)[a] for a in ids(before))

    def progress(operation_id):
        return sql(h, f"SELECT progress FROM core.operations_log WHERE id='{operation_id}'::uuid")

    def wait_operation(operation_id, state='completed'):
        return d.wait('replay ' + state, lambda: (p := progress(operation_id))['state'] == state and p)

    material = initial = before_trace = None

    def bad_reading():
        nonlocal material, initial, before_trace
        configure(collect=True)
        relaunch(d, 'bad-reading', '--test-bad-atuin-parse')
        run = d.wait('first read', lambda: (r := d.last_run()) and r['status'] == 'completed' and r)
        assert run['rows_read'] == 130 and run['admitted'] == 129 and run['record_errors'] == 1
        answer = cli('query', QUERY)
        assert answer['total'] == 65 and answer['complete'] is True
        bad = next(r for r in answer['rows'] if r['payload']['native_id'] == 'fixture-000')
        assert bad['payload']['hostname'] == 'controlled-historical-bad-interpretation'
        assert bad['written_by']['kind'] == 'test_fixture'
        before_trace = cli('trace', bad['id'], '--bytes')['row']
        assert before_trace['record']['hostname'] == 'fixture-host:fixture-user'
        assert before_trace['record']['timestamp'] == EPOCH
        material = before_trace['material']
        d.stop()
        source.unlink()
        configure()
        relaunch(d, 'stored-only')
        initial = live_rows()
        assert len(initial) == 129 and sum(r['record_state'] == 'removed' for r in initial) == 64
    scenario('a deliberately bad reading is stored and the source file removed', bad_reading)

    selection = f'materials where id = {material}'
    action = str(uuid.uuid4())
    plans = {}
    operation = None

    def plan(action_id):
        plans[action_id] = cli('replay', selection, '--dry-run')
        return plans[action_id]

    def execute(action_id=action, expected=0, with_plan=False):
        extra = ['--basis', plans[action_id]['basis']] if with_plan else []
        return cli('replay', selection, '--action-id', action_id, *extra, expected=expected, operator=True)

    def kill_and_restart(connection, frame, label, check_operator=False):
        kill(d)
        connection.close()
        if not check_operator:
            relaunch(d, label + '-restart')
            return
        configure(operator=False)
        relaunch(d, label + '-no-operator')
        current = progress(frame['evidence']['operation']['operation_id'])
        assert current['processed'] == 128 and current['state'] == 'running'
        assert cli('status')['replay_authorized'] is False
        before = audit()
        assert execute(action, 4, True)['kind'] == 'refused'
        assert audit() == before
        configure()
        relaunch(d, label + '-operator')

    def same_rows_new_ids(old_rows, new_rows):
        assert len(old_rows) == 129 and len(new_rows) == 130
        by_anchor = {row['anchor_byte']: row for row in new_rows}
        for old in old_rows:
            new = by_anchor[old['anchor_byte']]
            for field in ['source_material_id', 'anchor_byte', 'offset_start', 'offset_end', 'ordinal', 'event_type',
                          'equivalence_key', 'record_hash', 'ts_orig', 'ts_orig_tiebreak', 'record_state']:
                assert old[field] == new[field], field
            assert old['id'] != new['id'] and old['run_id'] != new['run_id']

    def kill_before_commit():
        nonlocal operation
        before = audit()
        preview = plan(action)
        assert preview['selected'] == 130 and preview['chunks'] == 3 and preview['chunk_max_records'] == 64
        assert audit() == before, 'dry run wrote state'
        listener = pause_at(d, 'replay-staged-2')
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(execute, action, (1, 4), True)
            connection, frame = paused(d, listener, 'replay-staged-2')
            operation = frame['evidence']['operation']['operation_id']
            running = progress(operation)
            assert running['processed'] == running['admitted'] == running['archived'] == 128 and running['state'] == 'running'
            assert cli('replay-status', operation, expected=5) == running
            held = live_rows()
            assert changed(initial, held) == 128
            assert all(r['record_state'] == 'removed' for r in held[64:128])
            answer = cli('query', QUERY)
            assert answer['total'] == 65
            corrected = next(r for r in answer['rows'] if r['payload']['native_id'] == 'fixture-000')
            assert corrected['payload']['hostname'] == 'fixture-host', 'a new ID alone does not show the reading was corrected'
            trace = cli('trace', corrected['id'], '--bytes')['row']
            assert all(trace[k] == before_trace[k] for k in ['material', 'anchor', 'end', 'record', 'representation'])
            old = cli('show', before_trace['event'])
            assert old['resolved'] == 'archived' and old['row']['archive']['reason'] == 'replay'
            assert old['row']['archive']['operation_id'] == operation
            kill_and_restart(connection, frame, 'kill-before-commit', check_operator=True)
            assert future.result(timeout=15)['kind'] in ('internal', 'refused')
        # Startup resumes the running operation without a new request.
        completed = wait_operation(operation)
        final = live_rows()
        same_rows_new_ids(initial, final)
        assert changed(held, final) == 1 and len(set(ids(final)) - set(ids(held))) == 1
        assert completed['processed'] == 130 and completed['archived'] == 129 and completed['admitted'] == 130 and completed['steps'] == 3
        restored = next(r for r in cli('query', QUERY)['rows'] if r['payload']['native_id'] == 'fixture-129')
        assert restored['payload']['command'] == 'echo fixture 129' and restored['operation'] == operation
        restored_trace = cli('trace', restored['id'], '--bytes')['row']
        assert restored_trace['record']['id'] == 'fixture-129' and restored_trace['record']['timestamp'] == EPOCH+129*1_000_000_000
        receipt = audit()
        assert execute(action, 0, True) == completed and audit() == receipt
        assert any(r['operation_id'] == operation and r['state'] == 'completed' for r in cli('status')['replays'])
    scenario('replay corrects the reading in 64-row chunks and resumes after a kill before commit', kill_before_commit)

    def kill_after_commit():
        other = str(uuid.uuid4())
        listener = pause_at(d, 'replay-committed-0')
        before = live_rows()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(execute, other, 1)
            connection, frame = paused(d, listener, 'replay-committed-0')
            op = frame['evidence']['operation']['operation_id']
            assert progress(op)['admitted'] == 64
            held = live_rows()
            assert changed(before, held) == 64
            kill_and_restart(connection, frame, 'kill-after-commit')
            assert future.result(timeout=15)['kind'] in ('internal', 'refused')
        assert wait_operation(op)['admitted'] == 130
        assert changed(held, live_rows()) == 66
        receipt = audit()
        assert execute(other)['admitted'] == 130 and audit() == receipt
    scenario('replay resumes after a kill right after a chunk commits', kill_after_commit)

    def disconnect():
        listener = pause_at(d, 'replay-staged-0', 'disconnect')
        request = str(uuid.uuid4())
        before = live_rows()
        out, err = (h['REPORT']/'disconnected-client.stdout').open('w'), (h['REPORT']/'disconnected-client.stderr').open('w')
        client = subprocess.Popen([str(h['BIN_DIR']/'sinexctl'), '--endpoint', d.endpoint, '--token-file', str(work/'operator'),
                                   'replay', selection, '--action-id', request, '--json'],
                                  env=h['ENV'], stdout=out, stderr=err)
        h['CHILDREN'].append((client, out, err))
        connection, frame = paused(d, listener, 'replay-staged-0')
        op = frame['evidence']['operation']['operation_id']
        client.kill()
        client.wait(timeout=10)
        connection.sendall(b'c')
        connection.close()
        completed = wait_operation(op)
        assert completed['admitted'] == 130
        assert not (h['REPORT']/'disconnected-client.stdout').read_text()
        assert changed(before, live_rows()) == 130
        receipt = audit()
        assert execute(request) == completed and audit() == receipt
    scenario('a killed client does not stop its replay', disconnect)

    def changed_binary():
        listener = pause_at(d, 'replay-staged-2', 'before-binary-change')
        request = str(uuid.uuid4())
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(execute, request, (1, 4))
            connection, frame = paused(d, listener, 'replay-staged-2')
            op = frame['evidence']['operation']['operation_id']
            assert progress(op)['processed'] == 128
            held, receipt = live_rows(), audit()
            artifact = "SELECT to_jsonb(m.artifact_digest) FROM core.runs r JOIN core.manifests m ON m.id=r.manifest_id ORDER BY r.started_at DESC LIMIT 1"
            original_artifact = sql(h, artifact)
            kill(d)
            connection.close()
            # A copy of the daemon with bytes appended runs the same code but has a different file digest.
            variant = work/'sinexd-variant'
            variant.write_bytes((h['BIN_DIR']/'sinexd').read_bytes() + b'\nsynthetic marker\n')
            variant.chmod(0o700)
            relaunch(d, 'changed-binary', binary=variant)
            refused = wait_operation(op, 'refused')
            assert refused['processed'] == refused['archived'] == refused['admitted'] == 128
            assert 'parser settings changed' in refused['reason']
            assert ids(live_rows()) == ids(held) and audit()['effects'] == receipt['effects']
            assert sql(h, artifact) != original_artifact
            assert future.result(timeout=15)['kind'] in ('internal', 'refused')
        relaunch(d, 'original-binary')
        receipt = audit()
        assert execute(request, 4)['details']['processed'] == 128 and audit() == receipt
    scenario('a different daemon binary refuses to continue a half-done replay', changed_binary)

    def missing_bytes():
        path = blob_path(d, material)
        encoded = path.read_bytes()
        hidden = path.with_suffix('.hidden')
        for mode in ['absent', 'corrupt']:
            original, before, request = audit(), ids(live_rows()), str(uuid.uuid4())
            if mode == 'absent':
                path.rename(hidden)
            else:
                path.write_bytes(b'corrupt synthetic stored object')
            try:
                refused = execute(request, 4)
                assert refused['kind'] == 'refused'
                assert ids(live_rows()) == before
                now = audit()
                assert all(now[k] == original[k] for k in ['events', 'archives', 'effects'])
                assert cli('query', QUERY)['total'] == 66
            finally:
                if mode == 'absent':
                    hidden.rename(path)
                else:
                    path.write_bytes(encoded)
            receipt = audit()
            assert execute(request, 4) == refused and audit() == receipt
        # A finished action still answers when its material is missing.
        path.rename(hidden)
        try:
            receipt = audit()
            assert execute(action, 0, True)['admitted'] == 130 and audit() == receipt
        finally:
            hidden.rename(path)
        # Material that disappears mid-replay keeps the two committed chunks.
        listener = pause_at(d, 'replay-staged-2', 'missing-mid-replay')
        request = str(uuid.uuid4())
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(execute, request, (1, 4))
            connection, frame = paused(d, listener, 'replay-staged-2')
            op = frame['evidence']['operation']['operation_id']
            held, receipt = live_rows(), audit()
            path.rename(hidden)
            kill_and_restart(connection, frame, 'missing-mid-replay')
            assert future.result(timeout=15)['kind'] in ('internal', 'refused')
        try:
            failed = wait_operation(op, 'refused')
            assert failed['admitted'] == 128 and ids(live_rows()) == ids(held)
            assert audit()['archives'] == receipt['archives'] and audit()['effects'] == receipt['effects']
            assert cli('query', QUERY)['total'] == 66
        finally:
            hidden.rename(path)
    scenario('missing or corrupt stored bytes refuse replay without changing anything', missing_bytes)

    def missing_action_records():
        stored = sql(h, f"SELECT json_build_object('request_ref',request_ref,'outcome_ref',outcome_ref) FROM core.operations_log WHERE id='{operation}'::uuid")
        for field in ['request_ref', 'outcome_ref']:
            path = blob_path(d, stored[field])
            encoded = path.read_bytes()
            hidden = path.with_suffix('.hidden')
            for mode in ['absent', 'corrupt']:
                receipt, before = audit(), ids(live_rows())
                if mode == 'absent':
                    path.rename(hidden)
                else:
                    path.write_bytes(b'corrupt synthetic action record')
                try:
                    assert execute(action, 4, True)['kind'] == 'refused'
                    assert cli('replay-status', operation, expected=4)['kind'] == 'refused'
                    assert audit() == receipt and ids(live_rows()) == before
                finally:
                    if mode == 'absent':
                        hidden.rename(path)
                    else:
                        path.write_bytes(encoded)
                receipt = audit()
                assert execute(action, 0, True)['state'] == 'completed' and audit() == receipt
    scenario('a retry is refused while the stored request or outcome is missing or corrupt', missing_action_records)

    def cancel_and_conflicts():
        receipt = audit()
        assert cli('replay', selection, '--action-id', str(uuid.uuid4()), expected=4)['kind'] == 'refused'
        assert cli('replay', selection, '--parser', 'invented', operator=True, expected=2)['kind'] == 'bad_query'
        assert cli('replay', 'events', '--dry-run', expected=2)['kind'] == 'bad_query'
        assert cli('replay', f'materials where id = {uuid.uuid4()}', '--action-id', action, '--basis', plans[action]['basis'],
                   operator=True, expected=4)['kind'] == 'refused'
        assert audit() == receipt
        stale = str(uuid.uuid4())
        plans[stale] = plans[action]
        assert execute(stale, 4, True)['kind'] == 'refused'
        # Release the paused writer only after the daemon has logged the cancel request.
        listener = pause_at(d, 'replay-staged-0', 'cancel')
        request = str(uuid.uuid4())
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            running = executor.submit(execute, request, 5)
            connection, frame = paused(d, listener, 'replay-staged-0')
            op = frame['evidence']['operation']['operation_id']
            cancelling = executor.submit(cli, 'replay-cancel', op, expected=5, operator=True)

            def cancel_logged():
                for line in (h['REPORT']/'cancel.stderr').read_text().splitlines():
                    try:
                        note = json.loads(line)
                    except json.JSONDecodeError:  # the last line may be half written
                        continue
                    if note.get('phase') == 'replay_cancel_enqueued' and note.get('operation_id') == op:
                        return True
            d.wait('cancel request in the daemon log', cancel_logged, 10)
            connection.sendall(b'c')
            connection.close()
            stopped = cancelling.result(timeout=15)
            assert stopped['state'] == 'cancelled' and stopped['admitted'] == 64
            assert running.result(timeout=15) == stopped
        receipt = audit()
        assert execute(request, 5) == stopped and audit() == receipt
        stored = sql(h, "SELECT json_build_object('operations',(SELECT json_agg(to_jsonb(o)) FROM core.operations_log o),"
                       "'effects',(SELECT json_agg(to_jsonb(e)) FROM core.operation_effects e))")
        assert all(o['request_ref'] and o['request_format'] == 'sinex.replay.request.v1' and o['action_principal'] == 'foundation.operator'
                   for o in stored['operations'])
        assert all(o['outcome_ref'] for o in stored['operations'])
        assert all(e['effects_ref'] and e['effect_format'] == 'sinex.replay.effects.v1' for e in stored['effects'])
        guard = h['run'](['psql', h['writer'], '-XAt', '-v', 'ON_ERROR_STOP=1', '-c',
                          f"UPDATE core.operations_log SET scope='{{}}'::jsonb WHERE id='{operation}'::uuid"],
                         expected=1, label='operation request is immutable')
        assert 'operation identity and admitted request are immutable' in guard.stderr
    scenario('cancellation, missing operator token and conflicting requests', cancel_and_conflicts)

    def malformed():
        d.stop()
        create_history(source, [
            ('malformed-control-good', EPOCH, 1, 0, 'malformed control valid', '/synthetic', 'control', 'fixture-host:user', None),
            ('malformed-control-bad', 'not-a-time', 1, 0, 'malformed control invalid', '/synthetic', 'control', 'fixture-host:user', None)])
        configure(collect=True)
        relaunch(d, 'malformed')
        d.wait('malformed source read', lambda: (r := d.last_run()) and r['status'] == 'completed' and r['rows_read'] == 2)
        listed = d.sources()[0]
        assert listed['last_run']['record_errors'] == 1
        assert listed['first_record_error']['record']['timestamp'] == 'not-a-time'
        good = cli('query', 'events where payload.command = "malformed control valid"')['rows'][0]
        traced = cli('trace', good['id'], '--bytes')['row']
        d.stop()
        source.unlink()
        configure()
        relaunch(d, 'malformed-stored-only')
        receipt = audit()
        bad_selection = f'materials where id = {traced["material"]}'
        assert cli('replay', bad_selection, '--dry-run', expected=4)['kind'] == 'refused'
        refused = cli('replay', bad_selection, '--action-id', str(uuid.uuid4()), expected=4, operator=True)
        assert refused['kind'] == 'refused' and refused['details']['admitted'] == 0
        assert cli('show', good['id'])['resolved'] == 'live'
        now = audit()
        assert all(now[k] == receipt[k] for k in ['events', 'archives', 'effects'])
    scenario('replay of a material with a malformed row is refused and keeps current answers', malformed)
    d.stop()
