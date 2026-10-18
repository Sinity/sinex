"""Atuin read plans: when the collector does a full read and when a reduced read of new
rows and still-unfinished commands, what each read stores, and that session recall
through the CLI and MCP keeps the same answer across these reads, restart and source loss."""
import datetime as dt
import json
import shutil
import sqlite3

from _atuin_daemon import ATUIN_QUERY, COLUMNS, INSERT, Daemon, Mcp, create_history, sql

FULL, REDUCED = 'full_value_comparison', 'new_coordinates_and_prior_unfinished'


def stamp(day):
    return int(dt.datetime.fromisoformat(day + '+00:00').timestamp())*1_000_000_000


def row(key, when, cwd='/work/other', duration=0, exit=0):
    return dict(zip(COLUMNS, [key, when, duration, exit, 'echo ' + key, cwd, 'synthetic-shell', 'synthetic-host', None]))


def values(r):
    return [r[k] for k in COLUMNS]


def exercise(h):
    work, scenario = h['WORK'], h['scenario']
    source, configured = work/'read-plan.db', work/'configured.db'
    facts = [row('very-old-unfinished', stamp('2026-10-01T00:00:00'), duration=-1, exit=-1)]
    facts += [row(f'history-{n:03}', stamp('2026-10-02T00:00:00') + n*60_000_000_000) for n in range(192)]
    facts += [row('recovery', stamp('2026-10-13T12:00:00'), '/work/tide'),
              row('end-unknown', stamp('2026-10-13T12:01:00'), '/work/tide', exit=-1)]
    create_history(source, map(values, facts))
    configured.symlink_to(source)
    read_model = work/'read-model'
    read_model.write_text(h['url']('sinex_read_model'))
    read_model.chmod(0o600)
    daemon = Daemon(h, 'read-plan')
    daemon.configure(configured, poll_ms=1200, full_ms=90000, automata=['session'],
                     top=f'read_model_connection_file = "{read_model}"\n',
                     tables='[[projects]]\nname = "tide"\nroots = ["/work/tide"]\n'
                            '[[projects]]\nname = "other"\nroots = ["/work/other"]\n')
    mcp = None

    def wait(label, predicate):
        return daemon.wait(label, predicate, 150)

    def plan(s):
        return s['last_run']['metadata']['_sinex_read_plan_v1']

    def completed(after=None, kind=None):
        s = daemon.sources()[0]
        o = s['last_run']
        p = o['metadata'].get('_sinex_read_plan_v1')
        if o['status'] == 'completed' and o['id'] != after and o['metadata'].get('_sinex_acquisition_v1', {}).get('schedule') \
                and p and (kind is None or p['kind'] == kind):
            return s
        return None

    def next_read(previous, kind=None):
        return wait(f'next completed {kind or "read"}', lambda: completed(previous['last_run']['id'], kind))

    def unavailable(after=None, minimum=0):
        s = daemon.sources()[0]
        o = s['last_run']
        return s if o['status'] == 'unavailable' and o['id'] != after and o['rows_read'] >= minimum else None

    def rows_read(s, n):
        assert s['last_run']['rows_read'] == n, (s['last_run'], n)

    def checkpoint():
        return sql(h, "SELECT carry FROM raw.source_cursors WHERE binding='terminal.atuin-history'", h['reader'])

    def read_orders():
        return sql(h, "SELECT jsonb_object_agg(e.payload->>'native_id',l.read_order) FROM core.live_equivalence l "
                      "JOIN core.events e ON e.id=l.event_id WHERE e.source='terminal.atuin-history' "
                      "AND e.payload->>'native_id' IN ('very-old-unfinished','history-000','end-unknown')", h['reader'])

    def stored_values(status, expected):
        """The material stored by this read holds exactly the `expected` source rows."""
        observation = status['last_run']['id']
        materials = sql(h, "SELECT coalesce(jsonb_agg(jsonb_build_object('observation',m.observation_id,'status',m.status,"
                           "'bytes',m.durable_bytes,'hash',b.blake3)),'[]') FROM raw.source_material_registry m "
                           f"LEFT JOIN core.blobs b ON b.id=m.blob_id WHERE m.observation_id='{observation}'::uuid", h['reader'])
        zstd = shutil.which('zstd')
        assert zstd, 'zstd is not on PATH'
        stored = []
        for m in materials:
            assert m['status'] == 'completed' and m['bytes'] > 0 and m['observation'] == observation
            digest = m['hash'].removeprefix('blake3:')
            blob = daemon.store_dir/'blobs'/digest[:2]/(digest + '.zst')
            decoded = h['run']([zstd, '-d', '-q', '-c', blob], label='decode stored material').stdout
            assert len(decoded.encode()) == m['bytes']
            for record in map(json.loads, decoded.splitlines()):
                assert record['_from']['observation'] == observation and record['_from']['table'] == 'history'
                assert isinstance(record['_from']['rowid'], int)
                stored.append({k: v for k, v in record.items() if k != '_from'})
        assert sorted(stored, key=lambda r: r['id']) == sorted(expected, key=lambda r: r['id'])

    def event(key):
        """The current event for a native id, paging through the raw query."""
        params = {'query': ATUIN_QUERY}
        while True:
            answer = daemon.api('query.run', params)['result']
            found = next((r for r in answer['rows'] if r['payload']['native_id'] == key), None)
            if found or not answer['page']['cursor']:
                return found
            params['cursor'] = answer['page']['cursor']

    def check_recall(answer):
        rows = answer['rows']
        assert sorted(r['payload']['native_id'] for r in rows) == ['end-unknown', 'recovery']
        by_id = {r['payload']['native_id']: r for r in rows}
        assert by_id['end-unknown']['payload']['exit_code'] is None
        assert by_id['end-unknown']['payload']['completion'] == 'ended'
        assert set(answer['session']['parents']) == {r['id'] for r in rows}
        assert answer['attribution'] == {'rule': 'direct_member_own_cwd', 'known_projects': {'tide': 2},
                                         'unconfigured_members': 0, 'unknown_members': [], 'total_members': 2, 'complete': True}
        assert answer['newer_unresolved_candidates'] == []
        assert answer['coverage'] == 'unknown; grouped by shell session rules; focus and attention are not inferred'
        assert int(answer['session']['payload']['start_ns']) == stamp('2026-10-13T12:00:00')
        assert int(answer['session']['payload']['end_ns']) == stamp('2026-10-13T12:01:00')

    def recall(prior=None, source_unavailable=False):
        """`recall last` for project tide via CLI and MCP; both agree and trace back to the source rows."""
        wait('session recall ready', lambda: daemon.api('query.run', {'query': 'recall last where project = tide'}).get('result'))
        answer = daemon.cli('recall', 'last', '--project', 'tide', expected=5 if source_unavailable else (0, 5))
        agent = mcp.tool('sinex_recall', {'project': 'tide'})
        check_recall(answer)
        check_recall(agent)
        for key in ['session', 'rows', 'attribution', 'newer_unresolved_candidates', 'coverage', 'read_as']:
            assert answer[key] == agent[key], key
        if prior:
            assert answer['session'] == prior['session'] and answer['rows'] == prior['rows']
        trace = daemon.cli('trace', 'sinex:ev/' + answer['session']['id'], '--bytes', expected=(0, 5))['row']
        for parent in trace['parents']:
            stored = parent['parents'][0]
            fact = next(r for r in facts if r['id'] == stored['record']['id'])
            assert all(stored['record'][key] == fact[key] for key in COLUMNS)
            assert stored['end'] > stored['anchor'] and stored['material']
        if source_unavailable:
            assert answer['complete'] is False
        return answer

    def change(key, **updates):
        next(r for r in facts if r['id'] == key).update(updates)
        with sqlite3.connect(source) as con:
            con.execute('UPDATE history SET ' + ','.join(k + '=?' for k in updates) + ' WHERE id=?', [*updates.values(), key])

    def insert(*rows):
        with sqlite3.connect(source) as con:
            con.executemany(INSERT, map(values, rows))

    baseline = None
    try:
        daemon.start('read-plan')
        daemon.ready()
        mcp = Mcp(h, daemon)

        def initial():
            nonlocal baseline
            first = wait('first full read', lambda: completed(kind=FULL))
            rows_read(first, len(facts))
            assert checkpoint() == plan(first)['checkpoint']
            first_orders = read_orders()
            second = next_read(first, REDUCED)
            rows_read(second, 1)
            second_orders = read_orders()
            # The reduced read rereads only the unfinished command.
            assert second_orders['history-000'] == first_orders['history-000']
            assert second_orders['end-unknown'] == first_orders['end-unknown']
            assert second_orders['very-old-unfinished'] > first_orders['very-old-unfinished']
            assert plan(second)['checkpoint']['full_due_at'] == plan(first)['checkpoint']['full_due_at']
            stored_values(second, [facts[0]])
            baseline = recall()
        scenario('the first read is full; the next reads only the unfinished row', initial)

        def late_completion():
            old = event('very-old-unfinished')
            assert old['payload']['completion'] == 'end_not_recorded'
            previous = wait('a reduced read', lambda: completed(kind=REDUCED))
            full_due = plan(previous)['checkpoint']['full_due_at']
            change('very-old-unfinished', duration=10, exit=7)
            updated = next_read(previous, REDUCED)
            rows_read(updated, 1)
            changed = event('very-old-unfinished')
            assert changed['id'] != old['id'] and changed['payload']['completion'] == 'ended' and changed['payload']['exit_code'] == 7
            fresh = row('fresh-native-coordinate', stamp('2026-10-03T00:00:00'))
            facts.append(fresh)
            insert(fresh)
            arrival = next_read(updated, REDUCED)
            rows_read(arrival, 1)
            assert event('fresh-native-coordinate')['payload']['command'] == fresh['command']
            zero = next_read(arrival, REDUCED)
            rows_read(zero, 0)
            assert plan(zero)['checkpoint']['full_due_at'] == full_due
            stored_values(zero, [])
            recall(baseline)
        scenario('a reduced read picks up a late completion and new rows', late_completion)

        def full_reads():
            before = wait('a reduced read', lambda: completed(kind=REDUCED))
            old = event('history-000')
            change('history-000', exit=9)
            reduced = next_read(before, REDUCED)
            rows_read(reduced, 0)
            assert event('history-000')['id'] == old['id'], 'a finished row changed without being read'
            full = next_read(reduced, FULL)
            rows_read(full, len(facts))
            assert plan(full)['checkpoint']['epoch'] == plan(reduced)['checkpoint']['epoch'], 'the due full read reopened the source'
            changed = event('history-000')
            assert changed['payload']['exit_code'] == 9 and changed['id'] != old['id']
            # A locked source fails the read without moving the checkpoint.
            prior = checkpoint()
            lock = sqlite3.connect(source)
            lock.execute('BEGIN EXCLUSIVE')
            try:
                first_failure = wait('locked source unavailable', unavailable)
                failed = wait('second read also unavailable', lambda: unavailable(first_failure['last_run']['id']))
                assert checkpoint() == prior, 'a failed read moved the checkpoint'
            finally:
                lock.rollback()
                lock.close()
            recovered = next_read(failed, FULL)
            rows_read(recovered, len(facts))
            assert plan(recovered)['checkpoint']['epoch'] != prior['epoch']
            # An oversized row fails the full read after a stored prefix; the checkpoint stays.
            prior_partial = checkpoint()
            oversized = row('oversized-final', stamp('2026-10-14T00:00:00'))
            oversized['command'] = 'x'*(1024*1024)
            with sqlite3.connect(source) as con:
                con.execute('PRAGMA user_version=42')
                con.execute(INSERT, values(oversized))
            partial = wait('full read fails on the oversized row', lambda: unavailable(minimum=64))
            assert checkpoint() == prior_partial, 'a partial full read moved the checkpoint'
            with sqlite3.connect(source) as con:
                con.execute("DELETE FROM history WHERE id='oversized-final'")
            after_partial = next_read(partial, FULL)
            rows_read(after_partial, len(facts))
            assert event('history-000')['id'] == changed['id']
            # A row that fails to parse forces full reads until it is gone.
            insert(row('malformed-new-coordinate', stamp('2026-10-14T01:00:00'), duration=-2))
            omission = next_read(after_partial)
            rows_read(omission, 1)
            assert omission['last_run']['record_errors'] == 1 and plan(omission)['checkpoint']['needs_full']
            forced = next_read(omission)
            assert plan(forced)['kind'] == FULL
            rows_read(forced, len(facts) + 1)
            assert forced['last_run']['record_errors'] == 1 and plan(forced)['checkpoint']['needs_full']
            with sqlite3.connect(source) as con:
                con.execute("DELETE FROM history WHERE id='malformed-new-coordinate'")
            cleared = next_read(forced)
            assert plan(cleared)['kind'] == FULL
            rows_read(cleared, len(facts))
            assert not plan(cleared)['checkpoint']['needs_full']
        scenario('changes outside the reduced read wait for the due full read; failed reads keep the checkpoint', full_reads)

        def file_changes():
            nonlocal source
            records = []
            for kind in ['vacuum', 'user-version', 'schema', 'rename-replace', 'symlink-retarget']:
                before = wait('a completed read before ' + kind, completed)
                old = plan(before)['checkpoint']
                if kind == 'vacuum':
                    with sqlite3.connect(source) as con:
                        con.execute('VACUUM')
                elif kind == 'user-version':
                    with sqlite3.connect(source) as con:
                        con.execute('PRAGMA user_version=7')
                elif kind == 'schema':
                    with sqlite3.connect(source) as con:
                        con.execute('CREATE TABLE synthetic_added(value INTEGER)')
                elif kind == 'rename-replace':
                    source.rename(source.with_name('renamed-old.db'))
                    create_history(source, map(values, facts))
                else:
                    target = work/'symlink-next.db'
                    create_history(target, map(values, facts))
                    configured.unlink()
                    configured.symlink_to(target)
                    source = target
                full = next_read(before)
                assert plan(full)['kind'] == FULL, (kind, full)
                rows_read(full, len(facts))
                new = plan(full)['checkpoint']
                if kind in ('rename-replace', 'symlink-retarget'):
                    assert new['epoch'] != old['epoch'], kind
                elif kind == 'user-version':
                    assert new['user_version'] != old['user_version']
                else:
                    assert new['schema_version'] != old['schema_version'], kind
                records.append(full)
            previous_epoch = plan(records[-1])['checkpoint']['epoch']
            daemon.restart('read-plan-restart')
            full = wait('full read after restart', lambda: (s if (s := completed(kind=FULL)) and plan(s)['checkpoint']['epoch'] != previous_epoch else None))
            rows_read(full, len(facts))
            recall(baseline)
            # More unfinished rows than one page (64) of the reduced read.
            pending = [row(f'pending-page-{n:03}', stamp('2026-10-04T00:00:00') + n*1_000_000_000, duration=-1, exit=-1) for n in range(65)]
            facts.extend(pending)
            before = wait('a completed read before pending rows', completed)
            insert(*pending)
            admitted = next_read(before)
            assert plan(admitted)['kind'] == REDUCED
            rows_read(admitted, 65)
            with sqlite3.connect(source) as con:
                con.execute("UPDATE history SET duration=1,exit=0 WHERE id LIKE 'pending-page-%'")
            for fact in pending:
                fact.update(duration=1, exit=0)
            updated = next_read(admitted)
            assert plan(updated)['kind'] == REDUCED
            rows_read(updated, 65)
            assert updated['last_run']['admitted'] == 65
            zero = next_read(updated)
            assert plan(zero)['kind'] == REDUCED
            rows_read(zero, 0)
        scenario('VACUUM, version, schema, replacement, symlink change and restart force a full read', file_changes)

        def source_loss():
            current = recall(baseline)
            before = checkpoint()
            daemon.stop()
            source.rename(work/'removed-source.db')
            daemon.start('read-plan-source-loss')
            wait('source unavailable', unavailable)
            assert checkpoint() == before
            recall(current, source_unavailable=True)
        scenario('after the source is removed, recall returns the same stored answer', source_loss)
    finally:
        if mcp is not None:
            mcp.close()
        if daemon.proc is not None and daemon.proc.poll() is None:
            daemon.stop()
