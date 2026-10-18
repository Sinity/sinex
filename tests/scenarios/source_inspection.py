"""`sinexctl sources` (and MCP `sinex_sources`) through Atuin outages: acquisition state,
schedule and overdue reads, admission receipts, history and input paging, record errors
and statement budgets."""
import json
import re
import socket
import sqlite3
import time
import uuid

from _atuin_daemon import COLUMNS, INSERT, Daemon, Mcp, create_history, sql, table_lock


def fixture_row(identity, command='echo synthetic acquisition'):
    return [identity, 1_700_000_000_000_000_000, 1_000_000_000, 0, command,
            '/synthetic/acquisition', 'fixture-session', 'fixture-host', None]


class Pause:
    """Controller for `sinexd serve --test-pause-at collector-scheduled`."""

    def __init__(self, path):
        self.path, self.held = path, None
        self.socket = socket.socket(socket.AF_UNIX)
        self.socket.bind(str(path))
        self.socket.listen(1)
        self.socket.settimeout(30)

    def start(self, daemon, label):
        daemon.start(label, '--test-pause-at', 'collector-scheduled', '--test-pause-socket', self.path)

    def wait_paused(self):
        self.held, _ = self.socket.accept()
        self.held.makefile('rb').readline()

    def resume(self):
        self.held.sendall(b'c')
        self.drop()

    def drop(self):
        if self.held:
            self.held.close()
            self.held = None

    def close(self):
        self.drop()
        self.socket.close()


def exercise(h):
    work, scenario = h['WORK'], h['scenario']
    source = work/'acquisition.db'
    operator = work/'operator-token'
    operator.write_text('synthetic-operator-token')
    operator.chmod(0o600)
    daemon = Daemon(h, 'acquisition')
    top = f'operator_token_file = "{operator}"\n'
    daemon.configure(source, top=top)
    mcp = lock = None
    pauses = []

    def pause(name):
        pauses.append(Pause(work/(name + '.sock')))
        return pauses[-1]

    def src(path=source, **params):
        """The source row for `path`, paging through inputs."""
        request = dict(params)
        while True:
            response = daemon.api('sources.list', request)['result']
            found = [r for r in response['sources'] if r['locator'] == str(path)]
            if found:
                return found[0]
            after = response['page']['next_after_input']
            if not after or 'input' in request:
                return None
            request['after_input'] = after

    def state(row, expected):
        assert row['acquisition']['state'] == expected, (expected, row['acquisition'])

    def observation(input_id, predicate):
        """The first observation in the input's history (newest first) matching `predicate`."""
        before = None
        while True:
            args = {'input': input_id}
            if before is not None:
                args['before_seq'] = before
            history = daemon.api('sources.list', args)['result']['sources'][0]['history']
            assert history['complete'] or history.get('next_before_seq'), history
            found = next((row for row in history['rows'] if predicate(row)), None)
            if found:
                return found
            before = history.get('next_before_seq')
            if before is None:
                return None

    def receipts(input_id, observation_id):
        """All admission receipts of one observation, paging through the CLI."""
        rows, after = [], None
        while True:
            args = ['--input', input_id, '--observation', observation_id] + (['--after-receipt', after] if after else [])
            page = daemon.cli('sources', *args)['sources'][0]['receipts']
            rows += page['rows']
            after = page['next_after_receipt']
            if not after:
                break
        assert page['complete'] and len({r['event'] for r in rows}) == len(rows)
        return rows

    def completed(path=source, after=None):
        row = src(path)
        if row and row['last_run']['status'] == 'completed' and row['acquisition']['schedule'] and row['last_run']['id'] != after:
            return row
        return None

    def with_status(status, path=source):
        row = src(path)
        return row if row and row['last_run']['status'] == status else None

    try:
        def initial():
            nonlocal lock
            daemon.start('acquisition-first-missing')
            first = daemon.wait('first read fails', lambda: (r if (r := with_status('unavailable')) and r['acquisition']['schedule'] else None))
            state(first, 'unavailable')
            assert first['last_success'] is None and first['last_failure']['id'] == first['last_run']['id']
            assert first['last_run']['observed_at'] < first['acquisition']['schedule']['settled_at']
            assert first['receipts']['rows'] == [] and first['receipts']['complete']
            daemon.stop()
            create_history(source)
            lock = sqlite3.connect(source)
            lock.execute('BEGIN EXCLUSIVE')
        scenario('a missing source fails its first read', initial)

        def overdue():
            nonlocal lock, mcp
            held = pause('collector')
            held.start(daemon, 'acquisition-held')
            reading = daemon.wait('blocked read', lambda: with_status('reading'))
            assert reading['acquisition']['state'] == 'reading' and reading['acquisition']['schedule'] is None
            assert reading['last_success'] is None and reading['last_failure']['id']
            lock.rollback()
            lock.close()
            lock = None
            held.wait_paused()
            mcp = Mcp(h, daemon)
            tools = mcp.call('tools/list', {})['result']['tools']
            assert 'observation' in next(t for t in tools if t['name'] == 'sinex_sources')['inputSchema']['properties']
            before = daemon.cli('sources')['sources'][0]
            assert before['last_run']['status'] == 'completed' and before['last_run']['rows_read'] == 0
            state(before, 'completed')
            assert before['last_run']['commit_seq'] > before['cursor']['commit_seq'], 'recording the schedule did not advance the read position'
            assert mcp.tool('sinex_sources', {})['sources'][0]['last_run']['id'] == before['last_run']['id']
            # The collector is paused, so its planned next start passes while the API keeps answering.
            after = daemon.wait('planned start becomes overdue', lambda: (r if (r := src())['acquisition']['state'] == 'overdue' else None))
            assert after['last_run'] == before['last_run'] and after['receipts']['rows'] == []
            assert daemon.api('status')['result']['writer']['state'] == 'owned'
            held.resume()
        scenario('a paused collector becomes overdue while clients stay responsive', overdue)

        def unchanged():
            prior = src()['last_run']['id']
            r = daemon.wait('empty source reread', lambda: completed(after=prior))
            assert r['last_run']['admitted'] == 0
            with sqlite3.connect(source) as con:
                con.execute(INSERT, fixture_row('old'))
            r = daemon.wait('row read', lambda: (x if (x := completed()) and x['last_run']['rows_read'] == 1 else None))
            state(r, 'completed')
            input_id = r['input']
            first_pass = observation(input_id, lambda o: o['status'] == 'completed' and o['admitted'] == 1 and o['rows_read'] == 1)
            assert first_pass and len(receipts(input_id, first_pass['id'])) == 1
            r = daemon.wait('row reread', lambda: (x if (x := completed(after=first_pass['id'])) and x['last_run']['repeated'] == 1 else None))
            state(r, 'completed')
            assert r['last_run']['admitted'] == 0 and receipts(input_id, r['last_run']['id']) == []
            daemon.stop()
            source.rename(work/'moved-away.db')
            daemon.start('acquisition-restart-unavailable')
            failed = daemon.wait('restart without the source fails', lambda: with_status('unavailable'))
            state(failed, 'unavailable')
            assert failed['last_success']['rows_read'] == 1 and failed['last_run']['admitted'] == 0
            daemon.stop()
            (work/'moved-away.db').rename(source)
            daemon.start('acquisition-repair-unchanged')
            repaired = daemon.wait('restored source reread', lambda: (x if (x := completed()) and x['last_run']['repeated'] == 1 else None))
            state(repaired, 'completed')
            assert repaired['last_failure']['id'] and repaired['last_run']['admitted'] == 0
            assert receipts(input_id, repaired['last_run']['id']) == []
        scenario('rereading unchanged rows, before or after an outage, admits nothing new', unchanged)

        def catchup():
            daemon.stop()
            backfill = fixture_row('backfill', 'echo offline backfill')
            with sqlite3.connect(source) as con:
                con.execute(INSERT, backfill)
            daemon.start('acquisition-catchup')
            r = daemon.wait('catch-up read', lambda: (x if (x := completed()) and x['last_run']['rows_read'] == 2 else None))
            state(r, 'completed')
            caught_up = observation(r['input'], lambda o: o['status'] == 'completed' and o['rows_read'] == 2 and o['admitted'] == 1)
            assert caught_up
            found = receipts(r['input'], caught_up['id'])
            assert len(found) == 1
            receipt = found[0]
            assert daemon.cli('show', receipt['event'])['row']['payload']['native_id'] == 'backfill'
            traced = daemon.cli('trace', receipt['event'], '--bytes')['row']
            assert traced['material'] == receipt['material'] and traced['anchor'] == receipt['start'] and traced['end'] == receipt['end']
            assert {k: traced['record'][k] for k in COLUMNS} == dict(zip(COLUMNS, backfill))
            daemon.stop()
            source.rename(work/'moved-away.db')
            daemon.start('acquisition-source-lost')
            state(daemon.wait('source unavailable', lambda: with_status('unavailable')), 'unavailable')
            assert daemon.cli('trace', receipt['event'], '--bytes')['row']['record']['id'] == 'backfill'
            # Replay makes new interpretations from the stored material; they are not new receipts.
            replay = daemon.cli('replay', f'materials where id = {receipt["material"]}', '--action-id', str(uuid.uuid4()),
                                token_file=operator)
            assert replay['state'] == 'completed'
            assert sql(h, 'SELECT count(*) FROM core.events WHERE created_by_operation IS NOT NULL') > 0
            assert receipts(r['input'], caught_up['id']) == found
            assert mcp.tool('sinex_sources', {'input': r['input'], 'observation': caught_up['id']})['sources'][0]['receipts']['rows'] == found
        scenario('rows added while stopped are admitted on restart; replay adds no receipts', catchup)

        def partial():
            daemon.stop()
            path = work/'partial.db'
            create_history(path, [fixture_row(f'partial-{n:03}') for n in range(65)] + [fixture_row('oversize', 'x'*1_100_000)])
            daemon.configure(path, top=top)
            held = pause('partial')
            held.start(daemon, 'acquisition-partial')
            held.wait_paused()
            r = daemon.wait('read fails after a stored prefix',
                            lambda: (x if (x := with_status('unavailable', path)) and x['last_run']['admitted'] == 64 else None))
            failed_id, input_id = r['last_run']['id'], r['input']
            first = receipts(input_id, failed_id)
            assert len(first) == 64
            daemon.stop()
            held.close()
            with sqlite3.connect(path) as con:
                con.execute("DELETE FROM history WHERE id='oversize'")
                con.execute(INSERT, fixture_row('fresh-after-partial'))
            daemon.start('acquisition-partial-repair')
            daemon.wait('next read', lambda: (x if (x := completed(path)) and x['last_run']['rows_read'] == 66 else None))
            admitted = observation(input_id, lambda o: o['status'] == 'completed' and o['admitted'] == 2 and o['rows_read'] == 66)
            assert admitted and len(receipts(input_id, admitted['id'])) == 2
            assert receipts(input_id, failed_id) == first
        scenario('rows admitted by a failed read keep their receipts on that read', partial)

        def paging():
            path = work/'partial.db'
            daemon.wait('more than one history page', lambda: (x if (x := src(path)) and not x['history']['complete'] else None))
            daemon.stop()
            held = pause('paging')
            held.start(daemon, 'acquisition-paging-held')
            held.wait_paused()
            first = src(path)
            input_id = first['input']
            seen, before = [], None
            while True:
                page = daemon.cli('sources', '--input', input_id, *(['--before-seq', str(before)] if before else []))['sources'][0]['history']
                assert len(page['rows']) <= 8
                seen += [x['id'] for x in page['rows']]
                before = page['next_before_seq']
                if before is None:
                    break
            assert len(seen) > 8 and len(set(seen)) == len(seen)
            assert first['last_failure']['id'], 'the last failure is reported even when it is beyond the first history page'
            # With receipt detail locked, the latest run is still reported and detail is marked unavailable.
            with table_lock(h, 'core.events'):
                limited = daemon.cli('sources', '--input', input_id)['sources'][0]
                assert limited['last_run']['id'] == first['last_run']['id'] and limited['acquisition']['state'] in ('completed', 'overdue')
                assert limited['inspection']['status'] == 'unavailable' and limited['history']['complete'] is False
            assert daemon.cli('sources', '--input', input_id)['sources'][0]['inspection']['status'] == 'available'
            # An observation without acquisition metadata (as written before it existed) reports unknowns.
            old = first['last_run']['id']
            sql(h, "WITH changed AS (UPDATE raw.observations SET metadata=metadata-'_sinex_acquisition_v1' "
                   f"WHERE id='{old}' RETURNING id) SELECT count(*) FROM changed")
            old_row = src(path, input=input_id, observation=old)
            assert old_row['acquisition']['schedule_evidence'] == 'unknown'
            assert old_row['receipts']['evidence'] == 'unknown_legacy_or_no_observation' and not old_row['receipts']['complete']
            assert 'error' in daemon.api('sources.list', {'observation': old})
            assert 'error' in daemon.api('sources.list', {'input': input_id, 'before_seq': 0})
            h['run']([h['BIN_DIR']/'sinexd', 'admin', 'schema', '--connection-file', work/'admin', '--check'],
                     label='schema check accepts observations without acquisition metadata')
            daemon.stop()
            held.close()
            # Register more inputs than one sources page (16) by configuring each in turn.
            extra = []
            for n in range(17):
                missing = work/f'additional-missing-{n}.db'
                extra.append(str(missing))
                daemon.configure(missing, top=top)
                daemon.start(f'acquisition-additional-{n}')
                daemon.ready()
                if n < 16:
                    daemon.stop()
            inputs, after = [], None
            while True:
                page = daemon.cli('sources', *(['--after-input', after] if after else []))
                assert len(page['sources']) <= 16
                inputs += page['sources']
                after = page['page']['next_after_input']
                if not after:
                    break
            assert len(inputs) == 19 and len({x['input'] for x in inputs}) == 19
            assert set(extra) <= {x['locator'] for x in inputs}
            assert all(x['acquisition']['state'] == 'unknown' and x['acquisition']['schedule_applicability'].startswith('historical_or_unknown')
                       for x in inputs if x['locator'] != extra[-1])
            assert inputs[-1]['acquisition']['schedule_applicability'] == 'current_collector_run'
        scenario('history and inputs page; locked detail and old observations report unknown', paging)

        def record_errors():
            daemon.stop()
            path = work/'diagnostic-counts.db'
            rows = [fixture_row(f'diagnostic-{n:04}') for n in range(2048)]
            for row in rows:
                row[2] = -2  # a duration Atuin never writes
            create_history(path, rows)
            daemon.configure(path, top=top)
            held = pause('diagnostics')
            held.start(daemon, 'diagnostics-errors')
            held.wait_paused()
            errors = src(path)
            input_id = errors['input']
            assert errors['last_run']['record_errors'] == 2048
            error = errors['first_record_error']
            assert error['evidence_status'] == 'available' and error['record']['id'] == 'diagnostic-0000'
            first_error = sql(h, "SELECT to_jsonb(f) FROM raw.record_failures f JOIN raw.source_material_registry m ON m.id=f.material_id "
                                 f"WHERE f.observation_id='{errors['last_run']['id']}' ORDER BY m.staged_at,m.id,f.anchor_byte LIMIT 1")
            for field in ['observation_id', 'material_id', 'anchor_byte', 'reason']:
                assert error[field] == first_error[field], field
            daemon.stop()
            held.drop()
            with sqlite3.connect(path) as c:
                c.execute('UPDATE history SET duration=0')
            held.start(daemon, 'diagnostics-clean')
            held.wait_paused()
            clean = src(path)
            assert clean['input'] == input_id and clean['last_run']['record_errors'] == 0 and clean['first_record_error'] is None
            assert sql(h, 'SELECT count(*) FROM raw.record_failures') >= 2048
            # The production source query must not scan record failures for a run without errors.
            rust = (h['ROOT']/'crate/sinex-store/src/source_inspection.rs').read_text()
            primary = re.search(r'const SOURCE_ROWS: &str = r#"(.*?)"#;', rust, re.S).group(1)
            plan = sql(h, f"PREPARE source_rows(uuid,uuid,bigint) AS {primary}; "
                          f"EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE source_rows('{input_id}',NULL,17)")

            def nodes(node):
                yield node
                for child in node.get('Plans', []):
                    yield from nodes(child)
            scans = [n for n in nodes(plan[0]['Plan']) if n.get('Relation Name') == 'record_failures']
            assert scans and all(n['Actual Loops'] == 0 for n in scans), scans
            with table_lock(h, 'raw.inputs'):
                error = daemon.cli('sources', '--input', input_id, expected=6)
                assert error['kind'] == 'budget'
                assert error['details'] == {'reason': 'statement_cancelled', 'operation': 'sources.list',
                                            'phase': 'primary_source_rows', 'configured_statement_timeout_ms': 2000}, error
            after = daemon.cli('sources', '--input', input_id)['sources'][0]
            assert after['last_run']['id'] == clean['last_run']['id'] and after['first_record_error'] is None
            assert daemon.cli('query', 'events where source = terminal.atuin-history', expected=5)['total'] >= 2048
            daemon.stop()
        scenario('the first record error is reported; a locked source query is cancelled at 2s', record_errors)
    finally:
        if lock:
            lock.close()
        # Stop a paused daemon before closing its pause socket, which would otherwise fail the pause.
        if daemon.proc is not None and daemon.proc.poll() is None:
            daemon.stop()
        for held in pauses:
            held.close()
        if mcp:
            mcp.close()
