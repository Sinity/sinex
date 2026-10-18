"""Killing the writer at each pause point and restarting on the same state ends with
the uninterrupted run's events; with the Atuin file gone, stored events stay
readable and answers are reported incomplete.
"""
import hashlib
import json
import re
import signal
import socket
import sqlite3
import time

POINTS = ['retained-fsynced', 'transaction-staged', 'transaction-committed']
QUERY = 'events where source = terminal.atuin-history and kind = command.executed'
TOKEN = 'synthetic-foundation-token'
SECRET = 'tok_synthetic_secret'
EPOCH = 1_700_000_000_123_456_789
SECOND = 1_000_000_000
# id, timestamp, duration, exit, command
ROWS = [
    ('kill-1', EPOCH, SECOND, 0, 'echo ' + SECRET),
    ('kill-2', EPOCH + SECOND, 2 * SECOND, 2, 'make synthetic'),
    ('kill-3', EPOCH + 2 * SECOND, -1, -1, 'sleep synthetic'),
    ('kill-bad', 'not-a-time', SECOND, 0, 'bad timestamp synthetic'),
]
# Written by hand from ROWS, not from the parser: what the stored events must say.
EXPECTED = {
    'kill-1': {'command': 'echo [withheld]', 'exit_code': 0, 'duration_ns': SECOND,
               'completion': 'ended', 'timestamp_ns': EPOCH, 'when': '2023-11-14T22:13:20.123456789Z'},
    'kill-2': {'command': 'make synthetic', 'exit_code': 2, 'duration_ns': 2 * SECOND,
               'completion': 'ended', 'timestamp_ns': EPOCH + SECOND, 'when': '2023-11-14T22:13:21.123456789Z'},
    'kill-3': {'command': 'sleep synthetic', 'exit_code': None, 'duration_ns': None,
               'completion': 'end_not_recorded', 'timestamp_ns': EPOCH + 2 * SECOND,
               'when': '2023-11-14T22:13:22.123456789Z'},
}
ROWS_READ, RECORD_ERRORS = len(ROWS), 1
FAILURE_REASON = 'invalid Atuin record field type or missing field'
UNAVAILABLE_REASON = 'Atuin database unavailable (read-only open failed)'


def facts(answer):
    """Event content without identities, for comparing separate runs."""
    return sorted(json.dumps({k: row[k] for k in ['source', 'kind', 'payload', 'when', 'time_quality']}, sort_keys=True)
                  for row in answer['rows'])


def masked(value):
    """The privacy rule applied independently to a stored record."""
    if isinstance(value, str):
        return re.sub(r'tok_[a-z0-9_]+', '[withheld]', value)
    if isinstance(value, dict):
        return {k: masked(v) for k, v in value.items()}
    if isinstance(value, list):
        return [masked(v) for v in value]
    return value


class Case:
    """One fixture copy, database, store and daemon configuration."""

    def __init__(self, h, name):
        self.h, self.name = h, name
        work = h['WORK']
        self.source = work/(name + '.db')
        con = sqlite3.connect(self.source)
        with con:
            con.execute('CREATE TABLE history (id TEXT PRIMARY KEY,timestamp INTEGER NOT NULL,duration INTEGER NOT NULL,exit INTEGER NOT NULL,command TEXT NOT NULL,cwd TEXT NOT NULL,session TEXT NOT NULL,hostname TEXT NOT NULL,deleted_at INTEGER)')
            con.executemany('INSERT INTO history VALUES (?,?,?,?,?,?,?,?,NULL)',
                            [(*row, '/synthetic/project', 'fixture', 'fixture-host:fixture-user') for row in ROWS])
        con.close()
        self.digest = hashlib.sha256(self.source.read_bytes()).hexdigest()
        urls = h['create_database']('kill_' + name.replace('-', '_'))
        self.reader_url = urls['reader']
        for role in ['writer', 'reader']:
            (work/f'{name}-{role}').write_text(urls[role])
            (work/f'{name}-{role}').chmod(0o600)
        port = h['free_port']()
        self.endpoint = f'http://127.0.0.1:{port}/rpc'
        self.store = work/(name + '-store')
        self.config = work/(name + '.toml')
        self.config.write_text(f'''listen = "127.0.0.1:{port}"
state_dir = "{work/(name + '-state')}"
store_dir = "{self.store}"
writer_connection_file = "{work/(name + '-writer')}"
reader_connection_file = "{work/(name + '-reader')}"
token_file = "{work/'token'}"
sources = ["terminal.atuin-history"]
automata = []
[atuin]
path = "{self.source}"
poll_interval_ms = 1000
full_interval_ms = 100 # every poll reads the whole file
[[privacy_rules]]
pattern = "tok_[a-z0-9_]+"
replacement = "[withheld]"
created_by = "operator"
''')
        self.proc = None

    def start(self, label, *extra):
        self.proc = self.h['start_daemon'](self.config, label, *extra)

    def cli(self, command, *args, expected=0):
        result = self.h['run']([self.h['BIN_DIR']/'sinexctl', '--endpoint', self.endpoint, '--token-file', self.h['WORK']/'token',
                                command, *args, '--json'], expected=expected, label=f'{self.name} {command}')
        assert SECRET not in result.stdout, f'{command} printed the unmasked fixture secret'
        return json.loads(result.stdout or result.stderr)

    def sources(self):
        return self.cli('sources')['sources'][0]

    def wait_run(self, status, after=None):
        """Wait until the latest source run has `status` and an id other than `after`; return the CLI source entry."""
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            assert self.proc.poll() is None, f'{self.name} daemon exited; see its stderr'
            try:
                listed = self.h['rpc'](self.endpoint, TOKEN, 'sources.list').get('result', {}).get('sources', [])
            except (OSError, ValueError):
                listed = []
            last = listed[0]['last_run'] if listed else None
            if last and last['id'] != after and last['status'] == status:
                # A new poll may have started in between; take the CLI view only if it shows the same state.
                current = self.sources()
                if current['last_run']['id'] != after and current['last_run']['status'] == status:
                    return current
            time.sleep(.025)
        raise AssertionError(f'{self.name}: no {status} source run within 30s')

    def query_complete(self):
        """Query until an answer is complete; a poll that starts in between makes it briefly incomplete."""
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            answer = self.cli('query', QUERY, expected=(0, 5))
            if answer['complete']:
                return answer
            assert answer['source_status'] and all(s['status'] in ('reading', 'completed') for s in answer['source_status'])
            time.sleep(.025)
        raise AssertionError(f'{self.name}: query stayed incomplete for 30s')

    def part(self, material):
        """Registry row of a stored part and its observation."""
        statement = ("SELECT jsonb_build_object('status',m.status,'durable_bytes',m.durable_bytes,'blob_bytes',b.size_bytes,"
                     "'observation_status',o.status,'observation_rows_read',o.rows_read) FROM raw.source_material_registry m "
                     "LEFT JOIN core.blobs b ON b.id=m.blob_id JOIN raw.observations o ON o.id=m.observation_id "
                     f"WHERE m.id='{material}'::uuid")
        return json.loads(self.h['run'](['psql', self.reader_url, '--no-psqlrc', '--tuples-only', '--no-align', '-v', 'ON_ERROR_STOP=1',
                                         '--command', statement], label=f'{self.name} stored part').stdout)

    def check_events(self, answer):
        """The answer holds exactly the expected events, once each; returns their traces by native id."""
        rows = {r['payload']['native_id']: r for r in answer['rows']}
        assert answer['total'] == len(answer['rows']) == len(rows) == len({r['id'] for r in answer['rows']}) == len(EXPECTED), answer
        for native, expected in EXPECTED.items():
            row = rows[native]
            assert row['when'] == expected['when'] and row['time_quality'] == 'intrinsic', row
            assert {k: row['payload'][k] for k in expected if k != 'when'} == {k: v for k, v in expected.items() if k != 'when'}, row
        traces = {}
        for native, row in rows.items():
            shown = self.cli('show', row['id'])['row']
            assert all(shown[k] == v for k, v in row.items())
            assert shown['source_state'] == 'present' and shown['archive'] is None
            trace = self.cli('trace', row['id'], '--bytes')['row']
            record = trace['record']
            assert record['id'] == native and record['timestamp'] == row['payload']['timestamp_ns']
            assert record['command'] == row['payload']['command'] and record['_from']['table'] == 'history'
            assert trace['material'] == row['material'] and trace['anchor'] == row['anchor'] < trace['end']
            assert trace['representation'] == 'canonical_sqlite_row'
            traces[native] = trace
        return traces

    def check_completed(self, source):
        """Counts, cursor and the malformed row's report after a completed read."""
        last = source['last_run']
        assert last['rows_read'] == ROWS_READ and last['record_errors'] == RECORD_ERRORS, last
        assert source['cursor']['status'] == 'completed' and source['cursor']['observation'] == last['id']
        assert source['cursor']['rows_read'] == ROWS_READ
        failure = source['first_record_error']
        assert failure['evidence_status'] == 'available' and failure['reason'] == FAILURE_REASON, failure
        assert failure['record']['timestamp'] == 'not-a-time' and failure['record']['command'] == 'bad timestamp synthetic'
        assert failure['record']['_from']['observation'] == last['id']
        assert failure['end'] > failure['anchor_byte'] >= 0

    def check_repeat(self, answer, after):
        """The next full read admits nothing new and keeps every event identity."""
        source = self.wait_run('completed', after=after)
        last = source['last_run']
        assert last['admitted'] == 0 and last['repeated'] == len(EXPECTED) and last['record_errors'] == RECORD_ERRORS, last
        again = self.query_complete()
        assert {(r['id'], r['material'], r['anchor']) for r in again['rows']} == {(r['id'], r['material'], r['anchor']) for r in answer['rows']}

    def check_unavailable(self, source):
        last = source['last_run']
        assert last['rows_read'] == last['admitted'] == last['repeated'] == last['record_errors'] == 0, last
        assert last['reason'] == UNAVAILABLE_REASON and source['cursor']['status'] == 'unavailable'
        assert source['first_record_error'] is None


def exercise(h):
    work, run, scenario = h['WORK'], h['run'], h['scenario']
    baseline = {}

    def uninterrupted():
        case = Case(h, 'baseline')
        sinexd = h['BIN_DIR']/'sinexd'
        run([sinexd, 'serve', '--config', case.config, '--test-pause-at', 'retained-fsynced'], expected=2, label='pause point without socket')
        run([sinexd, 'serve', '--config', case.config, '--test-pause-at', 'unknown', '--test-pause-socket', work/'absent.sock'],
            expected=1, label='unknown pause point')
        assert not case.store.exists() and not (work/'baseline-state').exists(), 'refused pause arguments created state'
        case.start('baseline')
        source = case.wait_run('completed')
        answer = case.query_complete()
        case.check_completed(source)
        case.check_events(answer)
        case.check_repeat(answer, source['last_run']['id'])
        h['stop'](case.proc)
        assert hashlib.sha256(case.source.read_bytes()).hexdigest() == case.digest, 'the daemon changed the Atuin file'
        baseline['facts'] = facts(answer)
    scenario('uninterrupted run reads the expected events', uninterrupted)

    def kill_at(point, lose_provider):
        case = Case(h, point + ('-provider-loss' if lose_provider else ''))
        committed = point == 'transaction-committed'
        socket_path = work/'pause.sock'
        listener = socket.socket(socket.AF_UNIX)
        listener.bind(str(socket_path))
        listener.listen(1)
        listener.settimeout(30)
        try:
            case.start(case.name + '-before-kill', '--test-pause-at', point, '--test-pause-socket', socket_path)
            connection, _ = listener.accept()
            with connection:
                connection.settimeout(30)
                frame = json.loads(connection.makefile('rb').readline(65536))
                assert frame['pid'] == case.proc.pid and frame['pause_point'] == point, frame
                step = frame['evidence'] if point == 'retained-fsynced' else frame['evidence']['custody']
                assert step['acknowledged'] is False and step['source_eof'] is True and step['step_rows'] == ROWS_READ, step
                part = case.store/'open'/step['material']
                stored = part.read_bytes()
                assert len(stored) == step['end'] and step['canonical_bytes'] > 0
                before = case.cli('query', QUERY, expected=(0, 5))
                assert before['total'] == (len(EXPECTED) if committed else 0) and before['complete'] is committed
                status = case.sources()
                assert status['last_run']['rows_read'] == status['cursor']['rows_read'] == (ROWS_READ if committed else 0)
                assert status['cursor']['observation'] == step['observation']
                if committed:
                    assert status['cursor'] == frame['evidence']['progress']
                    assert status['first_record_error']['record']['timestamp'] == 'not-a-time'
                    # With the part file unreadable, counts stay and the failed row is reported unavailable.
                    hidden = part.with_suffix('.hidden')
                    part.rename(hidden)
                    try:
                        unreadable = case.sources()
                        assert unreadable['last_run']['record_errors'] == RECORD_ERRORS
                        assert unreadable['first_record_error']['evidence_status'] == 'unavailable'
                        assert unreadable['first_record_error']['record'] is None
                    finally:
                        hidden.rename(part)
                else:
                    assert status['cursor']['status'] == 'reading'
                    assert all(status['cursor'][k] == 0 for k in ('admitted', 'repeated', 'removed', 'record_errors'))
                    assert status['first_record_error'] is None
                held_traces = {r['payload']['native_id']: case.cli('trace', r['id'], '--bytes')['row'] for r in before['rows']}
                held_part = case.part(step['material'])
                assert held_part['status'] == 'capturing' and held_part['durable_bytes'] == (step['end'] if committed else 0)
                case.proc.send_signal(signal.SIGKILL)
                case.proc.wait(timeout=15)
                assert case.proc.returncode == -signal.SIGKILL
                assert connection.recv(1) == b'', 'pause socket still open after the kill'
        finally:
            listener.close()
            socket_path.unlink(missing_ok=True)

        if lose_provider:
            case.source.unlink()
        case.start(case.name + '-restart')
        if lose_provider:
            source = case.wait_run('unavailable')
            case.check_unavailable(source)
            answer = case.cli('query', QUERY, expected=5)
            assert answer['complete'] is False and answer['source_status'], answer
            for state in answer['source_status']:
                # A poll registers as reading before its failed open is recorded as unavailable.
                assert state['status'] in ('reading', 'unavailable'), state
                assert state['reason'] == (UNAVAILABLE_REASON if state['status'] == 'unavailable' else None), state
            if committed:
                assert facts(answer) == baseline['facts']
                assert {r['id'] for r in answer['rows']} == {r['id'] for r in before['rows']}
                for native, trace in case.check_events(answer).items():
                    assert trace['material'] == step['material'] and trace['record']['_from']['observation'] == step['observation']
                    assert trace['record'] == masked(json.loads(stored[trace['anchor']:trace['end']]))
                    assert all(trace[k] == held_traces[native][k] for k in ['material', 'anchor', 'end', 'record', 'representation'])
            else:
                assert answer['total'] == 0 and answer['rows'] == []
            case.check_unavailable(case.wait_run('unavailable', after=source['last_run']['id']))
            assert not case.source.exists(), 'an unavailable poll created a replacement Atuin file'
        else:
            source = case.wait_run('completed', after=step['observation'])
            answer = case.query_complete()
            case.check_completed(source)
            last = source['last_run']
            if committed:
                assert (last['admitted'], last['repeated']) == (0, len(EXPECTED)), last
            else:
                assert (last['admitted'], last['repeated']) in [(len(EXPECTED), 0), (0, len(EXPECTED))], last
            traces = case.check_events(answer)
            assert facts(answer) == baseline['facts']
            if committed:
                assert {r['id'] for r in answer['rows']} == {r['id'] for r in before['rows']}
                assert all(t['material'] == step['material'] and t['record']['_from']['observation'] == step['observation'] for t in traces.values())
            else:
                assert all(t['material'] != step['material'] for t in traces.values())
            case.check_repeat(answer, last['id'])
            assert hashlib.sha256(case.source.read_bytes()).hexdigest() == case.digest, 'the daemon changed the Atuin file'
        # Restart truncates the interrupted part to its committed length and closes it.
        closed = case.part(step['material'])
        assert closed['status'] == 'recovered_partial', closed
        assert closed['durable_bytes'] == closed['blob_bytes'] == (step['end'] if committed else 0), closed
        assert closed['observation_status'] == ('completed' if committed else 'interrupted'), closed
        assert closed['observation_rows_read'] == (ROWS_READ if committed else 0), closed
        assert not part.exists(), 'the interrupted open part was not closed'
        h['stop'](case.proc)

    for point in POINTS:
        scenario(f'writer killed at {point} restarts with the same events', lambda point=point: kill_at(point, False))
    for point in ['transaction-staged', 'transaction-committed']:
        scenario(f'writer killed at {point} with the Atuin file removed keeps stored events, reported incomplete',
                 lambda point=point: kill_at(point, True))
