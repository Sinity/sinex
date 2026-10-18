"""Shell sessions from Atuin history: grouping, late merges and splits across logical days,
project recall through CLI and MCP, crash and timeout recovery, and a missing source file.

Expected sessions come from `_shell_history.independent`, never from the product.
"""
from contextlib import contextmanager
from datetime import datetime
import json
from pathlib import Path
import select
import socket
import sqlite3
import subprocess
import time

from _clients import Clients, wait
from _shell_history import check_trace, create, independent, query_all, write

PROJECT = '/synthetic/session/tide'
DAY = '2026-10-13'


def stamp(clock, day=DAY):
    return int(datetime.fromisoformat(day+'T'+clock+'+02:00').timestamp())*1_000_000_000+123


def row(identity, timestamp, command, cwd=PROJECT, duration=0, exit=0):
    return {'id': identity, 'timestamp': timestamp, 'duration': duration, 'exit': exit, 'command': command,
            'cwd': cwd, 'session': 'synthetic-shell', 'hostname': 'synthetic-host:user', 'deleted_at': None}


def fixture():
    """Two sessions of seven commands in the project, then two commands in a neighbouring directory."""
    rows = []
    for prefix, hour, first in [('a', 3, 30), ('b', 4, 4)]:
        for n in range(7):
            rows.append(row(f'{prefix}{n}', stamp(f'{hour:02}:{first+n*4:02}:00'),
                            f'cargo test shell-session {prefix}{n}', exit=101 if n == 1 else 0))
    for n, clock in enumerate(['05:00:00', '05:04:00']):
        rows.append(row(f'd{n}', stamp(clock), f'echo distractor {n}', cwd=PROJECT+'-neighbor'))
    return rows


def exercise(h):
    work, report = h['WORK'], h['REPORT']
    run, scenario, start, stop = (h[k] for k in ['run', 'scenario', 'start_daemon', 'stop'])
    operator_token = work/'session-operator-token'
    operator_token.write_text('synthetic-session-operator-token')
    operator_token.chmod(0o600)
    facts = fixture()
    source = work/'sessions.db'
    create(source)
    write(source, facts)
    # The first part runs on its own database; the rebuild from reordered input uses the scenario's.
    first = h['create_database']('sessions_first')
    for name, value in [('first-writer', first['writer']), ('first-reader', first['reader']),
                        ('first-read-model', first['reader'].replace('sinex_reader', 'sinex_read_model')),
                        ('main-read-model', h['reader'].replace('sinex_reader', 'sinex_read_model'))]:
        (work/name).write_text(value)
        (work/name).chmod(0o600)
    endpoint = f'http://127.0.0.1:{h["free_port"]()}/rpc'
    clients = Clients(h, endpoint, 'sessions')
    cli, api = clients.cli, clients.api
    active = {'admin': first['admin']}
    proc = None

    def config(name, sessions=True, main=False):
        active['admin'] = h['admin'] if main else first['admin']
        prefix = 'main' if main else 'first'
        path = work/(name+'.toml')
        path.write_text(f'''listen = "{endpoint.removeprefix('http://').removesuffix('/rpc')}"
state_dir = "{work/(prefix+'-state')}"
store_dir = "{work/(prefix+'-blobs')}"
writer_connection_file = "{work/('writer' if main else 'first-writer')}"
reader_connection_file = "{work/('reader' if main else 'first-reader')}"
operator_token_file = "{operator_token}"
read_model_connection_file = "{work/(prefix+'-read-model')}"
token_file = "{work/'token'}"
sources = ["terminal.atuin-history"]
automata = {'["session"]' if sessions else '[]'}
[[projects]]
name = "tide"
roots = ["{PROJECT}"]
[[projects]]
name = "large"
roots = ["/synthetic/session/large"]
[[projects]]
name = "long"
roots = ["/synthetic/session/long"]
[[projects]]
name = "span"
roots = ["/synthetic/session/span"]
[atuin]
path = "{source}"
poll_interval_ms = 500
full_interval_ms = 100
''')
        return path

    def sql(text):
        return run(['psql', active['admin'], '-XAt', '-v', 'ON_ERROR_STOP=1', '-c', text], label='session database read').stdout.strip()

    def settled(sessions=True):
        status = api().get('result', {})
        index = status.get('indexed_retrieval', {})
        return (index and index.get('serving') and index.get('position') == status['commit_seq']
                and index.get('pending_canonical') == 0 and index.get('failure') is None
                and (not sessions or (index.get('pending_sessions') == 0 and index.get('sessions', {}).get('failure') is None)))

    def raw_count():
        return api('query.run', {'query': 'events where kind = command.executed'})['result']['total']

    def caught_up(seconds=90):
        try:
            wait(proc, 'all commands admitted and sessions published',
                 lambda: raw_count() == len(facts) and settled(), seconds)
        except AssertionError as error:
            raise AssertionError(f'{error}; expected {len(facts)} commands, status {api()}') from None

    def raw():
        return query_all(cli, 'events where kind = command.executed')

    def current():
        return cli('query', 'events where kind = activity.session.boundary')['rows']

    def check_recall():
        """CLI and MCP recall for `tide` equal the independent grouping, and every session cites its commands."""
        answer = cli('recall', 'last', '--project', 'tide')
        agent = clients.tool('sinex_recall', {'project': 'tide'})
        assert answer['session'] == agent['session'] and answer['rows'] == agent['rows']
        groups = independent(facts)
        by_id = {r['id']: r for r in facts}
        expected = next(g for g in reversed(groups) if any(by_id[p]['cwd'] == PROJECT for p in g['parents']))
        names = sorted(r['payload']['native_id'] for r in answer['rows'])
        assert names == [p for p in expected['parents'] if by_id[p]['cwd'] == PROJECT], (names, expected)
        header = answer['session']
        assert int(header['payload']['start_ns']) == expected['start_ns'] and int(header['payload']['end_ns']) == expected['end_ns']
        assert all(header['payload'][k] is None for k in ['continuity', 'afk', 'attention', 'active_s'])
        assert header['payload']['sources'] == ['terminal.atuin-history']
        if 'bridge' in names:
            assert header['payload']['owner_day'] == '2026-10-12'
        sessions = current()
        assert len(sessions) == len(groups)
        raw_ids = {r['payload']['native_id']: r['id'] for r in raw()}
        canonical = query_all(cli, 'commands sort when limit 10000')
        native = {r['id']: r['payload']['native_id'] for r in canonical}
        for r in canonical:
            assert r['parents'] == [raw_ids[r['payload']['native_id']]], r
        actual = []
        for group in sessions:
            assert len(group['parents']) == len(set(group['parents']))
            actual.append({'start_ns': int(group['payload']['start_ns']), 'end_ns': int(group['payload']['end_ns']),
                           'parents': sorted(native[p] for p in group['parents'])})
        assert sorted(actual, key=lambda g: g['start_ns']) == groups
        members = [p for g in actual for p in g['parents']]
        assert len(members) == len(set(members)) == len(facts), 'each command belongs to exactly one session'
        assert check_trace(cli, header, facts)['representation'] == 'shell_session'
        return answer

    def discover():
        cli('catalog')
        tools = clients.connect_mcp()
        assert tools == {'sinex_search', 'sinex_query', 'sinex_recall', 'sinex_show', 'sinex_trace', 'sinex_sources', 'sinex_status'}, tools

    def pause_listener(name, seconds=30):
        listener = socket.socket(socket.AF_UNIX)
        listener.bind(str(work/name))
        listener.listen(1)
        listener.settimeout(seconds)
        return listener

    def close_listener(listener):
        # A socket file left behind cannot be copied when a failed run's directory is kept.
        path = listener.getsockname()
        listener.close()
        Path(path).unlink(missing_ok=True)

    def paused(listener):
        """Accept the daemon's pause notice; returns the connection (send b'c' to continue) and the notice."""
        connection, _ = listener.accept()
        connection.settimeout(5)
        return connection, json.loads(connection.makefile('rb').readline())

    @contextmanager
    def ledger_lock(mode, label):
        """Hold a lock on the dirty-day ledger from a separate psql session."""
        stderr = (report/(label+'.stderr')).open('w')
        argv = ['psql', active['admin'], '-XAt', '-v', 'ON_ERROR_STOP=1']
        client = subprocess.Popen(argv, env=h['ENV'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr, text=True, bufsize=1)
        h['CHILDREN'].append((client, client.stdout, stderr))
        client.stdin.write(f'BEGIN; LOCK TABLE derivation.session_dirty IN {mode} MODE;\n\\echo locked\n')
        client.stdin.flush()
        ready, _, _ = select.select([client.stdout], [], [], 5)
        assert ready, 'ledger lock not taken within 5s'
        for expected in ['BEGIN', 'LOCK TABLE', 'locked']:
            assert client.stdout.readline().strip() == expected
        try:
            yield
        finally:
            client.stdin.write('COMMIT;\n\\q\n')
            client.stdin.flush()
            client.wait(timeout=10)
            assert client.returncode == 0

    def disabled():
        nonlocal proc
        proc = start(config('sessions-disabled', sessions=False), 'sessions-disabled')
        wait(proc, 'commands admitted', lambda: raw_count() == len(facts) and settled(False), 90)
        absent = cli('query', 'events where kind = activity.session.boundary', expected=5)
        assert absent['kind'] == 'partial' and 'not configured' in absent['message'], absent
        assert cli('recall', 'last', '--project', 'tide', expected=5)['kind'] == 'partial'
        stop(proc)
    scenario('without the session automaton, commands are admitted and session queries are partial', disabled)

    def killed_after_publication():
        nonlocal proc
        listener = pause_listener('session-boundary.sock')
        proc = start(config('sessions'), 'sessions-initial', '--test-pause-at', 'session-replacement-committed',
                     '--test-pause-socket', work/'session-boundary.sock')
        connection, notice = paused(listener)
        assert notice['pid'] == proc.pid and notice['evidence']['pending']
        assert 'partial' in json.dumps(cli('recall', 'last', '--project', 'tide', expected=5))
        pending = cli('query', 'events where kind = activity.session.boundary', expected=5)
        assert not pending['complete'] and pending['sessions']['pending_keys'] > 0
        connection.close()
        close_listener(listener)
        proc.kill()
        proc.wait(timeout=10)
        proc = start(config('sessions-after-kill'), 'sessions-after-kill')
        wait(proc, 'sessions published after restart', settled, 90)
    scenario('a daemon killed right after publishing a session finishes the work on restart', killed_after_publication)

    initial_ids = set()

    def initial():
        discover()
        check_recall()
        initial_ids.update(r['id'] for r in current())
    scenario('two sessions; recall for a project through CLI and MCP matches the expected grouping', initial)

    def bridge():
        facts.append(row('bridge', stamp('03:59:00'), 'cargo test shell-session late bridge'))
        write(source, [facts[-1]])
        caught_up()
        answer = check_recall()
        assert answer['session']['id'] not in initial_ids
        live = current()
        assert len(live) == 2
        # Only the neighbouring-directory session keeps its identity.
        kept = initial_ids & {r['id'] for r in live}
        assert kept == {r['id'] for r in live if int(r['payload']['start_ns']) == stamp('05:00:00')}, kept
    scenario('a late command bridging the gap merges both sessions into a new one', bridge)

    def extension_and_rebuild():
        nonlocal proc
        facts.append(row('extension', stamp('04:32:00'), 'sleep shell-session unknown end', duration=-1, exit=-1))
        write(source, [facts[-1]])
        caught_up()
        check_recall()
        stop(proc)
        write(source, list(reversed(facts)), replace=True)
        proc = start(config('rebuilt', main=True), 'sessions-rebuilt')
        caught_up()
        check_recall()
    scenario('a command with an unknown end extends a session; a fresh database fed reordered input gives the same sessions', extension_and_rebuild)

    def large_session():
        large = [row(f'large-{n:03}', stamp('06:00:00')+n*1_000_000_000, f'echo bounded recall {n}', cwd='/synthetic/session/large')
                 for n in range(107)]
        facts.extend(large)
        write(source, large)
        caught_up()
        refused = cli('recall', 'last', '--project', 'large', expected=5)
        assert refused['kind'] == 'partial' and 'assembly exceeds' in refused['message'], refused
        commands = query_all(cli, 'commands where project = large sort when limit 200')
        assert len({r['id'] for r in commands}) == 107 and {r['payload']['native_id'] for r in commands} == {r['id'] for r in large}
        for command in [commands[0], commands[-1]]:
            assert cli('show', command['id'])['row']['current_eligible']
            assert cli('trace', command['id'], '--bytes')['row']['parents'][0]['record']['id'] == command['payload']['native_id']
    scenario('recall of a session over 100 commands is partial; its commands stay queryable and traceable', large_session)

    def long_spans():
        cwd = '/synthetic/session/long'
        spans = [row(name, stamp(clock)+days*86400_000_000_000, 'synthetic interval '+name, cwd=cwd, duration=seconds*1_000_000_000)
                 for name, clock, seconds, days in [('long-solo', '07:00:00', 600, 0), ('long-outer', '03:40:00', 3000, 1),
                                                    ('long-nested', '03:45:00', 300, 1), ('long-overlap', '04:25:00', 900, 1),
                                                    ('long-after', '04:46:00', 0, 1)]]
        facts.extend(spans)
        write(source, spans)
        caught_up()
        ids = {r['payload']['native_id']: r['id'] for r in query_all(cli, 'commands where project = long sort when limit 100')}
        groups = [g for g in current() if set(g['parents']) & set(ids.values())]
        solo = next(g for g in groups if ids['long-solo'] in g['parents'])
        assert solo['parents'] == [ids['long-solo']]
        assert int(solo['payload']['end_ns'])-int(solo['payload']['start_ns']) == 600_000_000_000
        crossing = next(g for g in groups if ids['long-outer'] in g['parents'])
        assert set(crossing['parents']) == {ids[k] for k in ['long-outer', 'long-nested', 'long-overlap']}
        assert crossing['payload']['owner_day'] == '2026-10-13'
        assert int(crossing['payload']['start_ns']) == stamp('03:40:00')+86400_000_000_000
        assert int(crossing['payload']['end_ns']) == stamp('04:40:00')+86400_000_000_000
        assert len(groups) == 3 and sum(len(g['parents']) for g in groups) == len(ids)
    scenario('long, nested and overlapping spans across the 04:00 boundary group by their recorded ends', long_spans)

    def multi_day_span():
        nonlocal proc
        cwd = '/synthetic/session/span'
        points = [row(f'span-day{day}', stamp('06:00:00', f'2026-10-{day:02}'), f'echo span day {day}', cwd=cwd) for day in [1, 3, 5]]
        facts.extend(points)
        write(source, points)
        caught_up(180)
        check_recall()
        point_ids = {r['id'] for r in query_all(cli, 'commands where project = span sort when limit 100')}
        before = [g for g in current() if set(g['parents']) & point_ids]
        assert len(before) == 3
        stop(proc)
        listener = pause_listener('multi-day.sock', 60)
        proc = start(config('multi-day', main=True), 'sessions-multi-day', '--test-pause-at', 'session-replacement-committed',
                     '--test-pause-socket', work/'multi-day.sock')
        wait(proc, 'restart caught up', lambda: api().get('result') and settled())
        span_start, span_end = stamp('05:59:00', '2026-10-01'), stamp('06:01:00', '2026-10-05')
        span = row('span-bridge', span_start, 'synthetic multi-day recorded span', cwd=cwd, duration=span_end-span_start)
        facts.append(span)
        write(source, [span])
        connection, notice = paused(listener)
        assert notice['pid'] == proc.pid and notice['evidence']['through_day'] >= '2026-10-05'
        pending = cli('query', 'events where kind = activity.session.boundary', expected=5)
        assert not pending['complete'] and pending['sessions']['pending_through_day'] >= '2026-10-05'
        # Block writes to the dirty-day ledger after publication. Reads still work; the
        # statement deadline makes the failure visible, and the work retries on its own.
        with ledger_lock('SHARE', 'session-retry-lock'):
            connection.sendall(b'c')
            connection.close()
            close_listener(listener)
            wait(proc, 'session failure visible in status',
                 lambda: api()['result']['indexed_retrieval']['sessions']['failure'], 15)
            assert cli('recall', 'last', '--project', 'span', expected=5)['kind'] == 'partial'
        caught_up(180)
        check_recall()
        merged = cli('recall', 'last', '--project', 'span')
        assert int(merged['session']['payload']['start_ns']) == span_start and int(merged['session']['payload']['end_ns']) == span_end
        assert len(merged['session']['parents']) == 4
        assert not {g['id'] for g in before} & {g['id'] for g in current()}
        span_raw = next(r for r in raw() if r['payload']['native_id'] == 'span-bridge')
        cli('delete', span_raw['id'], token_file=operator_token)
        with sqlite3.connect(source) as con:
            con.execute('DELETE FROM history WHERE id=?', ('span-bridge',))
        facts.remove(span)
        caught_up(180)
        check_recall()
        restored = [g for g in current() if set(g['parents']) & point_ids]

        def shape(groups):
            return sorted((g['payload']['start_ns'], g['payload']['end_ns'], sorted(g['parents'])) for g in groups)
        assert shape(restored) == shape(before)
        assert {g['payload']['owner_day'] for g in restored} == {'2026-10-01', '2026-10-03', '2026-10-05'}
    scenario('a late five-day span merges three daily sessions after a ledger timeout; deleting it restores them', multi_day_span)

    def replaced_parent():
        nonlocal proc
        original = check_recall()
        old_session = original['session']['id']
        old_command = next(r for r in original['rows'] if r['payload']['native_id'] == 'a0')
        stop(proc)
        listener = pause_listener('fusion.sock')
        proc = start(config('replaced-parent', main=True), 'sessions-replaced-parent', '--test-pause-at', 'fusion-replacement-committed',
                     '--test-pause-socket', work/'fusion.sock')
        wait(proc, 'restart caught up', lambda: api().get('result') and settled())
        changed = next(r for r in facts if r['id'] == 'a0')
        changed['command'] += ' revised'
        write(source, [changed])
        connection, notice = paused(listener)
        assert notice['pid'] == proc.pid
        assert cli('show', old_session)['row']['current_eligible'] is False
        command = cli('show', old_command['id'])['row']
        # The command itself is not archived yet, so this shows the session's own check.
        assert command['archive'] is None and command['current_eligible'] is False
        pending = cli('query', 'events where kind = activity.session.boundary', expected=5)
        assert not pending['complete'] and old_session not in {r['id'] for r in pending['rows']}
        assert cli('recall', 'last', '--project', 'tide', expected=5)['kind'] == 'partial'
        connection.sendall(b'c')
        connection.close()
        close_listener(listener)
        wait(proc, 'new session published', settled)
        assert check_recall()['session']['id'] != old_session
    scenario('while a command is being replaced, sessions citing it are not served', replaced_parent)

    def delete_and_undo():
        before = check_recall()
        removed = next(r for r in raw() if r['payload']['native_id'] == 'a0')
        assert cli('delete', removed['id'], token_file=operator_token)
        wait(proc, 'session replaced after delete', settled)
        excluded = cli('recall', 'last', '--project', 'tide')
        assert 'a0' not in {r['payload']['native_id'] for r in excluded['rows']}
        assert excluded['session']['id'] != before['session']['id']
        assert int(excluded['session']['payload']['start_ns']) == stamp('03:34:00')
        cli('delete', '--undo', removed['id'], token_file=operator_token)
        caught_up(300)
        check_recall()
    scenario('deleting a command replaces its session; undo restores it', delete_and_undo)

    def idle_timeout():
        before = {r['id'] for r in raw()}
        assert sql('SELECT count(*) FROM derivation.session_dirty') == '0'
        log = Path(sql('SHOW data_directory')).parent/'postgres.log'
        offset = len(log.read_text())
        with ledger_lock('ACCESS EXCLUSIVE', 'session-idle-lock'):
            wait(proc, 'writer blocked reading the empty ledger', lambda: sql(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE usename='sinex_writer' AND wait_event_type='Lock' "
                "AND query LIKE '%SELECT day,coalesce(through_day,day)%')") == 't', 15)
            time.sleep(2.2)  # past the two-second statement deadline
        wait(proc, 'failure cleared without new input', settled)
        logged = log.read_text()[offset:]
        assert 'canceling statement due to statement timeout' in logged and 'SELECT day,coalesce(through_day,day)' in logged
        assert {r['id'] for r in raw()} == before, 'recovery admitted new rows'
        check_recall()
    scenario('a timed-out read of the empty dirty-day ledger recovers without new input', idle_timeout)

    def restart_and_missing_source():
        nonlocal proc
        stop(proc)
        proc = start(config('restart', main=True), 'sessions-restart')
        caught_up()
        before = check_recall()
        stop(proc)
        source.rename(work/'removed-source.db')
        proc = start(config('source-missing', main=True), 'sessions-source-missing')
        wait(proc, 'source reported unavailable', lambda: settled() and any(
            s.get('last_run', {}).get('status') == 'unavailable' for s in api('sources.list')['result']['sources']))
        answer = cli('recall', 'last', '--project', 'tide', expected=5)
        assert answer['rows'] == before['rows'] and answer['session'] == before['session'] and answer['complete'] is False
        agent = clients.tool('sinex_recall', {'project': 'tide'})
        assert agent['rows'] == answer['rows'] and agent['complete'] is False
        stop(proc)
        clients.close_mcp()
    scenario('restart and a missing source file keep the same recall answer, marked incomplete', restart_and_missing_source)
