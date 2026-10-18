"""Recall of the latest session for a project: per-command project evidence, newer sessions
with unknown project, late arrivals, configuration change, restart, a missing source file,
archived parents and size limits, through CLI and MCP."""
import datetime as dt
import json
import socket

from _clients import Clients, wait
from _shell_history import COLUMNS, check_trace, create, independent, write


def fixture_row(identity, clock, cwd, duration=0):
    timestamp = int(dt.datetime.fromisoformat('2026-10-13T'+clock+'+02:00').timestamp())*1_000_000_000+123
    return dict(zip(COLUMNS, [identity, timestamp, duration, 0, 'echo recovery '+identity, cwd, 'fixture-shell', 'fixture-host', None]))


def valid_cwd(cwd):
    return cwd.startswith('/') and '\0' not in cwd and not any(part in ('.', '..') for part in cwd.split('/'))


def in_tide(cwd):
    return valid_cwd(cwd) and (cwd == '/work/tide' or cwd.startswith('/work/tide/'))


def unresolved(cwd):
    return not valid_cwd(cwd) or cwd == '/work/ambiguous'


def fixture():
    return [fixture_row('old', '03:50:00', '/work/tide'),
            fixture_row('positive', '04:10:00', '/work/tide/src'),
            fixture_row('other-context', '04:11:00', '/work/other'),
            fixture_row('unconfigured-context', '04:12:00', '/work/tide-neighbor'),
            fixture_row('missing-context', '04:13:00', ''),
            fixture_row('invalid-context', '04:14:00', '/work/tide/../other'),
            fixture_row('ambiguous-context', '04:15:00', '/work/ambiguous'),
            fixture_row('known-distractor', '05:00:00', '/work/other'),
            fixture_row('newer-unknown', '06:00:00', ''),
            fixture_row('newer-unconfigured', '06:30:00', '/unconfigured'),
            fixture_row('newer-ambiguous', '07:00:00', '/work/ambiguous')]


EVIDENCE = {'positive': ('known', None), 'other-context': ('known', None),
            'unconfigured-context': ('unconfigured', 'no_configured_root'), 'missing-context': ('unknown', 'missing_cwd'),
            'invalid-context': ('unknown', 'invalid_cwd'), 'ambiguous-context': ('unknown', 'ambiguous_roots')}


def exercise(h):
    work = h['WORK']
    run, scenario, start, stop = (h[k] for k in ['run', 'scenario', 'start_daemon', 'stop'])
    endpoint = f'http://127.0.0.1:{h["free_port"]()}/rpc'
    clients = Clients(h, endpoint, 'project-recall')
    cli, api, tool = clients.cli, clients.api, clients.tool
    source, config = work/'recovery.db', work/'recovery.toml'
    facts = fixture()
    daemon = None
    read_model = work/'recovery-read-model'
    read_model.write_text(h['reader'].replace('sinex_reader', 'sinex_read_model'))
    read_model.chmod(0o600)
    create(source)

    def add(rows):
        facts.extend(rows)
        write(source, rows)

    def configure(extra=''):
        config.write_text(f'''listen = "{endpoint.removeprefix('http://').removesuffix('/rpc')}"
state_dir = "{work/'recovery-state'}"
store_dir = "{work/'recovery-blobs'}"
writer_connection_file = "{work/'writer'}"
reader_connection_file = "{work/'reader'}"
read_model_connection_file = "{read_model}"
token_file = "{work/'token'}"
sources = ["terminal.atuin-history"]
automata = ["session"]
[[projects]]
name = "tide"
roots = ["/work/tide"]
[[projects]]
name = "other"
roots = ["/work/other"]
[[projects]]
name = "ambiguous-a"
roots = ["/work/ambiguous"]
[[projects]]
name = "ambiguous-b"
roots = ["/work/ambiguous"]
[[projects]]
name = "empty"
roots = ["/work/empty"]
{extra}
[atuin]
path = "{source}"
poll_interval_ms = 3000
full_interval_ms = 100
''')

    def query(text):
        return api('query.run', {'query': text})

    def settled():
        """The recall answer, once every command is admitted and recall answers."""
        answer = query('recall last where project = tide')
        if 'error' in answer:
            return None
        canonical = query('commands sort when limit 10000')
        if 'error' in canonical or canonical['result']['total'] != len(facts):
            return None
        return answer['result']

    def restart(label):
        nonlocal daemon
        daemon = start(config, label)
        wait(daemon, label+': ready', lambda: api().get('result'))
        wait(daemon, label+': caught up', settled)

    def last_run():
        return api('sources.list')['result']['sources'][0]['last_run']

    def next_unchanged_poll(after):
        return wait(daemon, 'a completed poll that admits nothing', lambda: (
            latest if (latest := last_run())['id'] != after and latest['status'] == 'completed'
            and latest['admitted'] == 0 and latest['repeated'] == len(facts) else None))

    def check(complete=True):
        """CLI and MCP recall agree and match the independent grouping and project evidence."""
        answer = cli('recall', 'last', '--project', 'tide')
        if not complete:
            assert answer['complete'] is False, answer
        agent = tool('sinex_recall', {'project': 'tide'})
        for key in ['session', 'rows', 'attribution', 'newer_unresolved_candidates']:
            assert answer[key] == agent[key], key
        groups = independent(facts)
        expected = next(g for g in reversed(groups) if any(r['id'] in g['parents'] and in_tide(r['cwd']) for r in facts))
        rows = answer['rows']
        assert sorted(r['payload']['native_id'] for r in rows) == expected['parents'], (rows, expected)
        header = answer['session']
        assert int(header['payload']['start_ns']) == expected['start_ns'] and int(header['payload']['end_ns']) == expected['end_ns']
        assert set(header['parents']) == {r['id'] for r in rows}
        by_name = {r['payload']['native_id']: r for r in rows}
        for name, (state, reason) in EVIDENCE.items():
            if name in by_name:
                evidence = by_name[name]['project_evidence']
                assert evidence['state'] == state and evidence.get('reason') == reason, (name, evidence)
        indexed = query('commands where project = tide sort when limit 10000')['result']['rows']
        assert sorted(r['payload']['native_id'] for r in indexed) == sorted(r['id'] for r in facts if in_tide(r['cwd']))
        summary = answer['attribution']
        assert summary['total_members'] == len(rows) and summary['complete'] == (not summary['unknown_members'])
        if 'positive' in by_name:
            assert summary['known_projects']['tide'] >= 1 and summary['known_projects']['other'] == 1
            assert summary['unconfigured_members'] == 1 and len(summary['unknown_members']) == 3
        canonical = query('commands sort when limit 10000')['result']['rows']
        native = {r['id']: r['payload']['native_id'] for r in canonical}
        newer = [g['parents'] for g in reversed(groups) if g['start_ns'] > expected['start_ns']
                 and any(r['id'] in g['parents'] and unresolved(r['cwd']) for r in facts)]
        assert [sorted(native[p] for p in c['session']['parents']) for c in answer['newer_unresolved_candidates']] == newer
        check_trace(cli, header, facts)
        assert header['id'] in json.dumps(cli('show', 'sinex:ev/'+header['id']))
        sessions = query('events where kind = activity.session.boundary')['result']['rows']
        actual = [{'start_ns': int(g['payload']['start_ns']), 'end_ns': int(g['payload']['end_ns']),
                   'parents': sorted(native[p] for p in g['parents'])} for g in sessions]
        assert sorted(actual, key=lambda g: g['start_ns']) == groups
        return answer

    try:
        write(source, facts)
        configure()

        def initial():
            restart('recovery')
            assert 'sinex_recall' in clients.connect_mcp()
            check()
            empty = cli('recall', 'last', '--project', 'empty')
            agent = tool('sinex_recall', {'project': 'empty'})
            assert empty['session'] is None and empty['rows'] == [] and len(empty['newer_unresolved_candidates']) == 3
            assert agent['session'] is None and agent['newer_unresolved_candidates'] == empty['newer_unresolved_candidates']
            assert 'not configured' in json.dumps(cli('recall', 'last', '--project', 'not-registered', expected=2))
        scenario('project evidence per command and newer sessions with unknown project, through CLI and MCP', initial)

        def unchanged_polls():
            before = check()
            events = {r['id'] for r in query('events')['result']['rows']}
            after = last_run()['id']
            for _ in range(2):
                latest = next_unchanged_poll(after)
                assert latest['rows_read'] == len(facts) and latest['record_errors'] == 0, latest
                after = latest['id']
                wait(daemon, 'caught up after the poll', settled)
                again = check()
                assert (again['session'], again['rows'], again['resolved']['generation']) == \
                    (before['session'], before['rows'], before['resolved']['generation'])
                assert {r['id'] for r in query('events')['result']['rows']} == events, 'an unchanged poll changed stored events'
        scenario('unchanged polls admit nothing and keep the same events and session', unchanged_polls)

        def bridge():
            add([fixture_row('late-bridge', '03:54:00', '/work/tide', 12*60*1_000_000_000)])
            wait(daemon, 'bridge admitted', settled)
            answer = check()
            assert answer['session']['payload']['owner_day'] == '2026-10-12'
            next_unchanged_poll(last_run()['id'])
            wait(daemon, 'caught up after the poll', settled)
            repeated = check()
            assert repeated['session']['id'] == answer['session']['id']
            assert repeated['resolved']['generation'] == answer['resolved']['generation']
        scenario('a late command across the day boundary merges sessions; a later unchanged poll keeps the answer', bridge)

        def continuation():
            add([fixture_row('later-positive', '08:00:00', '/work/tide')])
            wait(daemon, 'later command admitted', settled)
            answer = check()
            stop(daemon)
            configure('[[projects]]\nname = "new-registration"\nroots = ["/new-registration"]\n')
            restart('recovery-reconfigured')
            changed = check()
            assert changed['resolved']['generation'] != answer['resolved']['generation']
            assert changed['session']['id'] == answer['session']['id']
            stop(daemon)
            source.rename(work/'provider-removed.db')
            restart('recovery-source-missing')
            missing = check(complete=False)
            assert missing['session']['id'] == answer['session']['id']
            stop(daemon)
            (work/'provider-removed.db').rename(source)
            restart('recovery-source-back')
        scenario('a later command, a project added to the configuration, and a missing source file', continuation)

        def archived_parent():
            nonlocal daemon
            stop(daemon)
            controller = socket.socket(socket.AF_UNIX)
            controller.bind(str(work/'current-parent.sock'))
            controller.listen(1)
            controller.settimeout(30)
            daemon = start(config, 'recovery-parent-fence', '--test-pause-at', 'collector-scheduled',
                           '--test-pause-socket', work/'current-parent.sock')
            held, _ = controller.accept()
            held.settimeout(5)
            assert json.loads(held.makefile('rb').readline())['pid'] == daemon.pid
            try:
                # The pause holds only the collector; readers, the writer and the projections keep running.
                wait(daemon, 'caught up with the collector paused', settled)
                original = cli('recall', 'last', '--project', 'tide')
                header, command = original['session'], original['rows'][0]
                assert command['payload']['native_id'] == 'later-positive'
                for label, event in [('raw', command['parents'][0]), ('canonical', command['id'])]:
                    # Archive one parent directly in the database (not a supported route) without
                    # touching the commit sequence or dirty ledgers, so only the read-time parent
                    # check can refuse the session.
                    def archive(on):
                        sql = (f"INSERT INTO audit.archived(id,archived_seq,reason) SELECT '{event}'::uuid,seq,'supersede' FROM core.commit_counter WHERE one"
                               if on else f"DELETE FROM audit.archived WHERE id='{event}'::uuid")
                        run(['psql', h['admin'], '-XAt', '-v', 'ON_ERROR_STOP=1', '-c', sql], label=f'{"archive" if on else "restore"} {label} parent')
                    archive(True)
                    try:
                        def projection_caught_up():
                            index = api()['result']['indexed_retrieval']
                            return index if (index['pending_canonical'] == 0 and index['pending_sessions'] == 0
                                             and index['position'] == index['current_commit_seq'] and index['serving']) else None
                        index = wait(daemon, 'projection caught up', projection_caught_up)
                        assert index['generation'] == original['resolved']['generation']
                        assert cli('show', 'sinex:ev/'+header['id'])['row']['current_eligible'] is False
                        assert 'removed parents' in json.dumps(cli('recall', 'last', '--project', 'tide', expected=5))
                        assert 'removed parents' in json.dumps(tool('sinex_recall', {'project': 'tide'}, error=True))
                    finally:
                        archive(False)
                    restored = cli('recall', 'last', '--project', 'tide')
                    assert restored['session']['id'] == header['id'] and restored['rows'] == original['rows']
            finally:
                try:
                    held.sendall(b'c')
                finally:
                    held.close()
                    controller.close()
        scenario('recall refuses a session whose raw or canonical parent is archived, and serves it again once restored', archived_parent)

        def overflow():
            # 101 commands still form one session, but recall returns at most 100.
            add([fixture_row(f'large-{n}', '09:00:00', '/work/tide') for n in range(101)])
            wait(daemon, 'large session admitted', lambda: query('commands sort when limit 10000')['result']['total'] == len(facts))
            wait(daemon, 'recall reports the session too large',
                 lambda: 'assembly exceeds' in json.dumps(query('recall last where project = tide')))
            assert 'assembly exceeds' in json.dumps(cli('recall', 'last', '--project', 'tide', expected=5))
            tool('sinex_recall', {'project': 'tide'}, error=True)
            # 129 newer sessions with unknown project exceed the 128 recall inspects, even for a project with no match.
            base = fixture_row('unknown-base', '10:00:00', '')['timestamp']
            rows = []
            for n in range(129):
                row = fixture_row(f'unknown-{n}', '10:00:00', '')
                row['timestamp'] = base+n*600_000_000_000
                rows.append(row)
            add(rows)
            wait(daemon, 'newer sessions admitted', lambda: query('commands sort when limit 10000')['result']['total'] == len(facts))
            wait(daemon, 'recall reports too many sessions', lambda: '128 sessions' in json.dumps(query('recall last where project = empty')))
            assert '128 sessions' in json.dumps(cli('recall', 'last', '--project', 'empty', expected=5))
            tool('sinex_recall', {'project': 'empty'}, error=True)
            clients.close_mcp()
        scenario('recall is partial for a session over 100 commands or over 128 newer sessions to inspect', overflow)
    finally:
        if daemon is not None and daemon.poll() is None:
            stop(daemon)
