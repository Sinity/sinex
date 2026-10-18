"""Recall completeness with record errors: Atuin and zsh errors make recall and search
incomplete until clean, a stored zsh error keeps them incomplete, and pending or disabled
projections give a typed partial answer."""
import json
import socket
import sqlite3
import time

from _clients import Clients, wait


def exercise(h):
    work, bins = h['WORK'], h['BIN_DIR']
    run, scenario, start, stop = (h[k] for k in ['run', 'scenario', 'start_daemon', 'stop'])
    path, zsh = work/'coverage.db', work/'coverage-zsh'
    epoch = 1791849600
    with sqlite3.connect(path) as con:
        con.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT,deleted_at INTEGER)')
        con.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)', [
            ('valid', epoch*1_000_000_000, 1_000_000_000, 0, 'coverage valid atuin', '/synthetic/coverage', 'fixture', 'fixture-host:user', None),
            ('malformed', 'invalid-time', 1, 0, 'coverage malformed atuin', '/synthetic/coverage', 'fixture', 'fixture-host:user', None)])
    con.close()
    zsh.write_bytes(f': {epoch+4}:1;coverage valid zsh\n'.encode())
    (work/'read-model').write_text(h['reader'].replace('sinex_reader', 'sinex_read_model'))
    (work/'read-model').chmod(0o600)
    endpoint = f'http://127.0.0.1:{h["free_port"]()}/rpc'
    clients = Clients(h, endpoint, 'coverage')
    cli, api = clients.cli, clients.api
    config = work/'coverage.toml'
    config.write_text(f'''listen = "{endpoint.removeprefix('http://').removesuffix('/rpc')}"
state_dir = "{work/'coverage-state'}"
store_dir = "{work/'coverage-blobs'}"
writer_connection_file = "{work/'writer'}"
reader_connection_file = "{work/'reader'}"
read_model_connection_file = "{work/'read-model'}"
token_file = "{work/'token'}"
sources = ["terminal.atuin-history", "terminal.zsh-history"]
automata = ["session"]
[atuin]
path = "{path}"
poll_interval_ms = 3600000
full_interval_ms = 100
[zsh]
path = "{zsh}"
execution_host = "fixture-host:user"
protocol = "append_only_extended_v1"
poll_interval_ms = 3600000
''')
    daemon = None

    def settled():
        status = api()['result']
        index = status['indexed_retrieval']
        sources = api('sources.list')['result']['sources']
        if (len(sources) == 2 and all(s['last_run']['status'] == 'completed' for s in sources) and index['serving']
                and index['pending_sessions'] == index['pending_canonical'] == 0 and index['position'] == status['commit_seq']
                and index['failure'] is None and index['sessions']['failure'] is None):
            return sources
        return None

    def launch(label, *extra):
        nonlocal daemon
        daemon = start(config, label, *extra)
        return wait(daemon, label+': both sources read and projections caught up', settled)

    def answers(complete, names):
        """CLI and MCP recall agree; `complete` matches; query results report the same source status."""
        answer = cli('recall', 'last', expected=0 if complete else 5)
        assert clients.tool('sinex_recall', {}) == answer
        assert answer['complete'] is complete and answer['truncated'] is False
        assert sorted(row['payload']['command'] for row in answer['rows']) == sorted(names)
        assert set(answer['session']['parents']) == {row['id'] for row in answer['rows']}
        assert answer['coverage'].startswith('unknown;')
        indexed = cli('query', 'commands sort when limit 10000', expected=0 if complete else 5)
        assert indexed['complete'] is complete
        raw = cli('query', 'events')
        assert raw['complete'] is all(s.get('retained_record_errors', 0) == 0 for s in raw['source_status'])
        assert answer['source_status'] == indexed['source_status'] == raw['source_status']
        return answer

    def source_status(answer, name):
        return next(s for s in answer['source_status'] if s['source'] == name)

    initial_names = ['coverage valid atuin', 'coverage valid zsh']

    def current_error():
        sources = launch('current-error')
        assert next(s for s in sources if s['source'] == 'terminal.atuin-history')['last_run']['record_errors'] == 1
        clients.connect_mcp()
        status = source_status(answers(False, initial_names), 'terminal.atuin-history')
        assert status['status'] == 'completed' and status['record_errors'] == 1 and status['retained_record_errors'] == 0
        stop(daemon)
    scenario('a record error in the latest read makes recall and search incomplete', current_error)

    def clean():
        with sqlite3.connect(path) as con:
            con.execute("DELETE FROM history WHERE id='malformed'")
        con.close()
        launch('clean')
        answer = answers(True, initial_names)
        assert all(s['record_errors'] == s['retained_record_errors'] == 0 for s in answer['source_status'])
        cli('trace', answer['session']['id'], '--bytes', expected=0)
        stop(daemon)
    scenario('a clean read makes recall complete again', clean)

    all_names = initial_names+['coverage valid after malformed']

    def malformed_append():
        with zsh.open('ab') as out:
            out.write(f'plain unsupported record\n: {epoch+40}:1;coverage valid after malformed\n'.encode())
        launch('zsh-malformed')
        status = source_status(answers(False, all_names), 'terminal.zsh-history')
        assert status['record_errors'] == status['retained_record_errors'] == 1
        stop(daemon)
        launch('zsh-later-clean')
        status = source_status(answers(False, all_names), 'terminal.zsh-history')
        assert status['status'] == 'completed' and status['record_errors'] == 0 and status['retained_record_errors'] == 1
        stop(daemon)
        launch('zsh-restart')
        answer = answers(False, all_names)
        assert all(s['status'] == 'completed' and s['record_errors'] == 0 for s in answer['source_status'])
        assert source_status(answer, 'terminal.zsh-history')['retained_record_errors'] == 1
        stop(daemon)
    scenario('a malformed zsh line appended to the file keeps recall incomplete after later clean reads and restart', malformed_append)

    def typed_pending():
        nonlocal daemon
        # Adding a project makes the projection rebuild; the pause holds its first uncommitted step.
        config.write_text(config.read_text()+'\n[[projects]]\nname = "coverage"\nroots = ["/synthetic/coverage"]\n')
        boundary = work/'recall-projection.sock'
        with socket.socket(socket.AF_UNIX) as controller:
            controller.bind(str(boundary))
            controller.listen(1)
            controller.settimeout(20)
            daemon = start(config, 'projection-pending', '--test-pause-at', 'projection-staged-0', '--test-pause-socket', boundary)
            conn, _ = controller.accept()
            with conn:
                notice = json.loads(conn.makefile('r').readline())
                assert notice['pid'] == daemon.pid and notice['evidence']['through'] > notice['evidence']['from']
                wait(daemon, 'projection not serving', lambda: (lambda s: s['pending_sessions'] == s['pending_canonical'] == 0
                     and s['serving'] is False)(api()['result']['indexed_retrieval']), 20)
                began = time.monotonic()
                pending = run([bins/'sinexctl', '--endpoint', endpoint, '--token-file', work/'token', 'recall', 'last', '--json'],
                              expected=5, label='recall while the projection is pending')
                elapsed = time.monotonic()-began
                fault = json.loads(pending.stderr)
                assert fault['kind'] == 'partial' and fault['details']['reason'] == 'projection_unavailable', fault
                assert elapsed >= .45, 'recall did not wait 500 ms for the projection'
                conn.sendall(b'c')
        wait(daemon, 'projection released', settled)
        answers(False, all_names)
        stop(daemon)
        config.write_text(config.read_text().replace('automata = ["session"]', 'automata = []'))
        launch('sessions-disabled')
        fault = cli('recall', 'last', expected=5)
        assert fault['kind'] == 'partial' and fault['details']['reason'] == 'projection_unavailable', fault
        stop(daemon)
        clients.close_mcp()
    scenario('recall is partial with a typed reason while the projection is pending or sessions are disabled', typed_pending)
