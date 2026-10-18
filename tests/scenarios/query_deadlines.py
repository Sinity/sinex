"""Query deadlines and client disconnects release the PostgreSQL reader: a held table
lock blocks reads, and each case must free the reader backend and its snapshot."""
import concurrent.futures
import json
import signal
import socket
import subprocess
import time

from harness import TOKEN
from _atuin_daemon import Daemon, sql, table_lock


def exercise(h):
    work, report = h['WORK'], h['REPORT']
    daemon = Daemon(h, 'deadlines')
    daemon.configure()
    daemon.start('deadlines')
    daemon.ready()

    def readers():
        return sql(h, "SELECT coalesce(json_agg(json_build_object('pid',pid,'state',state,'xact_start',xact_start,"
                      "'backend_xmin',backend_xmin,'wait_event_type',wait_event_type,'query',query)),'[]') "
                      "FROM pg_stat_activity WHERE usename='sinex_reader'")

    def blocked(rows):
        return [r for r in rows if r['state'] == 'active' and r['wait_event_type'] == 'Lock' and 'WITH matching' in r['query']]

    def wait_blocked(count):
        return daemon.wait(f'{count} blocked reads', lambda: b if len(b := blocked(readers())) == count else None, 10)

    def released(pids):
        daemon.wait('reader backends released', lambda: all(r['pid'] not in pids for r in readers()), 3)

    def launch(name, timeout=None):
        args = [str(h['BIN_DIR']/'sinexctl'), '--endpoint', daemon.endpoint, '--token-file', str(work/'token'), 'query', 'events', '--json']
        if timeout is not None:
            args += ['--query-timeout-ms', str(timeout)]
        out, err = (report/(name + '.stdout')).open('w'), (report/(name + '.stderr')).open('w')
        cli = subprocess.Popen(args, env=h['ENV'], stdout=out, stderr=err)
        h['CHILDREN'].append((cli, out, err))
        return cli

    def api(params):
        return h['rpc'](daemon.endpoint, TOKEN, 'query.run', params, timeout=15)

    def assert_budget(answer, timeout_ms):
        assert answer['kind'] == 'budget', answer
        details = answer.get('details', answer.get('data'))
        assert details['reason'] == 'execution_deadline' and details['timeout_ms'] == timeout_ms
        assert details['estimate'] is None and details['suggestion']
        assert all(key not in answer for key in ['result', 'rows', 'total', 'page', 'complete'])

    def next_read_succeeds():
        result = daemon.cli('query', 'events')
        assert result['complete'] is True and result['total'] == 0 and result['rows'] == []
        assert result['page']['cursor'] is None and result['source_status'] == []
        daemon.wait('no reader holds a transaction', lambda: all(r['xact_start'] is None and r['backend_xmin'] is None for r in readers()), 10)

    def disconnect():
        with table_lock(h, 'core.events'):
            for n in range(3):
                cli = launch(f'disconnect-{n}')
                read = wait_blocked(1)[0]
                assert read['xact_start'] and read['backend_xmin']
                cli.send_signal(signal.SIGKILL)
                cli.wait(timeout=5)
                released({read['pid']})
                assert (report/f'disconnect-{n}.stdout').read_text() == ''
            # Closing the HTTP connection mid-request releases the reader the same way.
            body = json.dumps({'jsonrpc': '2.0', 'id': 8, 'method': 'query.run', 'params': {'query': 'events'}}).encode()
            connection = socket.create_connection(('127.0.0.1', daemon.port), timeout=5)
            try:
                connection.sendall(f'POST /rpc HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer {TOKEN}\r\n'
                                   f'Content-Type: application/json\r\nContent-Length: {len(body)}\r\n\r\n'.encode() + body)
                pid = wait_blocked(1)[0]['pid']
            finally:
                connection.close()
            released({pid})
        next_read_succeeds()
    h['scenario']('a disconnected client releases its blocked read', disconnect)

    def api_deadline():
        with table_lock(h, 'core.events'), concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            started = time.monotonic()
            future = executor.submit(api, {'query': 'events'})
            pid = wait_blocked(1)[0]['pid']
            reply = future.result(timeout=13)
            elapsed = time.monotonic() - started
            assert reply['id'] == 1 and 'result' not in reply
            assert_budget(reply['error'], 10_000)
            assert 9 <= elapsed < 13, elapsed
            released({pid})
        next_read_succeeds()
    h['scenario']('the API default 10s deadline returns budget and releases the read', api_deadline)

    def cli_deadline(timeout=None):
        name = 'cli-default' if timeout is None else 'cli-configured'
        milliseconds = 60_000 if timeout is None else timeout
        with table_lock(h, 'core.events'):
            started = time.monotonic()
            cli = launch(name, timeout)
            pid = wait_blocked(1)[0]['pid']
            cli.wait(timeout=milliseconds/1000 + 5)
            elapsed = time.monotonic() - started
            assert cli.returncode == 6, f'CLI deadline exit {cli.returncode}'
            assert (report/(name + '.stdout')).read_text() == ''
            assert_budget(json.loads((report/(name + '.stderr')).read_text()), milliseconds)
            assert milliseconds/1000*.85 <= elapsed < milliseconds/1000 + 4, elapsed
            released({pid})
        next_read_succeeds()
    h['scenario']('the CLI default 60s deadline returns budget and releases the read', cli_deadline)
    h['scenario']('a configured CLI deadline returns budget and releases the read', lambda: cli_deadline(500))

    def queued():
        with table_lock(h, 'core.events'):
            clients = [launch(f'saturated-{n}') for n in range(2)]
            try:
                pids = {r['pid'] for r in wait_blocked(2)}
                cli = launch('queued-deadline', 300)
                cli.wait(timeout=4)
                assert cli.returncode == 6
                assert_budget(json.loads((report/'queued-deadline.stderr').read_text()), 300)
                assert len(blocked(readers())) == 2
                for client in clients:
                    client.kill()
                    client.wait(timeout=5)
                released(pids)
            finally:
                for client in clients:
                    if client.poll() is None:
                        client.kill()
                        client.wait(timeout=5)
        next_read_succeeds()
    h['scenario']('a read waiting for a reader slot hits its deadline; disconnects free both slots', queued)

    def bad_timeouts():
        for value in [0, 3_600_001]:
            reply = api({'query': 'events', 'timeout_ms': value})
            assert reply['error']['kind'] == 'bad_query' and 'result' not in reply, value
        daemon.stop()
    h['scenario']('timeout_ms outside 1..3600000 is a bad query', bad_timeouts)
