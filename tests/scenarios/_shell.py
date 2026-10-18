"""Helpers for the search, shell_grouping and shell_publication scenarios: one
daemon at a time on one endpoint, its CLI, JSON-RPC and MCP clients, psql, and
synthetic Atuin history files."""
import contextlib
import json
import select
import socket
import sqlite3
import subprocess
import time

TOKEN = 'synthetic-foundation-token'
OPERATOR = 'synthetic-operator-token'
HISTORY_COLUMNS = ['id', 'timestamp', 'duration', 'exit', 'command', 'cwd', 'session', 'hostname', 'deleted_at']
MCP_TOOLS = {'sinex_recall', 'sinex_search', 'sinex_query', 'sinex_show', 'sinex_trace', 'sinex_sources', 'sinex_status'}


def create_history(path, rows=(), wal=False):
    """An Atuin history database with `rows` (tuples in HISTORY_COLUMNS order)."""
    with sqlite3.connect(path) as con:
        if wal:
            # A writer must be able to append while the daemon holds a read snapshot.
            assert con.execute('PRAGMA journal_mode=WAL').fetchone()[0] == 'wal'
        con.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER NOT NULL,duration INTEGER NOT NULL,'
                    'exit INTEGER NOT NULL,command TEXT NOT NULL,cwd TEXT NOT NULL,session TEXT NOT NULL,'
                    'hostname TEXT NOT NULL,deleted_at INTEGER)')
    add_history(path, *rows)


def add_history(path, *rows):
    with sqlite3.connect(path) as con:
        con.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)', rows)


def update_history(path, statement, *params):
    with sqlite3.connect(path) as con:
        con.execute(statement, params)


@contextlib.contextmanager
def pause_listener(path, timeout):
    """A Unix socket for `sinexd --test-pause-socket`; removed afterwards, since a
    socket file cannot be copied into a failed scenario's kept directory."""
    with socket.socket(socket.AF_UNIX) as listener:
        listener.bind(str(path))
        listener.listen()
        listener.settimeout(timeout)
        try:
            yield listener
        finally:
            path.unlink()


def sql(h, statement, url=None):
    """Run one statement with psql and decode its single JSON value."""
    result = h['run'](['psql', url or h['admin'], '-XAtq', '-v', 'ON_ERROR_STOP=1', '-c', statement], label='psql')
    return json.loads(result.stdout) if result.stdout.strip() else None


class Daemon:
    """The scenario's daemon on a fixed endpoint, with `config` rewritten between starts."""

    def __init__(self, h, config):
        self.h, self.config, self.work = h, config, h['WORK']
        self.address = f'127.0.0.1:{h["free_port"]()}'
        self.endpoint = f'http://{self.address}/rpc'
        self.proc = None
        (self.work/'operator').write_text(OPERATOR)
        (self.work/'read-model').write_text(h['url']('sinex_read_model'))

    def start(self, name, *extra):
        self.stop()
        self.proc = self.h['start_daemon'](self.config, name, *extra)
        return self.proc

    def stop(self):
        if self.proc is not None and self.proc.poll() is None:
            self.h['stop'](self.proc)
        self.proc = None

    def kill(self):
        self.proc.kill()
        self.proc.wait(timeout=10)
        self.proc = None

    def rpc(self, method, params=None, operator=False, timeout=20):
        return self.h['rpc'](self.endpoint, OPERATOR if operator else TOKEN, method, params, timeout=timeout)

    def wait(self, predicate=lambda status: True, seconds=45):
        """Poll `status` until `predicate(status)` holds; the daemon must stay up."""
        deadline = time.monotonic() + seconds
        last = None
        while True:
            assert self.proc is not None and self.proc.poll() is None, 'daemon exited; see its stderr in the report'
            try:
                last = self.rpc('status').get('result')
                if last is not None and predicate(last):
                    return last
            except (OSError, ValueError):
                pass
            assert time.monotonic() < deadline, f'status did not reach the expected state in {seconds}s; last: {json.dumps(last)[:3000]}'
            time.sleep(.025)

    def sources(self):
        return self.rpc('sources.list').get('result', {}).get('sources', [])

    def cli(self, command, *args, expected=0, operator=False):
        token = self.work/('operator' if operator else 'token')
        result = self.h['run']([self.h['BIN_DIR']/'sinexctl', '--endpoint', self.endpoint, '--token-file', token,
                                command, *args, '--json'], expected=expected, label='CLI ' + command)
        return json.loads(result.stdout or result.stderr)

    def query(self, text, page=100, cursor=None):
        """One page of `query.run`, waiting up to 10s while the projection catches up."""
        params = {'query': text}
        if not text.startswith('events where source'):
            params['page'] = page
        if cursor:
            params['cursor'] = cursor
        deadline = time.monotonic() + 10
        while True:
            answer = self.rpc('query.run', params)
            if 'result' in answer or answer.get('error', {}).get('kind') != 'partial' or time.monotonic() > deadline:
                assert 'result' in answer, answer
                return answer['result']
            time.sleep(.025)

    def query_all(self, text):
        """Every page; the last page's answer with all rows."""
        rows, cursor = [], None
        while True:
            answer = self.query(text, cursor=cursor)
            rows += answer['rows']
            cursor = answer['page'].get('cursor')
            if not cursor:
                return {**answer, 'rows': rows}

    def mcp(self, name):
        return Mcp(self.h, self.endpoint, name)


class Mcp:
    """`sinexctl mcp` over stdio, initialized and with the expected tool list."""

    def __init__(self, h, endpoint, name):
        args = [str(h['BIN_DIR']/'sinexctl'), '--endpoint', endpoint, '--token-file', str(h['WORK']/'token'), 'mcp']
        err = (h['REPORT']/(name + '.stderr')).open('w')
        self.proc = subprocess.Popen(args, env=h['ENV'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=err, text=True, bufsize=1)
        h['CHILDREN'].append((self.proc, self.proc.stdout, err))
        self.next_id = 1
        initialized = self.call('initialize', {'protocolVersion': '2025-03-26', 'capabilities': {},
                                               'clientInfo': {'name': 'sinex-scenario', 'version': '1'}})
        assert 'result' in initialized, initialized
        self.send({'jsonrpc': '2.0', 'method': 'notifications/initialized'})
        assert {t['name'] for t in self.call('tools/list', {})['result']['tools']} == MCP_TOOLS

    def send(self, message):
        self.proc.stdin.write(json.dumps(message) + '\n')
        self.proc.stdin.flush()

    def call(self, method, params, seconds=20):
        request_id = self.next_id
        self.next_id += 1
        self.send({'jsonrpc': '2.0', 'id': request_id, 'method': method, 'params': params})
        deadline = time.monotonic() + seconds
        while True:
            ready, _, _ = select.select([self.proc.stdout], [], [], max(0, deadline - time.monotonic()))
            assert ready, f'no MCP reply to {method} within {seconds}s'
            line = self.proc.stdout.readline()
            assert line, 'MCP server closed its output'
            reply = json.loads(line)
            if reply.get('id') == request_id:
                return reply

    def tool(self, name, arguments):
        reply = self.call('tools/call', {'name': name, 'arguments': arguments})
        result = reply['result']
        assert not result.get('isError'), reply
        return result.get('structuredContent') or json.loads(result['content'][0]['text'])

    def cancel(self, request_id):
        self.send({'jsonrpc': '2.0', 'method': 'notifications/cancelled',
                   'params': {'requestId': request_id, 'reason': 'scenario cancellation'}})

    def close(self):
        if self.proc.poll() is None:
            self.proc.stdin.close()
            self.proc.wait(timeout=5)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
