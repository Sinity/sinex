"""Shared pieces for scenarios that run the daemon on a synthetic Atuin history:
the history table, daemon config, CLI/API/MCP clients, SQL, a held table lock,
and restarting, killing and pausing the daemon's writer."""
import contextlib
import json
import select
import signal
import socket
import sqlite3
import subprocess
import time

from harness import TOKEN

COLUMNS = ['id', 'timestamp', 'duration', 'exit', 'command', 'cwd', 'session', 'hostname', 'deleted_at']
INSERT = 'INSERT INTO history VALUES (?,?,?,?,?,?,?,?,?)'
ATUIN_QUERY = 'events where source = terminal.atuin-history and kind = command.executed'


def create_history(path, rows=()):
    """An Atuin history database with the columns the collector reads."""
    with sqlite3.connect(path) as con:
        con.execute('CREATE TABLE history (id TEXT PRIMARY KEY,timestamp INTEGER,duration INTEGER,exit INTEGER,'
                    'command TEXT,cwd TEXT,session TEXT,hostname TEXT,deleted_at INTEGER)')
        con.executemany(INSERT, rows)


def sql(h, text, url=None):
    """Run one SQL statement with psql and decode its single JSON value."""
    result = h['run'](['psql', url or h['admin'], '--no-psqlrc', '-qAt', '-v', 'ON_ERROR_STOP=1', '-c', text],
                      label='SQL check')
    return json.loads(result.stdout)


@contextlib.contextmanager
def table_lock(h, table):
    """Hold ACCESS EXCLUSIVE on `table` in a separate psql session until the block ends."""
    lock = subprocess.Popen(['psql', h['admin'], '--no-psqlrc', '-qAt', '-v', 'ON_ERROR_STOP=1'], env=h['ENV'],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
    h['CHILDREN'].append((lock, lock.stdout, lock.stderr))
    try:
        lock.stdin.write(f"BEGIN; LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE; SELECT 'locked';\n")
        lock.stdin.flush()
        ready, _, _ = select.select([lock.stdout], [], [], 15)
        assert ready and lock.stdout.readline().strip() == 'locked', 'could not lock ' + table
        yield
    finally:
        if lock.poll() is None:
            lock.stdin.write('ROLLBACK;\n\\q\n')
            lock.stdin.flush()
            lock.wait(timeout=15)
        assert lock.returncode == 0, lock.stderr.read()


class Daemon:
    """One daemon config and its current process, with CLI and API helpers."""

    def __init__(self, h, name):
        self.h, self.name = h, name
        self.port = h['free_port']()
        self.endpoint = f'http://127.0.0.1:{self.port}/rpc'
        self.config = h['WORK']/(name + '.toml')
        self.proc = None

    def configure(self, atuin=None, *, poll_ms=1000, full_ms=100, sources=None, automata=(), top='', tables=''):
        """Write the config. `top` adds top-level keys, `tables` adds TOML tables after `[atuin]`."""
        work = self.h['WORK']
        if sources is None:
            sources = ['terminal.atuin-history'] if atuin else []
        text = f'''listen = "127.0.0.1:{self.port}"
state_dir = "{work/(self.name + '-state')}"
store_dir = "{self.store_dir}"
writer_connection_file = "{work/'writer'}"
reader_connection_file = "{work/'reader'}"
token_file = "{work/'token'}"
sources = {json.dumps(sources)}
automata = {json.dumps(list(automata))}
{top}'''
        if atuin:
            text += f'[atuin]\npath = "{atuin}"\npoll_interval_ms = {poll_ms}\nfull_interval_ms = {full_ms}\n'
        self.config.write_text(text + tables)

    @property
    def store_dir(self):
        return self.h['WORK']/(self.name + '-store')

    def start(self, label, *extra):
        self.proc = self.h['start_daemon'](self.config, label, *extra)
        return self.proc

    def stop(self):
        self.h['stop'](self.proc)

    def restart(self, label, *extra):
        self.stop()
        return self.start(label, *extra)

    def wait(self, label, predicate, seconds=30):
        """Poll `predicate` until it returns something truthy; the daemon must stay up."""
        deadline = time.monotonic() + seconds
        last = None
        while time.monotonic() < deadline:
            assert self.proc.poll() is None, f'daemon exited while waiting for: {label}'
            try:
                last = predicate()
                if last:
                    return last
            except (OSError, KeyError, ValueError, IndexError, TypeError):
                pass
            time.sleep(0.03)
        raise AssertionError(f'timed out after {seconds}s waiting for: {label}; last={last!r}')

    def ready(self):
        return self.wait('daemon answers status', lambda: 'result' in self.api())

    def api(self, method='status', params=None):
        return self.h['rpc'](self.endpoint, TOKEN, method, params, timeout=15)

    def sources(self, **params):
        return self.api('sources.list', params)['result']['sources']

    def last_run(self):
        return self.sources()[0]['last_run']

    def cli(self, command, *args, expected=0, token_file=None):
        """Run `sinexctl COMMAND ARGS --json` and decode stdout, or stderr when stdout is empty."""
        work = self.h['WORK']
        result = self.h['run']([self.h['BIN_DIR']/'sinexctl', '--endpoint', self.endpoint,
                                '--token-file', token_file or work/'token', command, *args, '--json'],
                               expected=expected, label='sinexctl ' + command)
        return json.loads(result.stdout or result.stderr)


class Mcp:
    """`sinexctl mcp` over stdio."""

    def __init__(self, h, daemon):
        argv = [str(h['BIN_DIR']/'sinexctl'), '--endpoint', daemon.endpoint, '--token-file', str(h['WORK']/'token'), 'mcp']
        stderr = (h['REPORT']/'mcp.stderr').open('a')
        self.proc = subprocess.Popen(argv, env=h['ENV'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=stderr, text=True, bufsize=1)
        h['CHILDREN'].append((self.proc, self.proc.stdout, stderr))
        self.last_id = 0
        assert 'result' in self.call('initialize', {'protocolVersion': '2025-03-26', 'capabilities': {},
                                                    'clientInfo': {'name': 'scenario', 'version': '1'}})
        self.send({'jsonrpc': '2.0', 'method': 'notifications/initialized'})

    def send(self, message):
        self.proc.stdin.write(json.dumps(message) + '\n')
        self.proc.stdin.flush()

    def call(self, method, params):
        self.last_id += 1
        self.send({'jsonrpc': '2.0', 'id': self.last_id, 'method': method, 'params': params})
        deadline = time.monotonic() + 25
        while True:
            ready, _, _ = select.select([self.proc.stdout], [], [], max(0, deadline - time.monotonic()))
            assert ready, f'no MCP reply to {method} within 25s'
            reply = json.loads(self.proc.stdout.readline())
            if reply.get('id') == self.last_id:
                return reply

    def tool(self, name, arguments):
        result = self.call('tools/call', {'name': name, 'arguments': arguments})['result']
        assert not result.get('isError'), result
        return result.get('structuredContent') or json.loads(result['content'][0]['text'])

    def close(self):
        if self.proc.poll() is None:
            self.proc.stdin.close()
            self.proc.wait(timeout=20)
        assert self.proc.returncode == 0, 'MCP server did not exit cleanly on stdin EOF'


OPERATOR_TOKEN = 'synthetic-operator-token'


def access_config(h, operator=True, read_model=True):
    """Top-level config lines for the operator token (replay, delete) and the search index's read-model connection."""
    work, lines = h['WORK'], ''
    for enabled, key, name, value in [(operator, 'operator_token_file', 'operator', OPERATOR_TOKEN),
                                      (read_model, 'read_model_connection_file', 'read-model', h['url']('sinex_read_model'))]:
        if enabled:
            (work/name).write_text(value)
            (work/name).chmod(0o600)
            lines += f'{key} = "{work/name}"\n'
    return lines


def append_history(path, rows):
    with sqlite3.connect(path) as con:
        con.executemany(INSERT, rows)


def relaunch(d, label, *extra, binary=None):
    """Stop the daemon if it is running, start it again and wait until it answers."""
    if d.proc is not None and d.proc.poll() is None:
        d.stop()
    d.proc = d.h['start_daemon'](d.config, label, *extra, binary=binary)
    d.ready()


def kill(d):
    d.proc.send_signal(signal.SIGKILL)
    d.proc.wait(timeout=10)
    assert d.proc.returncode == -signal.SIGKILL


def pause_at(d, point, label=None):
    """Restart the daemon so that its writer stops at `point`; pass the result to `paused`."""
    path = d.h['WORK']/'pause.sock'
    path.unlink(missing_ok=True)
    listener = socket.socket(socket.AF_UNIX)
    listener.bind(str(path))
    listener.listen(1)
    listener.settimeout(30)
    relaunch(d, label or point, '--test-pause-at', point, '--test-pause-socket', path)
    return listener


def paused(d, listener, point):
    """Wait for the writer to reach the pause point; returns the connection (send b'c' to resume) and its frame."""
    connection, _ = listener.accept()
    connection.settimeout(10)
    frame = json.loads(connection.makefile('rb').readline(262144))
    assert frame['pid'] == d.proc.pid and frame['pause_point'] == point, frame
    listener.close()
    (d.h['WORK']/'pause.sock').unlink()
    return connection, frame


def blob_path(d, material):
    """Where the daemon's store keeps the bytes of a source material or a stored action record."""
    digest = sql(d.h, "SELECT to_json(b.blake3) FROM raw.source_material_registry m JOIN core.blobs b "
                      f"ON b.id=m.blob_id WHERE m.id='{material}'::uuid").removeprefix('blake3:')
    return d.store_dir/'blobs'/digest[:2]/(digest + '.zst')
