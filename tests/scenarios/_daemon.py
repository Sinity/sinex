"""A daemon that a scenario reconfigures, restarts, pauses and kills, with CLI,
JSON-RPC and SQL access and a synthetic Atuin history to feed it."""
import json
import signal
import socket
import sqlite3
import time
import urllib.error
import urllib.request

TOKEN = 'synthetic-foundation-token'
OPERATOR_TOKEN = 'synthetic-operator-token'


def atuin_history(path, rows):
    """Add rows (id, timestamp, duration, exit, command, cwd, session, hostname, deleted_at) to an Atuin history file."""
    with sqlite3.connect(path) as con:
        con.execute('CREATE TABLE IF NOT EXISTS history(id TEXT PRIMARY KEY,timestamp INTEGER,duration INTEGER,'
                    'exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT,deleted_at INTEGER)')
        con.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)', rows)


def secret_file(path, value):
    path.write_text(value)
    path.chmod(0o600)
    return path


class Daemon:
    def __init__(self, h, name):
        self.h, self.name, self.work = h, name, h['WORK']
        self.port = h['free_port']()
        self.endpoint = f'http://127.0.0.1:{self.port}/rpc'
        self.config = self.work/(name + '.toml')
        self.store = self.work/(name + '-store')
        self.proc = None
        self.operator_file = secret_file(self.work/'operator', OPERATOR_TOKEN)
        secret_file(self.work/'read-model', h['url']('sinex_read_model'))

    def configure(self, *, atuin=None, poll_ms=3_600_000, full_ms=100, rules=(), projects=(),
                  read_model=True, operator=True, sources=None, extra='', database='', store=None):
        """Write the config. `atuin` is a history path to collect from; `database` prefixes the connection files."""
        work = self.work
        if sources is None:
            sources = ['terminal.atuin-history'] if atuin else []
        text = f'''listen = "127.0.0.1:{self.port}"
state_dir = "{work/((store or self.name) + '-state')}"
store_dir = "{work/((store or self.name) + '-store')}"
writer_connection_file = "{work/(database + 'writer')}"
reader_connection_file = "{work/(database + 'reader')}"
token_file = "{work/'token'}"
sources = {json.dumps(sources)}
automata = []
'''
        if read_model:
            text += f'read_model_connection_file = "{work/"read-model"}"\n'
        if operator:
            text += f'operator_token_file = "{self.operator_file}"\n'
        for name, roots in projects:
            text += f'[[projects]]\nname = {json.dumps(name)}\nroots = {json.dumps(roots)}\n'
        for pattern, replacement in rules:
            text += (f'[[privacy_rules]]\npattern = {json.dumps(pattern)}\n'
                     f'replacement = {json.dumps(replacement)}\ncreated_by = "operator"\n')
        if atuin:
            text += f'[atuin]\npath = "{atuin}"\npoll_interval_ms = {poll_ms}\nfull_interval_ms = {full_ms}\n'
        self.config.write_text(text + extra)

    def start(self, label, *flags, binary=None):
        """Start (stopping a running one first) and wait until it answers."""
        self.stop()
        self.proc = self.h['start_daemon'](self.config, label, *flags, binary=binary)
        self.wait(lambda: 'result' in self.api('status'), 'daemon to answer')

    def stop(self):
        if self.proc is not None and self.proc.poll() is None:
            self.h['stop'](self.proc)

    def kill(self):
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait(timeout=10)
        assert self.proc.returncode == -signal.SIGKILL

    def wait(self, fn, what, seconds=30):
        """Poll `fn` until it returns something truthy; the daemon must stay up."""
        deadline = time.monotonic() + seconds
        while True:
            assert self.proc.poll() is None, f'daemon exited while waiting for {what}'
            try:
                value = fn()
                if value:
                    return value
            except (OSError, ValueError, KeyError):
                pass
            assert time.monotonic() < deadline, f'timed out waiting for {what}'
            time.sleep(.025)

    def api(self, method, params=None, operator=False, timeout=30):
        request = urllib.request.Request(
            self.endpoint,
            data=json.dumps({'jsonrpc': '2.0', 'id': 7, 'method': method, 'params': params or {}}).encode(),
            headers={'Authorization': 'Bearer ' + (OPERATOR_TOKEN if operator else TOKEN),
                     'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            return json.load(error)

    def sources(self):
        return self.api('sources.list').get('result', {}).get('sources', [])

    def last_run(self):
        sources = self.sources()
        return sources[0]['last_run'] if sources else None

    def cli_argv(self, command, *args, operator=False):
        return [self.h['BIN_DIR']/'sinexctl', '--endpoint', self.endpoint, '--token-file',
                self.operator_file if operator else self.work/'token', command, *args, '--json']

    def cli(self, command, *args, expected=0, operator=False):
        """Run sinexctl with --json and decode its reply (stdout, or stderr for errors)."""
        result = self.h['run'](self.cli_argv(command, *args, operator=operator),
                               expected=expected, label='sinexctl ' + command)
        return json.loads(result.stdout or result.stderr)

    def psql(self, statement, url=None):
        return self.h['run'](['psql', url or self.h['reader'], '-XAtq', '-v', 'ON_ERROR_STOP=1', '-c', statement],
                             label='psql').stdout.strip()

    def sql(self, statement, url=None):
        return json.loads(self.psql(statement, url))

    def blob_path(self, material):
        """Where the store keeps the bytes of a source material (or a stored action record)."""
        digest = self.sql("SELECT to_json(b.blake3) FROM raw.source_material_registry m JOIN core.blobs b "
                          f"ON b.id=m.blob_id WHERE m.id='{material}'::uuid").removeprefix('blake3:')
        return self.store/'blobs'/digest[:2]/(digest + '.zst')

    def pause_at(self, point, label=None):
        """Restart the daemon so that its writer stops at `point`; returns the listener for `paused`."""
        self.stop()
        path = self.work/'pause.sock'
        path.unlink(missing_ok=True)
        listener = socket.socket(socket.AF_UNIX)
        listener.bind(str(path))
        listener.listen(1)
        listener.settimeout(30)
        self.start(label or point, '--test-pause-at', point, '--test-pause-socket', path)
        return listener

    def paused(self, listener, point):
        """Wait for the writer to reach the pause point; returns the connection and its frame."""
        connection, _ = listener.accept()
        connection.settimeout(10)
        frame = json.loads(connection.makefile('rb').readline(262144))
        assert frame['pid'] == self.proc.pid and frame['pause_point'] == point, frame
        listener.close()
        (self.work/'pause.sock').unlink()
        return connection, frame
