"""Shared scaffolding for end-to-end scenarios.

A scenario gets its own directory and a fresh database in the PostgreSQL
cluster that `xtask scenario` started. Commands and daemons log into the
directory; daemons are always stopped at the end. The directory is removed
when the scenario passes and moved to target/scenario-failures/ when it fails.
"""
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import stat
import subprocess
import tempfile
import threading
import time
import types
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
TOKEN = 'synthetic-foundation-token'


TEMPLATE = 'scenario_template'


def create_template():
    """Create the schema template that every scenario database is cloned from.

    Applying the schema once matters: it also creates cluster-wide roles, and
    concurrent applications race on them.
    """
    env = {k: v for k, v in os.environ.items() if k not in ('PGDATABASE', 'PGUSER')}
    subprocess.run(['createdb', TEMPLATE], env=env, check=True)
    url = f"postgresql:///{TEMPLATE}?host={env['PGHOST']}&port={env['PGPORT']}"
    with tempfile.NamedTemporaryFile('w', suffix='.url') as connection:
        connection.write(url)
        connection.flush()
        subprocess.run([str(Path(env['SINEX_BIN_DIR'])/'sinexd'), 'admin', 'schema', '--connection-file', connection.name],
                       env=env, check=True, stdout=subprocess.DEVNULL)


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def rpc(endpoint, token, method='status', params=None, timeout=2):
    request = urllib.request.Request(
        endpoint,
        data=json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params or {}}).encode(),
        headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        return json.load(error)


class Harness(dict):
    """What a scenario module's `exercise(h)` receives.

    It is a dict so modules can unpack what they use, e.g.
    `run, start, stop = (h[k] for k in ['run', 'start_daemon', 'stop'])`.
    """

    def __init__(self, name, packaged):
        super().__init__()
        self.name = name
        # The daemon fsyncs its state; on tmpfs that costs nothing, on /realm it
        # can take seconds per call and make startup time out.
        base = '/dev/shm' if os.path.isdir('/dev/shm') else None
        self.dir = Path(tempfile.mkdtemp(prefix=f'sinex-scenario-{name}-', dir=base))
        work, report = self.dir/'work', self.dir/'report'
        work.mkdir()
        report.mkdir()
        env = os.environ.copy()
        for key in ('DATABASE_URL', 'SQLX_OFFLINE', 'PGUSER', 'PGDATABASE'):
            env.pop(key, None)
        database = 'scenario_' + name.replace('-', '_')
        host, port = env['PGHOST'], env['PGPORT']

        def url(role, db=database):
            user = role + '@localhost' if role else ''
            return f'postgresql://{user}/{db}?host={host}&port={port}'

        self.update(
            ROOT=ROOT, WORK=work, REPORT=report, ENV=env,
            BIN_DIR=Path(env['SINEX_BIN_DIR']),
            OPTIONS=types.SimpleNamespace(packaged=packaged),
            database=database, url=url,
            admin=url(''), writer=url('sinex_writer'), reader=url('sinex_reader'),
            COMMANDS=[], COMMAND_LOCK=threading.Lock(), PROCESSES=[], CHILDREN=[],
            run=self.run, scenario=self.scenario, start_daemon=self.start_daemon,
            stop=stop, rpc=rpc, free_port=free_port, create_database=self.create_database)
        for key, value in [('admin', self['admin']), ('writer', self['writer']),
                           ('reader', self['reader']), ('token', TOKEN)]:
            (work/key).write_text(value)
            (work/key).chmod(0o600)

    def create_database(self, name):
        """A new database with the schema applied; returns its admin, writer and reader URLs."""
        self.run(['createdb', '--template', TEMPLATE, name], label='create database ' + name)
        return {role: self['url'](user, name) for role, user in
                [('admin', ''), ('writer', 'sinex_writer'), ('reader', 'sinex_reader')]}

    def run(self, args, *, expected=0, env=None, label=None, timeout=900):
        """Run a command, keep its output in the report, and check its exit status."""
        with self['COMMAND_LOCK']:
            index = len(self['COMMANDS'])
            self['COMMANDS'].append({'argv': [str(a) for a in args], 'label': label})
        report = self['REPORT']
        try:
            result = subprocess.run([str(a) for a in args], cwd=ROOT, env=env or self['ENV'],
                                    capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired as error:
            for stream in ('stdout', 'stderr'):
                value = getattr(error, stream) or b''
                (report/f'{index:03}.{stream}').write_text(
                    value.decode(errors='replace') if isinstance(value, bytes) else value)
            raise AssertionError(f'command {index} ({label or args[0]}) timed out') from error
        (report/f'{index:03}.stdout').write_text(result.stdout)
        (report/f'{index:03}.stderr').write_text(result.stderr)
        self['COMMANDS'][index]['exit'] = result.returncode
        allowed = expected if isinstance(expected, tuple) else (expected,)
        if result.returncode not in allowed:
            raise AssertionError(
                f'command {index} ({label or args[0]}) exited {result.returncode}, expected {expected}; '
                f'see {report}/{index:03}.stderr')
        return result

    def scenario(self, name, fn):
        """Run one step and print its outcome."""
        started = time.monotonic()
        try:
            fn()
        except BaseException:
            print(f'  FAIL {name} ({time.monotonic()-started:.1f}s)', flush=True)
            raise
        print(f'  ok   {name} ({time.monotonic()-started:.1f}s)', flush=True)

    def start_daemon(self, config, name, *extra, binary=None):
        report = self['REPORT']
        argv = [str(binary or self['BIN_DIR']/'sinexd'), 'serve', '--config', str(config), *map(str, extra)]
        stdout = (report/(name + '.stdout')).open('w')
        stderr = (report/(name + '.stderr')).open('w')
        (report/(name + '-config.toml')).write_text(Path(config).read_text())
        proc = subprocess.Popen(argv, env=self['ENV'], stdout=stdout, stderr=stderr)
        self['CHILDREN'].append((proc, stdout, stderr))
        self['PROCESSES'].append({'pid': proc.pid, 'argv': argv})
        return proc

    def close(self, failure=None):
        """Stop daemons and remove the directory; on failure (`failure` is the traceback) keep it and return where."""
        for proc, out, err in self['CHILDREN']:
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
            out.close()
            err.close()
        if failure is None:
            shutil.rmtree(self.dir)
            return None
        # TMPDIR is usually a nix-shell directory that disappears; keep failures in target/.
        (self['REPORT']/'traceback.txt').write_text(failure)
        kept = Path(os.environ.get('CARGO_TARGET_DIR', ROOT/'target'))/'scenario-failures'/self.dir.name
        kept.parent.mkdir(parents=True, exist_ok=True)
        # Copy rather than move: the directory is on tmpfs, and sockets cannot be copied.
        def sockets(directory, names):
            return [n for n in names if stat.S_ISSOCK(os.lstat(os.path.join(directory, n)).st_mode)]
        shutil.copytree(self.dir, kept, ignore=sockets, symlinks=True)
        shutil.rmtree(self.dir, ignore_errors=True)
        return kept


def stop(proc):
    """Stop a daemon with SIGTERM and require a clean exit."""
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=15)
    assert proc.returncode == 0, f'daemon did not exit cleanly: {proc.returncode}'
