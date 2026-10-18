"""Daemon startup, refusals, CLI routes, single-writer ownership and restart."""
import json
import socket
import time


def exercise(h):
    work, run, scenario, rpc, token = h['WORK'], h['run'], h['scenario'], h['rpc'], 'synthetic-foundation-token'
    daemon, cli = h['BIN_DIR']/'sinexd', h['BIN_DIR']/'sinexctl'
    port = h['free_port']()
    endpoint = f'http://127.0.0.1:{port}/rpc'

    def config_text(port=port, state='service'):
        return f'''listen = "127.0.0.1:{port}"
state_dir = "{work/state}"
store_dir = "{work/(state + '-blobs')}"
writer_connection_file = "{work/'writer'}"
reader_connection_file = "{work/'reader'}"
token_file = "{work/'token'}"
sources = []
automata = []
'''
    config = work/'config.toml'
    config.write_text(config_text())

    def wait_ready(proc):
        deadline = time.monotonic() + 15
        while True:
            assert proc.poll() is None, 'daemon exited before answering'
            try:
                if 'result' in rpc(endpoint, token):
                    return
            except (OSError, ValueError):
                pass
            assert time.monotonic() < deadline, 'daemon did not answer within 15s'
            time.sleep(0.05)

    def bad_config():
        bad = work/'bad.toml'
        bad.write_text(config_text() + 'unexpected = true\n')
        run([daemon, 'serve', '--config', bad], expected=1, label='unknown config key')
        assert not (work/'service').exists(), 'invalid config created state'
        bad.write_text(config_text().replace('sources = []', 'sources = ["terminal.atuin-history"]'))
        run([daemon, 'serve', '--config', bad], expected=1, label='source without [atuin]')
    scenario('invalid config fails before startup', bad_config)

    def occupied_endpoint():
        with socket.socket() as occupied:
            occupied.bind(('127.0.0.1', 0))
            occupied.listen()
            other = work/'occupied.toml'
            other.write_text(config_text(occupied.getsockname()[1], 'occupied-state'))
            run([daemon, 'serve', '--config', other], expected=1, label='occupied endpoint')
            assert not (work/'occupied-state').exists(), 'occupied endpoint created state'
    scenario('occupied endpoint fails before taking state', occupied_endpoint)

    proc = h['start_daemon'](config, 'daemon')
    wait_ready(proc)

    def client_routes():
        for command in ['status', 'version', 'catalog', 'config-show']:
            answer = run([cli, '--endpoint', endpoint, '--token-file', work/'token', command, '--json'], label='CLI ' + command)
            decoded = json.loads(answer.stdout)
            if command == 'status':
                assert decoded['supported_sources'] == [] and decoded['writer']['state'] == 'owned'
            assert token not in answer.stdout and h['writer'] not in answer.stdout, 'secret in output'
        (work/'wrong-token').write_text('wrong-token')
        refused = run([cli, '--endpoint', endpoint, '--token-file', work/'wrong-token', 'status', '--json'], expected=4, label='CLI wrong token')
        assert json.loads(refused.stderr)['kind'] == 'refused'
        assert rpc(endpoint, 'wrong-token')['error']['kind'] == 'refused'
        assert rpc(endpoint, token, 'replay')['error']['kind'] == 'not_found'
        refused = run([cli, '--endpoint', endpoint, '--token-file', work/'token', '--query-timeout-ms', '500', 'status'], expected=2, label='query deadline on a non-query command')
        assert json.loads(refused.stderr)['kind'] == 'bad_query'
    scenario('CLI commands, token refusal and secret-free output', client_routes)

    def single_writer_and_restart():
        other = work/'second.toml'
        other.write_text(config_text(h['free_port'](), 'other-state'))
        run([daemon, 'serve', '--config', other], expected=1, label='second writer on the same database')
        assert proc.poll() is None and 'result' in rpc(endpoint, token)
        h['stop'](proc)
        restarted = h['start_daemon'](config, 'restart')
        wait_ready(restarted)
        h['stop'](restarted)
    scenario('second writer is refused; clean shutdown releases ownership', single_writer_and_restart)
