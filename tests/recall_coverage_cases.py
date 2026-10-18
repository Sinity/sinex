"""Public recall coverage controls, including an exact immutable pre-fix contrast."""
import hashlib
import json
from pathlib import Path
import select
import socket
import sqlite3
import subprocess
import time

PLANNED = ['pre-fix completed poll falsely completes recall',
    'recall and search disclose current record failures',
    'clean polls restore interpreted completeness without asserting continuity',
    'zsh malformed append remains incomplete after later clean poll',
    'pre-fix retained-error contrast and corrected CLI MCP recall',
    'typed projection settling and disabled-capability refusal']


def exercise(h):
    work, report, bins = (h[k] for k in ['WORK', 'REPORT', 'BIN_DIR'])
    run, scenario, start, stop = (h[k] for k in ['run', 'scenario', 'start_daemon', 'stop'])
    pin_path = h['OPTIONS'].recall_coverage_before_pin
    pin = json.loads(pin_path.read_text())
    before = Path(pin['package'])
    assert pin['candidate'] == 'd303047a99bf4c5e105452693b554bb3e38956ab'
    assert pin['exit_code'] == 0 and pin['dirty'] is False and before.parent == Path('/nix/store')
    for name in ['sinexd', 'sinexctl']:
        assert hashlib.sha256((before/'bin'/name).read_bytes()).hexdigest() == pin['binaries'][name]['sha256']
    (report/'before-package-pin.json').write_bytes(pin_path.read_bytes())
    path, zsh = work/'coverage.db', work/'coverage-zsh'
    epoch = 1791849600
    with sqlite3.connect(path) as con:
        con.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT,deleted_at INTEGER)')
        con.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)', [
            ('valid', epoch*1_000_000_000, 1_000_000_000, 0, 'coverage valid atuin', '/synthetic/coverage', 'fixture', 'fixture-host:user', None),
            ('malformed', 'invalid-time', 1, 0, 'coverage malformed atuin', '/synthetic/coverage', 'fixture', 'fixture-host:user', None)])
    zsh.write_bytes(f': {epoch+4}:1;coverage valid zsh\n'.encode())
    (work/'read-model').write_text(h['reader'].replace('sinex_reader', 'sinex_read_model'))
    (work/'read-model').chmod(0o600)
    endpoint = f'http://127.0.0.1:{h["free_port"]()}/rpc'
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
full_interval_ms = 100 # this fixture deliberately compares the full provider
[zsh]
path = "{zsh}"
execution_host = "fixture-host:user"
protocol = "append_only_extended_v1"
poll_interval_ms = 3600000
''')
    mcp = None
    daemon = None
    mid = 0

    def save(name, value):
        (report/name).write_text(json.dumps(value, indent=2, sort_keys=True)+'\n')

    def cli(command, *args, expected=(0, 5)):
        result = run([bins/'sinexctl', '--endpoint', endpoint, '--token-file', work/'token', command, *args, '--json'], expected=expected, label='recall coverage CLI '+command)
        return json.loads(result.stdout)

    def launch(label, binary=None):
        nonlocal daemon
        daemon = start(config, label, binary=binary)
        return settled(label)

    def settled(label):
        deadline = time.monotonic()+120
        last = None
        while time.monotonic()<deadline:
            assert daemon.poll() is None, label+' daemon stopped'
            try:
                status = h['rpc'](endpoint, 'synthetic-foundation-token')['result']
                last = status['indexed_retrieval']
                sources = h['rpc'](endpoint, 'synthetic-foundation-token', 'sources.list')['result']['sources']
                if len(sources)==2 and all(s['last_run']['status']=='completed' for s in sources) and last['serving'] and last['pending_sessions']==last['pending_canonical']==0 and last['position']==status['commit_seq'] and last['failure'] is None and last['sessions']['failure'] is None:
                    return sources
            except (OSError, KeyError, ValueError):
                pass
            time.sleep(.1)
        raise AssertionError((label, last))

    def mcp_call(method, params):
        nonlocal mid
        mid += 1
        request = {'jsonrpc':'2.0', 'id':mid, 'method':method, 'params':params}
        mcp.stdin.write(json.dumps(request)+'\n'); mcp.stdin.flush()
        deadline = time.monotonic()+15
        while True:
            ready, _, _ = select.select([mcp.stdout], [], [], max(0, deadline-time.monotonic()))
            assert ready, 'recall coverage MCP deadline'
            line = mcp.stdout.readline(); assert line, 'MCP ended'
            response = json.loads(line)
            with (report/'coverage-mcp.jsonl').open('a') as log:
                log.write(json.dumps({'request':request, 'response':response})+'\n')
            if response.get('id')==mid:
                return response

    def answers(label, expected_complete, names, corrected=True):
        answer = cli('recall', 'last', expected=0 if expected_complete else 5)
        tool = mcp_call('tools/call', {'name':'sinex_recall', 'arguments':{}})['result']
        assert not tool.get('isError'), tool
        agent = tool.get('structuredContent') or json.loads(tool['content'][0]['text'])
        assert agent==answer
        assert answer['complete'] is expected_complete and answer['truncated'] is False
        assert sorted(row['payload']['command'] for row in answer['rows'])==sorted(names)
        assert set(answer['session']['parents'])=={row['id'] for row in answer['rows']}
        assert answer['coverage'].startswith('unknown;')
        indexed = cli('query', 'commands sort when limit 10000', expected=0 if expected_complete and corrected else 5)
        assert indexed['complete'] is (expected_complete if corrected else False)
        raw = cli('query', 'events')
        assert raw['complete'] is all(s.get('retained_record_errors', 0)==0 for s in raw['source_status'])
        if corrected:
            assert answer['source_status']==indexed['source_status']==raw['source_status']
        save(label+'.json', dict(recall=answer, indexed=indexed, raw=raw, sources=cli('sources')))
        return answer

    initial_names = ['coverage valid atuin', 'coverage valid zsh']

    def old_initial():
        nonlocal mcp
        sources = launch('before-current-error', before/'bin/sinexd')
        assert next(s for s in sources if s['source']=='terminal.atuin-history')['last_run']['record_errors']==1
        err = (report/'coverage-mcp.stderr').open('w')
        argv = [str(bins/'sinexctl'), '--endpoint', endpoint, '--token-file', str(work/'token'), 'mcp']
        mcp = subprocess.Popen(argv, env=h['ENV'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=err, text=True, bufsize=1)
        h['CHILDREN'].append((mcp, mcp.stdout, err)); h['PROCESSES'].append({'pid':mcp.pid, 'argv':argv, 'stderr':'coverage-mcp.stderr'})
        assert 'result' in mcp_call('initialize', {'protocolVersion':'2025-03-26', 'capabilities':{}, 'clientInfo':{'name':'recall-coverage', 'version':'1'}})
        mcp.stdin.write(json.dumps({'jsonrpc':'2.0', 'method':'notifications/initialized'})+'\n'); mcp.stdin.flush()
        answers('before-current-error', True, initial_names, corrected=False)
        stop(daemon)
    scenario(PLANNED[0], old_initial)

    def current_error():
        launch('corrected-current-error')
        answer = answers('corrected-current-error', False, initial_names)
        source = next(s for s in answer['source_status'] if s['source']=='terminal.atuin-history')
        assert source['status']=='completed' and source['record_errors']==1 and source['retained_record_errors']==0
        stop(daemon)
    scenario(PLANNED[1], current_error)

    def clean():
        with sqlite3.connect(path) as con:
            con.execute("DELETE FROM history WHERE id='malformed'")
        launch('corrected-clean')
        answer = answers('corrected-clean', True, initial_names)
        assert all(s['record_errors']==s['retained_record_errors']==0 for s in answer['source_status'])
        save('coverage-exact-trace.json', cli('trace', answer['session']['id'], '--bytes', expected=0))
        stop(daemon)
    scenario(PLANNED[2], clean)

    all_names = initial_names+['coverage valid after malformed']
    def malformed_append():
        with zsh.open('ab') as out:
            out.write(f'plain unsupported record\n: {epoch+40}:1;coverage valid after malformed\n'.encode())
        launch('corrected-zsh-malformed')
        answer = answers('corrected-zsh-malformed', False, all_names)
        source = next(s for s in answer['source_status'] if s['source']=='terminal.zsh-history')
        assert source['record_errors']==source['retained_record_errors']==1
        stop(daemon)
        launch('corrected-zsh-later-clean')
        answer = answers('corrected-zsh-later-clean', False, all_names)
        source = next(s for s in answer['source_status'] if s['source']=='terminal.zsh-history')
        assert source['status']=='completed' and source['record_errors']==0 and source['retained_record_errors']==1
        stop(daemon)
    scenario(PLANNED[3], malformed_append)

    def retained_contrast():
        launch('before-retained-error', before/'bin/sinexd')
        answers('before-retained-error', True, all_names, corrected=False)
        stop(daemon)
        launch('corrected-retained-restart')
        answer = answers('corrected-retained-restart', False, all_names)
        assert all(s['status']=='completed' and s['record_errors']==0 for s in answer['source_status'])
        assert next(s for s in answer['source_status'] if s['source']=='terminal.zsh-history')['retained_record_errors']==1
        stop(daemon)
    scenario(PLANNED[4], retained_contrast)

    def typed_pending():
        nonlocal daemon
        # A supported project change asks the actual projection owner to rebuild.
        # Its existing rendezvous holds an uncommitted projection step, without
        # modifying progress or depending on a race to acquire a database lock.
        config.write_text(config.read_text()+'\n[[projects]]\nname = "coverage"\nroots = ["/synthetic/coverage"]\n')
        boundary = work/'recall-projection.sock'
        with socket.socket(socket.AF_UNIX) as controller:
            controller.bind(str(boundary)); controller.listen(1); controller.settimeout(20)
            daemon = start(config, 'corrected-typed-pending', '--test-pause-at', 'projection-staged-0', '--test-pause-socket', boundary)
            conn, _ = controller.accept()
            with conn:
                frame = json.loads(conn.makefile('r').readline())
                assert frame['pid']==daemon.pid and frame['evidence']['through']>frame['evidence']['from']
                deadline = time.monotonic()+20
                while True:
                    try:
                        state = h['rpc'](endpoint, 'synthetic-foundation-token')['result']['indexed_retrieval']
                        if state['pending_sessions']==state['pending_canonical']==0 and state['serving'] is False:
                            break
                    except (OSError, KeyError, ValueError):
                        pass
                    assert time.monotonic()<deadline, 'pending projection RPC readiness'
                    time.sleep(.05)
                began = time.monotonic()
                pending = run([bins/'sinexctl', '--endpoint', endpoint, '--token-file', work/'token', 'recall', 'last', '--json'], expected=5, label='typed pending recall refuses after bounded settling')
                elapsed = time.monotonic()-began
                fault = json.loads(pending.stderr)
                assert fault['kind']=='partial' and fault['details']['reason']=='projection_unavailable'
                assert elapsed>=.45, 'projection pending skipped the existing 500ms settling window'
                save('typed-projection-pending.json', dict(fault=fault, seconds=elapsed, boundary=frame, status=state))
                conn.sendall(b'c')
        settled('projection released')
        answers('typed-projection-recovered', False, all_names)
        stop(daemon)
        config.write_text(config.read_text().replace('automata = ["session"]', 'automata = []'))
        launch('corrected-disabled')
        refused = run([bins/'sinexctl', '--endpoint', endpoint, '--token-file', work/'token', 'recall', 'last', '--json'], expected=5, label='disabled capability remains a permanent partial refusal')
        fault = json.loads(refused.stderr)
        assert fault['kind']=='partial' and fault['details']['reason']=='projection_unavailable'
        save('typed-disabled-refusal.json', fault)
        stop(daemon)
    scenario(PLANNED[5], typed_pending)
