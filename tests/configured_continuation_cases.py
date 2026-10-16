"""Finite configured Atuin continuation through the existing packaged clients."""
import hashlib
import json
from pathlib import Path
import select
import sqlite3
import subprocess
import time
import urllib.error
import urllib.request
import uuid

PLANNED = [
    'configured completed-empty source and public discovery',
    'same configured collector acquires three later producer batches',
    'autonomous repeat passes preserve interpretation identities',
    'same-state restart catches up offline activity and retains citations',
    'unavailable provider preserves qualified retained answers',
]
COLUMNS = ['id', 'timestamp', 'duration', 'exit', 'command', 'cwd', 'session', 'hostname', 'deleted_at']
SIZES = [96, 160, 128, 128]
EPOCH = 1_792_141_200_123_456_789
PROJECT = '/synthetic/continuation/tide'


def expected_batch(batch, start, count):
    """Authored provider facts, without invoking a product parser or query binder."""
    rows = []
    for offset in range(count):
        n = start + offset
        command = f'cargo check continuation batch-{batch} item-{offset:03}'
        code, duration = (101 if n % 17 == 0 else 0), 2_000_000_000 + n
        if offset == 1:
            command, code, duration = 'sleep continuation unknown-end', -1, -1
        elif offset == 2:
            command, code, duration = 'synthetic continuation ended-unknown-exit', -1, 0
        elif offset == 3:
            command, code = 'cargo test continuation | tee synthetic-output', 0
        elif offset == 4:
            command, code = 'agentctl job start synthetic continuation', 0
        elif offset in (5, 6):
            command = 'cargo check continuation repeated-command'
        rows.append(dict(zip(COLUMNS, [f'continuation-{n:04}', EPOCH+n*1_000_000_000,
            duration, code, command, PROJECT if n % 8 else PROJECT+'-neighbor',
            'synthetic-continuation-session', 'synthetic-continuation-host:user', None])))
    return rows


def exercise(h):
    work, report, env, bins = (h[k] for k in ['WORK', 'REPORT', 'ENV', 'BIN_DIR'])
    run, scenario, start, stop = (h[k] for k in ['run', 'scenario', 'start_daemon', 'stop'])
    endpoint = f'http://127.0.0.1:{h["free_port"]()}/rpc'
    source, config, store = work/'continuation.db', work/'continuation.toml', work/'continuation-blobs'
    inputs = report/'inputs'; inputs.mkdir()
    token = (work/'token').read_text()
    facts, snapshots, measurements, interventions = [], {}, [], []
    daemon = mcp = None
    mcp_id = 0
    materials = {}
    (work/'read-model').write_text(h['reader'].replace('sinex_reader', 'sinex_read_model'))
    (work/'read-model').chmod(0o600)
    con = sqlite3.connect(source)
    try:
        assert con.execute('PRAGMA journal_mode=WAL').fetchone()[0] == 'wal'
        con.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER NOT NULL,duration INTEGER NOT NULL,exit INTEGER NOT NULL,command TEXT NOT NULL,cwd TEXT NOT NULL,session TEXT NOT NULL,hostname TEXT NOT NULL,deleted_at INTEGER)')
        con.commit()
    finally: con.close()
    config.write_text(f'''listen = "{endpoint.removeprefix('http://').removesuffix('/rpc')}"
state_dir = "{work/'continuation-state'}"
store_dir = "{store}"
writer_connection_file = "{work/'writer'}"
reader_connection_file = "{work/'reader'}"
read_model_connection_file = "{work/'read-model'}"
token_file = "{work/'token'}"
sources = ["terminal.atuin-history"]
automata = []
[[projects]]
name = "tide"
roots = ["{PROJECT}"]
[atuin]
path = "{source}"
poll_interval_ms = 1000
''')
    config_hash = hashlib.sha256(config.read_bytes()).hexdigest()
    domain = {'source': 'one configured synthetic Atuin SQLite database', 'schema_columns': COLUMNS,
        'journal': 'WAL before native readers', 'batch_sizes': SIZES, 'total_rows': sum(SIZES),
        'project_root': PROJECT, 'neighbor_fraction': 'every eighth record is outside the exact project root',
        'cadence_ms': 1000, 'mutation': 'append-only row inserts; no per-batch import/configuration',
        'source_time_start_ns': EPOCH, 'source_time_end_ns': EPOCH+(sum(SIZES)-1)*1_000_000_000,
        'qualification_bound': 'finite 512 records, not a source capacity limit',
        'unknowns': ['complete capture', 'underlying check or queued-job result', 'causality', 'daily scalability'],
        'reused_evidence': ['real-history10235 and corrected equal Atuin baseline', 'joint10252/nonmodel10256',
            'schema10065', 'fusion10170/10176 and earlier split journey bindings'],
        'interventions': interventions, 'measurements': measurements}

    def save(name, value):
        (report/name).write_text(json.dumps(value, indent=2)+'\n')

    def api(method, params=None):
        request = {'jsonrpc':'2.0', 'id':1, 'method':method, 'params':params or {}}
        call = urllib.request.Request(endpoint, data=json.dumps(request).encode(),
            headers={'Authorization':'Bearer '+token, 'Content-Type':'application/json'})
        try:
            with urllib.request.urlopen(call, timeout=15) as reply: value = json.load(reply)
        except urllib.error.HTTPError as error:
            value = json.load(error)
        with (report/'continuation-api.jsonl').open('a') as log:
            log.write(json.dumps({'request':request, 'response':value})+'\n')
        return value

    def wait(label, predicate, seconds=180):
        began = time.monotonic(); progress = []; latest = None
        while time.monotonic()-began < seconds:
            assert daemon.poll() is None, 'owned collector exited; see daemon capture'
            try:
                latest = api('status').get('result')
                if latest:
                    index = latest['indexed_retrieval']
                    step = {k:index.get(k) for k in ['position', 'pending_canonical', 'fusion', 'failure']}
                    step['commit_seq'] = latest['commit_seq']
                    if not progress or step != progress[-1]: progress.append(step)
                    value = predicate(latest)
                    if value:
                        save('last-progress.json', {'phase':label, 'elapsed_seconds':time.monotonic()-began,
                            'progress':progress, 'latest':latest})
                        return value
            except (OSError, ValueError):
                pass
            time.sleep(.1)
        save('continuation-watchdog.json', {'phase':label, 'seconds':seconds, 'progress':progress, 'latest':latest})
        raise AssertionError(f'{label}: maintenance watchdog, not a performance threshold; see captured progress')

    def source_state():
        sources = api('sources.list').get('result', {}).get('sources', [])
        return next((s for s in sources if s['locator'] == str(source)), None)

    def settled(status):
        index = status['indexed_retrieval']
        return (index['serving'] and index['position'] == status['commit_seq']
            and index['pending_canonical'] == 0 and index['failure'] is None
            and index['fusion']['phase'] == 'settled' and index['fusion']['failure'] is None)

    def acquired(status):
        current = source_state()
        if not current or not current['last_run']: return False
        last = current['last_run']
        return current if (last['status'] == 'completed' and last['rows_read'] == len(facts)
            and last['record_errors'] == 0 and settled(status)) else False

    def cli(command, *args, expected=(0, 5)):
        result = run([bins/'sinexctl', '--endpoint', endpoint, '--token-file', work/'token',
            command, *args, '--json'], expected=expected, label='configured continuation CLI '+command)
        return json.loads(result.stdout or result.stderr)

    def mcp_call(method, params):
        nonlocal mcp_id
        mcp_id += 1
        request = {'jsonrpc':'2.0', 'id':mcp_id, 'method':method, 'params':params}
        mcp.stdin.write(json.dumps(request)+'\n'); mcp.stdin.flush()
        with (report/'continuation-mcp.jsonl').open('a') as log:
            log.write(json.dumps({'sent':request})+'\n')
            until = time.monotonic()+20
            while True:
                ready, _, _ = select.select([mcp.stdout], [], [], max(0, until-time.monotonic()))
                assert ready, 'ordinary rmcp response deadline exceeded'
                line = mcp.stdout.readline(); assert line, 'ordinary rmcp transport ended'
                reply = json.loads(line); log.write(json.dumps({'received':reply})+'\n'); log.flush()
                if reply.get('id') == request['id']: return reply

    def tool(name, arguments):
        reply = mcp_call('tools/call', {'name':name, 'arguments':arguments})
        result = reply['result']; assert not result.get('isError', False), reply
        return result.get('structuredContent') or json.loads(result['content'][0]['text'])

    def discover():
        nonlocal mcp
        cli('catalog', expected=0)
        err = (report/'continuation-mcp.stderr').open('w')
        args = [str(bins/'sinexctl'), '--endpoint', endpoint, '--token-file', str(work/'token'), 'mcp']
        mcp = subprocess.Popen(args, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=err, text=True, bufsize=1)
        h['CHILDREN'].append((mcp, mcp.stdout, err))
        h['PROCESSES'].append({'pid':mcp.pid, 'argv':args, 'stderr':'continuation-mcp.stderr'})
        assert 'result' in mcp_call('initialize', {'protocolVersion':'2025-03-26', 'capabilities':{},
            'clientInfo':{'name':'configured-continuation', 'version':'1'}})
        mcp.stdin.write(json.dumps({'jsonrpc':'2.0', 'method':'notifications/initialized'})+'\n'); mcp.stdin.flush()
        tools = mcp_call('tools/list', {})['result']['tools']
        assert {t['name'] for t in tools} == {'sinex_search', 'sinex_query', 'sinex_recall', 'sinex_show',
            'sinex_trace', 'sinex_sources', 'sinex_status'}
        save('discovered-tools.json', tools)

    def search(route):
        rows = []; cursor = None; pages = []
        while True:
            if route == 'CLI':
                args = ['continuation', '--query', 'and project = tide', '--page', '100']
                if cursor: args += ['--cursor', cursor]
                answer = cli('search', *args)
            else:
                args = {'words':'continuation', 'query':'and project = tide', 'page':100}
                if cursor: args['cursor'] = cursor
                answer = tool('sinex_search', args)
            rows += answer['rows']; pages.append({k:v for k,v in answer.items() if k != 'rows'})
            cursor = answer['page'].get('cursor')
            if not cursor: return rows, pages

    def check_rows(rows):
        expected = {f['id']:f for f in facts if f['cwd'] == PROJECT}
        assert len(rows) == len(expected) and {r['payload']['native_id'] for r in rows} == set(expected)
        assert len({r['id'] for r in rows}) == len(rows)
        for row in rows:
            fact = expected[row['payload']['native_id']]; payload = row['payload']
            assert row['kind'] == 'command.canonical' and row['origin'] == 'derived'
            assert row['project'] == 'tide' and len(row['parents']) == 1
            assert payload['command'] == fact['command'] and payload['cwd'] == fact['cwd']
            assert payload['timestamp_ns'] == fact['timestamp']
            assert payload['exit_code'] == (fact['exit'] if fact['exit'] >= 0 else None)
            assert payload['duration_ns'] == (fact['duration'] if fact['duration'] >= 0 else None)
            assert payload['completion'] == ('end_not_recorded' if fact['duration'] < 0 and fact['exit'] < 0 else 'ended')
            assert payload['exact_execution_count'] is None
            assert row['snippet']['text'], 'ranked snippet missing'

    def observe(label):
        shell, shell_pages = search('CLI'); agent, agent_pages = search('MCP')
        check_rows(shell); check_rows(agent); assert shell == agent
        assert all(p['total'] == len(shell) and not p['truncated'] for p in shell_pages+agent_pages)
        value = {'CLI':shell, 'MCP':agent, 'CLI_pages':shell_pages, 'MCP_pages':agent_pages}
        save(label+'-clients.json', value)
        snapshots[label] = {r['payload']['native_id']:r for r in shell}
        return shell

    def storage(label):
        began = time.monotonic()
        sql = "SELECT jsonb_build_object('database_bytes',pg_database_size(current_database()),'events',(SELECT count(*) FROM core.events),'live_raw',(SELECT count(*) FROM core.live_events WHERE source='terminal.atuin-history'),'live_canonical',(SELECT count(*) FROM core.live_events WHERE event_type='command.canonical'),'materials',(SELECT count(*) FROM raw.source_material_registry),'blob_rows',(SELECT count(*) FROM core.blobs),'blob_logical_bytes',(SELECT coalesce(sum(size_bytes),0) FROM core.blobs),'source_passes',(SELECT count(*) FROM raw.observations),'completed_passes',(SELECT count(*) FROM raw.observations WHERE status='completed'),'source_rows_read_total',(SELECT coalesce(sum(rows_read),0) FROM raw.observations),'source_repeats_total',(SELECT coalesce(sum(repeated),0) FROM raw.observations))"
        observed = json.loads(run(['psql', h['reader'], '-XAt', '-v', 'ON_ERROR_STOP=1', '-c', sql],
            label='read-only finite storage and source-pass accounting').stdout)
        for name, path in [('retained_store', store), ('owned_pg_directory', work/'pg')]:
            files = [p.stat() for p in path.rglob('*') if p.is_file()] if path.exists() else []
            observed[name] = {'files':len(files), 'logical_bytes':sum(s.st_size for s in files),
                'allocated_bytes':sum(s.st_blocks*512 for s in files)}
        observed.update(phase=label, source_rows=len(facts), project_rows=sum(f['cwd']==PROJECT for f in facts),
            filesystem_sampling_seconds=time.monotonic()-began,
            sampling='read-only DB snapshot then filesystem sizes, not one atomic physical snapshot')
        measurements.append(observed); save('measurements.json', domain)
        return observed

    def append(batch):
        additions = expected_batch(batch, len(facts), SIZES[batch]); began = time.monotonic()
        con = sqlite3.connect(source)
        try:
            con.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)', [[f[k] for k in COLUMNS] for f in additions])
            con.commit()
        finally: con.close()
        committed = time.monotonic(); facts.extend(additions)
        save('independent-source-facts.json', {'rows':facts, 'scope':'synthetic recorded shell outcomes; no project checks executed or result output supplied'})
        original = sqlite3.connect(source); destination = sqlite3.connect(inputs/f'batch-{batch}.db')
        try: original.backup(destination)
        finally: original.close(); destination.close()
        interventions.append({'kind':'synthetic producer commit', 'batch':batch, 'added':len(additions),
            'source_path':str(source), 'commit_seconds':committed-began, 'collector_was_running':daemon is not None and daemon.poll() is None})
        return committed

    def material_bytes(material):
        if material in materials: return materials[material]
        # A validated native UUID selects one retained descriptor. No product query
        # or fixture oracle is substituted for the actual addressed source bytes.
        identity = str(uuid.UUID(material))
        statement = "SELECT jsonb_build_object('hash',b.blake3,'bytes',b.size_bytes) FROM raw.source_material_registry m JOIN core.blobs b ON b.id=m.blob_id WHERE m.id='"+identity+"'"
        info = json.loads(run(['psql',h['reader'],'-XAt','-v','ON_ERROR_STOP=1','-c',statement],
            label='independent cited material descriptor').stdout)
        digest = info['hash'].removeprefix('blake3:')
        path = store/'blobs'/digest[:2]/(digest+'.zst')
        argv = ['zstd','-dc',str(path)]
        with h['COMMAND_LOCK']:
            index = len(h['COMMANDS'])
            h['COMMANDS'].append({'argv':argv,'exit':None,'expected_exit':0,
                'label':'independent cited logical material decompression',
                'stdout_encoding':'binary','stdout_file':f'{index:03}.stdout.bin'})
        result = subprocess.run(argv,env=env,capture_output=True,timeout=30)
        (report/f'{index:03}.stdout.bin').write_bytes(result.stdout)
        (report/f'{index:03}.stderr').write_bytes(result.stderr)
        h['COMMANDS'][index]['exit'] = result.returncode
        assert result.returncode == 0 and len(result.stdout) == info['bytes']
        materials[material] = result.stdout
        return result.stdout

    def citations(label, rows, references=None):
        # References come only from actual public discovery results, never fixture IDs.
        chosen = [next(r for r in rows if r['payload']['exit_code'] == 101),
            next(r for r in rows if r['payload']['command'] == 'cargo test continuation | tee synthetic-output'),
            next(r for r in rows if r['payload']['completion'] == 'end_not_recorded')]
        if references:
            chosen = [next(r for r in rows if r['id'] == ref) for ref in references]
        result = []
        for row in chosen:
            ref = row['id']; shown = cli('show', ref)['row']; traced = cli('trace', ref, '--bytes')['row']
            assert tool('sinex_show', {'ref':ref})['row'] == shown
            metadata = tool('sinex_trace', {'ref':ref})['row']
            assert {p['event'] for p in metadata['parents']} == set(row['parents'])
            assert len(traced['parents']) == 1
            parent = traced['parents'][0]; fact = next(f for f in facts if f['id'] == row['payload']['native_id'])
            assert parent['event'] == row['parents'][0] and parent['material'] and parent['end'] > parent['anchor']
            assert parent['representation'] == 'canonical_sqlite_row'
            original = material_bytes(parent['material'])
            assert 0 <= parent['anchor'] < parent['end'] <= len(original)
            addressed = original[parent['anchor']:parent['end']]
            decoded = json.loads(addressed)
            assert decoded == parent['record']
            for key in COLUMNS: assert decoded[key] == fact[key]
            result.append({'canonical':ref, 'shown':shown, 'trace':traced, 'MCP_metadata':metadata,
                'addressed_record_sha256':hashlib.sha256(addressed).hexdigest(),
                'outcome_scope':'recorded shell exit only; pipeline/queued invocation outcome remains unknown without separate result evidence'})
        save(label+'-citations.json', result)
        return result

    def empty():
        nonlocal daemon
        daemon = start(config, 'configured-collector')
        current = wait('initial configured empty', acquired)
        discover()
        def public_empty(_):
            raw = cli('query', 'events where source = terminal.atuin-history and kind = command.executed')
            agent = tool('sinex_query', {'query':'events where source = terminal.atuin-history and kind = command.executed'})
            assert raw['total'] == agent['total'] == 0
            return (raw, agent) if raw['complete'] and agent['complete'] else False
        raw, empty_mcp = wait('public completed-empty observation', public_empty)
        assert current['last_run']['rows_read'] == 0 and current['last_run']['status'] == 'completed'
        save('completed-empty.json', {'source':current, 'CLI':raw, 'MCP':empty_mcp})
        observe('empty'); storage('empty')
    scenario(PLANNED[0], empty)

    def later():
        previous = {}
        for batch in range(3):
            committed = append(batch); current = wait(f'batch-{batch} acquisition/projection', acquired)
            rows = observe(f'batch-{batch}')
            queried = time.monotonic()
            newest = max(rows, key=lambda r:r['payload']['timestamp_ns'])
            citations(f'batch-{batch}-newest', rows, [newest['id']])
            assert all(snapshots[f'batch-{batch}'][k]['id'] == v['id'] for k,v in previous.items())
            previous = snapshots[f'batch-{batch}']
            measured = storage(f'batch-{batch}')
            measured['producer_commit_to_query_seconds'] = queried-committed
            measured['source_observation'] = current['last_run']['id']
        save('before-restart-citations.json', citations('before-restart', rows))
        save('measurements.json', domain)
    scenario(PLANNED[1], later)

    def repeats():
        old = source_state()['last_run']['id']
        observations = []
        for _ in range(2):
            def repeated(status):
                current = source_state(); last = current['last_run']
                return current if (last['id'] != old and last['status'] == 'completed'
                    and last['repeated'] == len(facts) and last['admitted'] == 0 and settled(status)) else False
            current = wait('autonomous repeat source pass', repeated)
            old = current['last_run']['id']; observations.append(current)
        rows = observe('repeated')
        assert {r['id'] for r in rows} == {r['id'] for r in snapshots['batch-2'].values()}
        save('repeat-observations.json', observations); storage('repeated')
    scenario(PLANNED[2], repeats)

    def restart():
        nonlocal daemon
        old_input = source_state()['input']; old = snapshots['repeated']
        stop(daemon); interventions.append({'kind':'prescribed graceful restart', 'old_pid':daemon.pid, 'exit':daemon.returncode})
        committed = append(3)
        began = time.monotonic(); daemon = start(config, 'same-config-catchup')
        current = wait('same-state offline catch-up', acquired)
        rows = observe('restarted')
        queried = time.monotonic()
        newest = max(rows, key=lambda r:r['payload']['timestamp_ns'])
        citations('catchup-newest', rows, [newest['id']])
        assert current['input'] == old_input
        assert all(snapshots['restarted'][k]['id'] == v['id'] for k,v in old.items())
        assert hashlib.sha256(config.read_bytes()).hexdigest() == config_hash
        # The continuing MCP process keeps the same endpoint; it does not reacquire a database.
        before = json.loads((report/'before-restart-citations.json').read_text())
        after = citations('after-restart', rows, [b['canonical'] for b in before])
        assert after == before, 'same retained citation changed over compatible restart/catchup'
        measured = storage('restarted')
        measured.update(restart_to_query_seconds=queried-began,
            offline_commit_to_query_seconds=queried-committed)
        save('measurements.json', domain)
    scenario(PLANNED[3], restart)

    def unavailable():
        held = work/'producer-now-unavailable.db'
        source.rename(held); interventions.append({'kind':'prescribed provider-path unavailability', 'path':str(source), 'owned_held_copy':str(held)})
        def absent(_):
            current = source_state()
            return current if (current['last_run']['status'] == 'unavailable'
                and 'read-only open failed' in current['last_run']['reason']) else False
        current = wait('configured unavailable source', absent)
        assert not source.exists() and current['last_run']['rows_read'] == 0
        raw = cli('query', 'events where source = terminal.atuin-history and kind = command.executed', expected=5)
        assert raw['total'] == len(facts) and raw['complete'] is False
        assert tool('sinex_sources', {})['sources'][0]['last_run']['status'] == 'unavailable'
        rows = observe('unavailable')
        assert all(r['complete'] is False for r in json.loads((report/'unavailable-clients.json').read_text())['CLI_pages'])
        before = json.loads((report/'before-restart-citations.json').read_text())
        after = citations('after-unavailable', rows, [b['canonical'] for b in before])
        assert after == before
        save('unavailable.json', {'source':current, 'raw_query':raw, 'difference_from_empty':'retained512 records, completefalse, explicit missing-path source; initial empty completed0 was completetrue'})
        storage('unavailable')
        mcp.stdin.close(); mcp.wait(timeout=15); assert mcp.returncode == 0
        stop(daemon)
        save('lifecycle.json', {'daemon_exit':daemon.returncode, 'MCP_exit':mcp.returncode,
            'config_sha256_unchanged':config_hash, 'unplanned_interventions':[],
            'scope':'finite supported synthetic continuation; no product checks, activation, private input or broad milestone claim'})
    scenario(PLANNED[4], unavailable)
    table = ['| Phase | Source/project rows | Commit or restart to query (s) | Retained store bytes | Database bytes | Completed passes |',
        '|---|---:|---:|---:|---:|---:|']
    for value in measurements:
        elapsed = value.get('producer_commit_to_query_seconds', value.get('restart_to_query_seconds'))
        table.append(f"| {value['phase']} | {value['source_rows']}/{value['project_rows']} | "
            f"{elapsed if elapsed is not None else 'not a timed arrival'} | "
            f"{value['retained_store']['logical_bytes']} | {value['database_bytes']} | {value['completed_passes']} |")
    (report/'continuation-report.md').write_text('''# Configured Atuin continuation

One configured collector acquired three later batches in the same synthetic SQLite database, observed autonomous repeat passes, then caught up a fourth batch after a same-state restart. Ordinary project-scoped CLI search and advertised MCP tools returned independently checked source facts and identical event IDs. Selected canonical citations resolve exact singleton Atuin parents; compatible restart and later provider-path unavailability preserve them. Initial completed-empty evidence differs from unavailable retained answers.

'''+ '\n'.join(table)+'''

Timings start at the synthetic producer commit or daemon restart and end after both ordinary client traversals. They include polling, writer, canonical maintenance, projection and query work. Storage samples are a DB snapshot followed by filesystem sizes, not one atomic physical measurement. This is a 512-row finite workload with 448 selected project rows, neighboring-path controls, repeated command text, unknown end/exit and zero-exit pipeline/submission records. A command exit is not a check or queued-job result. The driver executes no project checks and supplies no separate result output.

The collector configuration, provider path, state and store are unchanged across all producer batches and restart. Only producer appends, one prescribed restart and one prescribed provider-path outage occur. The 180-second maintenance watchdog is a finite completion guard, not a performance acceptance target. Full-snapshot Atuin rereads and whole-input shell reconciliation remain explicit costs. No daily scalability, productivity benefit, source completeness, deployment or full milestone claim follows.

See [measurements and interventions](measurements.json), [independent source facts](independent-source-facts.json), [MCP discovery](discovered-tools.json), [actual MCP wire calls](continuation-mcp.jsonl), [actual API calls](continuation-api.jsonl), [first later batch parent](batch-0-newest-citations.json), [second later batch parent](batch-1-newest-citations.json), [third later batch parent](batch-2-newest-citations.json), [new catch-up parent](catchup-newest-citations.json), [unchanged restart citations](after-restart-citations.json), [unavailable retained citations](after-unavailable-citations.json), [lifecycle](lifecycle.json) and the enclosing [source/binary/command/cleanup result](result.json). Package build source and qualification harness identities remain distinct in [the retained pin](prebuilt-package-pin.json). Earlier real-history, combined-investigation, schema and split fusion qualifications retain their original bindings and were not rerun here.
''')
