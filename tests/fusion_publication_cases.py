"""Actual admitted source rows, native turn rollback, then public restart evidence."""
import json
import socket
import sqlite3
import time

PLANNED = [
    'mixed native publication turn cancels atomically and reuses committed identities',
    'source-hidden process restart keeps exact canonical payload and retained provenance',
]


def exercise(h):
    work, report, env, bins = (h[k] for k in ['WORK', 'REPORT', 'ENV', 'BIN_DIR'])
    run, scenario, start, stop = (h[k] for k in ['run', 'scenario', 'start_daemon', 'stop'])
    run(['createdb', 'shell_publication'], label='owned publication transaction database')
    urls = {key: h[key].replace('/foundation?', '/shell_publication?') for key in ['admin', 'writer', 'reader']}
    urls['read-model'] = urls['reader'].replace('sinex_reader', 'sinex_read_model')
    for key, value in urls.items():
        path = work/('turn-'+key); path.write_text(value); path.chmod(0o600)
    run([bins/'sinexd', 'admin', 'schema', '--connection-file', work/'turn-admin'], label='publication fixture canonical schema')
    source = work/'publication-history.db'
    columns = ['id', 'timestamp', 'duration', 'exit', 'command', 'cwd', 'session', 'hostname', 'deleted_at']
    facts = [(f'turn-{n}', 1_700_000_000_000_000_000+n, 9, n%2, f'echo publication {n}', '/synthetic/harbor', 'synthetic-session', 'fixture-host', None) for n in range(6)]
    with sqlite3.connect(source) as db:
        db.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER NOT NULL,duration INTEGER NOT NULL,exit INTEGER NOT NULL,command TEXT NOT NULL,cwd TEXT NOT NULL,session TEXT NOT NULL,hostname TEXT NOT NULL,deleted_at INTEGER)')
        db.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)', facts)
    port = h['free_port'](); endpoint = f'http://127.0.0.1:{port}/rpc'
    (work/'turn-token').write_text('synthetic-publication-token')
    config = work/'publication.toml'; daemon = None; proof = None; admitted = {}
    def configure(acquire):
        config.write_text(f'''listen="127.0.0.1:{port}"
state_dir="{work/'turn-state'}"
store_dir="{work/'turn-store'}"
writer_connection_file="{work/'turn-writer'}"
reader_connection_file="{work/'turn-reader'}"
token_file="{work/'turn-token'}"
sources={json.dumps(['terminal.atuin-history'] if acquire else [])}
automata=[]
'''+('' if acquire else f'read_model_connection_file="{work/"turn-read-model"}"\n')+(f'''[atuin]
path="{source}"
poll_interval_ms=3600000
''' if acquire else ''))
    def cli(command, *args, expected=0):
        result = run([bins/'sinexctl', '--endpoint', endpoint, '--token-file', work/'turn-token', command, *args, '--json'], expected=(expected,), label='publication public '+command)
        return json.loads(result.stdout or result.stderr)
    def wait(predicate, seconds=45):
        deadline = time.monotonic()+seconds
        while time.monotonic()<deadline:
            assert daemon.poll() is None, 'publication daemon exited; see retained stderr'
            try:
                value = predicate()
                if value:return value
            except (OSError, ValueError):pass
            time.sleep(.025)
        raise AssertionError('publication public state did not settle within fixture bound')
    def mixed():
        nonlocal daemon, proof, admitted
        configure(True); daemon = start(config, 'publication-source-admission')
        wait(lambda: 'result' in h['rpc'](endpoint, 'synthetic-publication-token'))
        wait(lambda: (s:=cli('sources'))['sources'] and s['sources'][0]['last_run'] and s['sources'][0]['last_run']['status']=='completed')
        captured = cli('query', 'events where source = terminal.atuin-history and kind = command.executed')['rows']
        assert len(captured)==6
        admitted = {row['id']:row for row in captured}
        stop(daemon); daemon = None
        isolated = env.copy(); isolated.update(SINEX_TEST_ADMIN_URL=urls['admin'], SINEX_TEST_WRITER_URL=urls['writer'])
        result = run(['cargo', 'test', '--locked', '-p', 'sinex-store', '--features', 'product', '--lib', 'shell::publication_tests::bounded_turn_rolls_back_mixed_groups_after_middle_statement_cancellation_and_restarts', '--', '--ignored', '--nocapture'], env=isolated, label='native mixed turn mid-insert cancellation, rollback and reconnect')
        lines = [x.removeprefix('PUBLICATION_TURN_PROOF ') for x in result.stdout.splitlines() if x.startswith('PUBLICATION_TURN_PROOF ')]
        assert len(lines)==1
        proof = json.loads(lines[0]); assert proof['whole_turn_rollback'] and proof['restart_stable'] and proof['mixed']=={'skip':1,'reuse':1,'new':4}
        (report/'publication-native-proof.json').write_text(json.dumps(proof, indent=2)+'\n')
    def restart():
        nonlocal daemon
        source.rename(work/'publication-provider-unavailable.db')
        configure(False); daemon = start(config, 'publication-source-hidden-restart')
        wait(lambda: 'result' in h['rpc'](endpoint, 'synthetic-publication-token'))
        def ready():
            status = cli('status')['indexed_retrieval']
            return status.get('serving') and status.get('pending_canonical')==0 and status['fusion']['phase']=='settled' and status['position']==cli('status')['commit_seq']
        wait(ready, seconds=90)
        actual = cli('query', 'commands'); rows = actual['rows']
        assert len(rows)==5 and {r['id'] for r in rows}==set(proof['canonical_ids'])
        before = {e['id']:e for e in proof['canonical']}
        navigation = []
        for row in rows:
            event = before[row['id']]; assert row['payload']==event['payload'] and row['parents']==event['source_event_ids']
            shown = cli('show', 'sinex:ev/'+row['id']); traced = cli('trace', 'sinex:ev/'+row['id'], '--bytes')
            supplier = admitted[row['parents'][0]]
            for field in ['when', 'time_quality', 'time_precision', 'time_clock', 'time_until', 'time_suspect']:
                assert row[field]==supplier[field], field
            parent = traced['row']['parents']; assert len(parent)==1 and parent[0]['event']==row['parents'][0]
            record = parent[0]['record']; record = json.loads(record) if isinstance(record,str) else record
            native = row['payload']['native_id']; expected = dict(zip(columns, facts[int(native.removeprefix('turn-'))]))
            assert {key:record[key] for key in columns}==expected
            assert row['payload']['field_evidence']['time']==row['parents'][0] and shown['row']['payload']==row['payload']
            navigation.append({'id':row['id'],'show':shown,'trace':traced})
        (report/'publication-public-restart.json').write_text(json.dumps({'query':actual,'navigation':navigation,'provider_absent':True}, indent=2)+'\n')
    try:
        scenario(PLANNED[0], mixed)
        scenario(PLANNED[1], restart)
    finally:
        if daemon is not None and daemon.poll() is None:stop(daemon)
