"""First-read coverage through packaged clients and an owned SQLite exclusive lock."""
import json
import sqlite3
import time


def exercise(h):
    work, report = h['WORK'], h['REPORT']
    run, scenario, start, stop, rpc = (h[k] for k in ['run', 'scenario', 'start_daemon', 'stop', 'rpc'])
    port = h['free_port']()
    endpoint = f'http://127.0.0.1:{port}/rpc'
    query = 'events where source = terminal.atuin-history and kind = command.executed'
    config = work/'startup.toml'
    path = work/'startup.db'
    store_dir = work/'blobs'
    (report/'inputs').mkdir()
    with sqlite3.connect(path) as con:
        con.execute('CREATE TABLE history (id TEXT PRIMARY KEY,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT,deleted_at INTEGER)')
        con.execute("INSERT INTO history VALUES ('startup',1700000000123456789,1,0,'synthetic startup','/synthetic','startup','fixture',NULL)")
    (report/'inputs'/'startup.db').write_bytes(path.read_bytes())

    def configure(source, suffix):
        text = f'''listen = "127.0.0.1:{port}"
state_dir = "{work/suffix}"
store_dir = "{store_dir}"
writer_connection_file = "{work/'writer'}"
reader_connection_file = "{work/'reader'}"
token_file = "{work/'token'}"
sources = {json.dumps(['terminal.atuin-history'] if source else [])}
automata = []
'''
        if source:
            text += f'[atuin]\npath = "{source}"\npoll_interval_ms = 1000\nfull_interval_ms = 100\n'
        config.write_text(text)

    def ready(proc):
        deadline = time.monotonic()+20
        while time.monotonic()<deadline:
            assert proc.poll() is None, 'daemon stopped before RPC became available'
            try:
                if 'result' in rpc(endpoint, 'synthetic-foundation-token'): return
            except (OSError, ValueError): pass
            time.sleep(.01)
        raise AssertionError('RPC unavailable while initial source read is blocked')

    def cli(command, *args, expected=0):
        result = run([h['BIN_DIR']/'sinexctl', '--endpoint', endpoint, '--token-file', work/'token', command, *args, '--json'], expected=expected, label='startup coverage '+command)
        return json.loads(result.stdout)

    def settled(expected_status, total):
        deadline = time.monotonic()+20
        while time.monotonic()<deadline:
            source = cli('sources')['sources'][0]
            if source['last_run']['status'] == expected_status:
                answer = cli('query', query, expected=0 if expected_status=='completed' else 5)
                assert answer['total']==total and answer['complete']==(expected_status=='completed')
                return source
            time.sleep(.025)
        raise AssertionError('source did not settle after read boundary released')

    def no_source():
        configure(None, 'unconfigured')
        proc = start(config, 'unconfigured'); ready(proc)
        answer = cli('query', query)
        assert answer['complete'] is True and answer['total']==0 and answer['source_status']==[]
        assert cli('sources')['sources']==[]
        stop(proc)
    scenario('unconfigured empty coverage', no_source)

    def locked_read(empty=False):
        nonlocal store_dir
        if empty:
            # A real zero-row case has its own canonical database; old retained facts
            # from the populated case must not be confused with source emptiness.
            run(['createdb', 'startup_empty'], label='isolated empty coverage database')
            admin = h['admin'].replace('foundation?', 'startup_empty?')
            (work/'empty-admin').write_text(admin)
            run([h['BIN_DIR']/'sinexd', 'admin', 'schema', '--connection-file', work/'empty-admin'], label='empty coverage schema')
            for role in ['writer', 'reader']:
                (work/role).write_text((work/role).read_text().replace('foundation?', 'startup_empty?'))
            store_dir = work/'blobs-empty'
            with sqlite3.connect(path) as con: con.execute('DELETE FROM history')
        # DELETE journaling plus an acquired EXCLUSIVE transaction blocks real read-only
        # SQLite access; no synthetic source hooks or elapsed-time observation boundary.
        lock = sqlite3.connect(path)
        lock.execute('PRAGMA journal_mode=DELETE')
        lock.execute('BEGIN EXCLUSIVE')
        configure(path, 'configured')
        proc = start(config, 'blocked-empty' if empty else 'blocked-first')
        try:
            ready(proc)
            status = cli('status')
            answer = cli('query', query, expected=(0,5))
            sources = cli('sources')['sources']
            (report/('blocked-empty.json' if empty else 'blocked-first.json')).write_text(json.dumps({'status':status,'query':answer,'sources':sources}, indent=2)+'\n')
            assert answer['complete'] is False, 'configured unread source falsely reported complete'
            assert answer['total']==0
            assert sources[0]['last_run']['rows_read']==0
            assert len(sources)==1 and sources[0]['locator']==str(path)
            assert sources[0]['last_run']['status'] in ('reading','unavailable')
            assert len(answer['source_status'])==1 and answer['source_status'][0]['status'] in ('reading','unavailable')
            assert status['supported_sources']==['terminal.atuin-history']
        finally:
            lock.rollback(); lock.close()
        settled('completed', 0 if empty else 1)
        stop(proc)
        # Same-state restart must establish a new reading observation before serving,
        # including when a previous observation completed with zero source rows.
        if empty:
            lock = sqlite3.connect(path); lock.execute('BEGIN EXCLUSIVE')
            proc = start(config, 'blocked-restart')
            try:
                ready(proc)
                answer=cli('query', query, expected=5)
                assert answer['complete'] is False
                source=cli('sources')['sources'][0]
                assert source['last_run']['status'] in ('reading','unavailable')
                stop(proc)
            finally:
                lock.rollback(); lock.close()
            proc=start(config,'released-restart'); ready(proc); settled('completed',0); stop(proc)
    scenario('configured blocked first read and recovery', locked_read)
    scenario('configured blocked empty read and restart', lambda: locked_read(True))

    def missing():
        absent=work/'never-created.db'
        configure(absent, 'missing')
        proc=start(config,'missing'); ready(proc)
        deadline=time.monotonic()+20
        while True:
            source=next(s for s in cli('sources')['sources'] if s['locator']==str(absent))
            if source['last_run']['status']=='unavailable': break
            assert time.monotonic()<deadline
            time.sleep(.025)
        assert source['last_run']['reason'] and source['last_run']['rows_read']==0
        assert cli('query',query,expected=5)['complete'] is False
        assert not absent.exists()
        stop(proc)
    scenario('missing source stays unavailable without creation', missing)
