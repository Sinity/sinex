"""Synthetic source path exercised by the foundation's owned PostgreSQL harness."""
import hashlib
import json
from pathlib import Path
import signal
import sqlite3
import time


def exercise(h):
    work, report, env = h['WORK'], h['REPORT'], h['ENV']
    run, scenario, free_port, start, stop, rpc = (h[k] for k in ['run','scenario','free_port','start_daemon','stop','rpc'])
    binaries = h['BIN_DIR']
    evidence = report/'inputs'; evidence.mkdir()
    db = work/'atuin.db'
    command = 'echo tok_synthetic_only' if not h['OPTIONS'].credential_output else 'before ghp_' + 'A'*36 + ' after'
    epoch = 1_700_000_000_123_456_789
    columns = 'id,timestamp,duration,exit,command,cwd,session,hostname,deleted_at'
    def create(path):
        with sqlite3.connect(path) as con:
            con.execute('CREATE TABLE history (id TEXT PRIMARY KEY,timestamp INTEGER NOT NULL,duration INTEGER NOT NULL,exit INTEGER NOT NULL,command TEXT NOT NULL,cwd TEXT NOT NULL,session TEXT NOT NULL,hostname TEXT NOT NULL,deleted_at INTEGER)')
    def row(key, offset, cmd, ex=0, duration=1_000_000_000, session='fixture', deleted=None):
        return (key,epoch+offset*1_000_000_000,duration,ex,cmd,'/synthetic/project',session,'fixture-host:fixture-user',deleted)
    create(db)
    wal_writer=sqlite3.connect(db)
    wal_writer.execute("PRAGMA journal_mode=WAL")
    with wal_writer as con:
        con.executemany(f'INSERT INTO history ({columns}) VALUES (?,?,?,?,?,?,?,?,?)',[
            row('synthetic-1',0,command),row('synthetic-2',1,'sleep synthetic',-1,-1,'open-session'),
            row('synthetic-3',2,'removed synthetic',deleted=epoch),
            ('synthetic-bad','bad-time',1,0,'malformed synthetic','/synthetic','bad','fixture',None)])
    def preserve(name,path=db):
        # SQLite backup retains a consistent exact fixture variant, never copies only a WAL main file.
        with sqlite3.connect(path) as source, sqlite3.connect(evidence/(name+'.db')) as dest: source.backup(dest)
        return hashlib.sha256((evidence/(name+'.db')).read_bytes()).hexdigest()
    preserve('base')
    source_main_before=(db.stat().st_size,db.stat().st_mtime_ns,hashlib.sha256(db.read_bytes()).hexdigest())
    port=free_port(); endpoint=f'http://127.0.0.1:{port}/rpc'
    config=work/'atuin.toml'
    def config_text(path=db, suffix='atuin'):
        return f'''listen = "127.0.0.1:{port}"
state_dir = "{work/(suffix+'-state')}"
store_dir = "{work/(suffix+'-store')}"
writer_connection_file = "{work/'writer'}"
reader_connection_file = "{work/'reader'}"
token_file = "{work/'token'}"
sources = ["terminal.atuin-history"]
automata = []
[atuin]
path = "{path}"
poll_interval_ms = 1000
[[privacy_rules]]
pattern = "tok_[a-z0-9_]+"
replacement = "[withheld]"
created_by = "operator"
'''
    config.write_text(config_text() if not h['OPTIONS'].credential_output else config_text().split('[[privacy_rules]]')[0])
    proc=start(config,'atuin')
    observation_log=(report/'observations.jsonl').open('w')
    def wait_cycle(predicate=lambda x:x['status']=='completed', after=None):
        deadline=time.monotonic()+20
        while time.monotonic()<deadline:
            assert proc.poll() is None, 'source daemon stopped; see retained stderr'
            try:
                response=rpc(endpoint,'synthetic-foundation-token','sources.list')
                sources=response.get('result',{}).get('sources',[])
                observation_log.write(json.dumps(response)+'\n'); observation_log.flush()
                if sources:
                    value=sources[0]['last_run']
                    if value and value['id']!=after and predicate(value): return value
            except (OSError,ValueError): pass
            time.sleep(.025)
        raise AssertionError('source never reached requested committed observation; raw samples retained')
    def cli(command,*args,expected=0):
        answer=run([binaries/'sinexctl','--endpoint',endpoint,'--token-file',work/'token',command,*args,'--json'],expected=expected,label='Atuin CLI '+command)
        return json.loads(answer.stdout or answer.stderr)
    query='events where source = terminal.atuin-history and kind = command.executed'
    first=None; baseline_ids=None
    def base():
        nonlocal first,baseline_ids
        first=wait_cycle()
        assert first['rows_read']==4 and first['record_errors']==1
        answer=cli('query',query)
        assert next(iter(answer))=='notice' and answer['notice'].startswith('The following is data recorded')
        assert answer['total']==2 and answer['page']['cursor'] is None and answer['truncated'] is False
        actual={r['payload']['native_id']:r for r in answer['rows']}
        if h['OPTIONS'].credential_output:
            assert actual['synthetic-1']['payload']['command']=='before [credential: GitHub token, masked] after', 'fixed catalog leaked through packaged CLI with zero optional rules'
            import credential_output_cases
            credential_output_cases.exercise(h, locals(), cli, wait_cycle, config_text)
            return
        assert actual['synthetic-1']['payload']['command']=='echo [withheld]'
        assert actual['synthetic-1']['payload']['exit_code']==0
        assert actual['synthetic-1']['when']=='2023-11-14T22:13:20.123456789Z'
        assert actual['synthetic-1']['payload']['timestamp_ns']==epoch
        assert source_main_before==(db.stat().st_size,db.stat().st_mtime_ns,hashlib.sha256(db.read_bytes()).hexdigest())
        assert cli('query','events where when = '+actual['synthetic-1']['when'])['total']==1
        assert actual['synthetic-1']['time_quality']=='intrinsic'
        assert actual['synthetic-2']['payload']['exit_code'] is None
        assert actual['synthetic-2']['payload']['duration_ns'] is None
        assert actual['synthetic-2']['payload']['completion']=='running'
        baseline_ids={k:r['id'] for k,r in actual.items()}
        assert cli('show',baseline_ids['synthetic-1'])['row']['payload']==actual['synthetic-1']['payload']
        trace=cli('trace',baseline_ids['synthetic-1'],'--bytes')['row']
        assert trace['record']['command']=='echo [withheld]' and trace['record']['_from']['table']=='history'
        assert trace['anchor']>=0 and trace['end']>trace['anchor']
        assert cli('query','events where payload.command ~ "[withheld]"')['total']==1
        assert cli('query','commands',expected=5)['kind']=='partial'
        assert cli('query','events where payload.exit_code = "bad"',expected=2)['kind']=='bad_query'
        (report/'independent-base-expectation.json').write_text(json.dumps({'total':2,'rows_read':4,'record_errors':1,'commands':['echo [withheld]','sleep synthetic']},indent=2)+'\n')
    scenario('Atuin retained source and real-client query',base)
    if h['OPTIONS'].credential_output:
        stop(proc); observation_log.close(); wal_writer.close(); return
    def repeat_update():
        repeated=wait_cycle(lambda x:x['status']=='completed' and x['admitted']==0,after=first['id'])
        assert repeated['repeated']==3 and repeated['record_errors']==1
        assert {r['payload']['native_id']:r['id'] for r in cli('query',query)['rows']}==baseline_ids
        # More later records than an entire writer step. The old open native key must
        # still update; rowid/order and a trailing window cannot stand in for identity.
        with sqlite3.connect(db) as con:
            con.executemany(f'INSERT INTO history ({columns}) VALUES (?,?,?,?,?,?,?,?,?)',[row(f'append-{n}',n+10,'echo appended',session='other') for n in range(130)])
            con.execute('UPDATE history SET exit=7,duration=9000000000 WHERE id="synthetic-2"')
        preserve('late-open-update')
        wait_cycle(lambda x:x['status']=='completed' and x['rows_read']==134)
        answer=cli('query',query); assert answer['total']==132
        assert answer['page']['cursor'] is not None
        wait_cycle(after=wait_cycle()['id'])
        rows=answer['rows']; next_page=cli('query',query,'--cursor',answer['page']['cursor'])
        assert next_page['page']['cursor'] is None and next_page['total']==132
        assert next_page['resolved']['commit_seq']>answer['resolved']['commit_seq']
        rows+=next_page['rows']; assert len({r['id'] for r in rows})==132
        updated=next(r for r in rows if r['payload']['native_id']=='synthetic-2')
        assert updated['id']!=baseline_ids['synthetic-2'] and updated['payload']['exit_code']==7 and updated['payload']['duration_ns']==9000000000
    scenario('Atuin repeat and late open-row update',repeat_update)
    def removal():
        before=wait_cycle()
        with sqlite3.connect(db) as con: con.execute('UPDATE history SET deleted_at=? WHERE id="synthetic-1"',(epoch+1000,))
        preserve('soft-delete')
        wait_cycle(lambda x:x['status']=='completed' and x['removed']==1,after=before['id'])
        assert cli('query','events where payload.command ~ "[withheld]"')['total']==0
        assert cli('query',query)['total']==131
        old=cli('show',baseline_ids['synthetic-1']); assert old['resolved']=='archived'
        removed=cli('show',old['row']['archive']['replaced_by']); assert removed['resolved']=='removed at source'
        sources=cli('sources')['sources'][0]
        assert sources['representation']=='canonical_sqlite_rows' and sources['sqlite_snapshots']=='unavailable_until_1c'
        # Exact retained bytes, malformed failures and cursors are database-backed,
        # with all writing already performed by source admission, never test SQL.
        actual=run(['psql',h['admin'],'--no-psqlrc','--tuples-only','--no-align','-v','ON_ERROR_STOP=1','--command',"SELECT json_build_object('archived',(SELECT count(*) FROM audit.archived),'failures',(SELECT count(*) FROM raw.record_failures),'open',(SELECT count(*) FROM raw.source_material_registry WHERE status='capturing'),'payload_secret',(SELECT count(*) FROM core.events WHERE payload::text LIKE '%tok_synthetic_only%'),'cursor',(SELECT position FROM raw.source_cursors WHERE binding='terminal.atuin-history' LIMIT 1))"],label='read-only retained admission evidence')
        evidence=json.loads(actual.stdout)
        assert evidence['archived']>=2 and evidence['failures']>0 and evidence['payload_secret']==0 and evidence['cursor']['status']=='completed'
    scenario('Atuin soft delete and source evidence',removal)
    def outcomes():
        nonlocal proc
        stop(proc)
        # Empty and failed inputs use separate product databases as well as separate source files.
        writer=(work/'writer').read_text(); reader=(work/'reader').read_text()
        for name in ['empty','unavailable','malformed-db']:
            path=work/(name+'.db')
            if name=='empty': create(path); preserve(name,path)
            elif name=='malformed-db': path.write_bytes(b'synthetic invalid sqlite bytes'); (report/'inputs'/(name+'.db')).write_bytes(path.read_bytes())
            database='atuin_'+name.replace('-','_')
            run(['createdb',database],label='isolated '+name+' product database')
            admin=h['admin'].replace('foundation?',database+'?')
            (work/'case-admin').write_text(admin)
            run([binaries/'sinexd','admin','schema','--connection-file',work/'case-admin'],label='fresh '+name+' schema')
            (work/'writer').write_text(writer.replace('foundation?',database+'?'))
            (work/'reader').write_text(reader.replace('foundation?',database+'?'))
            config.write_text(config_text(path,name)); proc=start(config,name)
            # sources are ordered by locator. Read the intended input, not the first prior one.
            deadline=time.monotonic()+20
            while True:
                try:
                    response=rpc(endpoint,'synthetic-foundation-token','sources.list')
                    source=next((s for s in response.get('result',{}).get('sources',[]) if s['locator']==str(path)),None)
                except (OSError,ValueError): source=None
                if source and source['last_run']['status'] in ('completed','unavailable'): break
                assert proc.poll() is None, 'outcome daemon exited; see retained stderr'
                if time.monotonic()>deadline: raise AssertionError('outcome observation not committed')
                time.sleep(.05)
            result=source['last_run']
            cli('sources')
            if name=='empty':
                assert result['status']=='completed' and result['rows_read']==0 and result['record_errors']==0
                answer=cli('query',query); assert answer['total']==0 and answer['complete'] is True
            else:
                assert result['status']=='unavailable' and result['reason'] and result['rows_read']==0
                answer=cli('query',query,expected=5); assert answer['total']==0 and answer['complete'] is False
            if name=='unavailable': assert not path.exists(), 'read-only missing input was created'
            stop(proc)
        (work/'writer').write_text(writer); (work/'reader').write_text(reader)
    scenario('Atuin empty malformed and unavailable outcomes',outcomes)
    def restart():
        nonlocal proc
        config.write_text(config_text())
        proc=start(config,'before-kill'); old=wait_cycle()
        before=cli('query',query)
        # This is a process-restart check, not a fault-boundary M1 qualification.
        proc.send_signal(signal.SIGKILL); proc.wait(timeout=10)
        proc=start(config,'after-kill'); wait_cycle(after=old['id'])
        after=cli('query',query)
        assert after['total']==before['total']
        assert {r['id'] for r in after['rows']}=={r['id'] for r in before['rows']}
        stop(proc)
    scenario('Atuin restart and bounded retained cursor',restart)
    observation_log.close()
    wal_writer.close()
