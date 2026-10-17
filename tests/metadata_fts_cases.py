"""Finite synthetic controls for independently complete metadata and exact FTS."""
import hashlib
import json
from pathlib import Path
import select
import sqlite3
import subprocess
import time
import urllib.request
import uuid

PLANNED = [
    'guarded stage-four metadata upgrade preserves evidence and refuses old writer',
    'blocked optional receipts preserve the primary source snapshot and recover',
    'oversized text preserves exact later metadata and refuses every FTS shape',
    'unavailable text diagnostics honor current parent eligibility',
    'retained metadata restart provider-loss exclusion and replay keep capability truth',
    'metadata maintenance failure remains globally unavailable',
    'unavailable-vector probes use their bounded partial-index path',
    'native shell working-set indexes preserve pending sets and membership',
]


def exercise(h):
    work, report, env, bins = (h[k] for k in ['WORK', 'REPORT', 'ENV', 'BIN_DIR'])
    run, scenario, start, stop = (h[k] for k in ['run', 'scenario', 'start_daemon', 'stop'])
    pin = json.loads(h['OPTIONS'].metadata_fts_predecessor_pin.read_text())
    old = Path(pin['package']) / 'bin/sinexd'
    assert pin['candidate'] == '5c508255eaa76f9f44bde5d93c0b2ab8bd1747e6'
    assert pin['exit_code'] == 0 and not pin['dirty']
    assert hashlib.sha256(old.read_bytes()).hexdigest() == pin['binaries']['sinexd']['sha256']
    (report/'metadata-fts-predecessor.json').write_text(json.dumps(pin, indent=2))
    run(['createdb', 'metadata_fts'], label='owned metadata upgrade database')
    connections = {k: h[k].replace('/foundation?', '/metadata_fts?') for k in ['admin','writer','reader']}
    connections['read-model'] = connections['reader'].replace('sinex_reader', 'sinex_read_model')
    for key, value in connections.items():
        (work/('fts-'+key)).write_text(value); (work/('fts-'+key)).chmod(0o600)
    (work/'fts-operator').write_text('synthetic-metadata-operator'); (work/'fts-operator').chmod(0o600)
    source = work/'metadata-history.db'
    epoch = 1_759_276_800_000_000_000  # 2025-10-01 UTC, an absolute fixture clock.
    columns = ['id','timestamp','duration','exit','command','cwd','session','hostname','deleted_at']
    facts = [('safe-'+str(n), epoch+n, 5, 0, 'needle safe '+str(n), '/synthetic/tide', 's', 'h', None) for n in range(32)]
    with sqlite3.connect(source) as c:
        c.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT,deleted_at INTEGER)')
        c.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)', facts)
    endpoint = f'http://127.0.0.1:{h["free_port"]()}/rpc'; token = (work/'token').read_text(); config = work/'metadata-fts.toml'
    config.write_text(f'''listen = "{endpoint.removeprefix('http://').removesuffix('/rpc')}"
state_dir = "{work/'fts-state'}"
store_dir = "{work/'fts-blobs'}"
writer_connection_file = "{work/'fts-writer'}"
reader_connection_file = "{work/'fts-reader'}"
read_model_connection_file = "{work/'fts-read-model'}"
token_file = "{work/'token'}"
operator_token_file = "{work/'fts-operator'}"
sources = ["terminal.atuin-history"]
automata = []
[[projects]]
name = "tide"
roots = ["/synthetic/tide"]
[atuin]
path = "{source}"
poll_interval_ms = 500
full_interval_ms = 1000
''')
    daemon = None
    def save(name, value): (report/(name+'.json')).write_text(json.dumps(value, indent=2))
    def api(method, params=None):
        request = urllib.request.Request(endpoint, data=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params or {}}).encode(), headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
        with urllib.request.urlopen(request, timeout=15) as reply: value=json.load(reply)
        with (report/'metadata-fts-rpc.jsonl').open('a') as log: log.write(json.dumps({'method':method,'params':params,'response':value})+'\n')
        return value
    def cli(command, *args, expected=0, operator=False):
        value=run([bins/'sinexctl','--endpoint',endpoint,'--token-file',work/('fts-operator' if operator else 'token'),command,*args,'--json'],expected=expected,label='metadata FTS '+command)
        return json.loads(value.stdout or value.stderr)
    def wait(reason, fn, seconds=90):
        until=time.monotonic()+seconds
        while time.monotonic()<until:
            assert daemon.poll() is None, reason+' daemon exited'
            try:
                result=fn()
                if result:return result
            except (OSError, KeyError):pass
            time.sleep(.08)
        raise AssertionError(reason)
    def query(text):return api('query.run',{'query':text}).get('result')
    broad='commands where project = tide sort when limit 100'
    selected='commands where project = tide and when >= 2025-10-03T00:00:00Z and when < 2025-10-04T00:00:00Z sort when'
    def settled(total=None, available=None):
        q=query(broad)
        if not q or (total is not None and q['total']!=total):return None
        if q['fusion']['phase']!='settled' or not q['fusion']['current_now_settled']:return None
        if available is not None and q['full_text']['available'] is not available:return None
        return q
    def schema(binary, *args, expected=0):
        r=run([binary,'admin','schema','--connection-file',work/'fts-admin',*args],expected=expected,label='metadata guarded schema')
        return json.loads(r.stdout) if r.returncode==0 else r
    def sql(text, uri=None):
        return run(['psql',uri or connections['admin'],'-XAtq','-v','ON_ERROR_STOP=1','-c',text],label='metadata independent SQL observation').stdout.strip()
    def fts_refusals():
        for text in ['commands where needle','commands where "needle safe"','commands where need*','commands where not needle','commands where project = tide or needle','commands where project = tide and needle','commands where needle sort when','events where needle']:
            value=cli('query',text,expected=5);assert value['kind']=='partial' and 'rows' not in value, value
        value=cli('search','needle',expected=5);assert value['kind']=='partial' and 'rows' not in value
    try:
        def upgrade():
            nonlocal daemon
            schema(old);daemon=start(config,'metadata-published',binary=old)
            before=wait('published indexed population',lambda:query(broad) if query(broad) and query(broad)['total']==32 else None)
            cursor=before['page']['cursor'];assert cursor
            raw=api('query.run',{'query':'events where source = terminal.atuin-history'})['result']['rows'][0]
            reference='sinex:ev/'+raw['id'];trace=api('object.trace',{'ref':reference,'bytes':True})['result'];stop(daemon)
            pending=schema(bins/'sinexd','--check');assert [x['number'] for x in pending['pending_changes']]==[5]
            schema(bins/'sinexd','--apply',expected=2)
            result=schema(bins/'sinexd','--apply','--dump-file',work/'metadata-before-change.dump')
            assert result['checked_changes']==[{'number':5,'outcome':'applied'}]
            assert sql("SELECT fingerprint FROM sinex_schemas.definition") == sql("SELECT fingerprint FROM sinex_schemas.definition", h['admin'])
            assert sql("SELECT catalog FROM sinex_schemas.definition") == sql("SELECT catalog FROM sinex_schemas.definition", h['admin'])
            rejected=run([old,'serve','--config',config],expected=1,label='published old writer refuses upgraded catalog')
            assert 'schema' in rejected.stderr
            daemon=start(config,'metadata-upgraded');wait('upgraded metadata available',lambda:settled(32,True))
            refusal=cli('query',broad,'--cursor',cursor,expected=2);assert refusal['kind']=='bad_query'
            assert api('object.trace',{'ref':reference,'bytes':True})['result']==trace
            save('metadata-populated-upgrade',{'before':before,'pending':pending,'applied':result,'old_cursor_refusal':refusal,'preserved_trace':trace})
        scenario(PLANNED[0],upgrade)
        def source_budget():
            nonlocal daemon
            # Freeze acquisition only for this control. An unchanged primary
            # snapshot distinguishes optional failure from a later poll.
            enabled=config.read_text();stop(daemon)
            config.write_text(enabled.replace('sources = ["terminal.atuin-history"]','sources = []').split('[atuin]')[0])
            daemon=start(config,'metadata-source-budget')
            lock_err=(report/'source-budget-lock.stderr').open('w')
            locker=None
            try:
                wait('source-budget metadata basis',lambda:settled(32,True))
                before=cli('sources')['sources'][0]
                locker=subprocess.Popen(['psql',connections['admin'],'--no-psqlrc','-qAt','-v','ON_ERROR_STOP=1'],env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=lock_err,text=True,bufsize=1)
                locker.stdin.write("BEGIN; LOCK TABLE core.events IN ACCESS EXCLUSIVE MODE; SELECT 'receipt-lock-held';\n");locker.stdin.flush()
                ready,_,_=select.select([locker.stdout],[],[],15)
                assert ready and locker.stdout.readline().strip()=='receipt-lock-held'
                blocked=[]
                for _ in range(3):
                    began=time.monotonic();limited=cli('sources','--input',before['input'])['sources'][0]
                    assert limited['last_run']==before['last_run'],'optional receipt timeout changed the primary observation'
                    assert limited['acquisition']==before['acquisition']
                    assert limited['inspection']['status']=='unavailable'
                    assert limited['history']['complete'] is False and limited['receipts']['complete'] is False
                    blocked.append({'seconds':time.monotonic()-began,'response':limited})
                locker.stdin.write('ROLLBACK;\n\\q\n');locker.stdin.flush();locker.stdin.close();locker.wait(timeout=15)
                assert locker.returncode==0
                recovered=cli('sources','--input',before['input'])['sources'][0]
                assert recovered['last_run']==before['last_run'] and recovered['inspection']['status']=='available'
                answer=cli('query',broad,expected=(0,5))
                assert answer['total']==32 and answer['full_text']['available']
                save('source-budget-recovery',{'before':before,'blocked':blocked,'recovered':recovered,'ordinary_query':answer,'lock_exit':locker.returncode,'scope':'server-cancelled blocked receipt reads; primary repeatable-read evidence and subsequent pool reuse; no claim to reproduce the previous exact future-drop timing'})
            finally:
                if locker and locker.poll() is None:
                    locker.stdin.write('ROLLBACK;\n\\q\n');locker.stdin.flush();locker.stdin.close();locker.wait(timeout=15)
                    assert locker.returncode==0
                lock_err.close();stop(daemon);config.write_text(enabled)
                daemon=start(config,'metadata-source-budget-restored')
                wait('source-budget acquisition restored',lambda:settled(32,True))
        scenario(PLANNED[1],source_budget)
        oversized=[
            ('frequency',epoch+86_400_000_000_000,0,0,'repeat '*257,'/synthetic/tide','s','h',None),
            ('positions',epoch+86_400_000_000_001,0,0,' '.join('w'+str(n) for n in range(16001)),'/synthetic/tide','s','h',None),
            ('token-bytes',epoch+86_400_000_000_002,0,0,'x'*901,'/synthetic/tide','s','h',None),
            ('empty-command',epoch+86_400_000_000_003,0,0,'','/synthetic/tide','s','h',None),
            ('later',epoch+172_800_000_000_000+123,9,-1,'needle later exact','/synthetic/tide','s','h',None),
        ]
        def metadata():
            with sqlite3.connect(source) as c:c.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',oversized)
            answer=wait('all metadata with unavailable FTS',lambda:settled(37,False));fts_refusals()
            def complete_selected():
                q=cli('query',selected,expected=(0,5))
                assert 'rows' in q and q['total']==1 and not q['full_text']['available'],q
                return q if q['complete'] else None
            q=wait('clean source metadata remains complete despite unavailable FTS',complete_selected)
            selected_answer=q
            p=q['rows'][0]['payload'];assert p['native_id']=='later' and p['command']=='needle later exact' and p['timestamp_ns']==oversized[-1][1] and p['exit_code'] is None and p['duration_ns']==9
            for key in ['frequency','positions','token-bytes','empty-command']:
                n=next(i for i,x in enumerate(oversized) if x[0]==key)
                q=cli('query',f'commands where project = tide and when = 2025-10-02T00:00:00.{n:09d}Z',expected=(0,5))
                assert q['total']==1 and q['rows'][0]['payload']['native_id']==key
            values=json.loads(sql("SELECT jsonb_build_object('null_vectors',(SELECT count(*) FROM core.search_text WHERE tsv IS NULL),'real_empty_distinct',(''::tsvector IS NOT NULL),'index',(SELECT pg_get_indexdef(indexrelid) FROM pg_index WHERE indexrelid='core.search_text_unavailable'::regclass))"))
            assert values['null_vectors']==3 and values['real_empty_distinct']
            reference=answer['full_text']['first_affected_ref'];assert reference
            assert cli('show',reference)['row']['current_eligible']
            save('metadata-later-and-capabilities',{'query':selected_answer,'counts':values,'first_reference_trace':cli('trace',reference,'--bytes')})
        scenario(PLANNED[2],metadata)
        def authority():
            nonlocal daemon
            # Stop acquisition for this direct-state fence so an unrelated source
            # commit cannot turn metadata pending and conceal a missing authority check.
            enabled=config.read_text();stop(daemon)
            config.write_text(enabled.split('[atuin]')[0].replace('sources = ["terminal.atuin-history"]','sources = []'))
            daemon=start(config,'metadata-authority-stable')
            wait('stable metadata authority basis',lambda:settled(37,False))
            status=api('status')['result']['indexed_retrieval'];reference=status['full_text']['first_affected_ref'];assert reference
            traced=cli('trace',reference,'--bytes')['row'];parent=traced['parents'][0]['event']
            # A direct-state read-fence control deliberately holds commit position
            # fixed, so missing metadata catch-up cannot mask a leaked reference.
            sql("INSERT INTO audit.archived(id,archived_seq,reason) SELECT '"+parent+"'::uuid,seq,'supersede' FROM core.commit_counter WHERE one")
            try:
                whole=api('status')['result']['indexed_retrieval']
                assert whole['serving'] and whole['position']==whole['current_commit_seq'] and whole['failure'] is None,whole
                value=whole['full_text']
                assert not value['available'] and value['first_affected_ref']!=reference, value
                assert not cli('show',reference)['row']['current_eligible']
                save('metadata-reference-authority-fence',{'before':reference,'after':value,'scope':'direct-state current-parent fence, not writer removal qualification'})
            finally:
                sql("DELETE FROM audit.archived WHERE id='"+parent+"'::uuid")
                stop(daemon);config.write_text(enabled);daemon=start(config,'metadata-authority-restored')
                wait('restored acquisition basis',lambda:settled(37,False))
        scenario(PLANNED[3],authority)
        def retained():
            nonlocal daemon
            before=cli('query',selected,expected=(0,5));ref='sinex:ev/'+before['rows'][0]['id'];trace=cli('trace',ref,'--bytes')
            stop(daemon);daemon=start(config,'metadata-restart');wait('restart unavailable-text metadata',lambda:settled(37,False))
            assert cli('query',selected,expected=(0,5))['rows']==before['rows'] and cli('trace',ref,'--bytes')==trace
            source.rename(work/'metadata-provider-held.db');stop(daemon);daemon=start(config,'metadata-provider-loss')
            wait('missing provider reported',lambda:any(x['last_run']['status']=='unavailable' for x in api('sources.list')['result']['sources']))
            after=cli('query',selected,expected=5);assert after['rows']==before['rows'];assert cli('trace',ref,'--bytes')==trace;fts_refusals()
            rows=api('query.run',{'query':'events where source = terminal.atuin-history'})['result']['rows']
            large=[r for r in rows if r['payload']['native_id'] in ['frequency','positions','token-bytes']];assert len(large)==3
            rawref='sinex:ev/'+large[0]['id'];material=cli('trace',rawref,'--bytes')['row']['material']
            for r in large:cli('delete','sinex:ev/'+r['id'],operator=True)
            wait('FTS recovers after excluded rows',lambda:settled(34,True))
            assert cli('query','commands where needle',expected=5)['total']==33
            cli('delete','--undo',rawref,operator=True)
            replay=cli('replay','materials where id = '+material,operator=True)
            assert replay['state']=='completed'
            wait('replay restores unavailable text',lambda:settled(35,False));fts_refusals()
            current=api('query.run',{'query':'events where source = terminal.atuin-history'})['result']['rows']
            restored=next(r for r in current if r['payload']['native_id']==large[0]['payload']['native_id'])
            cli('delete','sinex:ev/'+restored['id'],operator=True);wait('replay removal restores text capability',lambda:settled(34,True))
            save('metadata-retained-lifecycle',{'before':before,'after_provider_loss':after,'trace':trace,'replay':replay})
        scenario(PLANNED[4],retained)
        def maintenance():
            nonlocal daemon
            stop(daemon)
            sql("DELETE FROM core.search_text; DELETE FROM core.event_project; DELETE FROM derivation.read_model_position; DELETE FROM core.commit_log WHERE event_type_id=3 AND change=0")
            daemon=start(config,'metadata-real-maintenance-failure')
            wait('actual maintenance failure',lambda:api('status')['result']['indexed_retrieval'].get('failure'))
            for text in [selected,'commands where needle']:
                error=cli('query',text,expected=5);assert error['kind']=='partial' and 'rows' not in error
        scenario(PLANNED[5],maintenance)
        def plans():
            run(['createdb','fts_probe'],label='ordinary PostgreSQL readiness plan database')
            uri=connections['admin'].replace('/metadata_fts?','/fts_probe?')
            probe_sql="""CREATE SCHEMA core;
CREATE TABLE core.search_text(generation uuid NOT NULL,id uuid PRIMARY KEY,tsv tsvector);
CREATE INDEX search_text_unavailable ON core.search_text(generation,id) WHERE tsv IS NULL;
INSERT INTO core.search_text SELECT '00000000-0000-0000-0000-000000000000',md5(n::text)::uuid,''::tsvector FROM generate_series(1,20000)n;
INSERT INTO core.search_text SELECT '00000000-0000-0000-0000-000000000000',md5('null'||n)::uuid,NULL FROM generate_series(1,17)n;
EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT EXISTS(SELECT 1 FROM core.search_text WHERE generation='00000000-0000-0000-0000-000000000000' AND tsv IS NULL LIMIT 1);
EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT id FROM core.search_text WHERE generation='00000000-0000-0000-0000-000000000000' AND tsv IS NULL ORDER BY id LIMIT 16;
"""
            fixture=report/'unavailable-probe-plan.sql';fixture.write_text(probe_sql)
            text=run(['psql',uri,'-XAtq','-v','ON_ERROR_STOP=1','--file',fixture],label='ordinary PostgreSQL unavailable probe plans').stdout
            decoder=json.JSONDecoder();actual=[]
            while text.strip():
                value,end=decoder.raw_decode(text.lstrip());actual.append(value[0]);text=text.lstrip()[end:]
            def nodes(plan):
                yield plan
                for child in plan.get('Plans',[]):yield from nodes(child)
            assert len(actual)==2
            for plan in actual:
                assert any(n.get('Index Name')=='search_text_unavailable' for n in nodes(plan['Plan'])),plan
            assert actual[1]['Plan']['Actual Rows']==16
            save('unavailable-vector-query-plans',{'plans':actual,'scope':'ordinary PostgreSQL exact probe predicates; twenty-thousand real empty vectors and seventeen unavailable vectors; no forced planner settings or capacity claim'})
        scenario(PLANNED[6],plans)
        def shell_work_tables():
            isolated=env.copy();isolated['SINEX_TEST_WRITER_URL']=h['writer']
            result=run(['cargo','test','--locked','-p','sinex-store','--features','product','--lib','shell::work_tests::native_work_tables_preserve_pending_sets_and_membership_with_indexed_plans','--','--ignored','--nocapture'],env=isolated,label='native temporary work-table constructor semantics and ordinary PostgreSQL plans')
            proof=next(line.removeprefix('SHELL_WORK_PROOF ') for line in result.stdout.splitlines() if line.startswith('SHELL_WORK_PROOF '))
            save('shell-work-table-plans',json.loads(proof))
        scenario(PLANNED[7],shell_work_tables)
    finally:
        if daemon and daemon.poll() is None:stop(daemon)
