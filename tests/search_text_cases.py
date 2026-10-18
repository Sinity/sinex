"""Finite synthetic controls for complete normalized tokens, exact SQL search and metadata availability."""
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
    'guarded stage-six token-text upgrade preserves evidence and refuses old writer',
    'blocked optional receipts preserve the primary source snapshot and recover',
    'oversized text preserves exact complete words phrases prefixes and metadata',
    'unavailable text diagnostics honor current parent eligibility',
    'retained metadata restart provider-loss exclusion and replay keep capability truth',
    'metadata maintenance failure remains globally unavailable',
    'unavailable-text probes use their bounded partial-index path',
    'native shell working-set indexes preserve pending sets and membership',
]


def exercise(h):
    work, report, env, bins = (h[k] for k in ['WORK', 'REPORT', 'ENV', 'BIN_DIR'])
    run, scenario, start, stop = (h[k] for k in ['run', 'scenario', 'start_daemon', 'stop'])
    pin = json.loads(h['OPTIONS'].search_text_predecessor_pin.read_text())
    old = Path(pin['package']) / 'bin/sinexd'
    assert pin['candidate'] == '1729f952d60b697c286d846e7861a92869c34f89'
    assert pin['exit_code'] == 0 and not pin['dirty']
    assert hashlib.sha256(old.read_bytes()).hexdigest() == pin['binaries']['sinexd']['sha256']
    (report/'search-text-predecessor.json').write_text(json.dumps(pin, indent=2))
    run(['createdb', 'search_text'], label='owned metadata upgrade database')
    connections = {k: h[k].replace('/foundation?', '/search_text?') for k in ['admin','writer','reader']}
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
    endpoint = f'http://127.0.0.1:{h["free_port"]()}/rpc'; token = (work/'token').read_text(); config = work/'search-text.toml'
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
        with (report/'search-text-rpc.jsonl').open('a') as log: log.write(json.dumps({'method':method,'params':params,'response':value})+'\n')
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
            sql("INSERT INTO derivation.read_model_position(name,generation,configuration,position,serving,failure) VALUES('unrelated-fixture','00000000-0000-0000-0000-000000000099','independent',0,true,NULL)")
            unrelated=json.loads(sql("SELECT row_to_json(p) FROM derivation.read_model_position p WHERE name='unrelated-fixture'"))
            pending=schema(bins/'sinexd','--check');assert [x['number'] for x in pending['pending_changes']]==[7]
            schema(bins/'sinexd','--apply',expected=2)
            result=schema(bins/'sinexd','--apply','--dump-file',work/'metadata-before-change.dump')
            assert result['checked_changes']==[{'number':7,'outcome':'applied'}]
            assert json.loads(sql("SELECT row_to_json(p) FROM derivation.read_model_position p WHERE name='unrelated-fixture'"))==unrelated
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
            ('frequency',epoch+86_400_000_000_000,0,0,'repeat '*290+'late neighbor','/synthetic/tide','s','h',None),
            ('positions',epoch+86_400_000_000_001,0,0,' '.join('w'+str(n) for n in range(17001)),'/synthetic/tide','s','h',None),
            ('token-bytes',epoch+86_400_000_000_002,0,0,'x'*1100,'/synthetic/tide','s','h',None),
            ('empty-command',epoch+86_400_000_000_003,0,0,'','/synthetic/tide','s','h',None),
            ('delimiters',epoch+86_400_000_000_004,0,0,'first\u0001second\u0002third percent% underscore_ slash\\tail \"literal\"','/synthetic/tide','s','h',None),
            ('field-boundary',epoch+86_400_000_000_005,0,0,'fieldtail','/synthetic/tide/head','s','h',None),
            ('wildcard-control',epoch+86_400_000_000_006,0,0,'underscorex slashtail percentZ','/synthetic/tide','s','h',None),
            ('later',epoch+172_800_000_000_000+123,9,-1,'needle later exact','/synthetic/tide','s','h',None),
        ]
        def metadata():
            with sqlite3.connect(source) as c:c.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',oversized)
            answer=wait('complete token search and metadata',lambda:settled(40,True))
            q=wait('complete source metadata',lambda:cli('query',selected,expected=(0,5)) if cli('query',selected,expected=(0,5))['complete'] else None)
            assert q['total']==1 and q['full_text']['available']
            later=q['rows'][0]['payload'];assert later['native_id']=='later' and later['exit_code'] is None and later['duration_ns']==9
            checks={
                'repeat':{'frequency'}, '"late neighbor"':{'frequency'}, '"late repeat"':set(),
                'neigh*':{'frequency'}, 'w17000':{'positions'}, 'x'*1100:{'token-bytes'},
                '"first second third"':{'delimiters'}, json.dumps('first\u0001second\u0002third'):{'delimiters'},
                '"fieldtail synthetic"':set(), '"fieldtail head"':set(), 'fieldtail':{'field-boundary'},
                'commands where not repeat and command = "needle later exact"':{'later'},
                'commands where repeat or command = "needle later exact"':{'frequency','later'},
                'commands where repeat and exit_code = 0':{'frequency'},
                'wildcard':{'wildcard-control'},
                'underscore_':{'delimiters'}, 'underscore':set(), 'underscorex':{'wildcard-control'},
                json.dumps('slash\\tail'):{'delimiters'},
                '"slash tail"':{'delimiters'},
                'commands where "percent underscore_ slash tail"':{'delimiters'},
            }
            checks['commands where command ~ '+json.dumps('slash\\tail')]={'delimiters'}
            checks['commands where command ~ '+json.dumps('slashtail')]={'wildcard-control'}
            checks['commands where command ~ '+json.dumps('underscore_')]={'delimiters'}
            checks['commands where command ~ '+json.dumps('percent% underscore_ slash\\tail \"literal\"')]={'delimiters'}
            checks['commands where command ^ '+json.dumps('first\u0001second\u0002third percent% underscore_ slash\\')]={'delimiters'}
            observations={}
            for question,expected in checks.items():
                value=cli('query',question,expected=(0,5));keys={row['payload']['native_id'] for row in value['rows']}
                assert keys==expected and value['total']==len(expected),(question,keys,expected)
                observations[question]=value
            # Independent literal fixture membership, PostgreSQL's declared score,
            # and the complete relation establish the expected order before paging.
            expected_rank=json.loads(sql("SELECT jsonb_agg(jsonb_build_object('native_id',key,'rank',rank) ORDER BY rank DESC,at ASC,tie ASC,id ASC) FROM (SELECT e.payload->>'native_id' AS key,word_similarity('needle repeat' COLLATE \"C\",s.tokens COLLATE \"C\")::float8 AS rank,s.ts_orig AS at,coalesce(s.ts_orig_tiebreak,0) AS tie,s.id FROM core.search_text s JOIN core.live_events e ON e.id=s.id WHERE e.payload->>'native_id' LIKE 'safe-%' OR e.payload->>'native_id' IN ('later','frequency'))expected"))
            assert len(expected_rank)==34
            ranked=[];cursor=None;rank_pages=[]
            while True:
                page=api('query.run',{'query':'needle or repeat','page':7,**({'cursor':cursor} if cursor else {})})['result']
                assert page['total']==34 and 'word_similarity' in page['read_as']
                ranked.extend({'native_id':row['payload']['native_id'],'rank':row['rank']} for row in page['rows']);rank_pages.append(page)
                cursor=page['page']['cursor']
                if not cursor:break
            assert ranked==expected_rank,(ranked,expected_rank)
            for question,total in [('commands where not repeat',39),('commands where project = tide',40),('commands where not not repeat',1),('needle or repeat sort when',34)]:
                unranked=api('query.run',{'query':question,'page':100})['result']
                assert unranked['total']==total and all(row['rank']==0 for row in unranked['rows']),unranked
            bad=cli('query','"first\u0001second"',expected=2);assert bad['kind']=='bad_query'
            save('whole-population-relevance',{'independent_expected':expected_rank,'pages':rank_pages,'invalid_raw_control_query':bad})
            values=json.loads(sql("SELECT jsonb_build_object('unavailable',(SELECT count(*) FROM core.search_text WHERE tokens IS NULL),'real_empty_distinct',(''::text IS NOT NULL),'index',(SELECT pg_get_indexdef(indexrelid) FROM pg_index WHERE indexrelid='core.search_text_tokens'::regclass))"))
            assert values['unavailable']==0 and values['real_empty_distinct']
            assert cli('search','repeat')['total']==1
            save('complete-search-text-and-capabilities',{'later':q,'counts':values,'checks':observations})
        scenario(PLANNED[2],metadata)
        def authority():
            nonlocal daemon
            # Stop acquisition for this direct-state fence so an unrelated source
            # commit cannot turn metadata pending and conceal a missing authority check.
            enabled=config.read_text();stop(daemon)
            config.write_text(enabled.split('[atuin]')[0].replace('sources = ["terminal.atuin-history"]','sources = []'))
            daemon=start(config,'metadata-authority-stable')
            wait('stable metadata authority basis',lambda:settled(40,True))
            reference='sinex:ev/'+query('commands where repeat')['rows'][0]['id']
            event=reference.removeprefix('sinex:ev/')
            encoded=sql("SELECT encode(convert_to(tokens,'UTF8'),'hex') FROM core.search_text WHERE id='"+event+"'::uuid")
            # Existing unavailable representation state, not an oversized-text
            # admission failure. It must retain refusal and disclosure authority.
            sql("UPDATE core.search_text SET tokens=NULL WHERE id='"+event+"'::uuid")
            stop(daemon);daemon=start(config,'unavailable-text-stable-restart')
            wait('unavailable text state after compatible restart',lambda:settled(40,False))
            fts_refusals()
            status=api('status')['result']['indexed_retrieval'];assert status['full_text']['first_affected_ref']==reference
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
                sql("UPDATE core.search_text SET tokens=convert_from(decode('"+encoded+"','hex'),'UTF8') WHERE id='"+event+"'::uuid")
                stop(daemon);config.write_text(enabled);daemon=start(config,'metadata-authority-restored')
                wait('restored acquisition basis',lambda:settled(40,True))
        scenario(PLANNED[3],authority)
        def retained():
            nonlocal daemon
            before=cli('query',selected,expected=(0,5));ref='sinex:ev/'+before['rows'][0]['id'];trace=cli('trace',ref,'--bytes')
            stop(daemon);daemon=start(config,'metadata-restart');wait('restart complete-text metadata',lambda:settled(40,True))
            assert cli('query',selected,expected=(0,5))['rows']==before['rows'] and cli('trace',ref,'--bytes')==trace
            source.rename(work/'metadata-provider-held.db');stop(daemon);daemon=start(config,'metadata-provider-loss')
            wait('missing provider reported',lambda:any(x['last_run']['status']=='unavailable' for x in api('sources.list')['result']['sources']))
            after=cli('query',selected,expected=5);assert after['rows']==before['rows'];assert cli('trace',ref,'--bytes')==trace;assert cli('query','repeat',expected=5)['total']==1
            rows=api('query.run',{'query':'events where source = terminal.atuin-history'})['result']['rows']
            large=[r for r in rows if r['payload']['native_id'] in ['frequency','positions','token-bytes']];assert len(large)==3
            unavailable_ref=query('commands where repeat')['rows'][0]['id']
            sql("UPDATE core.search_text SET tokens=NULL WHERE id='"+unavailable_ref+"'::uuid")
            fts_refusals()
            rawref='sinex:ev/'+large[0]['id'];material=cli('trace',rawref,'--bytes')['row']['material']
            for r in large:cli('delete','sinex:ev/'+r['id'],operator=True)
            wait('FTS recovers after excluded rows',lambda:settled(37,True))
            assert cli('query','commands where needle',expected=5)['total']==33
            cli('delete','--undo',rawref,operator=True)
            replay=cli('replay','materials where id = '+material,operator=True)
            assert replay['state']=='completed'
            wait('replay restores complete text',lambda:settled(38,True));assert cli('query','repeat',expected=5)['total']==1
            current=api('query.run',{'query':'events where source = terminal.atuin-history'})['result']['rows']
            restored=next(r for r in current if r['payload']['native_id']==large[0]['payload']['native_id'])
            cli('delete','sinex:ev/'+restored['id'],operator=True);wait('replay removal restores text capability',lambda:settled(37,True))
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
            uri=connections['admin'].replace('/search_text?','/fts_probe?')
            probe_sql="""CREATE EXTENSION pg_trgm;
CREATE SCHEMA core;
CREATE TABLE core.search_text(generation uuid NOT NULL,id uuid PRIMARY KEY,tokens text);
CREATE INDEX search_text_unavailable ON core.search_text(generation,id) WHERE tokens IS NULL;
CREATE INDEX search_text_tokens ON core.search_text USING gin((tokens COLLATE "C") gin_trgm_ops);
INSERT INTO core.search_text SELECT '00000000-0000-0000-0000-000000000000',md5(n::text)::uuid,chr(1)||'ordinary'||chr(1)||n::text||chr(1)||chr(2) FROM generate_series(1,20000)n;
INSERT INTO core.search_text VALUES('00000000-0000-0000-0000-000000000000',md5('raretoken')::uuid,chr(1)||'raretoken'||chr(1)||chr(2));
ANALYZE core.search_text;
INSERT INTO core.search_text SELECT '00000000-0000-0000-0000-000000000000',md5('null'||n)::uuid,NULL FROM generate_series(1,17)n;
EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT EXISTS(SELECT 1 FROM core.search_text WHERE generation='00000000-0000-0000-0000-000000000000' AND tokens IS NULL LIMIT 1);
EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT id FROM core.search_text WHERE generation='00000000-0000-0000-0000-000000000000' AND tokens IS NULL ORDER BY id LIMIT 16;
"""
            probe_sql += "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT id FROM core.search_text WHERE tokens COLLATE \"C\" LIKE ('%'||chr(1)||'raretoken'||chr(1)||'%');\n"
            probe_sql += "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT count(*) FROM core.search_text WHERE tokens COLLATE \"C\" LIKE ('%'||chr(1)||'o%');\n"
            fixture=report/'unavailable-probe-plan.sql';fixture.write_text(probe_sql)
            text=run(['psql',uri,'-XAtq','-v','ON_ERROR_STOP=1','--file',fixture],label='ordinary PostgreSQL unavailable probe plans').stdout
            decoder=json.JSONDecoder();actual=[]
            while text.strip():
                value,end=decoder.raw_decode(text.lstrip());actual.append(value[0]);text=text.lstrip()[end:]
            def nodes(plan):
                yield plan
                for child in plan.get('Plans',[]):yield from nodes(child)
            assert len(actual)==4
            for plan in actual[:2]:
                assert any(n.get('Index Name')=='search_text_unavailable' for n in nodes(plan['Plan'])),plan
            assert actual[1]['Plan']['Actual Rows']==16
            assert actual[2]['Plan']['Actual Rows']==1
            assert any(n.get('Index Name')=='search_text_tokens' for n in nodes(actual[2]['Plan'])),actual[2]
            assert any(n.get('Node Type')=='Seq Scan' for n in nodes(actual[3]['Plan'])),actual[3]
            save('unavailable-text-query-plans',{'plans':actual,'scope':'ordinary PostgreSQL exact probe predicates; twenty-thousand complete ordinary token strings, one rare-token row and seventeen unavailable states; exact rare GIN and short-pattern sequential plans, no forced planner settings or capacity/speed claim'})
        scenario(PLANNED[6],plans)
        def shell_work_tables():
            isolated=env.copy();isolated['SINEX_TEST_WRITER_URL']=h['writer']
            result=run(['cargo','test','--locked','-p','sinex-store','--features','product','--lib','shell::work_tests::native_work_tables_preserve_pending_sets_and_membership_with_indexed_plans','--','--ignored','--nocapture'],env=isolated,label='native temporary work-table constructor semantics and ordinary PostgreSQL plans')
            proof=next(line.removeprefix('SHELL_WORK_PROOF ') for line in result.stdout.splitlines() if line.startswith('SHELL_WORK_PROOF '))
            save('shell-work-table-plans',json.loads(proof))
        scenario(PLANNED[7],shell_work_tables)
    finally:
        if daemon and daemon.poll() is None:stop(daemon)
