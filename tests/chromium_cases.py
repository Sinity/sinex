"""Generated Chromium History through public clients. Provider expectations precede import."""
import datetime as dt
import hashlib
import json
import select
import shutil
import socket
import sqlite3
import subprocess
import time
import urllib.request
import uuid

PLANNED = [
    'realistic multi-profile visit/download acquisition and public paging',
    'prior unfinished late completion new GUID and periodic full comparison',
    'retained exact rows and full database bases across restart and provider loss',
    'locked rollback custody quiet reuse and changed-copy refusal',
    'partial acquisition retains acknowledged receipts without settling a prefix',
    'declared generation replacement reset and unsupported schema refusal',
]
OFFSET=11644473600000000
BASE=int(dt.datetime(2026,10,1,tzinfo=dt.timezone.utc).timestamp()*1_000_000)
SCHEMA='''
CREATE TABLE urls(id INTEGER PRIMARY KEY AUTOINCREMENT,url TEXT,title TEXT,visit_count INTEGER DEFAULT 0,typed_count INTEGER DEFAULT 0,last_visit_time INTEGER DEFAULT 0,hidden INTEGER DEFAULT 0);
CREATE TABLE visits(id INTEGER PRIMARY KEY AUTOINCREMENT,url INTEGER NOT NULL,visit_time INTEGER NOT NULL,from_visit INTEGER,transition INTEGER DEFAULT 0,segment_id INTEGER,visit_duration INTEGER DEFAULT 0,incremented_omnibox_typed_score BOOLEAN DEFAULT FALSE,opener_visit INTEGER,originator_cache_guid TEXT,originator_visit_id INTEGER,originator_from_visit INTEGER,originator_opener_visit INTEGER,is_known_to_sync BOOLEAN DEFAULT FALSE,consider_for_ntp_most_visited BOOLEAN DEFAULT FALSE,external_referrer_url TEXT);
CREATE INDEX visits_url_index ON visits(url);
CREATE TABLE downloads(id INTEGER PRIMARY KEY,guid VARCHAR NOT NULL,current_path LONGVARCHAR NOT NULL,target_path LONGVARCHAR NOT NULL,start_time INTEGER NOT NULL,received_bytes INTEGER NOT NULL,total_bytes INTEGER NOT NULL,state INTEGER NOT NULL,danger_type INTEGER NOT NULL,interrupt_reason INTEGER NOT NULL,hash BLOB NOT NULL,end_time INTEGER NOT NULL,opened INTEGER NOT NULL,last_access_time INTEGER NOT NULL,transient INTEGER NOT NULL,referrer VARCHAR NOT NULL,site_url VARCHAR NOT NULL,embedder_download_data VARCHAR NOT NULL,tab_url VARCHAR NOT NULL,tab_referrer_url VARCHAR NOT NULL,http_method VARCHAR NOT NULL,by_ext_id VARCHAR NOT NULL,by_ext_name VARCHAR NOT NULL,etag VARCHAR NOT NULL,last_modified VARCHAR NOT NULL,mime_type VARCHAR NOT NULL,original_mime_type VARCHAR NOT NULL);
CREATE TABLE downloads_url_chains(id INTEGER NOT NULL,chain_index INTEGER NOT NULL,url LONGVARCHAR NOT NULL,PRIMARY KEY(id,chain_index));
'''

def create(path,n=50000,downloads=320):
    with sqlite3.connect(path) as c:
        c.executescript(SCHEMA)
        c.executemany('INSERT INTO urls(id,url,title) VALUES(?,?,?)',[(i,f'https://example.invalid/page/{i}',f'Synthetic page {i}') for i in range(1,1001)])
        c.executemany('INSERT INTO visits(id,url,visit_time,from_visit,transition,visit_duration,external_referrer_url) VALUES(?,?,?,?,?,?,?)',[(i,(i-1)%1000+1,OFFSET+BASE+i*1000001,0 if i==1 else i-1,0x80000000 if i%101==0 else 1,0 if i==1 else 1200001,'https://referrer.invalid/') for i in range(1,n+1)])
        for i in range(1,downloads+1):
            values=[i,str(uuid.UUID(int=i)),f'/downloads/.partial-{i}',f'/downloads/item-{i}.pdf',0 if i==1 else OFFSET+BASE+i*1000001,123 if i==1 else 4096,4096,0 if i==1 else 1,0,0,b'\x00\x01',0 if i==1 else OFFSET+BASE+i*1000001+990001,0,0,0,'https://referrer.invalid/','https://example.invalid/','','https://example.invalid/tab','','GET','','','','','application/pdf','application/pdf']
            c.execute('INSERT INTO downloads VALUES('+','.join('?' for _ in values)+')',values)
            c.executemany('INSERT INTO downloads_url_chains VALUES(?,?,?)',[(i,0,f'https://example.invalid/start/{i}'),(i,1,f'https://cdn.invalid/file/{i}.pdf')])
        c.commit()
    return {'visits':n,'redirects':n//101,'downloads':downloads,'sqlite_bytes':path.stat().st_size}

def exercise(h):
    work,report,env,bins=(h[k] for k in ['WORK','REPORT','ENV','BIN_DIR'])
    run,scenario,start,stop=(h[k] for k in ['run','scenario','start_daemon','stop'])
    source=work/'History';second=work/'OtherHistory';config=work/'chromium.toml';store=work/'chromium-blobs'
    focus=h['OPTIONS'].chromium_focus
    sdk_python=h['OPTIONS'].chromium_sdk_python
    expected=create(source,120,4) if focus else create(source);other=create(second,7,3)
    endpoint=f'http://127.0.0.1:{h["free_port"]()}/rpc';token=(work/'token').read_text()
    generation='95d3d942-c8a4-444e-8016-29d2e312664c';other_generation='86d3d942-c8a4-444e-8016-29d2e312664c'
    daemon=mcp=None;sequence=0;first=None;selected={};controls=[]
    def save(name,value): (report/(name+'.json')).write_text(json.dumps(value,indent=2)+'\n')
    save('chromium-independent-population',{'main':expected,'other':other,'upstream':'Chromium 3260a51c368eafe63bd55f037afd28d784ed03d4 visit_database.cc/download_database.cc/download_constants.h','mapping':'download mapping is new History SQL support, not predecessor extension parity'})
    current_generation=generation
    def configure(full=900000,path=source,gen=generation,profiles=True,snapshot=3600000):
        nonlocal current_generation,visit_query,download_query
        current_generation=gen
        visit_query='events where source = browser.history.chromium and kind = page.visited and payload.profile = main and payload.database_generation = "'+gen+'"'
        download_query='events where source = browser.history.chromium and kind = download.observed and payload.profile = main and payload.database_generation = "'+gen+'"'
        config.write_text(f'''listen = "{endpoint.removeprefix('http://').removesuffix('/rpc')}"
state_dir = "{work/'chromium-state'}"
store_dir = "{store}"
writer_connection_file = "{work/'writer'}"
reader_connection_file = "{work/'reader'}"
token_file = "{work/'token'}"
sources = ["browser.history.chromium"]
automata = []
[[chromium]]
profile = "main"
database_generation = "{gen}"
path = "{path}"
poll_interval_ms = 2500
full_interval_ms = {full}
snapshot_interval_ms = {snapshot}
'''+(f'''[[chromium]]
profile = "other"
database_generation = "{other_generation}"
path = "{second}"
poll_interval_ms = 2500
full_interval_ms = {full}
snapshot_interval_ms = {snapshot}
''' if profiles else ''))
    def api(method='status',params=None):
        req=urllib.request.Request(endpoint,data=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params or {}}).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
        with urllib.request.urlopen(req,timeout=25) as response: answer=json.load(response)
        with (report/'chromium-api.jsonl').open('a') as log: log.write(json.dumps({'method':method,'params':params,'response':answer})+'\n')
        return answer
    def cli(command,*args,expected=(0,5)):
        p=run([bins/'sinexctl','--endpoint',endpoint,'--token-file',work/'token',command,*args,'--json'],expected=expected,label='Chromium '+command)
        return json.loads(p.stdout or p.stderr)
    def wait(label,fn,seconds=600,context=None):
        deadline=time.monotonic()+seconds
        while time.monotonic()<deadline:
            assert daemon.poll() is None,'Chromium daemon exited'
            try: value=fn()
            except (OSError,KeyError):value=None
            if value:return value
            time.sleep(.1)
        raise AssertionError({"waiting_for":label,"last_context":context} if context is not None else label)
    def status(profile='main'):
        return next((s for s in api('sources.list')['result']['sources'] if json.loads(s['locator'])==[profile,current_generation if profile=='main' else other_generation]),None)
    def settled(after=None,kind=None,profile='main',state='completed'):
        s=status(profile)
        if not s:return None
        o=s['last_run'];p=o['metadata'].get('browser_read_plan_v1',{})
        return s if o['status']==state and o['id']!=after and (kind is None or p.get('kind')==kind) and (state!='completed' or o['metadata'].get('_sinex_acquisition_v1',{}).get('schedule')) else None
    def initial_full(profile):
        current=status(profile)
        if not current:return None
        page=current
        while True:
            candidates=[page['last_run']]+page.get('history',{}).get('rows',[])
            found=next((o for o in candidates if o['status']=='completed' and o['metadata'].get('browser_read_plan_v1',{}).get('kind')=='full_value_comparison' and o['metadata'].get('_sinex_acquisition_v1',{}).get('schedule')),None)
            if found:
                return dict(current,last_run=found,selected_historical_observation=found['id']!=current['last_run']['id'])
            before=page.get('history',{}).get('next_before_seq')
            if before is None:return None
            page=api('sources.list',{'input':current['input'],'before_seq':before})['result']['sources'][0]
    def next_poll(s,kind=None,**kw):return wait('completed Chromium '+str(kind),lambda:settled(s['last_run']['id'],kind,**kw))
    def plan(s):return s['last_run']['metadata']['browser_read_plan_v1']
    def sql(query):
        p=run(['psql',h['reader'],'-XAt','-v','ON_ERROR_STOP=1','-c',query],label='retained Chromium inspection')
        return json.loads(p.stdout)
    def query(text,cursor=None):
        value=api('query.run',{'query':text,**({'cursor':cursor} if cursor else {})})
        assert 'result' in value,value
        return value['result']
    def invoke(method,params):
        nonlocal sequence
        sequence+=1;mcp.stdin.write(json.dumps({'jsonrpc':'2.0','id':sequence,'method':method,'params':params})+'\n');mcp.stdin.flush()
        end=time.monotonic()+35
        while True:
            ready,_,_=select.select([mcp.stdout],[],[],max(0,end-time.monotonic()));assert ready,'MCP deadline'
            response=json.loads(mcp.stdout.readline())
            with (report/'chromium-mcp.jsonl').open('a') as log:log.write(json.dumps(response)+'\n')
            if response.get('id')==sequence:return response
    def tool(name,arguments):
        result=invoke('tools/call',{'name':name,'arguments':arguments})['result']
        return result.get('structuredContent') or json.loads(result['content'][0]['text'])
    def connect():
        nonlocal mcp
        err=(report/'chromium-mcp.stderr').open('a');argv=[str(bins/'sinexctl'),'--endpoint',endpoint,'--token-file',str(work/'token'),'mcp']
        mcp=subprocess.Popen(argv,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,text=True,bufsize=1)
        h['CHILDREN'].append((mcp,mcp.stdout,err));h['PROCESSES'].append({'pid':mcp.pid,'argv':argv,'stderr':'chromium-mcp.stderr'})
        invoke('initialize',{'protocolVersion':'2024-11-05','capabilities':{},'clientInfo':{'name':'chromium-public-qualification','version':'1'}})
        mcp.stdin.write(json.dumps({'jsonrpc':'2.0','method':'notifications/initialized'})+'\n');mcp.stdin.flush()
        names=json.dumps(invoke('tools/list',{}));assert all(n in names for n in ['sinex_query','sinex_show','sinex_trace'])
    def close_mcp():
        nonlocal mcp
        if mcp is not None:
            if mcp.poll() is None:
                mcp.stdin.close();mcp.wait(timeout=30)
            assert mcp.returncode==0,('MCP graceful exit',mcp.returncode)
            mcp=None
    visit_query='events where source = browser.history.chromium and kind = page.visited and payload.profile = main'
    download_query='events where source = browser.history.chromium and kind = download.observed and payload.profile = main'
    def one_visit(i):
        # URL repeats intentionally; exact occurrence identity remains provider visit ID.
        answer=query(visit_query+' and when = "'+(dt.datetime.fromtimestamp((BASE+i*1000001)/1000000,tz=dt.timezone.utc).isoformat().replace('+00:00','Z'))+'"')
        return next(r for r in answer['rows'] if r['payload']['visit_id']==i)
    def one_download(i):
        answer=query(download_query+' and payload.guid = "'+str(uuid.UUID(int=i))+'"')
        assert len(answer['rows'])==1,answer
        return answer['rows'][0]
    def sdk_public(label,text,baseline=None):
        if not sdk_python:return
        script=h['ROOT']/'tests/chromium_sdk_client.py';output=report/(label+'-official-sdk')
        run([sdk_python,script,'--binary',bins/'sinexctl','--endpoint',endpoint,'--token-file',work/'token','--query',text,'--output',output],label='official MCP SDK Chromium '+label)
        recorded=json.loads((output/'result.json').read_text())
        assert recorded['success'] and recorded['sdk_context_closed'] and recorded['sdk']['version']=='1.26.0',recorded
        cli_answer,shown,traced=baseline if baseline else (cli('query',text),None,None)
        assert recorded['query']['rows']==cli_answer['rows'] and recorded['query']['total']==cli_answer['total']
        reference='sinex:ev/'+cli_answer['rows'][0]['id']
        assert recorded['reference']==reference
        shown=shown or cli('show',reference)['row'];traced=traced or cli('trace',reference,'--bytes')['row']
        assert recorded['show']['row']==shown and 'record' not in recorded['trace']['row']
        assert all(recorded['trace']['row'][key]==traced[key] for key in ('material','anchor','end'))
    def independent_digest(path):
        # Closing a main-file FD in the SQLite writer process releases its POSIX
        # record locks. Inspect bytes only from another process while it is locked.
        program='import hashlib,json,sys;from pathlib import Path;p=Path(sys.argv[1]);print(json.dumps([hashlib.sha256(p.read_bytes()).hexdigest(),hashlib.sha256(Path(str(p)+"-journal").read_bytes()).hexdigest()]))'
        return json.loads(run([h['sys'].executable,'-c',program,path],label='independent locked provider digest').stdout)
    def assert_provider_locked(path):
        program='import sqlite3,sys;c=sqlite3.connect("file:"+sys.argv[1]+"?mode=ro",uri=True,timeout=0);print(c.execute("SELECT count(*) FROM visits").fetchone())'
        result=run([h['sys'].executable,'-c',program,path],expected=1,label='external SQLite exclusive-lock control')
        assert 'database is locked' in result.stderr,result.stderr
    def equivalent_public(label,text):
        cli_answer=cli('query',text)
        agent=tool('sinex_query',{'query':text})
        assert cli_answer['rows']==agent['rows'] and cli_answer['total']==agent['total']
        row=cli_answer['rows'][0];reference='sinex:ev/'+row['id']
        shown=cli('show',reference)['row'];agent_show=tool('sinex_show',{'ref':reference})['row'];assert shown==agent_show
        traced=cli('trace',reference,'--bytes')['row'];agent_trace=tool('sinex_trace',{'ref':reference})['row'];assert 'record' not in agent_trace
        assert traced['material']==agent_trace['material'] and traced['anchor']==agent_trace['anchor'] and traced['end']==agent_trace['end']
        assert traced['record']['_from']['table'] in ('visits','downloads')
        sdk_public(label,text,(cli_answer,shown,traced))
        save(label,{'CLI':cli_answer,'MCP':agent,'show':shown,'trace':traced,'MCP_trace':agent_trace})
        return cli_answer,traced
    def mutate(statement,parameters=()):
        with sqlite3.connect(source) as c:c.execute(statement,parameters);c.commit()
    try:
        if sdk_python:
            save('official-sdk-runtime',{'interpreter':str(sdk_python),'interpreter_resolved':str(sdk_python.resolve()),'interpreter_sha256':hashlib.sha256(sdk_python.read_bytes()).hexdigest(),'client_sha256':hashlib.sha256((h['ROOT']/'tests/chromium_sdk_client.py').read_bytes()).hexdigest(),'recording_seam_sha256':'c960ce13a42c3daa5f0d17d6dabeb35971cfb0b1217b19bced77a8158365a9c6'})
        if not focus:
            configure();daemon=start(config,'chromium');wait('startup',lambda:api().get('result'));connect()
        def population():
            nonlocal first
            other_first=wait('full other history',lambda:initial_full('other'))
            first=wait('full main history',lambda:initial_full('main'))
            assert first['last_run']['rows_read']==expected['visits']+expected['downloads'],first
            assert other_first['last_run']['rows_read']==other['visits']+other['downloads']
            answer=query(visit_query);assert answer['total']==expected['visits']-expected['redirects'],answer['total']
            # Traverse all pages without using imported IDs to seed answers.
            ids=set();count=0
            while True:
                for r in answer['rows']:
                    assert r['payload']['profile']=='main' and not r['payload']['redirect']
                    assert r['id'] not in ids;ids.add(r['id']);count+=1
                cursor=answer['page']['cursor']
                if not cursor:break
                answer=query(visit_query,cursor)
            assert count==expected['visits']-expected['redirects']
            hops=query(visit_query+' and payload.redirect = true');assert hops['total']==expected['redirects']
            assert query(download_query)['total']==expected['downloads']
            a=one_visit(1);assert a['payload']['duration_us']==0 and a['when']=='2026-10-01T00:00:01.000001000Z'
            assert a['payload']['duration_basis'].endswith('attention is unknown')
            assert a['payload']['referrer_url']=='https://referrer.invalid/' and a['payload']['referrer_basis']=='provider external_referrer_url'
            d=one_download(1);assert d['payload']['state_label']=='in_progress' and d['payload']['end_unix_us'] is None and d['when'] is None and d['time_quality']=='unknown'
            assert d['payload']['url_chain']==['https://example.invalid/start/1','https://cdn.invalid/file/1.pdf']
            equivalent_public('visit-public-clients',visit_query+' and payload.url = "https://example.invalid/page/1"')
            equivalent_public('download-public-clients',download_query+' and payload.guid = "'+str(uuid.UUID(int=1))+'"')
            reduced=next_poll(first,'new_keys_and_prior_unfinished');assert reduced['last_run']['rows_read']==2,reduced
            assert plan(reduced)['download_keys_examined']==expected['downloads']
            assert reduced['last_run']['metadata']['browser_checkpoint_v1']['full_due_at']==first['last_run']['metadata']['browser_checkpoint_v1']['full_due_at']
            assert 'physical_snapshot' not in reduced['last_run']['metadata']
            save('population-and-reduced',{'first':first,'other':other_first,'reduced':reduced,'pages_counted':count,'selected_reduction':'50320 values to 2; 320 GUIDs still examined; canonical eligibility still scans admitted historical events; no whole-loop IO/CPU claim'})
            with sqlite3.connect(source) as c:
                plans={k:list(c.execute('EXPLAIN QUERY PLAN '+v)) for k,v in {'new':'SELECT id FROM visits WHERE id>50000 ORDER BY id LIMIT 64','prior':'SELECT * FROM visits WHERE id=1','download_keys':'SELECT id,guid FROM downloads WHERE id>0 ORDER BY id LIMIT 64'}.items()}
            assert 'INTEGER PRIMARY KEY' in str(plans['new']) and 'INTEGER PRIMARY KEY' in str(plans['prior'])
            save('provider-query-plans',plans)
            from recovery_costs import process,filesystem
            save('population-phase-resources',{'daemon':process(daemon.pid,'generated Chromium daemon'),'postgres':process(int((work/'pg/postmaster.pid').read_text().splitlines()[0]),'owned PostgreSQL'),'retained_store':filesystem(store),'scope':'named phase samples; no simultaneous aggregate peak or device IO claim'})
            with sqlite3.connect(source.as_uri()+'?mode=ro',uri=True) as c:
                baseline=dict(zip(['id','visit_time','visit_duration','url','title'],c.execute('SELECT v.id,v.visit_time,v.visit_duration,u.url,u.title FROM visits v JOIN urls u ON u.id=v.url WHERE v.id=1').fetchone()))
            assert (a['payload']['visit_id'],a['payload']['visit_time_us'],a['payload']['duration_us'],a['payload']['url'],a['payload']['title'])==tuple(baseline[k] for k in ['id','visit_time','visit_duration','url','title'])
            damaged=dict(a['payload'],duration_us=1)
            assert damaged['duration_us']!=baseline['visit_duration'];controls.append('false positive duration/finished interpretation distinguished from zero-duration provider fact')
            save('equal-evidence-direct-provider-baseline',{'independently_selected_fact':baseline,'native_interpretation':a,'scope':'the fixed fact ties while the synthetic provider is accessible; retained citation and loss behavior are separate, no general superiority claim','negative_control':damaged})
        if not focus:scenario(PLANNED[0],population)
        def updates():
            nonlocal daemon
            def ordinary_values(after,label,visit_duration=None,download_fields=None,download_key=1):
                # The next completed poll may have pinned its SQLite view before
                # mutation. Wait for facts and inspect their actual capture, not
                # an assumed mutation-to-poll ordering.
                target=after['last_run']['metadata']['browser_checkpoint_v1']['full_due_at']
                context={'after_observation':after['last_run']['id'],'expected_visit_duration':visit_duration,'expected_download':download_fields,'expected_full_due_at':target}
                def observed():
                    current=settled(after['last_run']['id'],'new_keys_and_prior_unfinished')
                    if not current:return None
                    context['latest_observation']=current['last_run']
                    assert current['read_checkpoint']['full_due_at']==target,context
                    visit=one_visit(1) if visit_duration is not None else None
                    answer=query(download_query+' and payload.guid = "'+str(uuid.UUID(int=download_key))+'"') if download_fields else None
                    candidates=answer['rows'] if answer else []
                    assert len(candidates)<=1,context|{'actual_downloads':candidates}
                    download=candidates[0] if candidates else None
                    context.update(actual_visit=visit,actual_download=download)
                    if visit_duration is not None and visit['payload']['duration_us']!=visit_duration:return None
                    if download_fields and (download is None or any(download['payload'].get(k)!=v for k,v in download_fields.items())):return None
                    captures=[];context['captures']=captures
                    for row in (visit,download):
                        if row is None:continue
                        trace=cli('trace','sinex:ev/'+row['id'],'--bytes')['row']
                        detail=api('sources.list',{'input':current['input'],'observation':trace['material_observation']})['result']['sources'][0]
                        capture=detail['receipts']['observation'];captures.append(capture)
                        assert capture['id']==trace['material_observation'] and capture['status']=='completed',context
                        assert capture['metadata']['browser_read_plan_v1']['kind']=='new_keys_and_prior_unfinished',context
                        assert capture['metadata']['browser_checkpoint_v1']['full_due_at']==target,context
                    chosen=captures[-1]
                    save(label+'-ordinary-poll-evidence',context)
                    return dict(current,last_run=chosen,selected_historical_observation=chosen['id']!=current['last_run']['id']),visit,download
                return wait(label,observed,context=context)
            before=one_visit(1);old=one_download(1);s=wait('reduced before update',lambda:settled(kind='new_keys_and_prior_unfinished'))
            mutate('UPDATE visits SET visit_duration=98000001 WHERE id=1')
            # Numeric coordinates may change; the same GUID remains one item.
            with sqlite3.connect(source) as c:
                c.execute('UPDATE downloads SET id=1001,state=4,interrupt_reason=40,end_time=? WHERE id=1',(OFFSET+BASE+90000000,))
                c.execute('UPDATE downloads_url_chains SET id=1001 WHERE id=1');c.commit()
            s,late_visit,interrupted=ordinary_values(s,'late-visit-and-interruption',98000001,{'state':4,'download_id':1001})
            assert late_visit['id']!=before['id'] and late_visit['payload']['duration_us']==98000001,{'before':before,'actual':late_visit,'observation':s['last_run']}
            assert interrupted['id']!=old['id'] and interrupted['payload']['state_label']=='interrupted',{'before':old,'actual':interrupted,'observation':s['last_run']}
            assert interrupted['payload']['download_id']==1001,interrupted
            unchanged=next_poll(s,'new_keys_and_prior_unfinished');assert unchanged['last_run']['rows_read']==1
            assert one_download(1)['id']==interrupted['id']
            due_at=unchanged['last_run']['metadata']['browser_checkpoint_v1']['full_due_at']
            mutate('UPDATE downloads SET state=0,received_bytes=2048,end_time=0 WHERE id=1001')
            resumed,_,running=ordinary_values(unchanged,'resumed-download',download_fields={'state':0,'received_bytes':2048,'download_id':1001})
            assert resumed['last_run']['rows_read']==1,resumed
            assert running['id']!=interrupted['id'] and running['payload']['state_label']=='in_progress' and running['payload']['received_bytes']==2048,{'before':interrupted,'actual':running,'observation':resumed['last_run']}
            equivalent_public('resumed-download-public-clients',download_query+' and payload.guid = "'+str(uuid.UUID(int=1))+'"')
            mutate('UPDATE downloads SET state=1,received_bytes=4096,end_time=? WHERE id=1001',(OFFSET+BASE+120000000,))
            completed,_,done=ordinary_values(resumed,'completed-download',download_fields={'state':1,'received_bytes':4096,'download_id':1001})
            assert completed['last_run']['rows_read']==1,completed
            assert done['id']!=running['id'] and done['payload']['state_label']=='complete' and done['payload']['received_bytes']==4096,{'before':running,'actual':done,'observation':completed['last_run']}
            assert query(download_query)['total']==expected['downloads']
            assert all(v['last_run']['metadata']['browser_checkpoint_v1']['full_due_at']==due_at for v in (resumed,completed))
            equivalent_public('completed-download-public-clients',download_query+' and payload.guid = "'+str(uuid.UUID(int=1))+'"')
            save('same-GUID-interrupted-resumed-completed',{'interrupted':interrupted,'resumed':running,'completed':done,'ordinary_polls':[unchanged,resumed,completed],'identity':'one persisted GUID; versions differ; numeric coordinate is acquisition-only'})
            empty=next_poll(completed,'new_keys_and_prior_unfinished');assert empty['last_run']['rows_read']==0
            # A new persisted GUID may reuse a numeric download coordinate.
            replacement=str(uuid.UUID(int=10001));mutate('UPDATE downloads SET guid=?,state=1 WHERE id=2',(replacement,))
            fresh,_,replacement_row=ordinary_values(empty,'reused-coordinate-new-GUID',download_fields={'guid':replacement,'state':1,'download_id':2},download_key=10001)
            assert fresh['last_run']['rows_read']==1,{'actual':replacement_row,'observation':fresh['last_run']}
            assert query(download_query+' and payload.guid = "'+replacement+'"')['total']==1
            assert query(download_query+' and payload.guid = "'+str(uuid.UUID(int=2))+'"')['total']==1 # no fabricated deletion
            original=one_visit(3);mutate('UPDATE visits SET visit_duration=5555555 WHERE id=3')
            s=next_poll(fresh,'new_keys_and_prior_unfinished');assert one_visit(3)['id']==original['id']
            # Restart forces a real full. Then a due full finds a second completed-row edit.
            close_mcp();stop(daemon);configure(full=12000);daemon=start(config,'chromium-full');wait('reopened full',lambda:settled(kind='full_value_comparison'));connect()
            wait('completed edit after real full',lambda:one_visit(3)['payload']['duration_us']==5555555)
            original=one_visit(4);mutate('UPDATE visits SET visit_duration=7777777 WHERE id=4')
            wait('periodic full value comparison',lambda:one_visit(4)['payload']['duration_us']==7777777)
            assert one_visit(4)['id']!=original['id']
            save('late-update-control',{'old_unfinished':before,'completed':one_visit(1),'download':one_download(1),'replacement_GUID':replacement,'periodic':one_visit(4),'missing_GUID_deletion_inference':'not admitted'})
        if not focus:scenario(PLANNED[1],updates)
        def retained():
            nonlocal daemon
            baseline,trace=equivalent_public('before-provider-loss',download_query+' and payload.guid = "'+str(uuid.UUID(int=1))+'"')
            main_snapshot=first['last_run']['metadata']['physical_snapshot'];material=main_snapshot['material']
            digest=main_snapshot['blake3'].removeprefix('blake3:');blob=store/'blobs'/digest[:2]/(digest+'.zst')
            owned=work/'decoded-retained-History'
            with owned.open('wb') as out:
                result=subprocess.run(['zstd','-q','-d','-c',str(blob)],stdout=out,stderr=subprocess.PIPE,env=env);assert result.returncode==0
            with sqlite3.connect(owned.as_uri()+'?mode=ro',uri=True) as c:
                assert c.execute('PRAGMA quick_check').fetchall()==[('ok',)]
                assert c.execute('SELECT count(*) FROM visits').fetchone()[0]==50000
                assert c.execute('SELECT state FROM downloads WHERE id=1').fetchone()[0]==0 # exact old base, not present provider state
            owned.unlink();close_mcp();stop(daemon)
            source.rename(work/'provider-held');second.rename(work/'other-held')
            daemon=start(config,'chromium-provider-loss');wait('provider loss outcome',lambda:settled(state='unavailable'));connect()
            failed,retained_trace=equivalent_public('after-provider-loss',download_query+' and payload.guid = "'+str(uuid.UUID(int=1))+'"')
            assert failed['rows']==baseline['rows'] and failed['complete'] is False and retained_trace['record']==trace['record']
            save('retained-costs',sql("SELECT jsonb_build_object('observations',(SELECT count(*) FROM raw.observations),'retained_logical_bytes',(SELECT sum(durable_bytes) FROM raw.source_material_registry),'event_versions',(SELECT count(*) FROM core.events),'snapshot_bases',(SELECT count(*) FROM raw.source_material_registry WHERE material_kind='file'),'scope','generated fixture logical custody dimensions; compressed file sizes below, no throughput or inclusive CPU/IO claim')"))
            save('physical-blob-files',{'files':len(list((store/'blobs').rglob('*.zst'))),'compressed_bytes':sum(p.stat().st_size for p in (store/'blobs').rglob('*.zst')),'snapshot_material':material})
            close_mcp();stop(daemon);(work/'provider-held').rename(source);(work/'other-held').rename(second)
        if not focus:scenario(PLANNED[2],retained)
        # Smaller private-safe generated database exercises locked custody without rereading the large baseline.
        def locked():
            nonlocal daemon
            locked_path=work/'LockedHistory';create(locked_path,120,4)
            generation_locked='77d3d942-c8a4-444e-8016-29d2e312664c'
            con=sqlite3.connect(locked_path);con.execute('PRAGMA locking_mode=EXCLUSIVE');con.execute('PRAGMA cache_size=1');con.execute('BEGIN EXCLUSIVE');con.execute('UPDATE visits SET visit_duration=999999999')
            assert_provider_locked(locked_path);before=independent_digest(locked_path);assert_provider_locked(locked_path)
            configure(path=locked_path,gen=generation_locked,profiles=False);daemon=start(config,'chromium-locked')
            try:
                s=wait('locked owned recovery',lambda:settled(kind='full_value_comparison'))
                assert plan(s)['custody']['kind']=='verified_locked_rollback_copy',s
                assert query(visit_query)['rows'][0]['payload']['duration_us']==0
                quiet=next_poll(s,'new_keys_and_prior_unfinished');assert plan(quiet)['custody']['kind']=='verified_locked_copy_reused'
                after=independent_digest(locked_path);assert_provider_locked(locked_path);assert before==after
                sdk_public('locked-custody-public',visit_query+' and payload.url = "https://example.invalid/page/1"')
                save('locked-custody',{'first':s,'quiet':quiet,'provider_bytes_unchanged':True})
            finally:stop(daemon);con.rollback();con.close()
            # Actual copy-boundary mutation must invalidate copied data, retry, and never settle that copy.
            con=sqlite3.connect(locked_path);con.execute('PRAGMA locking_mode=EXCLUSIVE');con.execute('BEGIN EXCLUSIVE');con.execute('UPDATE visits SET visit_duration=888888888')
            boundary=work/'browser-copy.sock';listener=socket.socket(socket.AF_UNIX);listener.bind(str(boundary));listener.listen(1);listener.settimeout(30)
            daemon=start(config,'chromium-copy-race','--test-pause-at','browser-copy-before-check','--test-pause-socket',boundary)
            try:
                connection,_=listener.accept();message=b''
                while not message.endswith(b'\n'):message+=connection.recv(65536)
                notice=json.loads(message);assert notice['pause_point']=='browser-copy-before-check'
                con.rollback();con.close() # journal disappears during the owned copy
                connection.sendall(b'c');connection.close()
                s=wait('retry after journal disappearance',lambda:settled(kind='full_value_comparison'))
                assert plan(s)['custody']['attempts']>=2,plan(s)
                assert query(visit_query)['rows'][0]['payload']['duration_us']==0
                save('copy-race-refused-and-retried',{'boundary':notice,'result':s})
            finally:listener.close();stop(daemon)
            con=sqlite3.connect(locked_path);con.execute('PRAGMA locking_mode=EXCLUSIVE');con.execute('BEGIN EXCLUSIVE')
            boundary.unlink();listener=socket.socket(socket.AF_UNIX);listener.bind(str(boundary));listener.listen(1);listener.settimeout(30)
            daemon=start(config,'chromium-journal-appearance','--test-pause-at','browser-copy-before-check','--test-pause-socket',boundary)
            try:
                connection,_=listener.accept();message=b''
                while not message.endswith(b'\n'):message+=connection.recv(65536)
                notice=json.loads(message);assert notice['evidence']['source_pair']['journal'] is None
                con.execute("UPDATE urls SET title='uncommitted journal appearance'")
                connection.sendall(b'c');connection.close()
                s=wait('retry after journal appearance',lambda:settled(kind='full_value_comparison'))
                assert plan(s)['custody']['attempts']>=2 and query(visit_query)['rows'][0]['payload']['title']=='Synthetic page 1'
                save('journal-appearance-refused-and-retried',{'boundary':notice,'result':s})
            finally:listener.close();stop(daemon);con.rollback();con.close()
        scenario(PLANNED[3],locked)
        def partial():
            nonlocal daemon
            path=work/'PartialHistory';create(path,192,4);gen='33d3d942-c8a4-444e-8016-29d2e312664c'
            with sqlite3.connect(path) as c:c.execute('UPDATE urls SET title=? WHERE id=130',('x'*1100000,));c.commit()
            configure(path=path,gen=gen,profiles=False);daemon=start(config,'chromium-partial')
            failed=wait('retained prefix unavailable',lambda:settled(state='unavailable'))
            assert failed['last_run']['rows_read']==128 and 'before provider transfer' in failed['last_run']['reason'],failed
            assert 'browser_checkpoint_v1' not in failed['last_run']['metadata']
            carry=sql("SELECT carry FROM raw.source_cursors WHERE input='"+failed['input']+"' AND binding='browser.history.chromium'");assert carry=={}
            old_observation=failed['last_run']['id']
            old_receipts=sql("SELECT coalesce(jsonb_agg(e.payload->'visit_id' ORDER BY (e.payload->>'visit_id')::bigint),'[]') FROM core.events e JOIN raw.source_material_registry m ON m.id=e.source_material_id WHERE m.observation_id='"+old_observation+"'")
            assert old_receipts==list(range(1,129)),old_receipts
            with sqlite3.connect(path) as c:c.execute("UPDATE urls SET title='repaired title' WHERE id=130");c.commit()
            recovered=wait('partial recovery full',lambda:settled(kind='full_value_comparison'))
            assert recovered['last_run']['rows_read']==196 and recovered['last_run']['repeated']==128,recovered
            same_receipts=sql("SELECT coalesce(jsonb_agg(e.payload->'visit_id' ORDER BY (e.payload->>'visit_id')::bigint),'[]') FROM core.events e JOIN raw.source_material_registry m ON m.id=e.source_material_id WHERE m.observation_id='"+old_observation+"'")
            assert same_receipts==old_receipts
            # The repair admits 129+ only under its own observation; later retained reads cannot fabricate old recovery.
            direct=api('sources.list',{'input':failed['input'],'observation':old_observation})['result']['sources'][0]
            assert direct['receipts']['observation']['id']==old_observation
            seen=[];after=None
            while True:
                page=api('sources.list',{'input':failed['input'],'observation':old_observation,**({'after_receipt':after} if after else {})})['result']['sources'][0]['receipts']
                seen += [r['event'] for r in page['rows']]
                if page['complete']:break
                after=page['next_after_receipt'];assert after
            assert len(seen)==128 and len(set(seen))==128
            save('partial-acquisition-ownership',{'failed':failed,'checkpoint_before':carry,'recovered':recovered,'old_visit_ids':old_receipts,'public_receipt_ids':seen,'old_receipts_stable':True})
            stop(daemon)
        scenario(PLANNED[4],partial)
        def refusals():
            nonlocal daemon
            path=work/'GuardHistory';create(path,30,2);gen='66d3d942-c8a4-444e-8016-29d2e312664c';configure(path=path,gen=gen,profiles=False)
            daemon=start(config,'chromium-guard');s=wait('guard initial',lambda:settled(kind='full_value_comparison'))
            initial_schema=s['read_checkpoint']['schema_version']
            with sqlite3.connect(path) as c:c.execute('ALTER TABLE visits ADD COLUMN retained_extra INTEGER DEFAULT 9');c.commit()
            altered=wait('actual schema invalidation full',lambda:settled(s['last_run']['id'],'full_value_comparison'))
            assert altered['read_checkpoint']['schema_version']!=initial_schema and altered['last_run']['rows_read']==32
            with sqlite3.connect(path) as c:c.execute('VACUUM')
            vacuum=wait('VACUUM full comparison',lambda:settled(altered['last_run']['id'],'full_value_comparison'))
            assert vacuum['last_run']['rows_read']==32 and vacuum['read_checkpoint']['identity']==altered['read_checkpoint']['identity']
            checkpoint=vacuum['read_checkpoint']
            with sqlite3.connect(path) as c:c.execute('UPDATE downloads SET guid=? WHERE id=2',(str(uuid.UUID(int=1)),));c.commit()
            duplicate=wait('duplicate GUID refusal',lambda:settled(state='unavailable'))
            assert duplicate['last_run']['rows_read']==0 and 'duplicate' in duplicate['last_run']['reason'] and duplicate['read_checkpoint']==checkpoint
            with sqlite3.connect(path) as c:c.execute('UPDATE downloads SET guid=? WHERE id=2',(str(uuid.UUID(int=2)),));c.commit()
            s=wait('GUID repair forces reopened full',lambda:settled(duplicate['last_run']['id'],'full_value_comparison'))
            save('schema-vacuum-GUID-basis',{'altered':altered,'vacuum':vacuum,'duplicate':duplicate,'recovered':s})
            replacement=work/'ReplacementHistory';create(replacement,30,2);path.rename(work/'OldGuardHistory');replacement.rename(path)
            failed=wait('declared generation replacement refusal',lambda:settled(state='unavailable'))
            assert 'reconciliation required' in failed['last_run']['reason']
            stop(daemon);daemon=start(config,'chromium-guard-restart');failed2=wait('persistent generation fence',lambda:settled(state='unavailable'));assert 'fenced' in failed2['last_run']['reason'];stop(daemon)
            # Explicit different generation is reconciliation, never inferred from the new file.
            newgen='55d3d942-c8a4-444e-8016-29d2e312664c';configure(path=path,gen=newgen,profiles=False);daemon=start(config,'chromium-reconciled');s=wait('explicit generation',lambda:settled(kind='full_value_comparison'))
            with sqlite3.connect(path) as c:c.execute("UPDATE sqlite_sequence SET seq=0 WHERE name='visits'");c.commit()
            reset=wait('same-file visit sequence reset refusal',lambda:settled(state='unavailable'));assert 'reset' in reset['last_run']['reason'];stop(daemon)
            # Valid empty tables are not required-column failure; unsupported schema is not accessible-empty.
            invalid=work/'UnsupportedHistory';sqlite3.connect(invalid).execute('CREATE TABLE visits(id INTEGER PRIMARY KEY)');configure(path=invalid,gen='44d3d942-c8a4-444e-8016-29d2e312664c',profiles=False);daemon=start(config,'chromium-schema-refusal')
            unsupported=wait('unsupported schema',lambda:settled(state='unavailable'));assert unsupported['last_run']['rows_read']==0
            save('generation-and-schema-refusals',{'replacement':failed,'restart':failed2,'reset':reset,'unsupported':unsupported,'limits':'explicit generation continuity premise; no census/deletion, arbitrary overwrite detection, page patches, independent backup or live import qualification'})
        scenario(PLANNED[5],refusals)
    finally:
        if mcp and mcp.poll() is None:close_mcp()
        if daemon and daemon.poll() is None:stop(daemon)
