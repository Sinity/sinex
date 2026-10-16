"""Selected two-source policy grouping through real clients, never fixture group admission."""
import contextlib
import concurrent.futures
import hashlib
import http.client
import importlib.util
import json
from pathlib import Path
import select
import signal
import socket
import sqlite3
import subprocess
import time
import urllib.error
import urllib.request
import uuid

ORACLE_REVISION='b2373e95f054b327bb13fad357b74b9b06a207c4'
ORACLE_DIR='sinex-spec/prototype/fusion'
PLANNED=[
    'pinned independent selected journey and original sources',
    'before-late exact members fields temporal evidence and retained bindings',
    'observed committed pending replacement kill and same-state restart',
    'late-settled actual CLI API MCP and failed-command question',
    'exact raw exclusion replay undo and provider-free restoration',
    'global distance symmetric uniqueness exact boundaries and source identity',
    'complete competition across hours and removal reconstruction',
    'pinned pass live admission fairness and compatible restart',
    'zsh original bytes Unicode continuation duplicate and rewrite refusal',
    'lossy privacy and historical comparison basis refusal and replay recovery',
    'cross-material replay fixed-head basis and divergent newer reading refusal',
    'pending replay same-hash new-head race preserves committed ranges',
    'canonical reconstruction and strict missing-log refusal',
]


def planned(focus):
    if focus=='source-codec':return [PLANNED[0],PLANNED[8]]
    return [PLANNED[0],'independent changed-seam source setup',PLANNED[11],PLANNED[12],PLANNED[8]] if focus else PLANNED


def exercise(h):
    work,report,bins=(h[k] for k in ['WORK','REPORT','BIN_DIR'])
    run,scenario,start,stop=(h[k] for k in ['run','scenario','start_daemon','stop'])
    token='synthetic-foundation-token';operator='synthetic-fusion-operator'
    (work/'operator').write_text(operator)
    (work/'read-model').write_text(h['reader'].replace('sinex_reader','sinex_read_model'))
    source=work/'atuin.db';zsh=work/'zsh-history';store=work/'fusion-store';config=work/'fusion.toml'
    port=h['free_port']();endpoint=f'http://127.0.0.1:{port}/rpc'
    raw_a='events where source = terminal.atuin-history and kind = command.executed'
    raw_z='events where source = terminal.zsh-history and kind = command.imported'
    oracle=report/'oracle';oracle.mkdir()
    manifest={'revision':ORACLE_REVISION,'files':{}}
    for name in ['selected-journey.json','selected_journey.py','test_selected_journey.py']:
        content=subprocess.check_output(['git','-C',str(h['OPTIONS'].fusion_spec),'show',f'{ORACLE_REVISION}:{ORACLE_DIR}/{name}'])
        (oracle/name).write_bytes(content);manifest['files'][name]=hashlib.sha256(content).hexdigest()
    (report/'oracle-basis.json').write_text(json.dumps(manifest,indent=2)+'\n')
    spec=importlib.util.spec_from_file_location('selected_fusion_oracle',oracle/'selected_journey.py');checker=importlib.util.module_from_spec(spec);spec.loader.exec_module(checker)
    fixture=checker.load();observations={o['key']:o for o in fixture['observations']}
    def rfc3339(ns):
        from datetime import datetime,timezone
        sec,nano=divmod(ns,1_000_000_000)
        return datetime.fromtimestamp(sec,timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')+f'.{nano:09}Z'
    question_query='commands where project = harbor and exit_code != 0 and when >= '+rfc3339(fixture['question']['start_ns'])+' and when < '+rfc3339(fixture['question']['end_ns'])
    prepared={};aliases={};versions={};old_evidence={};daemon=None
    focus=h['OPTIONS'].fusion_focus
    with sqlite3.connect(source) as con:
        con.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER NOT NULL,duration INTEGER NOT NULL,exit INTEGER NOT NULL,command TEXT NOT NULL,cwd TEXT NOT NULL,session TEXT NOT NULL,hostname TEXT NOT NULL,deleted_at INTEGER)')
    def arow(o):return (o['native_key'],o['start_ns'],o['duration_ns'] or 1_000_000_000,o['exit'] if o['exit'] is not None else -1,o['command'],o['cwd'] or '', 'synthetic-fusion-session',o['host'],None)
    def metafy(data):
        result=bytearray()
        for c in data:
            if c==0 or c>=0x80:result.extend([0x83,c^0x20])
            else:result.append(c)
        return bytes(result)
    def zrecord(at,command):
        wire=command.replace('\n','\\\n')
        if wire.rstrip(' ').endswith('\\'):wire+=' '
        return metafy(f': {at//1_000_000_000}:0;'.encode()+wire.encode()+b'\n')
    def prepare():
        run(['python3','-m','unittest','discover','-s',oracle,'-p','test_selected_journey.py'],label='pinned independent selected oracle controls')
        with sqlite3.connect(source) as con:
            for key in ['a1','f1']:con.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',arow(observations[key]))
        content=b''
        for key in ['z1','fz']:
            o=observations[key];raw=zrecord(o['start_ns'],o['command']);prepared[key]={'native_id':f'chain-record:{len(content)}','bytes_hex':raw.hex(),'source_offset':len(content)};content+=raw
        zsh.write_bytes(content);(report/'prepared-zsh-original').write_bytes(content)
        with sqlite3.connect(source) as con,sqlite3.connect(report/'prepared-before-late.db') as copy:con.backup(copy)
        (report/'independent-prepared-bindings.json').write_text(json.dumps({'zsh':prepared,'atuin_rows':{k:arow(v) for k,v in observations.items() if v['source'].endswith('atuin-history')},'identity_scope':'retained records under declared append protocol; hidden execution labels are never product input'},indent=2)+'\n')
    scenario(PLANNED[0],prepare)
    def configure(acquire=True,poll=3600000,rules='',zpath=None,host='fixture-host',urls=None):
        paths=urls or {n:work/n for n in ['writer','reader','read-model']}
        config.write_text(f'''listen="127.0.0.1:{port}"
state_dir="{work/'fusion-state'}"
store_dir="{store}"
writer_connection_file="{paths['writer']}"
reader_connection_file="{paths['reader']}"
read_model_connection_file="{paths['read-model']}"
token_file="{work/'token'}"
operator_token_file="{work/'operator'}"
sources={json.dumps(['terminal.atuin-history','terminal.zsh-history'] if acquire else [])}
automata=[]
[[projects]]
name="harbor"
roots=["/work/harbor"]
'''+(f'''[atuin]
path="{source}"
poll_interval_ms={poll}
[zsh]
path="{zpath or zsh}"
execution_host="{host}"
protocol="append_only_extended_v1"
poll_interval_ms={poll}
''' if acquire else '')+rules)
    api_log=report/'actual-api.jsonl'
    def api(method,params=None,operator_profile=False):
        request={'jsonrpc':'2.0','id':19,'method':method,'params':params or {}}
        call=urllib.request.Request(endpoint,data=json.dumps(request).encode(),headers={'Authorization':'Bearer '+(operator if operator_profile else token),'Content-Type':'application/json'})
        try:
            with urllib.request.urlopen(call,timeout=20) as reply:answer=json.load(reply)
        except urllib.error.HTTPError as error:answer=json.load(error)
        with api_log.open('a') as log:log.write(json.dumps({'request':request,'profile':'operator' if operator_profile else 'reader','answer':answer},ensure_ascii=False)+'\n')
        return answer
    def cli(command,*args,expected=(0,5),op=False):
        result=run([bins/'sinexctl','--endpoint',endpoint,'--token-file',work/('operator' if op else 'token'),command,*args,'--json'],expected=expected,label='actual fusion CLI '+command)
        return json.loads(result.stdout or result.stderr)
    def sql(statement,url=None):return json.loads(run(['psql',url or h['reader'],'-XAt','-v','ON_ERROR_STOP=1','-c',statement],label='independent read-only fusion binding audit').stdout)
    def wait(predicate,seconds=45):
        started=time.monotonic();deadline=started+seconds;latest=None;progress=[]
        while True:
            assert daemon.poll() is None,'owned daemon exited; see exact stderr'
            try:
                status=api('status').get('result')
                if status:
                    latest=status;indexed=status.get('indexed_retrieval') or {}
                    step={'commit_seq':status.get('commit_seq'),'position':indexed.get('position'),'pending_canonical':indexed.get('pending_canonical'),'fusion':indexed.get('fusion'),'projection_failure':indexed.get('failure')}
                    if not progress or progress[-1]!=step:progress.append(step)
                    if predicate(status):return status
            except (OSError,ValueError):pass
            if time.monotonic()>=deadline:
                (report/'watchdog-failure.json').write_text(json.dumps({'seconds':seconds,'elapsed':time.monotonic()-started,'latest_status':latest,'ordered_progress':progress,'wire_evidence':'actual-api.jsonl'},indent=2)+'\n')
                raise AssertionError('fusion frontier did not reach required state; see watchdog-failure.json')
            time.sleep(.025)
    def ready():return wait(lambda _:True)
    def settled():
        return wait(lambda s:s['indexed_retrieval'].get('serving') and s['indexed_retrieval']['position']==s['commit_seq'] and s['indexed_retrieval']['pending_canonical']==0 and s['indexed_retrieval']['fusion']['phase']=='settled' and s['indexed_retrieval']['fusion']['failure'] is None,seconds=180)
    def acquired():return wait(lambda s:len((sources:=api('sources.list').get('result',{}).get('sources',[])))==2 and all(x['last_run'] and x['last_run']['status']=='completed' for x in sources))
    def launch(name,acquire=True,*extra,**kw):
        nonlocal daemon
        if daemon is not None:stop(daemon)
        configure(acquire,**kw);daemon=start(config,name,*extra);ready()
        return daemon
    def query(q,page=100):
        cursor=None;rows=[]
        while True:
            params={'query':q}
            if not q.startswith('events'):params['page']=page
            if cursor:params['cursor']=cursor
            answer=api('query.run',params)
            assert 'result' in answer,answer
            value=answer['result'];rows+=value['rows'];cursor=value['page'].get('cursor')
            if not cursor:
                return {**value,'rows':rows}
    def raw():return query(raw_a)['rows']+query(raw_z)['rows']
    def trace(ref,bytes=True):return cli('trace',ref,*(['--bytes'] if bytes else []))['row']
    def bind(key,row):
        o=observations[key];assert row['source']==o['source'] and row['payload']['command']==o['command'] and row['payload']['timestamp_ns']==o['start_ns']
        assert row['payload']['native_id']==(o['native_key'] if o['source'].endswith('atuin-history') else prepared[key]['native_id'])
        if row['id'] not in aliases:
            versions[key]=versions.get(key,0)+1;aliases[row['id']]=f'{key}.v{versions[key]}'
        return aliases[row['id']]
    def material_bytes(material):
        info=sql("SELECT jsonb_build_object('hash',b.blake3,'bytes',b.size_bytes) FROM raw.source_material_registry m JOIN core.blobs b ON b.id=m.blob_id WHERE m.id='"+str(uuid.UUID(material))+"'")
        digest=info['hash'].removeprefix('blake3:');path=store/'blobs'/digest[:2]/(digest+'.zst')
        # Binary subprocess: no replacement decoder may alter metafied source bytes.
        argv=['zstd','-dc',str(path)]
        with h['COMMAND_LOCK']:
            index=len(h['COMMANDS']);h['COMMANDS'].append({'argv':argv,'exit':None,'expected_exit':0,'label':'independent retained original-byte decompression','stdout_encoding':'binary','stdout_file':f'{index:03}.stdout.bin'})
        result=subprocess.run(argv,cwd=h['ROOT'],capture_output=True,env=h['ENV'],check=False,timeout=30)
        (report/f'{index:03}.stdout.bin').write_bytes(result.stdout);(report/f'{index:03}.stderr').write_bytes(result.stderr)
        h['COMMANDS'][index]['exit']=result.returncode
        assert result.returncode==0,result.stderr
        (report/('material-'+material+'.original')).write_bytes(result.stdout)
        assert len(result.stdout)==info['bytes']
        return result.stdout
    def verify_record(key,row):
        t=trace(row['id']);assert t['event']==row['id'] and t['material']==row['material'] and t['anchor']==row['anchor']
        data=material_bytes(t['material']);record=data[t['anchor']:t['end']]
        if row['source'].endswith('zsh-history'):
            assert record==bytes.fromhex(prepared[key]['bytes_hex'])
            assert t['record']['unmetafied_record']==f": {observations[key]['start_ns']//1_000_000_000}:0;{observations[key]['command']}\n"
        else:
            decoded=json.loads(record);columns=['id','timestamp','duration','exit','command','cwd','session','hostname','deleted_at'];assert {c:decoded[c] for c in columns}==dict(zip(columns,arow(observations[key]))),decoded
            assert decoded['_from']['table']=='history' and decoded['_from']['rowid']>0
        old_evidence.setdefault(row['id'],t)
        return t
    def normalized_time(row):
        # Intrinsic time has no typed Captured clock; retain native acquisition
        # channel independently in the unmodified actual wire capture.
        return {'at_ns':row['payload']['timestamp_ns'],'quality':row['time_quality'],'precision':row['time_precision'],'clock':row['time_clock'].title() if row['time_quality']=='captured' else None,'until_ns':None if row['time_until'] is None else int(__import__('datetime').datetime.fromisoformat(row['time_until']).timestamp()*1_000_000_000)}
    def capture(stage_name):
        stage=checker.stage_named(fixture,stage_name);rr=raw();by_id={r['id']:r for r in rr};by_key={}
        for r in rr:
            key=next(k for k,o in observations.items() if r['source']==o['source'] and r['payload']['native_id']==(o['native_key'] if o['source'].endswith('atuin-history') else prepared[k]['native_id']))
            by_key[key]=r;bind(key,r);verify_record(key,r)
        actual=query('commands');assert actual['complete'],actual
        assert sql("SELECT jsonb_agg(DISTINCT payload_schema_id ORDER BY payload_schema_id) FROM core.live_events WHERE event_type='command.canonical'")==[5]
        actual_rows=[]
        for r in actual['rows']:
            p=r['payload'];parents=r['parents'];keys=[next(k for k,v in by_key.items() if v['id']==pid) for pid in parents]
            keys.sort(key=lambda k:(observations[k]['source']!='terminal.atuin-history',k))
            evidence=p['field_evidence'];assoc=p['association']
            supplier=by_id[evidence['time']]
            for field in ['when','time_quality','time_precision','time_clock','time_until','time_suspect']:assert r[field]==supplier[field],field
            t=trace(r['id']);assert {v['event'] for v in t['parents']}==set(parents)
            for v in t['parents']:assert v=={k:value for k,value in old_evidence[v['event']].items() if k!='secret'}
            actual_rows.append({'group':'+'.join(keys),'parents':keys,'owner_hour':p['owner_hour'],'command':p['command'],'cwd':p['cwd'],'exit':p['exit_code'],'field_origins':{name:next(k for k,v in by_key.items() if v['id']==evidence[field]) for name,field in [('command','command'),('cwd','cwd'),('exit','exit_code'),('time','time')]},'association':{'basis':assoc['basis'],'strategy':assoc['strategy'],'ambiguous':assoc['ambiguous'],'candidate_counts':{next(k for k,v in by_key.items() if v['id']==pid):count for pid,count in assoc['candidate_counts'].items()}},'trace':[{'record':observations[k]['record'],'source':by_key[k]['source'],'native_key':observations[k]['native_key'],'start_ns':by_key[k]['payload']['timestamp_ns'],'time':normalized_time(by_key[k]),'interpretation':aliases[by_key[k]['id']]} for k in keys],'time':normalized_time(r),'parent_interpretations':[aliases[by_key[k]['id']] for k in keys]})
        actual_rows.sort(key=lambda r:(r['owner_hour'],r['group']))
        archives=sql("SELECT coalesce(jsonb_agg(a.id ORDER BY a.id),'[]') FROM audit.archived a JOIN core.events e ON e.id=a.id WHERE e.source IN ('terminal.atuin-history','terminal.zsh-history')")
        # Exact membership of exclusion owner is independently audited below; map
        # raw stable keys, never infer eligibility from answer absence.
        excluded_keys=[]
        deleted=sql("SELECT coalesce(jsonb_agg(equivalence_key),'[]') FROM core.deleted_occurrences WHERE kind='exclude'")
        for key,o in observations.items():
            if o['source'].endswith('atuin-history') and any(json.loads(k)[1]==o['native_key'] for k in deleted):excluded_keys.append(key)
        failed=query(question_query)['rows']
        failed_groups=sorted('+'.join(next(k for k,v in by_key.items() if v['id']==pid) for pid in r['parents']) for r in failed)
        result={'stage':stage_name,'live_interpretations':{k:aliases[r['id']] for k,r in by_key.items()},'excluded_occurrences':sorted(excluded_keys),'archived_interpretations':sorted(aliases[i] for i in archives),'answer':{'complete':actual['complete'],'pending_owner_hours':[],'coverage':'continuous-capture-unknown' if all(r['payload']['capture_coverage']=='unknown' for r in actual['rows']) else 'unexpected','claim_scope':'selected-policy-grouping' if all(r['payload']['claim_scope']=='selected-policy-grouping' for r in actual['rows']) else 'unexpected','exact_execution_count':None if all(r['payload']['exact_execution_count'] is None for r in actual['rows']) else -1,'rows':actual_rows,'failed_groups':failed_groups,'failed_groups_exact':actual['complete']}}
        path=report/('selected-'+stage_name+'.json');path.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
        checker.check_capture(fixture,stage_name,result)
        run(['python3',oracle/'selected_journey.py',stage_name,path],label='independent selected projection '+stage_name)
        (report/('wire-'+stage_name+'.json')).write_text(json.dumps({'raw':rr,'commands':actual,'failed':failed},ensure_ascii=False,indent=2)+'\n')
        return by_key,actual
    def before():
        launch('before-late');acquired();settled();capture('before-late')
    if not focus:scenario(PLANNED[1],before)
    late_boundary=None
    def late():
        nonlocal daemon,late_boundary
        stop(daemon)
        with sqlite3.connect(source) as con:con.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',arow(observations['a2']))
        with sqlite3.connect(source) as con,sqlite3.connect(report/'prepared-late.db') as copy:con.backup(copy)
        address=work/'late-boundary.sock'
        with socket.socket(socket.AF_UNIX) as listener:
            listener.bind(str(address));listener.listen();listener.settimeout(40)
            configure();daemon=start(config,'late-committed','--qualification-boundary','fusion-replacement-committed','--qualification-socket',address)
            conn,_=listener.accept();conn.settimeout(10)
            with conn:
                notice=json.loads(conn.makefile('r').readline());assert notice['pid']==daemon.pid and notice['qualification_boundary']=='fusion-replacement-committed';late_boundary=notice
                # Projection owner remains independent while writer is parked.
                wait(lambda s:s['indexed_retrieval']['position']==s['commit_seq'])
                pending=query('commands');assert pending['complete'] is False and pending['fusion']['current_now_settled'] is False
                assert pending['rows'],'bounded replacement retains usable qualified evidence'
                (report/'actual-late-pending.json').write_text(json.dumps({'notice':notice,'query':pending},indent=2)+'\n')
                daemon.kill();daemon.wait(timeout=10)
        daemon=None;launch('late-same-state-restart');acquired();settled();capture('late-settled')
    if not focus:scenario(PLANNED[2],late)
    @contextlib.contextmanager
    def mcp_session(name):
        args=[str(bins/'sinexctl'),'--endpoint',endpoint,'--token-file',str(work/'token'),'mcp']
        err=(report/(name+'.stderr')).open('w');mcp=subprocess.Popen(args,env=h['ENV'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,text=True,bufsize=1)
        h['CHILDREN'].append((mcp,mcp.stdout,err));h['PROCESSES'].append({'pid':mcp.pid,'argv':args,'stderr':name+'.stderr'})
        log=[]
        def call(method,params):
            request={'jsonrpc':'2.0','id':len(log)+1,'method':method,'params':params};mcp.stdin.write(json.dumps(request)+'\n');mcp.stdin.flush();deadline=time.monotonic()+20
            while True:
                readable,_,_=select.select([mcp.stdout],[],[],max(0,deadline-time.monotonic()));assert readable,'MCP response deadline'
                reply=json.loads(mcp.stdout.readline());log.append({'request':request,'response':reply})
                if reply.get('id')==request['id']:return reply['result']
        def tool(name,args):
            r=call('tools/call',{'name':name,'arguments':args});assert not r.get('isError');return r.get('structuredContent') or json.loads(r['content'][0]['text'])
        call('initialize',{'protocolVersion':'2025-03-26','capabilities':{},'clientInfo':{'name':'independent-selected-fusion','version':'1'}})
        mcp.stdin.write(json.dumps({'jsonrpc':'2.0','method':'notifications/initialized'})+'\n');mcp.stdin.flush()
        assert {t['name'] for t in call('tools/list',{})['tools']}=={'sinex_search','sinex_query','sinex_show','sinex_trace','sinex_sources','sinex_status'}
        try:yield tool,call
        finally:
            (report/(name+'.wire.json')).write_text(json.dumps(log,ensure_ascii=False,indent=2)+'\n')
            mcp.stdin.close();mcp.wait(timeout=5)
    def clients():
        launch('stable-no-provider-poll',False);settled();members,actual=capture('late-settled')
        direct=cli('query','commands');assert direct['rows']==actual['rows']
        canonical=actual['rows'][0];ref='sinex:ev/'+canonical['id']
        with mcp_session('actual-mcp-transport') as (tool,call):
            assert tool('sinex_query',{'query':'commands','page':100})['rows']==actual['rows']
            assert tool('sinex_query',{'query':question_query,'page':100})['rows']==cli('query',question_query)['rows']==query(question_query)['rows']
            assert tool('sinex_show',{'ref':ref})['row']==cli('show',ref)['row']
            assert tool('sinex_trace',{'ref':ref})['row']==trace(ref,False)
            indexed=tool('sinex_search',{'words':'cargo','page':100});assert indexed['rows']==cli('search','cargo','--page','100')['rows']
            assert len(indexed['rows'])==2 and all(r['snippet']['spans'] for r in indexed['rows'])
    if not focus:scenario(PLANNED[3],clients)
    def lifecycle():
        members,canonical= capture('late-settled');a=members['a1'];material=trace(members['a2']['id'])['material']
        denied=cli('delete',a['id'],'--action-id',str(uuid.uuid4()),expected=4);assert denied['kind']=='refused'
        pair=next(r for r in canonical['rows'] if len(r['parents'])==2)
        canonical_refusal=cli('delete',pair['id'],'--action-id',str(uuid.uuid4()),op=True,expected=4);assert 'canonical references are not followed' in canonical_refusal['details']['reason']
        action=str(uuid.uuid4());deleted=cli('delete',a['id'],'--action-id',action,op=True,expected=0);assert deleted['state']=='completed'
        settled();capture('excluded');assert cli('delete',a['id'],'--action-id',action,op=True,expected=0)==deleted
        archive_types=sql("SELECT jsonb_agg(jsonb_build_object('event',e.id,'kind',e.event_type,'schema',e.payload_schema_id,'logged_type',l.event_type_id,'registered_type',t.id) ORDER BY e.id) FROM core.commit_log l JOIN core.events e ON e.id=l.event_id JOIN core.event_types t ON t.name=e.event_type WHERE l.change=1 AND e.id IN ("+','.join("'"+i+"'" for i in deleted['archived'])+")")
        assert {r['event'] for r in archive_types}==set(deleted['archived'])
        assert all(r['logged_type']==r['registered_type'] for r in archive_types)
        assert any(r['schema']==5 and r['logged_type']==3 for r in archive_types),archive_types
        (report/'actual-exclusion-archive-types.json').write_text(json.dumps({'outcome':deleted,'archive_types':archive_types},indent=2)+'\n')
        # Physical providers removed with source tasks stopped; retained bytes and
        # identities remain available through ordinary authenticated clients.
        source.rename(work/'atuin-provider-removed.db');zsh.rename(work/'zsh-provider-removed')
        launch('provider-free-retained',False);settled()
        selection=f'materials where id = {material}'
        replay=cli('replay',selection,'--action-id',str(uuid.uuid4()),op=True,expected=0);assert replay['state']=='completed' and replay['admitted']==2 and replay['processed']==3,replay
        settled();capture('replayed-excluded')
        undone=cli('delete',a['id'],'--undo','--action-id',str(uuid.uuid4()),op=True,expected=0);assert undone['state']=='completed'
        settled();capture('undo-only')
        restored=cli('replay',selection,'--action-id',str(uuid.uuid4()),op=True,expected=0);assert restored['admitted']==3
        settled();capture('replayed-restored')
        for identity,old in old_evidence.items():assert trace(identity)==(old if old.get('archive') else {**old,'archive':trace(identity)['archive']})
        (report/'actual-lifecycle-operations.json').write_text(json.dumps({'exclude':deleted,'undo':undone,'excluded_replay':replay,'restored_replay':restored},indent=2)+'\n')
    if not focus:scenario(PLANNED[4],lifecycle)
    # Additional controls append distinct commands to the same declared chain;
    # every expected parent set below is independent literal synthetic truth.
    if not focus:
        (work/'atuin-provider-removed.db').rename(source);(work/'zsh-provider-removed').rename(zsh)
    def replay_binding():
        with sqlite3.connect(source) as con:con.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',('replay-binding',10_000_000_000_000,1,0,'echo replay-binding-original','/work/harbor','replay-binding','fixture-host',None))
        launch('replay-binding-initial');acquired();settled()
        row=next(r for r in query(raw_a)['rows'] if r['payload']['native_id']=='replay-binding');selected=trace(row['id'])['material']
        launch('replay-binding-stable',False);settled();selection=f'materials where id = {selected}'
        preview=cli('replay',selection,'--dry-run',expected=0)
        first=cli('replay',selection,'--action-id',str(uuid.uuid4()),op=True,expected=0);settled()
        current=next(r for r in query(raw_a)['rows'] if r['payload']['native_id']=='replay-binding');assert current['id']!=row['id']
        stale=cli('replay',selection,'--basis',preview['basis'],'--action-id',str(uuid.uuid4()),op=True,expected=4);assert stale['details']['admitted']==0 and 'preview basis changed' in stale['details']['reason']
        stop(daemon)
        with sqlite3.connect(source) as con:con.execute("UPDATE history SET command='echo replay-binding-divergent' WHERE id='replay-binding'")
        configure();daemon_start('replay-binding-newer-divergent');acquired();settled()
        before=sql("SELECT jsonb_agg(id ORDER BY id) FROM core.live_events WHERE source='terminal.atuin-history' AND event_type='command.executed'")
        refused=cli('replay',selection,'--action-id',str(uuid.uuid4()),op=True,expected=4);assert refused['details']['admitted']==0 and 'differs from the current occurrence reading' in refused['details']['reason'],refused
        after=sql("SELECT jsonb_agg(id ORDER BY id) FROM core.live_events WHERE source='terminal.atuin-history' AND event_type='command.executed'");assert after==before
        stop(daemon)
        with sqlite3.connect(source) as con:con.execute("UPDATE history SET command='echo replay-binding-original' WHERE id='replay-binding'")
        configure();daemon_start('replay-binding-equal-other-material');acquired();settled()
        other=next(r for r in query(raw_a)['rows'] if r['payload']['native_id']=='replay-binding');assert other['material']!=selected
        positive=cli('replay',selection,'--action-id',str(uuid.uuid4()),op=True,expected=0);settled()
        replacement=next(r for r in query(raw_a)['rows'] if r['payload']['native_id']=='replay-binding');assert replacement['material']==selected and replacement['id']!=other['id']
        proof=sql("SELECT jsonb_agg(jsonb_build_object('id',id,'material',source_material_id,'key',equivalence_key,'hash',encode(record_hash,'hex'))) FROM core.events WHERE id IN ('"+row['id']+"','"+current['id']+"','"+other['id']+"','"+replacement['id']+"')")
        assert len({r['key'] for r in proof})==1 and len({r['hash'] for r in proof})==1 and len({r['material'] for r in proof})==2
        (report/'actual-replay-head-binding.json').write_text(json.dumps({'preview':preview,'first':first,'stale_same_hash_head':stale,'newer_divergent':refused,'positive_other_material':positive,'immutable_bindings':proof},indent=2)+'\n')
    def daemon_start(name):
        nonlocal daemon
        daemon=start(config,name);ready()
    if not focus:scenario(PLANNED[10],replay_binding)
    controls={};next_start=20_000
    def control(name,aa,zz,expected,host='fixture-host'):
        nonlocal next_start
        base=next_start*1_000_000_000;next_start+=2000;command='echo fusioncontrol-'+name
        with sqlite3.connect(source) as con:
            for label,offset in aa:
                con.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',(name+'-'+label,base+offset,1,0,command,'/work/harbor','control-session',host,None))
        with zsh.open('ab') as out:
            for label,offset in zz:
                anchor=out.tell();out.write(zrecord(base+offset,command));controls[f'{name}-{label}']=f'chain-record:{anchor}'
        for label,_ in aa:controls[f'{name}-{label}']=name+'-'+label
        controls[name]={'command':command,'expected':expected}
    def check_control(name):
        rows=query('commands where command ~ '+json.dumps('fusioncontrol-'+name))['rows']
        rr=raw();ids={r['id']:r['payload']['native_id'] for r in rr}
        actual=sorted(sorted(next(k.split(name+'-',1)[1] for k,v in controls.items() if isinstance(v,str) and k.startswith(name+'-') and v==ids[pid]) for pid in r['parents']) for r in rows)
        expected=sorted(sorted(group) for group in controls[name]['expected']);assert actual==expected,(name,expected,actual)
        return rows
    def matching():
        control('global', [('a',0),('b',1_600_000_000)],[('x',1_000_000_000),('y',2_000_000_000)],[['a','x'],['b','y']])
        control('one-two',[('a',0)],[('x',30_000_000_000),('y',40_000_000_000)],[['a'],['x'],['y']])
        control('two-one',[('a',0),('b',10_000_000_000)],[('x',40_000_000_000)],[['a'],['b'],['x']])
        control('one-one',[('a',0)],[('x',30_000_000_000)],[['a','x']])
        control('strong-first',[('a',0),('b',35_000_000_000)],[('x',0),('y',65_000_000_000)],[['a','x'],['b','y']])
        control('two-inclusive',[('a',2_000_000_000)],[('x',0)],[['a','x']])
        control('two-plus-nano',[('a',2_000_000_001)],[('x',0)],[['a','x']])
        control('sixty-inclusive',[('a',60_000_000_000)],[('x',0)],[['a','x']])
        control('sixty-plus-nano',[('a',60_000_000_001)],[('x',0)],[['a'],['x']])
        control('host',[('a',0)],[('x',0)],[['a'],['x']],host='other-host')
        control('duplicates',[('a',0)],[('x',0),('y',0)],[['a','x'],['y']])
        control('outer-trim',[('a',0)],[('x',0)],[['a','x']])
        control('inner-space',[('a',0)],[('x',0)],[['a'],['x']])
        with sqlite3.connect(source) as con:
            con.execute("UPDATE history SET command='  echo fusioncontrol-outer-trim  ' WHERE id='outer-trim-a'")
            con.execute("UPDATE history SET command='echo  fusioncontrol-inner-space' WHERE id='inner-space-a'")
        launch('matching-controls');acquired();settled();results={name:check_control(name) for name,v in controls.items() if isinstance(v,dict)}
        assert results['two-inclusive'][0]['payload']['association']['strategy']=='window-2s'
        assert results['two-plus-nano'][0]['payload']['association']['strategy']=='window-60s-unique'
        # Distinct zsh records remain distinct even with same timestamp/text.
        rr=raw();dup=[r for r in rr if r['payload']['native_id'] in [controls['duplicates-x'],controls['duplicates-y']]];assert len(dup)==2 and len({r['id'] for r in dup})==2
        (report/'independent-match-controls.json').write_text(json.dumps({'expected':controls,'actual':results},indent=2)+'\n')
    if not focus:scenario(PLANNED[5],matching)
    def component():
        # Alternating 59-second edges span >3h. Every endpoint sees two
        # competing groups except physical ends; mutual uniqueness pairs none.
        count=190
        aa=[(f'a{i}',i*118_000_000_000) for i in range(count//2)]
        zz=[(f'z{i}',(i*118+59)*1_000_000_000) for i in range(count//2)]
        expected=[[x] for x,_ in aa+zz];control('hours',aa,zz,expected)
        launch('hours-component');acquired();settled();before=check_control('hours');assert len(before)==count
        with sqlite3.connect(source) as con:con.execute("UPDATE history SET deleted_at=1 WHERE id='hours-a40'")
        launch('component-removal');acquired();settled();controls['hours']['expected'].remove(['a40']);after=check_control('hours');assert len(after)==count-1
        (report/'actual-hours-removal.json').write_text(json.dumps({'before':before,'after':after,'independent_expected':controls['hours']['expected']},indent=2)+'\n')
    if not focus:scenario(PLANNED[6],component)
    def fairness():
        nonlocal daemon
        stop(daemon)
        with sqlite3.connect(source) as con:
            for n in range(150):con.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',(f'fair-{n}',900_000_000_000_000+n*1_000_000_000,1,0,f'echo fair-{n}','/work/harbor','fair-session','fixture-host',None))
        address=work/'fair-boundary.sock'
        with socket.socket(socket.AF_UNIX) as listener:
            listener.bind(str(address));listener.listen();listener.settimeout(60)
            configure(poll=100);daemon=start(config,'fair-live','--qualification-boundary','fusion-replacement-committed','--qualification-socket',address)
            conn,_=listener.accept();conn.settimeout(15)
            with conn:
                notice=json.loads(conn.makefile('r').readline());basis=notice['evidence']['basis'];assert notice['pid']==daemon.pid
                with sqlite3.connect(source) as con:con.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',('fair-later',901_000_000_000_000,1,0,'echo fair-later','/work/harbor','fair-session','fixture-host',None))
                # New source read can enqueue while writer is parked. Release it;
                # bounded turns allow the queued admission before old pass ends.
                time.sleep(.2);conn.sendall(b'c')
        during=[]
        deadline=time.monotonic()+30
        seed=None
        while time.monotonic()<deadline:
            state=api('status')['result'];during.append(state)
            atomic=sql("SELECT jsonb_build_object('seed',(SELECT max(commit_seq) FROM core.events WHERE source='terminal.atuin-history' AND payload->>'native_id'='fair-later'),'progress',(SELECT to_jsonb(p) FROM derivation.shell_progress p WHERE one))")
            seed=atomic['seed']
            if seed is not None and atomic['progress']['basis']==basis and atomic['progress']['phase']!='settled':break
            time.sleep(.01)
        assert seed is not None and seed>basis and atomic['progress']['basis']==basis and atomic['progress']['phase']!='settled',atomic
        wait(lambda s:s['indexed_retrieval']['fusion']['last_evaluated']>=seed)
        settled()
        audit=sql("SELECT jsonb_build_object('new_seed',(SELECT commit_seq FROM core.events WHERE source='terminal.atuin-history' AND payload->>'native_id'='fair-later' ORDER BY commit_seq DESC LIMIT 1),'progress',(SELECT to_jsonb(p) FROM derivation.shell_progress p WHERE one),'dirty',(SELECT count(*) FROM derivation.shell_dirty))")
        assert audit['new_seed']>basis and audit['dirty']==0 and audit['progress']['last_evaluated']>=audit['new_seed']
        # Stable reconstruction on compatible restart preserves settled IDs.
        before=sql("SELECT jsonb_agg(id ORDER BY id) FROM core.live_events WHERE event_type='command.canonical'")
        launch('fair-restart',False);settled();after=sql("SELECT jsonb_agg(id ORDER BY id) FROM core.live_events WHERE event_type='command.canonical'");assert before==after
        (report/'actual-fair-frontier.json').write_text(json.dumps({'boundary':notice,'audit':audit,'during_old_pass':during,'atomic_seed_and_old_pass':atomic,'stable_ids':before,'scope':'bounded whole-input pass; no corpus-performance claim'},indent=2)+'\n')
    if not focus:scenario(PLANNED[7],fairness)
    def pending_head_race():
        nonlocal daemon
        raw_before=query(raw_a)['rows'];original=next(r for r in raw_before if r['payload']['native_id']=='hours-a90')
        selected=trace(next(r for r in raw_before if r['payload']['native_id']=='fair-later')['id'])['material'];selection=f'materials where id = {selected}'
        assert original['material']!=selected,'race must pin a head outside selected acquisition'
        preview=cli('replay',selection,'--dry-run',expected=0);assert preview['selected']>128
        before_canonical=query('commands')['rows']
        action=str(uuid.uuid4());stop(daemon)
        address=work/'replay-head-race.sock';lost={}
        def request():
            try:return api('replay.start',{'query':selection,'action_id':action},True)
            except (OSError,http.client.HTTPException) as error:
                lost['transport_error']=type(error).__name__+': '+str(error);return None
        with socket.socket(socket.AF_UNIX) as listener:
            listener.bind(str(address));listener.listen();listener.settimeout(45)
            configure(False);daemon=start(config,'head-race-committed','--qualification-boundary','replay-committed-0','--qualification-socket',address);ready()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future=executor.submit(request);conn,_=listener.accept();conn.settimeout(10)
                with conn:
                    frame=json.loads(conn.makefile('r').readline());progress=frame['evidence']['operation'];assert frame['pid']==daemon.pid and progress['processed']==64 and progress['state']=='running'
                    wait(lambda status:status['indexed_retrieval']['serving'] and status['indexed_retrieval']['position']==status['commit_seq'] and status['indexed_retrieval']['failure'] is None)
                    archived={r['old'] for r in frame['evidence']['effects']['replacements'] if r['old']}
                    expected_ids={r['id'] for r in before_canonical if not archived.intersection(r['parents'])}
                    pending=query('commands');assert pending['total']==len(expected_ids) and {r['id'] for r in pending['rows']}==expected_ids and pending['complete'] is False
                    inactive=next(r for r in before_canonical if archived.intersection(r['parents']))
                    ref=inactive['id'];id_query='commands where id = '+ref
                    hidden=query(id_query);assert hidden['rows']==[] and hidden['total']==0 and hidden['complete'] is False
                    direct=cli('query',id_query,expected=5);assert direct['rows']==hidden['rows'] and direct['total']==0
                    shown=cli('show',ref);navigation=trace(ref,False);assert shown['row']['current_eligible'] is False and navigation['current_eligible'] is False and any(p['archive'] for p in navigation['parents'])
                    token=inactive['payload']['command'].strip().split()[0];search_text=token+' and id = '+ref
                    search_api=api('search',{'query':search_text,'page':20})['result'];assert search_api['rows']==[] and search_api['total']==0 and search_api['complete'] is False
                    search_cli=cli('search',token,'--query','and id = '+ref,expected=5);assert search_cli['rows']==[] and search_cli['total']==0
                    with mcp_session('pending-mcp') as (tool,call):
                        mq=tool('sinex_query',{'query':id_query,'page':20});ms=tool('sinex_search',{'words':token,'query':'and id = '+ref,'page':20})
                        assert mq['rows']==ms['rows']==[] and mq['total']==ms['total']==0 and mq['complete'] is ms['complete'] is False
                        assert tool('sinex_show',{'ref':ref})['row']==shown['row'] and tool('sinex_trace',{'ref':ref})['row']==navigation
                    internal=sql("SELECT jsonb_build_object('canonical_live',EXISTS(SELECT 1 FROM core.live_events WHERE id='"+ref+"'),'indexed',EXISTS(SELECT 1 FROM core.search_text s JOIN derivation.read_model_position p ON p.generation=s.generation WHERE s.id='"+ref+"'),'ineligible_parents',(SELECT count(*) FROM core.events e,unnest(e.source_event_ids) parent(id) WHERE e.id='"+ref+"' AND NOT EXISTS(SELECT 1 FROM core.live_events raw WHERE raw.id=parent.id AND raw.record_state='present')))")
                    assert internal['canonical_live'] and internal['indexed'] and internal['ineligible_parents']>0
                    (report/'actual-pending-index-fence.json').write_text(json.dumps({'frame':frame,'expected_current_ids':sorted(expected_ids),'actual_pending':pending,'hidden_api':hidden,'hidden_cli':direct,'show':shown,'trace':navigation,'search_api':search_api,'search_cli':search_cli,'internal_body_fact':internal},indent=2)+'\n')
                    daemon.kill();daemon.wait(timeout=10)
                assert future.result(timeout=20) is None and lost
        daemon=None
        # While authority is absent the admitted operation stays parked, but the
        # ordinary source can produce a changed reading and then equal content.
        for phase,command in [('changed','echo head-race-temporary'),('equal',original['payload']['command'])]:
            with sqlite3.connect(source) as con:con.execute('UPDATE history SET command=? WHERE id=?',(command,'hours-a90'))
            configure(True);config.write_text(config.read_text().replace('operator_token_file="'+str(work/'operator')+'"\n',''))
            daemon_start('head-race-authority-disabled-'+phase);acquired();settled()
            assert api('status')['result']['replay_authorized'] is False
            retained=sql("SELECT progress FROM core.operations_log WHERE id='"+progress['operation_id']+"'");assert retained['processed']==64 and retained['state']=='running'
            stop(daemon)
        configure(False);daemon_start('head-race-reauthorized')
        def refused(_):
            reply=api('replay.status',{'operation_id':progress['operation_id']})
            value=reply.get('result') or reply.get('error',{}).get('data',{})
            return value.get('state')=='refused'
        wait(refused);settled()
        refused_outcome=cli('replay',selection,'--action-id',action,op=True,expected=4)['details']
        assert 64<=refused_outcome['processed']<preview['selected'] and refused_outcome['admitted']>=64 and 'head changed' in refused_outcome['reason'],refused_outcome
        current=next(r for r in query(raw_a)['rows'] if r['payload']['native_id']=='hours-a90');assert current['id']!=original['id']
        proof=sql("SELECT jsonb_agg(jsonb_build_object('id',id,'hash',encode(record_hash,'hex'),'archived',EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=e.id))) FROM core.events e WHERE id IN ('"+original['id']+"','"+current['id']+"')")
        assert len({p['hash'] for p in proof})==1 and next(p for p in proof if p['id']==current['id'])['archived'] is False
        audit=sql("SELECT jsonb_build_object('effects',(SELECT count(*) FROM core.operation_effects WHERE operation_id='"+progress['operation_id']+"'),'progress',(SELECT progress FROM core.operations_log WHERE id='"+progress['operation_id']+"'))")
        assert audit['effects']==refused_outcome['steps'] and audit['progress']==refused_outcome
        fresh=cli('replay',selection,'--action-id',str(uuid.uuid4()),op=True,expected=0);assert fresh['state']=='completed' and fresh['processed']==preview['selected'];settled()
        (report/'actual-pending-replay-head-race.json').write_text(json.dumps({'preview':preview,'committed_frame':frame,'lost_response':lost,'same_hash_different_heads':proof,'refused_prior_operation':refused_outcome,'preserved_effects':audit,'fresh_action':fresh},indent=2)+'\n')
    if focus=='changed-seams':
        def focus_setup():
            rows=[('hours-a90' if n==90 else f'focus-{n}',20_000_000_000_000+n*1_000_000_000,1,0,'echo focus-target' if n==90 else f'echo focus-{n}','/work/harbor','focus-session','fixture-host',None) for n in range(130)]
            with sqlite3.connect(source) as con:con.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',rows)
            launch('focus-earlier-material');acquired();settled()
            stop(daemon)
            with sqlite3.connect(source) as con:con.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',('fair-later',21_000_000_000_000,1,0,'echo focus-later','/work/harbor','focus-session','fixture-host',None))
            configure();daemon_start('focus-later-material');acquired();settled();launch('focus-stable',False);settled()
            schema_ids=sql("SELECT jsonb_agg(DISTINCT payload_schema_id ORDER BY payload_schema_id) FROM core.live_events WHERE event_type='command.canonical'");assert schema_ids==[5]
            (report/'independent-changed-seam-inputs.json').write_text(json.dumps({'earlier_rows':rows,'later_native_id':'fair-later','expected_new_canonical_schema_ids':[5]},indent=2)+'\n')
        scenario('independent changed-seam source setup',focus_setup)
    if focus!='source-codec':scenario(PLANNED[11],pending_head_race)
    def reconstruction():
        nonlocal daemon
        launch('reconstruction-before',False);settled();before=query('commands');old_generation=api('status')['result']['indexed_retrieval']['generation']
        stop(daemon);configure(False);config.write_text(config.read_text().replace('roots=["/work/harbor"]','roots=["/work/harbor","/synthetic/rebuild-one"]'));daemon_start('complete-log-reconstruction');settled()
        after=query('commands');rebuilt_generation=api('status')['result']['indexed_retrieval']['generation'];assert rebuilt_generation!=old_generation and before['rows']==after['rows'] and before['total']==after['total']
        log=sql("SELECT to_jsonb(l) FROM core.commit_log l WHERE l.event_id='"+before['rows'][0]['id']+"' AND l.change=0")
        stop(daemon)
        run(['psql',h['admin'],'-XAt','-v','ON_ERROR_STOP=1','-c','DELETE FROM core.commit_log WHERE commit_seq='+str(log['commit_seq'])],label='controlled synthetic missing canonical admission-log row')
        configure(False);config.write_text(config.read_text().replace('roots=["/work/harbor"]','roots=["/work/harbor","/synthetic/rebuild-two"]'));daemon_start('missing-log-refusal')
        unavailable=wait(lambda status:status['indexed_retrieval']['failure'] is not None)
        refused=api('query.run',{'query':'commands'});assert refused['error']['kind']=='partial' and 'result' not in refused
        refused_cli=cli('query','commands',expected=5);assert refused_cli['kind']=='partial' and 'rows' not in refused_cli
        raw_control=query(raw_a);assert raw_control['total']>128 and raw_control['complete'] is True
        stop(daemon)
        # Restore the exact deliberately removed synthetic log row. The negative
        # fixture is not a product pruning or recovery capability.
        statement="INSERT INTO core.commit_log(commit_seq,event_id,change,store,event_type_id) VALUES("+str(log['commit_seq'])+",'"+log['event_id']+"',"+str(log['change'])+","+str(log['store'])+","+str(log['event_type_id'])+")"
        run(['psql',h['admin'],'-XAt','-v','ON_ERROR_STOP=1','-c',statement],label='restore exact controlled synthetic log fixture')
        configure(False);daemon_start('restored-log-reconstruction');settled();restored=query('commands');assert restored['rows']==before['rows'] and restored['total']==before['total']
        (report/'actual-canonical-reconstruction.json').write_text(json.dumps({'before':before,'rebuilt':after,'old_generation':old_generation,'rebuilt_generation':rebuilt_generation,'controlled_missing_log':log,'unavailable_status':unavailable,'api_refusal':refused,'cli_refusal':refused_cli,'valid_raw_control':raw_control,'restored':restored},indent=2)+'\n')
    if focus!='source-codec':scenario(PLANNED[12],reconstruction)
    def source_controls():
        # Exact retained-byte framing, including metafied UTF-8 and continuation.
        unicode_command='printf zażółć\nnext';credential='echo ghp_'+'A'*36
        original=zsh.read_bytes();offset=len(original);record=zrecord(950_000_000_000_000,unicode_command)
        # Literal installed zsh 5.9.2 fc-W output, independently reimported
        # through fc-R. One sentinel ASCII space follows trailing slash/spaces.
        codec=[('echo slash\\',b'echo slash\\ '),('echo slashspace\\ ',b'echo slashspace\\  '),('echo slashspaces\\  ',b'echo slashspaces\\   '),('echo slashtab\\\t',b'echo slashtab\\\t'),('echo slashtabspace\\\t ',b'echo slashtabspace\\\t '),('echo slashnewline\\\nnext',b'echo slashnewline\\\\\nnext'),('echo plainspace  ',b'echo plainspace  ')]
        codec_bytes=b''.join(b': 954000:0;'+wire+b'\n' for _,wire in codec)
        source_bytes=original+record+zrecord(951_000_000_000_000,credential)+codec_bytes
        zsh.write_bytes(source_bytes)
        launch('zsh-unicode');acquired();settled()
        row=sql("SELECT jsonb_build_object('id',id,'material',source_material_id,'anchor',anchor_byte,'end',offset_end,'payload',payload) FROM core.live_events WHERE source='terminal.zsh-history' AND payload->>'native_id'='chain-record:"+str(offset)+"'")
        assert row['payload']['command']==unicode_command;original_trace=trace(row['id']);data=material_bytes(row['material']);assert data[row['anchor']:row['end']]==record
        codec_actual=sql("SELECT jsonb_agg(jsonb_build_object('id',id,'command',payload->>'command','anchor',anchor_byte,'end',offset_end,'material',source_material_id) ORDER BY anchor_byte) FROM core.live_events WHERE source='terminal.zsh-history' AND ts_orig=to_timestamp(954000)")
        assert [r['command'] for r in codec_actual]==[expected for expected,_ in codec],codec_actual
        codec_query=raw_z+' and when >= '+rfc3339(954_000_000_000_000)+' and when < '+rfc3339(954_001_000_000_000)
        codec_api=query(codec_query);codec_cli=cli('query',codec_query)
        assert codec_api['total']==codec_cli['total']==len(codec)
        for answer in [codec_api,codec_cli]:
            rows=sorted(answer['rows'],key=lambda r:r['anchor'])
            assert [r['id'] for r in rows]==[r['id'] for r in codec_actual]
            assert [r['payload']['command'] for r in rows]==[expected for expected,_ in codec]
        for actual,(_,wire) in zip(codec_actual,codec):
            assert material_bytes(actual['material'])[actual['anchor']:actual['end']]==b': 954000:0;'+wire+b'\n'
        (report/'actual-installed-zsh-codec.json').write_text(json.dumps({'expected_commands':[expected for expected,_ in codec],'literal_source_hex':codec_bytes.hex(),'actual':codec_actual,'api':codec_api,'cli':codec_cli},indent=2)+'\n')
        secret=cli('query',raw_z);assert 'A'*30 not in json.dumps(secret)
        safe_trace=trace(sql("SELECT to_jsonb(id) FROM core.live_events WHERE source='terminal.zsh-history' AND payload->>'command' LIKE 'echo ghp_%' LIMIT 1"));assert 'A'*30 not in json.dumps(safe_trace)
        stop(daemon);zsh.write_bytes(b': 1:0;rewrite\n');configure();daemon_start('zsh-truncated')
        wait(lambda _:any(x['source']=='terminal.zsh-history' and x['last_run']['status']=='unavailable' for x in api('sources.list')['result']['sources']))
        assert trace(row['id'])==original_trace
        stop(daemon);zsh.write_bytes(source_bytes);configure();daemon_start('zsh-restored-prefix-refused')
        wait(lambda _:any(x['source']=='terminal.zsh-history' and x['last_run']['status']=='unavailable' for x in api('sources.list')['result']['sources']))
        state=sql("SELECT jsonb_agg(position) FROM raw.source_cursors WHERE binding='terminal.zsh-history'");assert any(p.get('chain_broken') for p in state)
        assert trace(row['id'])==original_trace
        stop(daemon)
        bad=work/'zsh-unsupported';bad_bytes=b'plain-command\n'+zrecord(952_000_000_000_000,'valid-after-malformed');bad.write_bytes(bad_bytes)
        configure(zpath=bad);daemon_start('zsh-malformed-record')
        def bad_status(status):
            rows=api('sources.list')['result']['sources']
            return next((x for x in rows if x['locator']==str(bad) and x['last_run']['status']==status),None)
        wait(lambda _:bad_status('completed') is not None)
        malformed=bad_status('completed');assert malformed['last_run']['record_errors']==1 and malformed['last_run']['admitted']==1
        failure=malformed['first_record_error'];assert material_bytes(failure['material_id']).startswith(bad_bytes)
        stop(daemon);bad.write_bytes(bad_bytes+b': 953000:0;unfinished')
        configure(zpath=bad);daemon_start('zsh-partial-record')
        wait(lambda _:bad_status('unavailable') is not None);partial=bad_status('unavailable');assert partial['cursor']['offset']==len(bad_bytes)
        stop(daemon);bad.write_bytes(bad.read_bytes()+b'\n')
        configure(zpath=bad);daemon_start('zsh-finished-later-record')
        wait(lambda _:bad_status('completed') is not None);finished=bad_status('completed');assert finished['last_run']['record_errors']==0 and finished['cursor']['record_errors']==1 and finished['cursor']['offset']==bad.stat().st_size
        source_answer=query(raw_z);source_fact=next(x for x in source_answer['source_status'] if x['input']==finished['input']);assert source_fact['record_errors']==0 and source_fact['retained_record_errors']==1 and source_answer['complete'] is False
        (report/'actual-zsh-source-controls.json').write_text(json.dumps({'row':row,'trace':original_trace,'durable_fence':state,'restored_prefix_sources':api('sources.list'),'malformed':malformed,'partial':partial,'finished_later':finished},indent=2)+'\n')
    scenario(PLANNED[8],source_controls)
    def privacy():
        nonlocal daemon,store
        stop(daemon);daemon=None
        # Independent empty store; no reset of prior captured qualification state.
        run(['createdb','fusion_privacy'],label='isolated privacy comparison database')
        urls={}
        for name,url in [('admin',h['admin']),('writer',h['writer']),('reader',h['reader']),('read-model',h['reader'].replace('sinex_reader','sinex_read_model'))]:
            path=work/('privacy-'+name);path.write_text(url.replace('/foundation?','/fusion_privacy?'));urls[name]=path
        run([bins/'sinexd','admin','schema','--connection-file',urls['admin']],label='isolated privacy schema')
        store=work/'privacy-store'
        with sqlite3.connect(source) as con:
            con.execute('DELETE FROM history');con.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',[(f'private-{i}',1_000_000_000_000_000+i*10_000_000_000,1,0,'echo tok_'+s,'/work/harbor','privacy','fixture-host',None) for i,s in enumerate(['first','second'])])
        privacy_zsh=work/'privacy-zsh';privacy_zsh.write_bytes(zrecord(1_000_000_000_000_000,'echo [withheld]'))
        rules='''[[privacy_rules]]
pattern="tok_[a-z]+"
replacement="[withheld]"
created_by="operator"
'''
        configure(rules=rules,zpath=privacy_zsh,urls=urls)
        refused=run([bins/'sinexd','serve','--config',config],expected=1,label='selected cross-source policy refuses optional privacy rules');assert 'privacy' in refused.stderr
        # Admit lossy Atuin parents under existing ordinary masking, with the new
        # two-source capability absent. A later empty config cannot relabel them.
        text=config.read_text().replace('"terminal.atuin-history", "terminal.zsh-history"','"terminal.atuin-history"');text=text[:text.index('[zsh]')]+rules;config.write_text(text)
        daemon_start('privacy-atuin-only')
        wait(lambda _:any(x['last_run'] and x['last_run']['status']=='completed' for x in api('sources.list')['result']['sources']))
        q=query(raw_a);assert q['total']==2 and {r['payload']['command'] for r in q['rows']}=={'echo [withheld]'}
        material=trace(q['rows'][0]['id'])['material'];stop(daemon)
        configure(zpath=privacy_zsh,urls=urls);daemon_start('privacy-historical-empty-rules')
        blocked=wait(lambda s:s['indexed_retrieval']['fusion']['failure'] is not None)
        unavailable=query('commands');assert unavailable['complete'] is False and 'comparison unavailable' in unavailable['fusion']['failure']
        # Unchanged polling must retain actionable failure, not clear/relabel it.
        basis=blocked['indexed_retrieval']['fusion'];time.sleep(.2);assert api('status')['result']['indexed_retrieval']['fusion']['failure']==basis['failure']
        restored=cli('replay',f'materials where id = {material}','--action-id',str(uuid.uuid4()),op=True,expected=0);assert restored['admitted']==2
        settled();answer=query('commands');assert answer['total']==3 and all(len(r['parents'])==1 for r in answer['rows']),answer
        (report/'actual-privacy-basis.json').write_text(json.dumps({'lossy_atuin':q,'blocked_status':blocked,'unavailable_query':unavailable,'retained_replay':restored,'recovered':answer},indent=2)+'\n')
    if not focus:scenario(PLANNED[9],privacy)
    stop(daemon)
