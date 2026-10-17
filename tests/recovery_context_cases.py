"""Independent finite shell facts exercised through configured public clients."""
import datetime as dt
import hashlib
import shutil
import json
from pathlib import Path
import select
import socket
import sqlite3
import subprocess
import time
import urllib.request
from session_cases import COLUMNS, independent

ACCOUNTING = 'three held completed fixed-provider polls account for exact retained bytes'
PLANNED = ['mixed context, positive selection and unresolved evidence through CLI MCP',
    'late cross-day bridge and unchanged polls preserve independent spans and citations',
    'later arrival, current configuration transition and restart provider loss',
    'whole-session and candidate overflow refuse through CLI MCP',
    'current canonical and raw parent fences refuse without older fallback', ACCOUNTING]


WHOLE_UNCHANGED = 'completed unchanged polling preserves the repeated recovery answer'
WHOLE_CONTINUATION = 'later arrival, compatible restart and continuous provider loss'
WHOLE_READING = 'observed active reading and retained recovery question'

def planned(whole=False):
    return PLANNED[:2]+[WHOLE_CONTINUATION,WHOLE_UNCHANGED,WHOLE_READING] if whole else PLANNED


def fixture_row(identity, clock, cwd, duration=0):
    timestamp = int(dt.datetime.fromisoformat('2026-10-13T'+clock+'+02:00').timestamp())*1_000_000_000+123
    return dict(zip(COLUMNS, [identity,timestamp,duration,0,'echo recovery '+identity,cwd,'fixture-shell','fixture-host',None]))


def fixture():
    return [fixture_row('old','03:50:00','/work/tide'),
        fixture_row('positive','04:10:00','/work/tide/src'),
        fixture_row('other-context','04:11:00','/work/other'),
        fixture_row('unconfigured-context','04:12:00','/work/tide-neighbor'),
        fixture_row('missing-context','04:13:00',''),
        fixture_row('invalid-context','04:14:00','/work/tide/../other'),
        fixture_row('ambiguous-context','04:15:00','/work/ambiguous'),
        fixture_row('known-distractor','05:00:00','/work/other'),
        fixture_row('newer-unknown','06:00:00',''),
        fixture_row('newer-unconfigured','06:30:00','/unconfigured'),
        fixture_row('newer-ambiguous','07:00:00','/work/ambiguous')]


def exercise(h):
    work,report,env,bins=(h[k] for k in ['WORK','REPORT','ENV','BIN_DIR'])
    run,scenario,start,stop=(h[k] for k in ['run','scenario','start_daemon','stop'])
    endpoint=f'http://127.0.0.1:{h["free_port"]()}/rpc'
    source,config=work/'recovery.db',work/'recovery.toml'
    facts=fixture();daemon=mcp=None;identifier=0;metrics=[];calls=[];transport=[];restarts=0
    whole=bool(h['OPTIONS'].recovery_prior_accounting)
    journey_started=time.monotonic();interventions=[];costs=[];reuse=None
    def intervention(kind, **details):
        interventions.append(dict(kind=kind,elapsed_seconds=time.monotonic()-journey_started,**details))
    if whole:
        from recovery_costs import phase_snapshot, reuse_accounting
        reuse=reuse_accounting(h['OPTIONS'].recovery_prior_accounting,h['PREBUILT_PIN']['pin'])
        (report/'reused-accounting.json').write_text(json.dumps(reuse,indent=2)+'\n')
        intervention('owned fixture setup',scope='isolated PostgreSQL and synthetic provider/configuration; package selection/bootstrap receipts in result.json; no live state')
    token=(work/'token').read_text()
    baseline_samples=[]
    read_model=work/'recovery-read-model'
    read_model.write_text(h['reader'].replace('sinex_reader','sinex_read_model'));read_model.chmod(0o600)
    with sqlite3.connect(source) as con:
        con.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT,deleted_at INTEGER)')

    def save(name,value): (report/name).write_text(json.dumps(value,indent=2)+'\n')
    def fill(rows):
        began=time.monotonic()
        with sqlite3.connect(source) as con:con.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',[[r[c] for c in COLUMNS] for r in rows])
        save('independent-inputs.json',facts)
        if whole:intervention('synthetic producer commit',rows=len(rows),commit_seconds=time.monotonic()-began)
    def configure(extra=''):
        config.write_text(f'''listen = "{endpoint.removeprefix('http://').removesuffix('/rpc')}"
state_dir = "{work/'recovery-state'}"
store_dir = "{work/'recovery-blobs'}"
writer_connection_file = "{work/'writer'}"
reader_connection_file = "{work/'reader'}"
read_model_connection_file = "{read_model}"
token_file = "{work/'token'}"
sources = ["terminal.atuin-history"]
automata = ["session"]
[[projects]]
name = "tide"
roots = ["/work/tide"]
[[projects]]
name = "other"
roots = ["/work/other"]
[[projects]]
name = "ambiguous-a"
roots = ["/work/ambiguous"]
[[projects]]
name = "ambiguous-b"
roots = ["/work/ambiguous"]
[[projects]]
name = "empty"
roots = ["/work/empty"]
{extra}
[atuin]
path = "{source}"
poll_interval_ms = 3000
''')
    def api(method='status',params=None):
        req=urllib.request.Request(endpoint,data=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params or {}}).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
        with urllib.request.urlopen(req,timeout=12) as response:body=response.read();answer=json.loads(body)
        transport.append({'elapsed_seconds':time.monotonic()-journey_started,'channel':'API','role':'qualification polling or validation','method':method,'response_body_bytes':len(body)})
        with (report/'api.jsonl').open('a') as log:log.write(json.dumps({'method':method,'params':params,'response':answer})+'\n')
        return answer
    def cli(command,*args,expected=(0,5)):
        result=run([bins/'sinexctl','--endpoint',endpoint,'--token-file',work/'token',command,*args,'--json'],expected=expected,label='recovery '+command)
        answer=json.loads(result.stdout or result.stderr);calls.append({'command':command,'args':list(args),'bytes':len(result.stdout.encode()),'expected_exit':expected,'stderr_bytes':len(result.stderr.encode()),'role':'recovery question' if command=='recall' and list(args)==['last','--project','tide'] else 'qualification/citation','elapsed_seconds':time.monotonic()-journey_started})
        return answer
    def invoke(method,params):
        nonlocal identifier
        identifier+=1;request={'jsonrpc':'2.0','id':identifier,'method':method,'params':params}
        mcp.stdin.write(json.dumps(request)+'\n');mcp.stdin.flush()
        until=time.monotonic()+20
        while True:
            ready,_,_=select.select([mcp.stdout],[],[],max(0,until-time.monotonic()));assert ready,'MCP deadline'
            body=mcp.stdout.readline();response=json.loads(body)
            transport.append({'elapsed_seconds':time.monotonic()-journey_started,'role':'recovery question' if method=='tools/call' and params.get('name')=='sinex_recall' and params.get('arguments')=={'project':'tide'} else 'qualification/discovery','channel':'MCP','method':method,'response_line_bytes':len(body.encode())})
            with (report/'mcp.jsonl').open('a') as log:log.write(json.dumps({'request':request,'response':response})+'\n')
            if response.get('id')==identifier:return response
    def tool(project='tide',error=False):
        response=invoke('tools/call',{'name':'sinex_recall','arguments':{'project':project}})['result']
        assert bool(response.get('isError'))==error,response
        return response.get('structuredContent') or json.loads(response['content'][0]['text'])
    def discover():
        nonlocal mcp
        err=(report/'mcp.stderr').open('a')
        argv=[str(bins/'sinexctl'),'--endpoint',endpoint,'--token-file',str(work/'token'),'mcp']
        mcp=subprocess.Popen(argv,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,text=True,bufsize=1)
        h['CHILDREN'].append((mcp,mcp.stdout,err));h['PROCESSES'].append({'pid':mcp.pid,'argv':argv,'stderr':'mcp.stderr'})
        assert 'result' in invoke('initialize',{'protocolVersion':'2025-03-26','capabilities':{},'clientInfo':{'name':'recovery-context','version':'1'}})
        mcp.stdin.write(json.dumps({'jsonrpc':'2.0','method':'notifications/initialized'})+'\n');mcp.stdin.flush()
        tools=invoke('tools/list',{})['result']['tools'];assert 'sinex_recall' in {t['name'] for t in tools};save('tools.json',tools)
    def wait(label,predicate,seconds=120):
        until=time.monotonic()+seconds
        while time.monotonic()<until:
            assert daemon.poll() is None,'owned daemon stopped'
            try: value=predicate()
            except (OSError, KeyError): value=None
            if value:return value
            time.sleep(.05)
        raise AssertionError(label)
    def settled():
        response=api('query.run',{'query':'recall last where project = tide'})
        if 'error' in response:return None
        canonical=api('query.run',{'query':'commands sort when limit 10000'})
        if 'error' in canonical or canonical['result']['total']!=len(facts):return None
        return response['result']
    def expected_group():
        return next(g for g in reversed(independent(facts)) if any(r['id'] in g['parents'] and r['cwd'].startswith('/work/tide/') or r['id'] in g['parents'] and r['cwd']=='/work/tide' for r in facts))
    def qualify(label):
        call_start=len(calls);transport_start=len(transport)
        began=time.monotonic();answer=cli('recall','last','--project','tide');agent=tool();elapsed=time.monotonic()-began
        assert answer['session']==agent['session'] and answer['rows']==agent['rows'] and answer['attribution']==agent['attribution'] and answer['newer_unresolved_candidates']==agent['newer_unresolved_candidates']
        expected=expected_group();rows=answer['rows'];names=sorted(r['payload']['native_id'] for r in rows)
        assert names==expected['parents'],(names,expected)
        header=answer['session'];assert int(header['payload']['start_ns'])==expected['start_ns'] and int(header['payload']['end_ns'])==expected['end_ns']
        assert set(header['parents'])=={r['id'] for r in rows}
        byname={r['payload']['native_id']:r for r in rows}
        labels={'positive':('known',None),'other-context':('known',None),'unconfigured-context':('unconfigured','no_configured_root'),'missing-context':('unknown','missing_cwd'),'invalid-context':('unknown','invalid_cwd'),'ambiguous-context':('unknown','ambiguous_roots')}
        for name,(state,reason) in labels.items():
            if name in byname:
                evidence=byname[name]['project_evidence'];assert evidence['state']==state and evidence.get('reason')==reason,(name,evidence)
        indexed=api('query.run',{'query':'commands where project = tide sort when limit 10000'})['result']['rows']
        assert sorted(r['payload']['native_id'] for r in indexed)==sorted(r['id'] for r in facts if r['cwd']=='/work/tide' or r['cwd'].startswith('/work/tide/') and '/..' not in r['cwd'])
        summary=answer['attribution'];assert summary['total_members']==len(rows) and summary['complete']==(not summary['unknown_members'])
        if 'positive' in byname:assert summary['known_projects']['tide']>=1 and summary['known_projects']['other']==1 and summary['unconfigured_members']==1 and len(summary['unknown_members'])==3
        canonical=api('query.run',{'query':'commands sort when limit 10000'})['result']['rows'];mapping={r['id']:r['payload']['native_id'] for r in canonical}
        expected_unresolved=[g for g in reversed(independent(facts)) if g['start_ns']>expected['start_ns'] and any(r['id'] in g['parents'] and (not r['cwd'] or r['cwd']=='/work/ambiguous' or not r['cwd'].startswith('/') or '/..' in r['cwd']) for r in facts)]
        actual=[sorted(mapping[p] for p in candidate['session']['parents']) for candidate in answer['newer_unresolved_candidates']]
        assert actual==[g['parents'] for g in expected_unresolved],(actual,expected_unresolved)
        trace=cli('trace','sinex:ev/'+header['id'],'--bytes')['row'];assert sorted(p['event'] for p in trace['parents'])==sorted(header['parents'])
        for parent in trace['parents']:
            retained=parent['parents'];assert len(retained)==1
            raw=retained[0];fact=next(r for r in facts if r['id']==raw['record']['id'])
            assert raw['material'] and raw['end']>raw['anchor'] and all(raw['record'][c]==fact[c] for c in COLUMNS)
        shown=cli('show','sinex:ev/'+header['id']);assert header['id'] in json.dumps(shown)
        current=api('query.run',{'query':'events where kind = activity.session.boundary'})['result']['rows']
        actual_groups=[{'start_ns':int(g['payload']['start_ns']),'end_ns':int(g['payload']['end_ns']),'parents':sorted(mapping[p] for p in g['parents'])} for g in current]
        assert sorted(actual_groups,key=lambda g:g['start_ns'])==independent(facts)
        if source.exists():
            baseline_started=time.monotonic()
            with sqlite3.connect(source.as_uri()+'?mode=ro',uri=True) as con:
                provider_rows=[dict(zip(COLUMNS,row)) for row in con.execute('SELECT '+','.join(COLUMNS)+' FROM history ORDER BY timestamp,id')]
            assert provider_rows==sorted(facts,key=lambda r:(r['timestamp'],r['id']))
            baseline_groups=independent(provider_rows)
            baseline_selected=next(g for g in reversed(baseline_groups) if any(r['id'] in g['parents'] and (r['cwd']=='/work/tide' or r['cwd'].startswith('/work/tide/')) for r in provider_rows))
            assert baseline_selected==expected
            baseline_samples.append({'phase':label,'SQLite_calls':1,'rows':len(provider_rows),'returned_bytes':len(json.dumps(provider_rows).encode()),'read_and_independent_assembly_seconds':time.monotonic()-baseline_started,'selected_native_ids':baseline_selected['parents'],'same_selected_answer':True,'independent_groups':baseline_groups,'own_cwd_labels':{r['id']:('unknown' if not r['cwd'] or '/..' in r['cwd'] or r['cwd']=='/work/ambiguous' else 'known:tide' if r['cwd']=='/work/tide' or r['cwd'].startswith('/work/tide/') else 'known:other' if r['cwd']=='/work/other' or r['cwd'].startswith('/work/other/') else 'unconfigured') for r in provider_rows},'limits':'scripted finite assembly, no model effort or historical completeness claim'})
            save('provider-comparison-phases.json',baseline_samples)
        save(label+'.json',{'CLI':answer,'MCP':agent,'trace':trace,'independent_groups':independent(facts)})
        metrics.append({'phase':label,'CLI_plus_MCP_plus_trace_seconds':time.monotonic()-began,'CLI_MCP_recall_seconds':elapsed,'retained_bytes':sum(p.stat().st_size for p in (work/'recovery-blobs').rglob('*') if p.is_file()),'rows':len(facts)})
        if whole:
            validated_duration=time.monotonic()-began
            snapshot=phase_snapshot(h,label,daemon,journey_started,calls,transport,interventions)
            snapshot.update(question_elapsed_seconds=elapsed,validated_answer_seconds=validated_duration,
                phase_CLI_calls=calls[call_start:],phase_API_MCP_calls=transport[transport_start:],
                provider_baseline=baseline_samples[-1] if source.exists() else {'available':False,'reason':'configured provider path absent; no empty answer substituted'})
            snapshot['source']=api('sources.list')['result']['sources'][0]
            costs.append(snapshot);save('whole-loop-phases.json',costs)
        return answer
    try:
        fill(facts);configure()
        def accounting():
            nonlocal daemon
            from retention_accounting import account, manifest
            decoder=shutil.which('zstd');assert decoder, 'owner-prescribed zstd decoder unavailable'
            decoder_identity={'path':decoder,'sha256':hashlib.sha256(Path(decoder).resolve().read_bytes()).hexdigest(),
                'version':run([decoder,'--version'],label='existing owner-format decoder version').stdout.strip()}
            snapshots=[];identities=None;provider_before=hashlib.sha256(source.read_bytes()).hexdigest()
            directory=report/'retention-accounting';directory.mkdir()
            socket_path=work/'retention.sock'
            controller=socket.socket(socket.AF_UNIX);controller.bind(str(socket_path));controller.listen(1);controller.settimeout(30)
            daemon=start(config,'recovery','--qualification-boundary','collector-scheduled','--qualification-socket',socket_path)
            held,_=controller.accept();held.settimeout(5)
            frame=json.loads(held.makefile('rb').readline())
            assert frame['pid']==daemon.pid and frame['qualification_boundary']=='collector-scheduled'
            previous=None
            try:
                for index in range(3):
                    if index:
                        wait('next autonomous completed poll',lambda:(s if (s:=api('sources.list')['result']['sources'][0])['last_run']['id']!=previous and s['last_run']['status']=='completed' and s['acquisition']['schedule'] else None))
                    wait('poll derived state settled',settled)
                    if index==0:discover()
                    answer=qualify('retention-poll-'+str(index))
                    status=cli('sources')['sources'][0];observation=status['last_run']
                    if index==0:assert observation['id']==frame['evidence']['observation']
                    assert observation['status']=='completed' and status['acquisition']['schedule']
                    assert observation['rows_read']==len(facts) and observation['admitted']==(len(facts) if index==0 else 0)
                    assert observation['repeated']==(0 if index==0 else len(facts)) and observation['record_errors']==0
                    sql="SELECT jsonb_build_object('observations',(SELECT jsonb_agg(to_jsonb(o) ORDER BY seq) FROM raw.observations o),'cursor',(SELECT jsonb_agg(to_jsonb(c)) FROM raw.source_cursors c),'materials',(SELECT jsonb_agg(to_jsonb(m)||jsonb_build_object('blake3',b.blake3,'size_bytes',b.size_bytes,'blob_status',b.status) ORDER BY m.staged_at,m.id) FROM raw.source_material_registry m JOIN core.blobs b ON b.id=m.blob_id),'events',(SELECT jsonb_agg(jsonb_build_object('id',id,'type',event_type,'material',source_material_id,'anchor',anchor_byte,'end',offset_end) ORDER BY id) FROM core.events))"
                    def basis():
                        return json.loads(run(['psql',h['admin'],'-XqAt','-v','ON_ERROR_STOP=1','-c',sql],label='consistent retained custody and event audit').stdout)
                    database=basis()
                    assert len(database['observations'])==index+1 and len(database['materials'])==index+1
                    assert database['observations'][-1]['id']==observation['id'] and database['observations'][-1]['status']=='completed'
                    assert [o['seq'] for o in database['observations']]==list(range(1,index+2))
                    current={'events':database['events'],'session':answer['session'],'rows':answer['rows'],'generation':answer['resolved']['generation']}
                    if identities is None:identities=current
                    else:assert current==identities, 'unchanged poll changed event/session evidence'
                    poll_dir=directory/str(index);poll_dir.mkdir()
                    save('retention-basis-'+str(index)+'.json',{'source':status,'database':database,'files':manifest(work/'recovery-blobs')})
                    measured=account(work/'recovery-blobs',poll_dir,database['materials'],observation['id'],facts,run,decoder)
                    new=[m for m in measured['materials'] if m['new_this_observation']];assert len(new)==1
                    if snapshots:
                        assert new[0]['source_rows_sha256']==snapshots[0]['account']['materials'][0]['source_rows_sha256']
                        assert new[0]['envelopes_sha256']!=snapshots[-1]['account']['materials'][-1]['envelopes_sha256']
                        assert measured['file_bytes']-snapshots[-1]['account']['file_bytes']==new[0]['file']['bytes']
                    assert hashlib.sha256(source.read_bytes()).hexdigest()==provider_before
                    assert manifest(work/'recovery-blobs')==measured['files']
                    assert basis()==database, 'source/material basis changed across file capture; refuse raced accounting'
                    after=cli('sources')['sources'][0]
                    assert after['last_run']==status['last_run'] and after['cursor']==status['cursor'], 'collector advanced across accounting'
                    snapshots.append({'index':index,'boundary':frame if index==0 else None,'source':status,'database':database,'identity':current,'account':measured,
                        'coherence':'same observation/cursor/material basis before and after file reads; file manifests stable; no whole-filesystem atomicity claim'})
                    save('retention-accounting.json',{'decoder':decoder_identity,'provider_sha256':provider_before,'snapshots':snapshots,
                        'same_state_restarts':0,'poll_interval_ms':3000,'limit':'first baseline held by one-shot rendezvous, two later autonomous polls captured before next observation; finite file lengths, not physical IO, RSS, sustained capacity or everyday advantage; historical aggregate decomposition remains unavailable'})
                    previous=observation['id']
                    if index==0:held.sendall(b'c');held.close();held=None;controller.close()
            finally:
                if held is not None:
                    if daemon.poll() is None:stop(daemon)
                    held.close()
                controller.close()
            lines=['# Fixed-provider retained byte accounting','',
                'Three completed polls use one continuous collector, the same synthetic provider, configuration and store. The first baseline uses the existing one-shot collector rendezvous. Two later autonomous polls are sampled after EOF closure and scheduling, with equal observation/cursor/material basis before and after capture; raced accounting refuses. Directory manifests are stable observations, not atomic filesystem snapshots.','',
                '| Poll | Rows | Admitted | Repeated | Closed file bytes | New compressed blob | Logical source JSONL | Acquisition envelope extra | Encoding metadata | Open/tmp |',
                '| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |']
            for snapshot in snapshots:
                new=next(m for m in snapshot['account']['materials'] if m['new_this_observation']);o=snapshot['source']['last_run']
                lines.append(f"| {snapshot['index']} | {o['rows_read']} | {o['admitted']} | {o['repeated']} | {snapshot['account']['file_bytes']} | {new['file']['bytes']} | {new['logical_source_rows_jsonl_bytes']} | {new['acquisition_envelope_extra_bytes']} | unknown | 0/0 |")
            lines+=['','Logical source JSONL includes row serialization and newline bytes. Acquisition envelope extra is the exact incremental serialization of `_from`; encoding frame bytes include compressed content and zstd framing/checksum. The owner decoder does not separately expose verified seek-metadata bytes, so that finer split remains unknown. There is no additive attribution of compressed bytes to individual JSON fields.','',
                'Event IDs, retained event coordinates, canonical rows, session and projection generation remain stable. Observation IDs, acquisition envelopes, material/blob identities and source counters change. Exact blobs and decoded JSONL are preserved privately. The historical 1048/2687/3802 aggregate has no retained per-file manifest and is not retrospectively decomposed. This finite run measures file lengths, not physical IO, allocated blocks, RSS, sustained capacity or everyday benefit.']
            (report/'retention-accounting.md').write_text('\n'.join(lines)+'\n')
        if whole:
            intervention('collector start',configuration_sha256=hashlib.sha256(config.read_bytes()).hexdigest())
            daemon=start(config,'recovery');wait('startup',lambda:api() if daemon.poll() is None else None);wait('baseline settled',settled);discover()
        else:scenario(ACCOUNTING,accounting)
        def initial():
            answer=qualify('initial')
            if whole:return
            atuin_config=work/'atuin-baseline-config';atuin_config.mkdir()
            atuin_copy=work/'atuin-baseline.db'
            with sqlite3.connect(source.as_uri()+'?mode=ro',uri=True) as original, sqlite3.connect(atuin_copy) as copy:original.backup(copy)
            (atuin_config/'config.toml').write_text(f'db_path = "{atuin_copy}"\nauto_sync = false\nkey_path = "{work/"atuin-key"}"\n')
            isolated=env.copy();isolated.update(ATUIN_CONFIG_DIR=str(atuin_config),XDG_DATA_HOME=str(work/'atuin-data'),XDG_CACHE_HOME=str(work/'atuin-cache'))
            atuin=run(['atuin','search','--include-duplicates','--reverse','--limit','10000','--format','{time}\t{directory}\t{command}'],env=isolated,expected=(0,1,2),label='existing Atuin on owned same-input copy')
            save('atuin-baseline.json',{'exit':atuin.returncode,'stdout':atuin.stdout,'stderr':atuin.stderr,'input':'owned same-input SQLite backup; no live Atuin state','limitation':'Atuin formatting omits exact native IDs/retained citations; SQLite remains the exact-field fallback; provider schema refusal is preserved if present'})
            # Initialize Atuin's own migration history, then supply exactly the
            # same logical fields. A fixture-copy migration refusal is not an
            # Atuin capability result.
            native_copy=work/'atuin-native-baseline.db'
            (atuin_config/'config.toml').write_text(f'db_path = "{native_copy}"\nauto_sync = false\nkey_path = "{work/"atuin-key"}"\n')
            version=run(['atuin','--version'],env=isolated,label='existing Atuin baseline version').stdout.strip()
            run(['atuin','search','--limit','10000','--include-duplicates'],env=isolated,expected=(0,1),label='initialize owned native Atuin empty schema')
            with sqlite3.connect(native_copy) as con:
                con.executemany('INSERT INTO history('+','.join(COLUMNS)+') VALUES(?,?,?,?,?,?,?,?,?)',[[r[c] for c in COLUMNS] for r in facts])
            native=run(['atuin','search','--include-duplicates','--reverse','--limit','10000','--format','{time} | {duration} | {directory} | {command}'],env=isolated,label='existing Atuin same-input native schema')
            lines=native.stdout.splitlines();assert len(lines)==len(facts)
            assert all(any(line.endswith(r['cwd']+' | '+r['command']) for line in lines) for r in facts)
            save('atuin-native-baseline.json',{'version':version,'exit':native.returncode,'stdout':native.stdout,'stderr':native.stderr,'input_fields':COLUMNS,'input_count':len(facts),'same_commands_and_own_cwd':True,'limits':'native own-schema copy with the same logical fields; formatted time/duration do not preserve every exact nanosecond/ID; no retained Sinex citations; SQLite is the stronger exact-field fallback; grouping/attribution still require client processing'})
            empty=cli('recall','last','--project','empty');agent=tool('empty')
            assert empty['session'] is None and empty['rows']==[] and len(empty['newer_unresolved_candidates'])==3 and agent['session'] is None and agent['newer_unresolved_candidates']==empty['newer_unresolved_candidates']
            invalid=cli('recall','last','--project','not-registered',expected=2);assert 'not configured' in json.dumps(invalid)
            save('qualified-empty.json',empty)
        scenario(PLANNED[0],initial)
        if whole:
            def repeated():
                initial_answer=json.loads((report/'initial.json').read_text())['CLI']
                prior=api('sources.list')['result']['sources'][0]['last_run']['id']
                wait('completed unchanged source',lambda:(v if (v:=api('sources.list')['result']['sources'][0])['last_run']['id']!=prior and v['last_run']['status']=='completed' and v['last_run']['admitted']==0 and v['last_run']['repeated']==len(facts) else None))
                answer=qualify('completed-unchanged')
                assert answer['session']==initial_answer['session'] and answer['rows']==initial_answer['rows']
            scenario(WHOLE_UNCHANGED,repeated)
            def active_reading():
                prior=api('sources.list')['result']['sources'][0]['last_run']['id']
                baseline=json.loads((report/'completed-unchanged.json').read_text())['CLI']
                lock=sqlite3.connect(source);lock.execute('BEGIN EXCLUSIVE')
                intervention('controlled SQLite exclusive read block',scope='synthetic active-reading observation, not normal workload')
                try:
                    reading=wait('actual reading observation',lambda:(v if (v:=api('sources.list')['result']['sources'][0])['last_run']['id']!=prior and v['last_run']['status']=='reading' else None))
                    began=time.monotonic();answer=cli('recall','last','--project','tide');agent=tool()
                    after=api('sources.list')['result']['sources'][0]
                    assert after['last_run']['id']==reading['last_run']['id'] and after['last_run']['status']=='reading', 'active-read question was not bracketed by the same reading observation'
                    assert answer['session']==agent['session']==baseline['session'] and answer['rows']==agent['rows']==baseline['rows']
                    assert answer['complete'] is False and agent['complete'] is False
                    provider={'available':False,'reason':None}
                    try:
                        with sqlite3.connect(source.as_uri()+'?mode=ro',uri=True,timeout=.05) as con:con.execute('SELECT '+','.join(COLUMNS)+' FROM history').fetchall()
                    except sqlite3.OperationalError as error:provider['reason']=str(error)
                    assert provider['reason'] and 'locked' in provider['reason']
                    save('active-reading.json',{'before':reading,'after':after,'CLI':answer,'MCP':agent,'question_seconds':time.monotonic()-began,
                        'SQLite':provider,'scope':'controlled actual reading while provider access is blocked; previous retained facts answer with incomplete coverage; no fresh-acquisition or ordinary-load latency claim'})
                finally:
                    lock.rollback();lock.close();intervention('release controlled SQLite read block')
                wait('reading control completed',lambda:(v if (v:=api('sources.list')['result']['sources'][0])['last_run']['status']=='completed' else None))
                qualify('reading-control-recovered')
            scenario(WHOLE_READING,active_reading)
        def bridge():
            began=time.monotonic();row=fixture_row('late-bridge','03:54:00','/work/tide',12*60*1_000_000_000);facts.append(row);fill([row]);wait('bridge settled',settled)
            answer=qualify('bridged');assert answer['session']['payload']['owner_day']=='2026-10-12'
            generation=answer['resolved']['generation'];identity=answer['session']['id']
            prior=api('sources.list')['result']['sources'][0]['last_run']['id']
            wait('completed unchanged poll',lambda:(s if (s:=api('sources.list')['result']['sources'][0])['last_run']['id']!=prior and s['last_run']['status']=='completed' and s['last_run']['admitted']==0 and s['last_run']['repeated']==len(facts) else None))
            wait('unchanged poll derived state',settled)
            repeated=qualify('unchanged');assert repeated['session']['id']==identity and repeated['resolved']['generation']==generation
            metrics.append({'phase':'bridge-whole-loop','source_append_through_CLI_MCP_trace_seconds':time.monotonic()-began,'includes_unchanged_poll_wait':True})
        scenario(PLANNED[1],bridge)
        def continuation():
            row=fixture_row('later-positive','08:00:00','/work/tide');facts.append(row);began=time.monotonic();fill([row]);wait('later settled',settled);answer=qualify('later')
            metrics.append({'phase':'later-whole-loop','source_append_through_CLI_MCP_trace_seconds':time.monotonic()-began})
            oldgeneration=answer['resolved']['generation'];oldid=answer['session']['id'];stop(daemon)
            if whole:
                intervention('prescribed compatible restart',configuration_sha256=hashlib.sha256(config.read_bytes()).hexdigest())
                restart();changed=qualify('compatible-restart');assert changed['resolved']['generation']==oldgeneration and changed['session']['id']==oldid
                source.rename(work/'provider-removed.db');intervention('prescribed synthetic provider path removal',collector_running=daemon.poll() is None)
                wait('provider unavailable',lambda:(v if (v:=api('sources.list')['result']['sources'][0])['last_run']['status']=='unavailable' else None))
                absent=qualify('provider-loss-retained');assert absent['session']['id']==oldid and absent['complete'] is False;save('provider-loss.json',absent)
            else:
                configure('[[projects]]\nname = "new-registration"\nroots = ["/new-registration"]\n')
                restart();changed=qualify('configuration-rebuilt');assert changed['resolved']['generation']!=oldgeneration and changed['session']['id']==oldid
                stop(daemon);source.rename(work/'provider-removed.db');restart(retained=True)
                absent=qualify('provider-loss-retained');assert absent['session']['id']==oldid and absent['complete'] is False;save('provider-loss.json',absent)
                stop(daemon);(work/'provider-removed.db').rename(source);restart()
        def restart(retained=False):
            nonlocal daemon,restarts
            restarts+=1
            daemon=start(config,'recovery-restarted-'+str(restarts))
            wait('restart ready',lambda:api() if daemon.poll() is None else None)
            wait('restart settled',settled)
        scenario(WHOLE_CONTINUATION if whole else PLANNED[2],continuation)
        def current_fence():
            nonlocal daemon
            stop(daemon)
            controller=socket.socket(socket.AF_UNIX);controller.bind(str(work/'current-parent.sock'));controller.listen(1);controller.settimeout(30)
            daemon=start(config,'recovery-parent-fence','--qualification-boundary','collector-scheduled','--qualification-socket',work/'current-parent.sock')
            held,_=controller.accept();held.settimeout(5)
            frame=json.loads(held.makefile('rb').readline());assert frame['pid']==daemon.pid
            save('current-parent-collector-frame.json',frame)
            try:
                # The existing rendezvous holds only the collector after its
                # completed observation and schedule commit. Readers, writer and
                # derived maintenance remain responsive on the same configuration.
                wait('held collector settled',settled)
                original=cli('recall','last','--project','tide');header=original['session'];canonical=original['rows'][0]
                assert canonical['payload']['native_id']=='later-positive'
                for label,event in [('raw',canonical['parents'][0]),('canonical',canonical['id'])]:
                    # A deliberate read-boundary control, not a supported mutation
                    # route: keep seq/config/dirty unchanged while invalidating one
                    # exact synthetic parent through reversible archive state. Event
                    # rows and their immutability guard remain untouched.
                    # This distinguishes the parent guard
                    # from an incidental pending-ledger refusal.
                    def state(value):
                        sql=(f"INSERT INTO audit.archived(id,archived_seq,reason) SELECT '{event}'::uuid,seq,'supersede' FROM core.commit_counter WHERE one" if value=='archived' else f"DELETE FROM audit.archived WHERE id='{event}'::uuid")
                        run(['psql',h['admin'],'-XAt','-v','ON_ERROR_STOP=1','-c',sql],label='owned '+label+' current-parent control '+value)
                    state('archived')
                    try:
                        def caught_up():
                            status=api()['result'];index=status['indexed_retrieval']
                            # Poll observations may independently advance the commit
                            # position. Compare the index's same-statement frontier,
                            # rather than assuming a wall-clock quiescent collector.
                            return status if index['pending_canonical']==0 and index['pending_sessions']==0 and index['position']==index['current_commit_seq'] and index['serving'] else None
                        status=wait('current-parent control projection caught up',caught_up)
                        assert status['indexed_retrieval']['generation']==original['resolved']['generation']
                        shown=cli('show','sinex:ev/'+header['id'])['row'];assert shown['current_eligible'] is False
                        refused=cli('recall','last','--project','tide',expected=5);assert 'ineligible current parents' in json.dumps(refused)
                        agent=tool(error=True);assert 'ineligible current parents' in json.dumps(agent)
                        save('current-'+label+'-fence.json',{'control':'owned direct read-boundary invalidation; no writer-recovery claim','status':status,'old_header':header,'current_show':shown,'CLI':refused,'MCP':agent})
                    finally:state('restored')
                    restored=cli('recall','last','--project','tide');assert restored['session']['id']==header['id'] and restored['rows']==original['rows']
            finally:
                try:held.sendall(b'c')
                finally:held.close();controller.close()
        if not whole:scenario(PLANNED[4],current_fence)
        def overflow():
            # 101 direct members remain a supported session but cannot be assembled.
            rows=[fixture_row('large-'+str(n),'09:00:00','/work/tide') for n in range(101)];facts.extend(rows);fill(rows)
            wait('large canonical',lambda:api('query.run',{'query':'commands sort when limit 10000'}).get('result',{}).get('total')==len(facts))
            wait('large refusal',lambda:'assembly exceeds' in json.dumps(api('query.run',{'query':'recall last where project = tide'})))
            refused=cli('recall','last','--project','tide',expected=5);assert 'assembly exceeds' in json.dumps(refused);tool(error=True);save('whole-session-refusal.json',refused)
            # Newer separated unknown sessions force overflow even for empty-positive.
            rows=[]
            base=fixture_row('candidate-base','10:00:00','')['timestamp']
            for n in range(129):
                row=fixture_row('candidate-'+str(n),'10:00:00','');row['timestamp']=base+n*600_000_000_000;rows.append(row)
            facts.extend(rows);fill(rows)
            wait('candidate canonical',lambda:api('query.run',{'query':'commands sort when limit 10000'}).get('result',{}).get('total')==len(facts))
            wait('candidate refusal',lambda:'128 sessions' in json.dumps(api('query.run',{'query':'recall last where project = empty'})))
            refused=cli('recall','last','--project','empty',expected=5);assert '128 sessions' in json.dumps(refused);tool('empty',error=True);save('candidate-overflow.json',refused)
        if not whole:scenario(PLANNED[3],overflow)
        if whole:
            save('whole-loop.json',{'reused_accounting':reuse,'phases':costs,'public_CLI_calls':calls,'public_API_MCP_calls':transport,'interventions':interventions,
                'finite_source_change_to_answer':metrics,'source_facts':facts,'restart_count':restarts,'runtime_repairs':0,
                'measurement_scope':'one synthetic scripted journey; phase work includes response validation/citation reads; raw API/MCP bodies and CLI streams exclude requests/headers/transport overhead; phase DB/file/process reads are not atomic',
                'whole_loop_CPU':'whole-loop-rusage.json is emitted after final child reaping/PostgreSQL shutdown; reaped-child CPU is not an inclusive process-tree total',
                'unknowns':{'physical_IO':'no device-level workload attribution','simultaneous_aggregate_peak_RSS':'no continuous simultaneous process-tree RSS sample; individual VmHWM and named samples cannot recover this peak','everyday_benefit':'no human/model productivity trial or cost threshold','source_history':'observed acquisitions do not prove complete capture'}})
            lines=['# Repeated project recovery and observed costs','',
                'One fixed configured synthetic collector answers the same project recovery question through CLI and MCP. The journey includes completed unchanged polling, a late bridge, a later arrival, compatible restart and provider-path loss. Exact source membership, own-context qualifications and retained citations are checked independently. Prior three-poll custody accounting is reused, not rerun.','',
                '| Phase | Last poll rows read | CLI/MCP question seconds | Question plus validation seconds | DB bytes | Retained file bytes | Workspace total file bytes |',
                '| --- | --- | --- | --- | --- | --- | --- |']
            for c in costs:
                lines.append(f"| {c['phase']} | {c['source']['last_run']['rows_read']} | {c['question_elapsed_seconds']:.6f} | {c['validated_answer_seconds']:.6f} | {c['database']['database_bytes']} | {c['files']['retained']['file_length_bytes']} | {c['files']['workspace_total']['file_length_bytes']} |")
            lines+=['','Independent read-only SQLite plus finite grouping obtains the same selected facts while provider access exists. During the deliberately blocked read, independent SQLite access is also explicitly blocked; retained native answers remain qualified incomplete. Provider-path loss is unavailable, not empty. Public question calls, polling/validation traffic and baseline reads are retained separately.','',
                'Setup, producer commits, the controlled read block/release, one compatible restart and provider-path removal are explicit interventions. Costs are observed finite timings, not performance targets or productivity estimates. Acquisition-to-validated-answer timings include polling waits, client work and independent validation.','',
                'Named process samples retain CPU counters, VmRSS and individual VmHWM for the daemon and observed PostgreSQL processes. They are not simultaneous aggregate peaks. Whole-loop OS rusage is recorded after child/PostgreSQL shutdown and before final report serialization/workspace deletion; reaped-child CPU covers only what Linux accounts through wait, not every descendant. Workspace total includes PostgreSQL, retained blobs, provider fixtures and other owned files; it overlaps the component figures and must not be added to them. Scratch-only growth is unknown because no separate scratch directory exists. File lengths and allocated blocks are not physical IO. Physical IO and simultaneous aggregate peak RSS remain unknown for the concrete reasons in whole-loop.json. No everyday advantage, sustained capacity, full milestone or complete source-history claim follows.']
            (report/'whole-loop-report.md').write_text('\n'.join(lines)+'\n')
            return
        # Existing SQLite access is the strongest exact provider fallback for this
        # synthetic Atuin schema. It receives the same rows and project settings.
        began=time.monotonic()
        with sqlite3.connect(source) as con:baseline=[dict(zip(COLUMNS,row)) for row in con.execute('SELECT '+','.join(COLUMNS)+' FROM history ORDER BY timestamp,id')]
        assert baseline==sorted(facts,key=lambda r:(r['timestamp'],r['id']))
        save('comparison.json',{'route':'one read-only SQLite SELECT over the same configured synthetic Atuin provider; independent interval assembly and own-cwd rules require client processing','provider_rows':len(baseline),'provider_bytes':len(json.dumps(baseline).encode()),'provider_read_seconds':time.monotonic()-began,'fidelity':'all exact source fields available before removal; finite grouping and labels equal independent expected facts','ties_and_limits':['same available source facts can answer equally after client processing','native source-loss retention qualified separately','no model trial, everyday superiority or sustained memory/IO claim','native candidate overflow refuses while SQLite full read remains available'],'native_CLI_calls':calls,'public_transport_calls':transport,'measurement_basis':'CLI stdout UTF-8 bytes and actual API response bodies/MCP response lines; excludes request bodies, headers and transport overhead; polling and validation are recorded separately','same_phase_SQLite_comparison':baseline_samples,'measurements':metrics,'whole_loop_boundary':'append through observed settled recall, CLI/MCP and retained trace; selected process/database/retained bytes do not measure peak RSS, CPU or physical IO'})
    finally:
        if daemon is not None and daemon.poll() is None:stop(daemon)
