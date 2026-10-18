"""Finite public-client read-plan qualification; all provider records are synthetic."""
import copy
import datetime as dt
import json
import select
import shutil
import socket
import sqlite3
import subprocess
import time
import urllib.request

COLUMNS = ['id','timestamp','duration','exit','command','cwd','session','hostname','deleted_at']
PLANNED = [
    'initial full and unchanged reduced values preserve recovery and exact receipts',
    'prior unfinished late completion and end-known unknown-exit eligibility',
    'outside-readset change waits for due full and failed full preserves checkpoint',
    'VACUUM version rename replacement symlink and reopen force actual full',
    'restart provider loss preserves CLI MCP facts qualifications and citations',
]

def exercise(h):
    work,report,env,bins=(h[k] for k in ['WORK','REPORT','ENV','BIN_DIR'])
    run,scenario,start,stop=(h[k] for k in ['run','scenario','start_daemon','stop'])
    source=work/'read-plan.db'; configured=work/'configured.db'; config=work/'read-plan.toml'
    endpoint=f'http://127.0.0.1:{h["free_port"]()}/rpc';token=(work/'token').read_text()
    read_model=work/'read-plan-read-model'
    read_model.write_text(h['reader'].replace('sinex_reader','sinex_read_model'));read_model.chmod(0o600)
    stamp=lambda day: int(dt.datetime.fromisoformat(day+'+00:00').timestamp())*1_000_000_000
    def row(key,when,cwd='/work/other',duration=0,exit=0):
        return dict(zip(COLUMNS,[key,when,duration,exit,'echo '+key,cwd,'synthetic-shell','synthetic-host',None]))
    facts=[row('very-old-unfinished',stamp('2026-10-01T00:00:00'),duration=-1,exit=-1)]
    facts += [row(f'history-{n:03}',stamp('2026-10-02T00:00:00')+n*60_000_000_000) for n in range(192)]
    facts += [row('recovery',stamp('2026-10-13T12:00:00'),'/work/tide'),row('end-unknown',stamp('2026-10-13T12:01:00'),'/work/tide',exit=-1)]
    def create(path):
        with sqlite3.connect(path) as con:
            con.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT,deleted_at INTEGER)')
            con.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',[[r[k] for k in COLUMNS] for r in facts])
    create(source);configured.symlink_to(source)
    def configure(full=90000):
        config.write_text(f'''listen = "{endpoint.removeprefix('http://').removesuffix('/rpc')}"
state_dir = "{work/'read-plan-state'}"
store_dir = "{work/'read-plan-blobs'}"
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
[atuin]
path = "{configured}"
poll_interval_ms = 1200
full_interval_ms = {full}
''')
    def save(name,value): (report/(name+'.json')).write_text(json.dumps(value,indent=2)+'\n')
    save('independent-provider-facts',facts)
    daemon=mcp=None;identifier=0;controls=[]
    def api(method='status',params=None):
        request=urllib.request.Request(endpoint,data=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params or {}}).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
        with urllib.request.urlopen(request,timeout=12) as response: answer=json.load(response)
        with (report/'read-plan-api.jsonl').open('a') as log:log.write(json.dumps({'method':method,'params':params,'response':answer})+'\n')
        return answer
    def cli(command,*args,expected=(0,5)):
        result=run([bins/'sinexctl','--endpoint',endpoint,'--token-file',work/'token',command,*args,'--json'],expected=expected,label='read plan '+command)
        return json.loads(result.stdout or result.stderr)
    def wait(label,predicate,seconds=150):
        end=time.monotonic()+seconds
        while time.monotonic()<end:
            assert daemon.poll() is None,'owned daemon exited'
            try: result=predicate()
            except (OSError,KeyError): result=None
            if result:return result
            time.sleep(.06)
        raise AssertionError(label)
    def source_status():
        response=api('sources.list')
        return response['result']['sources'][0]
    def completed(after=None,kind=None):
        s=source_status();o=s['last_run'];p=o['metadata'].get('_sinex_read_plan_v1')
        return s if o['status']=='completed' and o['id']!=after and o['metadata'].get('_sinex_acquisition_v1',{}).get('schedule') and p and (kind is None or p['kind']==kind) else None
    def next_poll(previous,kind=None): return wait('next completed '+str(kind),lambda:completed(previous['last_run']['id'],kind))
    def unavailable(after=None,minimum=0):
        s=source_status();o=s['last_run']
        return s if o['status']=='unavailable' and o['id']!=after and o['rows_read']>=minimum else None
    def plan(s):return s['last_run']['metadata']['_sinex_read_plan_v1']
    def check_count(s,n):assert s['last_run']['rows_read']==n,(s['last_run'],n)
    def carry():
        result=run(['psql',h['reader'],'--no-psqlrc','--tuples-only','--no-align','-v','ON_ERROR_STOP=1','-c',"SELECT carry FROM raw.source_cursors WHERE binding='terminal.atuin-history'"],label='reader checkpoint inspection')
        return json.loads(result.stdout)
    def read_orders():
        sql="SELECT jsonb_object_agg(e.payload->>'native_id',l.read_order) FROM core.live_equivalence l JOIN core.events e ON e.id=l.event_id WHERE e.source='terminal.atuin-history' AND e.payload->>'native_id' IN ('very-old-unfinished','history-000','end-unknown')"
        result=run(['psql',h['reader'],'--no-psqlrc','--tuples-only','--no-align','-v','ON_ERROR_STOP=1','-c',sql],label='value reading-order inspection')
        return json.loads(result.stdout)
    def retained_values(status, expected, label):
        observation=status['last_run']['id']
        sql="SELECT coalesce(jsonb_agg(jsonb_build_object('id',m.id,'observation',m.observation_id,'status',m.status,'bytes',m.durable_bytes,'hash',b.blake3)), '[]') FROM raw.source_material_registry m LEFT JOIN core.blobs b ON b.id=m.blob_id WHERE m.observation_id='"+observation+"'::uuid"
        result=run(['psql',h['reader'],'--no-psqlrc','--tuples-only','--no-align','-v','ON_ERROR_STOP=1','-c',sql],label='retained selected values')
        materials=json.loads(result.stdout);actual=[]
        decoder=shutil.which('zstd');assert decoder,'owner-prescribed decoder unavailable'
        for m in materials:
            assert m['status']=='completed' and m['bytes']>0 and m['observation']==observation
            digest=m['hash'].removeprefix('blake3:');blob=work/'read-plan-blobs'/'blobs'/digest[:2]/(digest+'.zst')
            preserved=report/(m['id']+'.zst');shutil.copy2(blob,preserved)
            decoded=run([decoder,'-d','-q','-c',preserved],label='decode retained selected values').stdout
            assert len(decoded.encode())==m['bytes']
            records=[json.loads(line) for line in decoded.splitlines()]
            for record in records:
                assert record['_from']['observation']==observation and record['_from']['table']=='history'
                assert isinstance(record['_from']['rowid'],int)
                actual.append({k:v for k,v in record.items() if k!='_from'})
            (report/(m['id']+'.jsonl')).write_text(decoded)
        assert sorted(actual,key=lambda r:r['id'])==sorted(expected,key=lambda r:r['id'])
        save(label,{'source':status,'materials':materials,'decoded_values':actual,'scope':'complete original envelopes for actually selected values only; zero-read polls create no value confirmation'})
    def raw(key):
        answer=api('query.run',{'query':'events where source = terminal.atuin-history and kind = command.executed'})['result']
        # Native keys below are on the first time-sorted page; no whole-history client cache.
        rows=answer['rows'];cursor=answer.get('page',{}).get('cursor')
        while not any(r['payload']['native_id']==key for r in rows) and cursor:
            answer=api('query.run',{'query':'events where source = terminal.atuin-history and kind = command.executed','cursor':cursor})['result'];rows=answer['rows'];cursor=answer.get('page',{}).get('cursor')
        return next(r for r in rows if r['payload']['native_id']==key)
    def invoke(method,params):
        nonlocal identifier
        identifier+=1;mcp.stdin.write(json.dumps({'jsonrpc':'2.0','id':identifier,'method':method,'params':params})+'\n');mcp.stdin.flush()
        until=time.monotonic()+25
        while True:
            ready,_,_=select.select([mcp.stdout],[],[],max(0,until-time.monotonic()));assert ready,'MCP deadline'
            response=json.loads(mcp.stdout.readline())
            with (report/'read-plan-mcp.jsonl').open('a') as log:log.write(json.dumps(response)+'\n')
            if response.get('id')==identifier:return response
    def discover():
        nonlocal mcp
        err=(report/'read-plan-mcp.stderr').open('a');argv=[str(bins/'sinexctl'),'--endpoint',endpoint,'--token-file',str(work/'token'),'mcp']
        mcp=subprocess.Popen(argv,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,text=True,bufsize=1)
        h['CHILDREN'].append((mcp,mcp.stdout,err));h['PROCESSES'].append({'pid':mcp.pid,'argv':argv,'stderr':'read-plan-mcp.stderr'})
        invoke('initialize',{'protocolVersion':'2024-11-05','capabilities':{},'clientInfo':{'name':'read-plan-qualification','version':'1'}})
        mcp.stdin.write(json.dumps({'jsonrpc':'2.0','method':'notifications/initialized'})+'\n');mcp.stdin.flush()
        assert 'sinex_recall' in json.dumps(invoke('tools/list',{}))
    def verify_recovery(answer):
        rows=answer['rows'];assert sorted(r['payload']['native_id'] for r in rows)==['end-unknown','recovery']
        by={r['payload']['native_id']:r for r in rows}
        assert by['end-unknown']['payload']['exit_code'] is None
        assert by['end-unknown']['payload']['completion']=='ended'
        assert set(answer['session']['parents'])=={r['id'] for r in rows}
        assert answer['attribution']=={'rule':'direct_member_own_cwd','known_projects':{'tide':2},'unconfigured_members':0,'unknown_members':[],'total_members':2,'complete':True}
        assert answer['newer_unresolved_candidates']==[]
        assert answer['coverage']=='unknown; retained shell policy grouping; focus, AFK, attention and underlying command outcomes are not inferred'
        assert int(answer['session']['payload']['start_ns'])==stamp('2026-10-13T12:00:00')
        assert int(answer['session']['payload']['end_ns'])==stamp('2026-10-13T12:01:00')
    def recovery(label,prior=None,unavailable=False):
        wait('recovery projection ready',lambda:api('query.run',{'query':'recall last where project = tide'}).get('result'))
        answer=cli('recall','last','--project','tide',expected=5 if unavailable else (0,5))
        result=invoke('tools/call',{'name':'sinex_recall','arguments':{'project':'tide'}})['result'];agent=result.get('structuredContent') or json.loads(result['content'][0]['text'])
        verify_recovery(answer);verify_recovery(agent)
        assert all(answer[k]==agent[k] for k in ['session','rows','attribution','newer_unresolved_candidates','coverage','read_as'])
        if prior:assert answer['session']==prior['session'] and answer['rows']==prior['rows']
        trace=cli('trace','sinex:ev/'+answer['session']['id'],'--bytes')['row']
        for parent in trace['parents']:
            retained=parent['parents'][0];fact=next(r for r in facts if r['id']==retained['record']['id'])
            assert all(retained['record'][key]==fact[key] for key in COLUMNS)
            assert retained['end']>retained['anchor'] and retained['material']
        if unavailable: assert answer['complete'] is False
        direct=None
        if source.exists():
            with sqlite3.connect(source.as_uri()+'?mode=ro',uri=True) as con:
                direct=[dict(zip(COLUMNS,r)) for r in con.execute("SELECT "+','.join(COLUMNS)+" FROM history WHERE cwd='/work/tide' ORDER BY timestamp,id")]
            assert direct==[next(f for f in facts if f['id']==key) for key in ['recovery','end-unknown']]
        save(label,{'CLI':answer,'MCP':agent,'trace':trace,'same_fact_direct_SQLite':direct,'baseline_limit':'fixed independent synthetic answer ties while provider is accessible; retained citations and source-loss qualification are separate; no general superiority claim'})
        return answer
    def change(key,**values):
        fact=next(r for r in facts if r['id']==key);fact.update(values)
        with sqlite3.connect(source) as con:con.execute('UPDATE history SET '+','.join(k+'=?' for k in values)+' WHERE id=?',[*values.values(),key])
        save('independent-provider-facts',facts)
    try:
        configure();daemon=start(config,'read-plan');wait('startup',lambda:api().get('result'));discover()
        def initial():
            nonlocal first,baseline,checkpoint
            first=wait('initial actual full',lambda:completed(kind='full_value_comparison'));check_count(first,len(facts));checkpoint=carry()
            assert checkpoint==plan(first)['checkpoint']
            first_orders=read_orders()
            second=next_poll(first,'new_coordinates_and_prior_unfinished');check_count(second,1)
            second_orders=read_orders()
            assert second_orders['history-000']==first_orders['history-000'] and second_orders['end-unknown']==first_orders['end-unknown']
            assert second_orders['very-old-unfinished']>first_orders['very-old-unfinished']
            save('selected-value-reading-order',{'full':first_orders,'reduced':second_orders})
            assert plan(second)['checkpoint']['full_due_at']==checkpoint['full_due_at']
            retained_values(second,[facts[0]],'unchanged-full-original-envelope')
            baseline=recovery('reduced-recovery')
            bad=copy.deepcopy(baseline);next(r for r in bad['rows'] if r['payload']['native_id']=='end-unknown')['payload']['completion']='running'
            try:verify_recovery(bad)
            except AssertionError:controls.append('unsupported running-answer mutation rejected')
            else:raise AssertionError('damaging answer mutation survived')
            with sqlite3.connect(source.as_uri()+'?mode=ro',uri=True) as con:
                queries={
                    'new_range':'SELECT * FROM history WHERE rowid>195 ORDER BY rowid LIMIT 1',
                    'prior_native_key':"SELECT * FROM history WHERE id='very-old-unfinished' AND rowid<=195 ORDER BY rowid LIMIT 1",
                    'wrong_provider_unfinished_or': 'SELECT * FROM history WHERE rowid>195 OR (duration=-1 AND exit=-1)',
                }
                plans={k:list(con.execute('EXPLAIN QUERY PLAN '+sql)) for k,sql in queries.items()}
                assert 'INTEGER PRIMARY KEY' in str(plans['new_range']) and 'INDEX' in str(plans['prior_native_key'])
                assert 'SCAN' in str(plans['wrong_provider_unfinished_or'])
                save('provider-query-plans',{'plans':plans,'indexes':list(con.execute('PRAGMA index_list(history)')),'scope':'selected records and retained bytes only; no scan IO CPU reduction claim'})
            pg_plan=run(['psql',h['reader'],'--no-psqlrc','--tuples-only','--no-align','-v','ON_ERROR_STOP=1','-c',"EXPLAIN (FORMAT JSON) SELECT e.id,e.equivalence_key::jsonb->>1 FROM core.events e WHERE e.source='terminal.atuin-history' AND e.event_type='command.executed' AND e.record_state='present' AND e.payload->>'completion'='end_not_recorded' AND (e.equivalence_key::jsonb->>0)='"+first['input']+"' AND e.commit_seq<="+str(first['last_run']['commit_seq'])+" AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.id=e.id AND a.archived_seq<="+str(first['last_run']['commit_seq'])+") ORDER BY e.id LIMIT 64"],label='canonical unfinished eligibility query plan')
            save('canonical-eligibility-query-plan',json.loads(pg_plan.stdout))
            save('initial-and-reduced',{'full':first,'reduced':second,'checkpoint':checkpoint})
        first=baseline=checkpoint=None;scenario(PLANNED[0],initial)
        def unfinished():
            old=raw('very-old-unfinished');assert old['payload']['completion']=='end_not_recorded'
            previous=wait('fresh reduced',lambda:completed(kind='new_coordinates_and_prior_unfinished'))
            phase_due=plan(previous)['checkpoint']['full_due_at']
            change('very-old-unfinished',duration=10,exit=7)
            updated=next_poll(previous,'new_coordinates_and_prior_unfinished');check_count(updated,1)
            changed=raw('very-old-unfinished');assert changed['id']!=old['id'] and changed['payload']['completion']=='ended' and changed['payload']['exit_code']==7
            fresh=row('fresh-native-coordinate',stamp('2026-10-03T00:00:00'))
            facts.append(fresh)
            with sqlite3.connect(source) as con:con.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',[fresh[k] for k in COLUMNS])
            arrival=next_poll(updated,'new_coordinates_and_prior_unfinished');check_count(arrival,1)
            assert raw('fresh-native-coordinate')['payload']['command']==fresh['command']
            zero=next_poll(arrival,'new_coordinates_and_prior_unfinished');check_count(zero,0)
            assert plan(zero)['checkpoint']['full_due_at']==phase_due
            retained_values(zero,[],'zero-read-no-confirmation')
            recovery('after-old-late-completion',baseline)
            save('late-completion',{'before':old,'after':changed,'read':updated,'zero':zero})
        scenario(PLANNED[1],unfinished)
        def due_full():
            before=wait('reduced before outside change',lambda:completed(kind='new_coordinates_and_prior_unfinished'))
            old=raw('history-000');change('history-000',exit=9)
            reduced=next_poll(before,'new_coordinates_and_prior_unfinished');check_count(reduced,0)
            assert raw('history-000')['id']==old['id'],'outside-readset key advanced without a value read'
            full=next_poll(reduced,'full_value_comparison');check_count(full,len(facts))
            assert plan(full)['checkpoint']['epoch']==plan(reduced)['checkpoint']['epoch'],'due full substituted a reopen'
            changed=raw('history-000');assert changed['payload']['exit_code']==9 and changed['id']!=old['id']
            prior=carry();lock=sqlite3.connect(source);lock.execute('BEGIN EXCLUSIVE')
            try:
                first_failure=wait('actual SQLite lock unavailable',lambda:unavailable())
                failed=wait('reopen full also unavailable',lambda:unavailable(first_failure['last_run']['id']))
                assert carry()==prior,'failed full advanced checkpoint/due'
            finally:lock.rollback();lock.close()
            recovered=next_poll(failed,'full_value_comparison');check_count(recovered,len(facts))
            assert plan(recovered)['checkpoint']['epoch']!=prior['epoch']
            prior_partial=carry()
            bad=row('oversized-final',stamp('2026-10-14T00:00:00'));bad['command']='x'*(1024*1024)
            with sqlite3.connect(source) as con:
                con.execute('PRAGMA user_version=42')
                con.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',[bad[k] for k in COLUMNS])
            partial=wait('full prefix retained before oversized final row',lambda:unavailable(minimum=64))
            assert carry()==prior_partial,'partial full advanced checkpoint/due'
            with sqlite3.connect(source) as con:con.execute("DELETE FROM history WHERE id='oversized-final'")
            after_partial=next_poll(partial,'full_value_comparison');check_count(after_partial,len(facts))
            assert raw('history-000')['id']==changed['id']
            malformed=row('malformed-new-coordinate',stamp('2026-10-14T01:00:00'),duration=-2)
            with sqlite3.connect(source) as con:con.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',[malformed[k] for k in COLUMNS])
            omission=next_poll(after_partial);check_count(omission,1)
            assert omission['last_run']['record_errors']==1 and plan(omission)['checkpoint']['needs_full']
            forced=next_poll(omission);assert plan(forced)['kind']=='full_value_comparison'
            check_count(forced,len(facts)+1);assert forced['last_run']['record_errors']==1 and plan(forced)['checkpoint']['needs_full']
            with sqlite3.connect(source) as con:con.execute("DELETE FROM history WHERE id='malformed-new-coordinate'")
            cleared=next_poll(forced);assert plan(cleared)['kind']=='full_value_comparison'
            check_count(cleared,len(facts));assert not plan(cleared)['checkpoint']['needs_full']
            save('known-parser-omission-forces-full',{'new_omission':omission,'forced_full':forced,'actual_error_free_full':cleared,'scope':'no physical deletion admission; retained failed rows stay in observation/material history'})
            save('outside-change-and-failed-full',{'old':old,'unread':reduced,'failed':failed,'prior_checkpoint':prior,'due_full':full,'reopened_full':recovered,'partial':partial,'partial_checkpoint':prior_partial,'after_partial':after_partial,'changed':changed})
        scenario(PLANNED[2],due_full)
        def invalidations():
            nonlocal daemon,source
            records=[]
            for kind in ['vacuum','user-version','schema','rename-replace','symlink-retarget']:
                before=wait('settled before '+kind,lambda:completed());old_plan=plan(before)['checkpoint']
                if kind=='vacuum':
                    with sqlite3.connect(source) as con:con.execute('VACUUM')
                elif kind=='user-version':
                    with sqlite3.connect(source) as con:con.execute('PRAGMA user_version=7')
                elif kind=='schema':
                    with sqlite3.connect(source) as con:con.execute('CREATE TABLE synthetic_added(value INTEGER)')
                elif kind=='rename-replace':
                    old_path=source.with_name('renamed-old.db');source.rename(old_path);create(source)
                else:
                    new=work/'symlink-next.db';create(new);configured.unlink();configured.symlink_to(new);source=new
                full=next_poll(before);assert plan(full)['kind']=='full_value_comparison',(kind,full)
                check_count(full,len(facts));cp=plan(full)['checkpoint']
                if kind in ('rename-replace','symlink-retarget'):assert cp['epoch']!=old_plan['epoch']
                elif kind=='user-version':assert cp['user_version']!=old_plan['user_version']
                else:assert cp['schema_version']!=old_plan['schema_version']
                records.append({'kind':kind,'before':before,'after':full})
            previous_epoch=plan(records[-1]['after'])['checkpoint']['epoch']
            stop(daemon);daemon=start(config,'read-plan-reopen');full=wait('restart full',lambda:(s if (s:=completed(kind='full_value_comparison')) and plan(s)['checkpoint']['epoch']!=previous_epoch else None))
            check_count(full,len(facts));recovery('after-reopen',baseline)
            save('basis-invalidations',{'controls':records,'reopen':full})
            pending=[row(f'pending-page-{n:03}',stamp('2026-10-04T00:00:00')+n*1_000_000_000,duration=-1,exit=-1) for n in range(65)]
            facts.extend(pending)
            before=wait('settled before pending pages',lambda:completed())
            with sqlite3.connect(source) as con:con.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',[[r[k] for k in COLUMNS] for r in pending])
            admitted=next_poll(before);assert plan(admitted)['kind']=='new_coordinates_and_prior_unfinished';check_count(admitted,65)
            with sqlite3.connect(source) as con:con.execute("UPDATE history SET duration=1,exit=0 WHERE id LIKE 'pending-page-%'")
            for fact in pending:fact.update(duration=1,exit=0)
            updated=next_poll(admitted);assert plan(updated)['kind']=='new_coordinates_and_prior_unfinished';check_count(updated,65)
            assert updated['last_run']['admitted']==65
            zero=next_poll(updated);assert plan(zero)['kind']=='new_coordinates_and_prior_unfinished';check_count(zero,0)
            save('bounded-prior-unfinished-pages',{'new_coordinates':admitted,'late_completions':updated,'zero':zero,'records':65,'page_size':64,'scope':'canonical eligibility fixed before poll; no whole-history cache'})
            save('independent-provider-facts',facts)
        scenario(PLANNED[3],invalidations)
        def provider_loss():
            nonlocal daemon
            current=recovery('before-provider-loss',baseline);before=carry();stop(daemon)
            source.rename(work/'removed-provider.db');daemon=start(config,'read-plan-provider-loss')
            failed=wait('provider loss unavailable',lambda:unavailable())
            assert carry()==before
            retained=recovery('provider-loss-retained',current,unavailable=True)
            save('qualification',{'fixture_records':len(facts),'controls':controls,'provider_loss':failed,'scope':'one synthetic sequential collector, selected value reads and exact retained recovery facts; no physical generation, completed census, deletion proof, inclusive scan IO CPU, sustained resource or live-provider claim'})
        scenario(PLANNED[4],provider_loss)
    finally:
        if mcp is not None and mcp.poll() is None:
            mcp.stdin.close()
            mcp.wait(timeout=20)
            assert mcp.returncode==0, 'owned MCP did not close cleanly after stdin EOF'
        if daemon is not None and daemon.poll() is None:stop(daemon)
