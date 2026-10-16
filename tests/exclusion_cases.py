"""Independent synthetic occurrence facts through pinned packaged clients and real boundaries."""
import concurrent.futures
import hashlib
import json
from pathlib import Path
import shutil
import signal
import socket
import sqlite3
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid

PLANNED = [
    'independent exclusion reply identity controls',
    'identical command occurrences and read-only mutation refusal',
    'exact exclusion keeps history and removes only selected raw and index rows',
    'changed source replay and restart preserve exclusion',
    'provider loss undo stale preview and explicit retained restoration',
    'fresh normal acquisition after undo remains allowed',
    'precommit interruption and one-effect retry',
    'postcommit interruption and pending projection refusal',
    'lost real CLI reply preserves finalized action and effects',
    'finalized no-op conflict refusal and retained comparison controls',
]

def exercise(h):
    work,report,bins=(h[k] for k in ['WORK','REPORT','BIN_DIR'])
    run,scenario,start,stop=(h[k] for k in ['run','scenario','start_daemon','stop'])
    token='synthetic-foundation-token';operator='synthetic-exclusion-operator'
    (work/'operator').write_text(operator);(work/'operator').chmod(0o600)
    (work/'read-model').write_text(h['reader'].replace('sinex_reader','sinex_read_model'))
    (work/'read-model').chmod(0o600)
    source=work/'history.db';store=work/'exclusion-store';config=work/'exclusion.toml'
    port=h['free_port']();endpoint=f'http://127.0.0.1:{port}/rpc'
    raw_query='events where source = terminal.atuin-history and kind = command.executed'
    indexed_query='"cargo test"'
    epoch=1_700_000_000_000_000_000
    expected={
        'command':'cargo test exclusion-fixture', 'timestamp_ns':epoch,
        'initial':['occurrence-a','occurrence-b'], 'excluded':['occurrence-b'],
        'undo_without_acquisition':['occurrence-b'], 'restored':['occurrence-a','occurrence-b'],
        'scope':'Two identical command texts with distinct Atuin native IDs and stable keys. Expected membership is independent of product query answers.',
    }
    (report/'independent-exclusion-expectations.json').write_text(json.dumps(expected,indent=2)+'\n')
    def fixture_row(key,exit_code=0):return (key,epoch,1_000_000_000,exit_code,expected['command'],'/synthetic/exclusion','same-session','synthetic-host',None)
    with sqlite3.connect(source) as con:
        con.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER NOT NULL,duration INTEGER NOT NULL,exit INTEGER NOT NULL,command TEXT NOT NULL,cwd TEXT NOT NULL,session TEXT NOT NULL,hostname TEXT NOT NULL,deleted_at INTEGER)')
        con.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',fixture_row('occurrence-a'))
    def snapshot(name):
        with sqlite3.connect(source) as original,sqlite3.connect(report/(name+'.db')) as copy:original.backup(copy)
    snapshot('initial-a-only')
    def configure(acquire=False):
        config.write_text(f'''listen="127.0.0.1:{port}"
state_dir="{work/'exclusion-state'}"
store_dir="{store}"
writer_connection_file="{work/'writer'}"
reader_connection_file="{work/'reader'}"
read_model_connection_file="{work/'read-model'}"
token_file="{work/'token'}"
operator_token_file="{work/'operator'}"
sources={json.dumps(['terminal.atuin-history'] if acquire else [])}
automata=[]
'''+(f'''[atuin]
path="{source}"
poll_interval_ms=3600000
''' if acquire else ''))
    api_lock=threading.Lock();api_path=report/'actual-exclusion-api.jsonl'
    def api(method,params=None,operator_profile=False,timeout=30):
        document={'jsonrpc':'2.0','id':7,'method':method,'params':params or {}}
        request=urllib.request.Request(endpoint,data=json.dumps(document).encode(),headers={'Authorization':'Bearer '+(operator if operator_profile else token),'Content-Type':'application/json'})
        receipt={'request':document,'profile':'operator' if operator_profile else 'read-only'}
        try:
            try:
                with urllib.request.urlopen(request,timeout=timeout) as response:
                    receipt['http_status']=response.status;answer=json.load(response)
            except urllib.error.HTTPError as error:
                receipt['http_status']=error.code;answer=json.load(error)
            receipt['answer']=answer
            return answer
        except Exception as error:
            receipt['transport_error']=type(error).__name__+': '+str(error)
            raise
        finally:
            with api_lock:
                with api_path.open('a') as log:log.write(json.dumps(receipt,ensure_ascii=False)+'\n')
    def cli(command,*args,operator_profile=False,expected_exit=0):
        raw=run([bins/'sinexctl','--endpoint',endpoint,'--token-file',work/('operator' if operator_profile else 'token'),command,*args,'--json'],expected=expected_exit,label='actual exclusion CLI '+command)
        return json.loads(raw.stdout or raw.stderr)
    def sql(statement):
        raw=run(['psql',h['reader'],'-XAt','-v','ON_ERROR_STOP=1','-c',statement],label='read-only exclusion action/evidence audit')
        return json.loads(raw.stdout)
    def action_audit(action):
        action=str(uuid.UUID(action))
        return sql("SELECT jsonb_build_object('operations',(SELECT count(*) FROM core.operations_log WHERE action_id='"+action+"'::uuid),'rows',(SELECT coalesce(jsonb_agg(jsonb_build_object('id',o.id,'actor',o.actor,'request_format',o.request_format,'request_ref',o.request_ref,'outcome_format',o.outcome_format,'outcome_ref',o.outcome_ref,'progress',o.progress,'effects',(SELECT coalesce(jsonb_agg(to_jsonb(f)),'[]') FROM core.operation_effects f WHERE f.operation_id=o.id),'archives',(SELECT coalesce(jsonb_agg(a.id ORDER BY a.id),'[]') FROM audit.archived a WHERE a.operation_id=o.id),'exclusions',(SELECT coalesce(jsonb_agg(to_jsonb(d)),'[]') FROM core.deleted_occurrences d WHERE d.operation_id=o.id))),'[]') FROM core.operations_log o WHERE action_id='"+action+"'::uuid))")
    daemon=None
    def wait(predicate,seconds=30):
        deadline=time.monotonic()+seconds
        while True:
            assert daemon.poll() is None,'owned exclusion daemon exited; inspect retained stderr'
            try:
                value=api('status')
                if 'result' in value and predicate(value['result']):return value['result']
            except (OSError,ValueError):pass
            assert time.monotonic()<deadline,'expected exclusion frontier did not settle'
            time.sleep(.025)
    def ready():return wait(lambda _:True)
    def settled(source_state=None):
        def predicate(status):
            projection=status['indexed_retrieval']
            if not (projection.get('serving') and projection['position']==status['commit_seq'] and not projection['pending_canonical']):return False
            if source_state is None:return True
            sources=api('sources.list').get('result',{}).get('sources',[])
            return len(sources)==1 and sources[0]['last_run']['status']==source_state
        return wait(predicate)
    def launch(name,acquire=False,*extra):
        nonlocal daemon
        if daemon is not None:stop(daemon)
        configure(acquire);daemon=start(config,name,*extra);ready();return daemon
    def population(members,complete=True):
        settled()
        raw=api('query.run',{'query':raw_query})['result']
        indexed=api('query.run',{'query':indexed_query,'page':100})['result']
        for answer in [raw,indexed]:
            assert sorted(row['payload']['native_id'] for row in answer['rows'])==members,answer
            assert answer['total']==len(members) and answer['complete']==complete,answer
        raw_cli=cli('query',raw_query,expected_exit=0 if complete else 5)
        index_cli=cli('query',indexed_query,'--page','100',expected_exit=0 if complete else 5)
        assert raw_cli['rows']==raw['rows'] and index_cli['rows']==indexed['rows']
        assert all(row['kind']=='command.canonical' and len(row['parents'])==1 for row in indexed['rows'])
        for row in indexed['rows']:
            assert row['snippet']['text']
            assert row['snippet']['spans'],row
        return {row['payload']['native_id']:row for row in raw['rows']},{row['payload']['native_id']:row for row in indexed['rows']}
    def new_action():return str(uuid.uuid4())
    def deletion(ref,action=None,undo=False,expected_exit=0,expected_key=None,operator_profile=True):
        args=[ref,'--action-id',action or new_action()]
        if undo:args+=['--undo']
        if expected_key is not None:args+=['--expected-key',expected_key]
        return cli('delete',*args,operator_profile=operator_profile,expected_exit=expected_exit)
    def trace(ref):return cli('trace',ref,'--bytes')['row']
    def same_evidence(before,after):
        for field in ['material','anchor','end','record']:assert before[field]==after[field],field
    originals={};canonicals={};old_trace=None;material=None;a_ref=None;b_ref=None;initial_action=None;initial_outcome=None;preview=None
    def mock():
        run(['python3',h['ROOT']/'tests/exclusion_reply_mock.py','--binary',bins/'sinexctl','--output',report/'mock-reply-binding.json','--expect','bound'],label='independent real CLI wrong/missing action method target responses')
    scenario(PLANNED[0],mock)
    def acquire():
        nonlocal originals,canonicals,old_trace,material,a_ref,b_ref
        launch('a-first-material',True);settled('completed')
        a=api('query.run',{'query':raw_query})['result'];assert a['total']==1
        a_ref=a['rows'][0]['id'];old_trace=trace(a_ref);material=old_trace['material']
        stop(daemon)
        with sqlite3.connect(source) as con:con.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',fixture_row('occurrence-b'))
        snapshot('identical-a-b')
        configure(True)
        # The first material contains only A, so restoration replay need not remint B.
        nonlocal_daemon_start('two-identical-occurrences')
        settled('completed')
        launch('stable-two-occurrences',False);settled()
        originals,canonicals=population(expected['initial'])
        b_ref=originals['occurrence-b']['id'];assert originals['occurrence-a']['id']==a_ref and a_ref!=b_ref
        keys=sql("SELECT jsonb_object_agg(payload->>'native_id',equivalence_key) FROM core.live_events WHERE event_type='command.executed'")
        assert keys['occurrence-a']!=keys['occurrence-b']
        denied_action=new_action();denied=deletion(a_ref,denied_action,operator_profile=False,expected_exit=4)
        assert denied['kind']=='refused' and action_audit(denied_action)['operations']==0
        (report/'actual-before.json').write_text(json.dumps({'raw':originals,'canonical':canonicals,'trace_a':old_trace,'keys':keys,'denied':denied},indent=2)+'\n')
    def nonlocal_daemon_start(name):
        nonlocal daemon
        daemon=start(config,name);ready()
    scenario(PLANNED[1],acquire)
    def exclude():
        nonlocal initial_action,initial_outcome
        initial_action=new_action();initial_outcome=deletion(a_ref,initial_action)
        assert initial_outcome['state']=='completed'
        assert set(initial_outcome['archived'])=={a_ref,canonicals['occurrence-a']['id']}
        raw,indexed=population(expected['excluded'])
        assert raw['occurrence-b']['id']==b_ref and indexed['occurrence-b']['id']==canonicals['occurrence-b']['id']
        archived=trace(a_ref);same_evidence(old_trace,archived)
        assert archived['archive']['operation_id']==initial_outcome['operation_id'] and archived['archive']['reason']=='delete'
        api_show=api('object.show',{'ref':'sinex:ev/'+a_ref})['result']
        assert api_show==cli('show',a_ref) and api_show['resolved']=='archived'
        audit=action_audit(initial_action);assert audit['operations']==1 and len(audit['rows'][0]['effects'])==1
        assert len(audit['rows'][0]['exclusions'])==1 and set(audit['rows'][0]['archives'])==set(initial_outcome['archived'])
        assert deletion(a_ref,initial_action)==initial_outcome and action_audit(initial_action)==audit
        (report/'actual-excluded.json').write_text(json.dumps({'outcome':initial_outcome,'raw':raw,'indexed':indexed,'archived_trace':archived,'audit':audit},indent=2)+'\n')
    scenario(PLANNED[2],exclude)
    selection=lambda:f'materials where id = {material}'
    def replay(expected_exit=0,basis=None):
        args=[selection(),'--action-id',new_action()]
        if basis is not None:args+=['--basis',basis]
        return cli('replay',*args,operator_profile=True,expected_exit=expected_exit)
    def changed():
        stop(daemon)
        with sqlite3.connect(source) as con:con.execute("UPDATE history SET exit=7 WHERE id='occurrence-a'")
        snapshot('changed-a-same-key');configure(True);nonlocal_daemon_start('changed-source-excluded');settled('completed')
        account=api('sources.list')['result']['sources'][0]['last_run']
        assert account['excluded']==1 and account['admitted']==0 and account['rows_read']==2,account
        population(expected['excluded'])
        launch('same-state-excluded-restart',True);settled('completed');population(expected['excluded'])
        blocked=replay();assert blocked['state']=='completed' and blocked['processed']==1 and blocked['admitted']==0 and blocked['archived']==0
        population(expected['excluded'])
        (report/'actual-excluded-source-replay.json').write_text(json.dumps({'account':account,'blocked_replay':blocked},indent=2)+'\n')
    scenario(PLANNED[3],changed)
    def provider_loss():
        nonlocal preview
        stop(daemon);source.rename(work/'provider-removed.db');configure(True);nonlocal_daemon_start('provider-unavailable');settled('unavailable')
        population(expected['excluded'],False);assert not source.exists()
        preview=cli('replay',selection(),'--dry-run')
        assert preview['selected']==preview['excluded']==1 and preview['would_admit']==preview['would_archive']==0
        undone=deletion(a_ref,undo=True);assert undone['state']=='completed' and undone['removed_exclusion_operation']==initial_outcome['operation_id']
        population(expected['undo_without_acquisition'],False)
        stale=replay(expected_exit=4,basis=preview['basis']);assert stale['kind']=='refused' and stale['details']['admitted']==0
        population(expected['undo_without_acquisition'],False)
        restored=replay();assert restored['state']=='completed' and restored['admitted']==1 and restored['archived']==0
        raw,indexed=population(expected['restored'],False)
        assert raw['occurrence-a']['id']!=a_ref and raw['occurrence-a']['when']==originals['occurrence-a']['when']
        assert raw['occurrence-b']['id']==b_ref and indexed['occurrence-b']['id']==canonicals['occurrence-b']['id']
        same_evidence(old_trace,trace(raw['occurrence-a']['id']))
        assert raw['occurrence-a']['operation']==restored['operation_id']
        assert deletion(a_ref,initial_action)==initial_outcome
        population(expected['restored'],False)
        (report/'actual-undo-restored-without-provider.json').write_text(json.dumps({'preview':preview,'undo':undone,'stale_preview_refusal':stale,'restored':restored,'raw':raw,'indexed':indexed},indent=2)+'\n')
    scenario(PLANNED[4],provider_loss)
    def fresh_acquisition():
        deletion(a_ref);population(expected['excluded'],False)
        deletion(a_ref,undo=True);population(expected['undo_without_acquisition'],False)
        stop(daemon);(work/'provider-removed.db').rename(source)
        with sqlite3.connect(source) as con:con.execute("UPDATE history SET exit=9 WHERE id='occurrence-a'")
        snapshot('fresh-after-undo');configure(True);nonlocal_daemon_start('fresh-acquisition-after-undo');settled('completed')
        raw,_=population(expected['restored']);assert raw['occurrence-a']['payload']['exit_code']==9
        assert raw['occurrence-a']['operation'] is None and raw['occurrence-b']['id']==b_ref
        launch('stable-boundary-basis',False);settled()
        (report/'actual-fresh-acquisition-after-undo.json').write_text(json.dumps(raw,indent=2)+'\n')
    scenario(PLANNED[5],fresh_acquisition)
    def hold(point,name):
        nonlocal daemon
        stop(daemon)
        path=work/'delete.sock'
        if path.exists():path.unlink()
        listener=socket.socket(socket.AF_UNIX);listener.bind(str(path));listener.listen(1);listener.settimeout(30)
        configure(False);daemon=start(config,name,'--qualification-boundary',point,'--qualification-socket',path);ready();settled()
        return listener
    def accept(listener,point):
        connection,_=listener.accept();connection.settimeout(10)
        frame=json.loads(connection.makefile('rb').readline())
        assert frame['pid']==daemon.pid and frame['qualification_boundary']==point,frame
        return connection,frame
    def recover_population():
        deletion(a_ref,undo=True)
        # Provider is disabled; no implicit readmission occurs.
        population(expected['undo_without_acquisition'])
        restored=replay();assert restored['admitted']==1
        return population(expected['restored'])
    def interrupt(point,committed):
        nonlocal daemon
        population(expected['restored'])
        action=new_action();listener=hold(point,point)
        request={'ref':'sinex:ev/'+a_ref,'action_id':action}
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future=executor.submit(api,'delete.run',request,True,30)
            connection,frame=accept(listener,point)
            before=action_audit(action)
            assert before['operations']==(1 if committed else 0),before
            if committed:
                assert before['rows'][0]['progress']==frame['evidence']['outcome']
                assert len(before['rows'][0]['effects'])==len(before['rows'][0]['exclusions'])==1
                # The real writer is parked after commit with canonical dirty work.
                partial=api('query.run',{'query':indexed_query,'page':100,'timeout_ms':250})
                assert ('error' in partial and partial['error']['kind'] in ['budget','partial']) or partial.get('result',{}).get('complete') is False,partial
                (report/'pending-projection-refusal.json').write_text(json.dumps(partial,indent=2)+'\n')
            killed=daemon;daemon.send_signal(signal.SIGKILL);daemon.wait(timeout=10);assert daemon.returncode==-signal.SIGKILL
            connection.close();listener.close()
            try:reply=future.result(timeout=10)
            except (OSError,ValueError) as error:reply={'transport_error':type(error).__name__+': '+str(error)}
        configure(False);daemon=start(config,point+'-restart');ready();settled()
        if committed:population(expected['excluded'])
        else:
            assert action_audit(action)['operations']==0
            population(expected['restored'])
        recovered=deletion(a_ref,action)
        assert recovered['state']=='completed'
        if committed:assert recovered==frame['evidence']['outcome']
        audit=action_audit(action)
        assert audit['operations']==1 and len(audit['rows'][0]['effects'])==len(audit['rows'][0]['exclusions'])==1
        assert set(audit['rows'][0]['archives'])==set(recovered['archived']) and len(recovered['archived'])==2
        assert deletion(a_ref,action)==recovered and action_audit(action)==audit
        population(expected['excluded'])
        (report/(point+'-receipt.json')).write_text(json.dumps({'frame':frame,'before':before,'killed_pid':killed.pid,'exit':killed.returncode,'wire_reply':reply,'recovered':recovered,'after':audit},indent=2)+'\n')
        recover_population()
    scenario(PLANNED[6],lambda:interrupt('delete-staged',False))
    scenario(PLANNED[7],lambda:interrupt('delete-committed',True))
    def lost_reply():
        population(expected['restored']);action=new_action();listener=hold('delete-committed','lost-cli-reply')
        args=[str(bins/'sinexctl'),'--endpoint',endpoint,'--token-file',str(work/'operator'),'delete',a_ref,'--action-id',action,'--json']
        out=(report/'lost-cli-reply.stdout').open('w');err=(report/'lost-cli-reply.stderr').open('w')
        client=subprocess.Popen(args,cwd=h['ROOT'],env=h['ENV'],stdout=out,stderr=err)
        h['CHILDREN'].append((client,out,err));h['PROCESSES'].append({'pid':client.pid,'argv':args,'stdout':'lost-cli-reply.stdout','stderr':'lost-cli-reply.stderr'})
        connection,frame=accept(listener,'delete-committed')
        before=action_audit(action);assert before['operations']==1
        client.send_signal(signal.SIGKILL);client.wait(timeout=10);assert client.returncode==-signal.SIGKILL
        connection.sendall(b'c');connection.close();listener.close();settled()
        assert daemon.poll() is None and not (report/'lost-cli-reply.stdout').read_text()
        recovered=deletion(a_ref,action);assert recovered==frame['evidence']['outcome']
        after=action_audit(action);assert before==after
        population(expected['excluded'])
        (report/'lost-cli-reply-receipt.json').write_text(json.dumps({'frame':frame,'client_pid':client.pid,'client_argv':args,'client_exit':client.returncode,'before':before,'recovered':recovered,'after':after,'daemon_alive':daemon.poll() is None},indent=2)+'\n')
    scenario(PLANNED[8],lost_reply)
    def finalized():
        before=sql("SELECT jsonb_build_object('archives',(SELECT count(*) FROM audit.archived),'deleted',(SELECT count(*) FROM core.deleted_occurrences),'events',(SELECT count(*) FROM core.events))")
        noop=deletion(a_ref);assert noop['state']=='noop' and noop['archived']==[]
        assert deletion(a_ref,initial_action)==initial_outcome
        for target,undo,key in [(a_ref,True,None),(b_ref,False,None),(a_ref,False,'different')]:
            conflict=deletion(target,initial_action,undo=undo,expected_key=key,expected_exit=4);assert conflict['kind']=='refused'
        wrong_action=new_action();wrong=deletion(a_ref,wrong_action,expected_key='wrong stable occurrence key',expected_exit=4)
        assert wrong['details']['state']=='refused' and wrong['details']['archived']==[]
        assert deletion(a_ref,wrong_action,expected_key='wrong stable occurrence key',expected_exit=4)==wrong
        absent_action=new_action();absent_ref=new_action()
        absent=deletion(absent_ref,absent_action,expected_exit=3);assert absent['details']['state']=='not_found'
        assert deletion(absent_ref,absent_action,expected_exit=3)==absent
        canonical=deletion(canonicals['occurrence-b']['id'],expected_exit=4)
        assert canonical['details']['raw_ref']=='sinex:ev/'+b_ref and canonical['details']['archived']==[]
        invalid=api('delete.run',{'ref':'events where kind = command.executed','action_id':new_action()},True)
        assert invalid['error']['kind']=='bad_query'
        # Remove only owned synthetic comparison backing; retry must refuse new effects.
        initial_id=initial_outcome['operation_id']
        metadata=sql("SELECT jsonb_build_object('hash',b.blake3,'request_ref',m.id) FROM core.operations_log o JOIN raw.source_material_registry m ON m.id=o.request_ref JOIN core.blobs b ON b.id=m.blob_id WHERE o.id='"+str(uuid.UUID(initial_id))+"'::uuid")
        digest=metadata['hash'].removeprefix('blake3:');path=store/'blobs'/digest[:2]/(digest+'.zst');held=path.with_suffix('.qualification-missing')
        blob_hash=hashlib.sha256(path.read_bytes()).hexdigest();path.rename(held)
        try:
            missing=deletion(a_ref,initial_action,expected_exit=4);assert missing['kind']=='refused'
        finally:held.rename(path)
        assert hashlib.sha256(path.read_bytes()).hexdigest()==blob_hash
        assert deletion(a_ref,initial_action)==initial_outcome
        after=sql("SELECT jsonb_build_object('archives',(SELECT count(*) FROM audit.archived),'deleted',(SELECT count(*) FROM core.deleted_occurrences),'events',(SELECT count(*) FROM core.events))")
        assert before==after
        population(expected['excluded'])
        full=sql("SELECT jsonb_build_object('operations',(SELECT jsonb_agg(to_jsonb(o) ORDER BY id) FROM core.operations_log o),'effects',(SELECT jsonb_agg(to_jsonb(f) ORDER BY operation_id,step_no) FROM core.operation_effects f),'excluded',(SELECT jsonb_agg(to_jsonb(d)) FROM core.deleted_occurrences d),'archives',(SELECT jsonb_agg(to_jsonb(a) ORDER BY id) FROM audit.archived a))")
        (report/'actual-finalized-action-controls.json').write_text(json.dumps({'before':before,'after':after,'noop':noop,'wrong_key':wrong,'absent':absent,'canonical_refusal':canonical,'invalid_shape':invalid,'missing_request':missing,'request_blob_sha256':blob_hash,'audit':full},indent=2)+'\n')
        stop(daemon)
        shutil.copytree(store,report/'retained-store')
        original={str(p.relative_to(store)):hashlib.sha256(p.read_bytes()).hexdigest() for p in store.rglob('*') if p.is_file()}
        copied={str(p.relative_to(report/'retained-store')):hashlib.sha256(p.read_bytes()).hexdigest() for p in (report/'retained-store').rglob('*') if p.is_file()}
        assert original==copied
        (report/'retained-store-sha256.json').write_text(json.dumps(original,indent=2)+'\n')
    scenario(PLANNED[9],finalized)
