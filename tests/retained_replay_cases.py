"""Independent synthetic facts, real packaged clients and observed replacement boundaries.

The bad reading is admitted by the explicitly named native typed fixture mode;
no claim is made that a predecessor binary produced it. Expected facts below do
not call the product parser. This is not the Kestrel fusion/10-to-9 example.
"""
import concurrent.futures
import hashlib
import json
import signal
import socket
import sqlite3
import time
import uuid


def exercise(h):
    work, report, binaries = (h[k] for k in ['WORK', 'REPORT', 'BIN_DIR'])
    run, scenario, start, stop = (h[k] for k in ['run', 'scenario', 'start_daemon', 'stop'])
    source = work/'replay.db'
    epoch = 1_700_000_000_123_456_789
    with sqlite3.connect(source) as con:
        con.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT,deleted_at INTEGER)')
        con.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)', [
            (f'fixture-{i:03}', epoch+i*1_000_000_000, 1_000_000_000, 0, f'echo fixture {i}', '/synthetic/replay', 'fixture', 'fixture-host:fixture-user', epoch if 64<=i<128 else None)
            for i in range(130)])
    (report/'independent-expected.json').write_text(json.dumps({'fixture':'native independent replay fixture; not Kestrel','selected':130,'before_visible':65,'after_visible':66,'removed':64,'chunks':[64,64,2],'timestamp_ns':epoch,'corrected_hostname':'fixture-host','historical_bad_hostname':'controlled-historical-bad-interpretation','correction_native_id':'fixture-000','restored_native_id':'fixture-129','archived':129,'admitted':130,'processed':130}, indent=2)+'\n')
    (report/'source.db').write_bytes(source.read_bytes())
    (report/'input-digests.json').write_text(json.dumps({'source.db':hashlib.sha256(source.read_bytes()).hexdigest(),'independent-expected.json':hashlib.sha256((report/'independent-expected.json').read_bytes()).hexdigest()},indent=2)+'\n')
    (work/'operator-token').write_text('synthetic-explicit-replay-operator')
    (work/'operator-token').chmod(0o600)
    port=h['free_port'](); endpoint=f'http://127.0.0.1:{port}/rpc'
    config=work/'replay.toml'; store=work/'replay-store'
    def configure(acquire=False,authority=True):
        config.write_text(f'''listen = "127.0.0.1:{port}"
state_dir = "{work/'replay-state'}"
store_dir = "{store}"
writer_connection_file = "{work/'writer'}"
reader_connection_file = "{work/'reader'}"
token_file = "{work/'token'}"
operator_token_file = "{work/'operator-token'}"
sources = {json.dumps(['terminal.atuin-history'] if acquire else [])}
automata = []
'''+(f'''[atuin]
path = "{source}"
poll_interval_ms = 3600000
full_interval_ms = 100 # this fixture deliberately compares the full provider
''' if acquire else ''))
        if not authority:config.write_text(config.read_text().replace(f'operator_token_file = "{work/"operator-token"}"\n',''))
    configure(True)
    daemon=start(config,'replay-historical-fixture','--test-bad-atuin-parse')
    def wait_ready():
        deadline=time.monotonic()+30
        while True:
            assert daemon.poll() is None, 'owned replay daemon exited'
            try:
                if 'result' in h['rpc'](endpoint,'synthetic-foundation-token'): return
            except (OSError,ValueError): pass
            assert time.monotonic()<deadline
            time.sleep(.025)
    def cli(command,*args,expected=0,operator=False):
        raw=run([binaries/'sinexctl','--endpoint',endpoint,'--token-file',work/('operator-token' if operator else 'token'),command,*args,'--json'],expected=expected,label='actual replay client '+command)
        return json.loads(raw.stdout if raw.returncode in (0,5) else raw.stderr)
    def sql(statement):
        raw=run(['psql',h['reader'],'-XAt','-v','ON_ERROR_STOP=1','-c',statement],label='read-only replay database receipt')
        return json.loads(raw.stdout)
    def audit():
        return sql("SELECT json_build_object('seq',(SELECT seq FROM core.commit_counter WHERE one),'events',(SELECT count(*) FROM core.events),'archives',(SELECT count(*) FROM audit.archived),'effects',(SELECT count(*) FROM core.operation_effects),'operations',(SELECT count(*) FROM core.operations_log))")
    def state():
        return sql("SELECT coalesce(json_agg(to_jsonb(e) ORDER BY e.anchor_byte),'[]') FROM core.events e WHERE source_material_id='"+material+"'::uuid AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=e.id)")
    def identity(rows): return {r['anchor_byte']:r['id'] for r in rows}
    query='events where source = terminal.atuin-history and kind = command.executed'
    material=None; initial=None; before_trace=None
    def acquire():
        nonlocal material,initial,before_trace,daemon
        wait_ready()
        deadline=time.monotonic()+30
        while True:
            reply=h['rpc'](endpoint,'synthetic-foundation-token','sources.list')
            sources=reply.get('result',{}).get('sources',[])
            if sources and sources[0]['last_run']['status']=='completed': break
            assert time.monotonic()<deadline
            time.sleep(.025)
        assert sources[0]['last_run']['rows_read']==130 and sources[0]['last_run']['admitted']==129 and sources[0]['last_run']['record_errors']==1
        answer=cli('query',query)
        assert answer['total']==65 and answer['complete'] is True
        bad=next(r for r in answer['rows'] if r['payload']['native_id']=='fixture-000')
        assert bad['payload']['hostname']=='controlled-historical-bad-interpretation'
        assert bad['written_by']['kind']=='test_fixture'
        before_trace=cli('trace',bad['id'],'--bytes')['row']
        assert before_trace['record']['hostname']=='fixture-host:fixture-user'
        assert before_trace['record']['timestamp']==epoch
        material=before_trace['material']
        stop(daemon)
        source.unlink()
        assert not source.exists(), 'provider still available'
        configure(False)
        daemon=start(config,'replay-retained-only');wait_ready()
        initial=state()
        assert len(initial)==129 and sum(r['record_state']=='removed' for r in initial)==64
        (report/'before-actual.json').write_text(json.dumps({'answer':answer,'trace':before_trace,'canonical_rows':initial},indent=2)+'\n')
    scenario('retained fixture and independent bad-interpretation contrast',acquire)
    selection=f'materials where id = {material}'
    action=str(uuid.uuid4()); operation=None
    bases={}
    def plan(action_id):
        value=cli('replay',selection,'--dry-run')
        bases[action_id]=value['basis']
        return value
    def execute(action_id=action,expected=0,use_basis=False):
        extra=['--basis',bases[action_id]] if use_basis else []
        return cli('replay',selection,'--action-id',action_id,*extra,expected=expected,operator=True)
    def wait_operation(operation_id, state_name='completed'):
        deadline=time.monotonic()+30
        while True:
            value=sql("SELECT progress FROM core.operations_log WHERE id='"+operation_id+"'::uuid")
            if value['state']==state_name:return value
            assert time.monotonic()<deadline, value
            time.sleep(.025)
    def hold(point,name=None):
        nonlocal daemon
        stop(daemon)
        sock_path=work/'replay.sock'
        if sock_path.exists():sock_path.unlink()
        listener=socket.socket(socket.AF_UNIX);listener.bind(str(sock_path));listener.listen(1);listener.settimeout(30)
        daemon=start(config,name or point,'--test-pause-at',point,'--test-pause-socket',sock_path);wait_ready()
        return listener,sock_path
    def accept(listener,point):
        connection,_=listener.accept();connection.settimeout(10)
        frame=json.loads(connection.makefile('rb').readline(262144))
        assert frame['pid']==daemon.pid and frame['pause_point']==point
        return connection,frame
    def kill_and_restart(connection,listener,path,frame,name,check_authority=False):
        nonlocal daemon
        daemon.send_signal(signal.SIGKILL);daemon.wait(timeout=10);assert daemon.returncode==-signal.SIGKILL
        connection.close();listener.close();path.unlink()
        (report/(name+'-frame.json')).write_text(json.dumps({'frame':frame,'daemon_exit':daemon.returncode},indent=2)+'\n')
        if check_authority:configure(False,False)
        daemon=start(config,name+'-restart');wait_ready()
        if check_authority:
            current=sql("SELECT progress FROM core.operations_log WHERE id='"+frame['evidence']['operation']['operation_id']+"'::uuid")
            assert current['processed']==128 and current['state']=='running'
            assert cli('status')['replay_authorized'] is False
            before=audit();assert execute(action,4,True)['kind']=='refused'
            assert audit()==before
            stop(daemon);configure(False,True);daemon=start(config,name+'-reauthorized');wait_ready()
    def replacement_invariants(old_rows,new_rows):
        assert len(old_rows)==129 and len(new_rows)==130
        by_anchor={row['anchor_byte']:row for row in new_rows}
        for old in old_rows:
            new=by_anchor[old['anchor_byte']]
            for field in ['source_material_id','anchor_byte','offset_start','offset_end','ordinal','event_type','equivalence_key','record_hash','ts_orig','ts_orig_tiebreak','record_state']:
                assert old[field]==new[field],field
            assert old['id']!=new['id'] and old['run_id']!=new['run_id']
    def precommit():
        nonlocal operation
        before=audit();preview=plan(action)
        assert preview['selected']==130 and preview['chunks']==3 and preview['chunk_max_records']==64
        assert audit()==before, 'plan wrote state'
        listener,path=hold('replay-staged-2')
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future=executor.submit(execute,action,(1,4),True)
            connection,frame=accept(listener,'replay-staged-2')
            operation=frame['evidence']['operation']['operation_id']
            progress=sql("SELECT progress FROM core.operations_log WHERE id='"+operation+"'::uuid")
            assert progress['processed']==progress['admitted']==progress['archived']==128 and progress['state']=='running'
            assert cli('replay-status',operation,expected=5)==progress
            held=state();assert sum(identity(initial)[a]!=identity(held)[a] for a in identity(initial))==128
            assert all(r['record_state']=='removed' for r in held[64:128])
            answer=cli('query',query);assert answer['total']==65
            corrected=next(r for r in answer['rows'] if r['payload']['native_id']=='fixture-000')
            assert corrected['payload']['hostname']=='fixture-host', 'remint alone did not prove correction'
            trace=cli('trace',corrected['id'],'--bytes')['row']
            assert all(trace[k]==before_trace[k] for k in ['material','anchor','end','record','representation'])
            old=cli('show',before_trace['event'])
            assert old['resolved']=='archived' and old['row']['archive']['reason']=='replay'
            assert old['row']['archive']['operation_id']==operation
            (report/'two-committed-ranges.json').write_text(json.dumps({'plan':preview,'progress':progress,'rows':held,'answer':answer,'trace':trace,'old':old},indent=2)+'\n')
            kill_and_restart(connection,listener,path,frame,'later-precommit',True)
            assert future.result(timeout=15)['kind'] in ('internal','refused')
        # Same-state startup resumes the admitted operation without a client start.
        completed=wait_operation(operation);assert completed['admitted']==130
        final=state();replacement_invariants(initial,final)
        assert sum(identity(held)[a]!=identity(final)[a] for a in identity(held))==1
        assert len(set(identity(final))-set(identity(held)))==1
        assert completed['processed']==130 and completed['archived']==129 and completed['admitted']==130 and completed['steps']==3
        restored=next(r for r in cli('query',query)['rows'] if r['payload']['native_id']=='fixture-129')
        assert restored['payload']['command']=='echo fixture 129' and restored['operation']==operation
        restored_trace=cli('trace',restored['id'],'--bytes')['row']
        assert restored_trace['record']['id']=='fixture-129' and restored_trace['record']['timestamp']==epoch+129*1_000_000_000
        receipt=audit();assert execute(action,0,True)==completed and audit()==receipt
        assert any(r['operation_id']==operation and r['state']=='completed' for r in cli('status')['replays'])
        (report/'after-actual.json').write_text(json.dumps({'outcome':completed,'rows':final,'answer':cli('query',query),'audit':receipt},indent=2)+'\n')
    scenario('bounded replay removed-range progress and later precommit recovery',precommit)
    def postcommit():
        other=str(uuid.uuid4());listener,path=hold('replay-committed-0');before=state()
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future=executor.submit(execute,other,1)
            connection,frame=accept(listener,'replay-committed-0')
            op=frame['evidence']['operation']['operation_id']
            progress=sql("SELECT progress FROM core.operations_log WHERE id='"+op+"'::uuid")
            assert progress['admitted']==64
            held=state();assert sum(identity(before)[a]!=identity(held)[a] for a in identity(before))==64
            kill_and_restart(connection,listener,path,frame,'postcommit')
            assert future.result(timeout=15)['kind'] in ('internal','refused')
        assert wait_operation(op)['admitted']==130
        final=state();assert sum(identity(held)[a]!=identity(final)[a] for a in identity(held))==66
        receipt=audit();assert execute(other)['admitted']==130 and audit()==receipt
    scenario('postcommit kill and request recovery',postcommit)
    def disconnect():
        # Kill only the recorded client while the real writer transaction is held.
        # Releasing that writer must complete all ranges without a connected waiter.
        listener,path=hold('replay-staged-0','disconnect-owner')
        request=str(uuid.uuid4());before=state()
        args=[str(binaries/'sinexctl'),'--endpoint',endpoint,'--token-file',str(work/'operator-token'),'replay',selection,'--action-id',request,'--json']
        out=(report/'disconnected-client.stdout').open('w');err=(report/'disconnected-client.stderr').open('w')
        proc=h['subprocess'].Popen(args,env=h['ENV'],stdout=out,stderr=err)
        h['CHILDREN'].append((proc,out,err));h['PROCESSES'].append({'pid':proc.pid,'argv':args,'stdout':'disconnected-client.stdout','stderr':'disconnected-client.stderr'})
        connection,frame=accept(listener,'replay-staged-0');op=frame['evidence']['operation']['operation_id']
        proc.send_signal(signal.SIGKILL);proc.wait(timeout=10);assert proc.returncode==-signal.SIGKILL
        connection.sendall(b'c');connection.close();listener.close();path.unlink()
        completed=wait_operation(op);assert completed['admitted']==130
        assert not (report/'disconnected-client.stdout').read_text()
        final=state();assert sum(identity(before)[a]!=identity(final)[a] for a in identity(before))==130
        receipt=audit();assert execute(request)==completed and audit()==receipt
        (report/'disconnected-operation.json').write_text(json.dumps({'frame':frame,'client_pid':proc.pid,'client_exit':proc.returncode,'completed':completed,'audit':receipt},indent=2)+'\n')
    scenario('disconnected client leaves admitted replay running',disconnect)
    def parser_basis_change():
        nonlocal daemon
        listener,path=hold('replay-staged-2','basis-owner');request=str(uuid.uuid4())
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future=executor.submit(execute,request,(1,4))
            connection,frame=accept(listener,'replay-staged-2');op=frame['evidence']['operation']['operation_id']
            progress=sql("SELECT progress FROM core.operations_log WHERE id='"+op+"'::uuid")
            assert progress['processed']==128
            held=state();receipt=audit()
            original_artifact=sql("SELECT to_jsonb(m.artifact_digest) FROM core.runs r JOIN core.manifests m ON m.id=r.manifest_id ORDER BY r.started_at DESC LIMIT 1")
            daemon.send_signal(signal.SIGKILL);daemon.wait(timeout=10);assert daemon.returncode==-signal.SIGKILL
            connection.close();listener.close();path.unlink()
            # A real runnable copy has an appended qualification marker. Its actual
            # file digest changes; no producer/artifact identity is invented in SQL.
            variant=report/'parser-artifact-variant.sinexd'
            original=(binaries/'sinexd').read_bytes()
            variant.write_bytes(original+b'\nSINEX-QUALIFICATION-ARTIFACT-IDENTITY-V1\n');variant.chmod(0o700)
            run([variant,'config','check','--config',config],label='actual changed-artifact fixture loader')
            daemon=start(config,'basis-variant',binary=variant);wait_ready()
            refused=wait_operation(op,'refused')
            assert refused['processed']==refused['archived']==refused['admitted']==128
            assert 'interpretation basis changed' in refused['reason']
            assert identity(state())==identity(held) and audit()['effects']==receipt['effects']
            variant_artifact=sql("SELECT to_jsonb(m.artifact_digest) FROM core.runs r JOIN core.manifests m ON m.id=r.manifest_id ORDER BY r.started_at DESC LIMIT 1")
            assert variant_artifact!=original_artifact
            assert future.result(timeout=15)['kind'] in ('internal','refused')
        stop(daemon);daemon=start(config,'basis-restored');wait_ready()
        receipt=audit();assert execute(request,4)['details']['processed']==128 and audit()==receipt
        (report/'parser-basis-refusal.json').write_text(json.dumps({'fixture':'packaged executable copy with appended marker; actual file digest, unchanged instruction bytes','frame':frame,'refused':refused,'original_artifact':original_artifact,'variant_artifact':variant_artifact,'original_sha256':hashlib.sha256(original).hexdigest(),'variant_sha256':hashlib.sha256(variant.read_bytes()).hexdigest()},indent=2)+'\n')
    scenario('changed parser artifact refuses mixed-operation continuation',parser_basis_change)
    def backing():
        info=sql("SELECT json_build_object('hash',b.blake3,'bytes',b.size_bytes) FROM raw.source_material_registry m JOIN core.blobs b ON b.id=m.blob_id WHERE m.id='"+material+"'::uuid")
        digest=info['hash'].removeprefix('blake3:')
        return store/'blobs'/digest[:2]/(digest+'.zst')
    def refusals():
        path=backing();encoded=path.read_bytes();(report/'exact-retained-encoded.zst').write_bytes(encoded)
        for mode in ['absent','corrupt']:
            original=audit();ids=identity(state());request=str(uuid.uuid4())
            if mode=='absent':path.rename(path.with_suffix('.hidden'))
            else:path.write_bytes(b'corrupt synthetic retained object')
            try:
                refused=execute(request,4);assert refused['kind']=='refused'
                assert identity(state())==ids
                now=audit();assert all(now[k]==original[k] for k in ['events','archives','effects'])
                assert cli('query',query)['total']==66
            finally:
                if mode=='absent':path.with_suffix('.hidden').rename(path)
                else:path.write_bytes(encoded)
            receipt=audit();assert execute(request,4)==refused and audit()==receipt
            (report/(mode+'-refusal.json')).write_text(json.dumps(refused,indent=2)+'\n')
        # Completed action recovery precedes now-missing material preconditions.
        path.rename(path.with_suffix('.hidden'))
        try:
            receipt=audit();assert execute(action,0,True)['admitted']==130 and audit()==receipt
        finally:path.with_suffix('.hidden').rename(path)
        # A later failed range preserves two committed ranges, including removed-only output.
        listener,socket_path=hold('replay-staged-2','failed-later-range')
        request=str(uuid.uuid4())
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future=executor.submit(execute,request,(1,4))
            connection,frame=accept(listener,'replay-staged-2');op=frame['evidence']['operation']['operation_id']
            held=state();receipt=audit();path.rename(path.with_suffix('.hidden'))
            kill_and_restart(connection,listener,socket_path,frame,'failed-later')
            assert future.result(timeout=15)['kind'] in ('internal','refused')
        try:
            failed=wait_operation(op,'refused')
            assert failed['admitted']==128 and identity(state())==identity(held)
            assert audit()['archives']==receipt['archives'] and audit()['effects']==receipt['effects']
            assert cli('query',query)['total']==66
        finally:path.with_suffix('.hidden').rename(path)
        (report/'failed-later-range.json').write_text(json.dumps({'refused':failed,'audit':audit()},indent=2)+'\n')
    scenario('retained evidence refusal and durable outcomes',refusals)
    def action_authority():
        bound=sql("SELECT json_build_object('request_ref',request_ref,'outcome_ref',outcome_ref) FROM core.operations_log WHERE id='"+operation+"'::uuid")
        observations=[]
        for field in ['request_ref','outcome_ref']:
            info=sql("SELECT json_build_object('hash',b.blake3,'bytes',b.size_bytes) FROM raw.source_material_registry m JOIN core.blobs b ON b.id=m.blob_id WHERE m.id='"+bound[field]+"'::uuid")
            digest=info['hash'].removeprefix('blake3:');path=store/'blobs'/digest[:2]/(digest+'.zst')
            encoded=path.read_bytes()
            for mode in ['absent','corrupt']:
                receipt=audit();ids=identity(state())
                if mode=='absent':path.rename(path.with_suffix('.authority-hidden'))
                else:path.write_bytes(b'corrupt synthetic action authority')
                try:
                    reply=execute(action,4,True);assert reply['kind']=='refused'
                    assert cli('replay-status',operation,expected=4)['kind']=='refused'
                    assert audit()==receipt and identity(state())==ids
                    observations.append({'reference':field,'mode':mode,'reply':reply,'audit':receipt})
                finally:
                    if mode=='absent':path.with_suffix('.authority-hidden').rename(path)
                    else:path.write_bytes(encoded)
                receipt=audit();assert execute(action,0,True)['state']=='completed' and audit()==receipt
        (report/'retained-action-authority.json').write_text(json.dumps(observations,indent=2)+'\n')
    scenario('retained request and outcome authority refuse unavailable comparison',action_authority)
    def authority():
        receipt=audit()
        assert cli('replay',selection,'--action-id',str(uuid.uuid4()),expected=4)['kind']=='refused'
        assert cli('replay',selection,'--parser','invented',operator=True,expected=2)['kind']=='bad_query'
        assert cli('replay','events','--dry-run',expected=2)['kind']=='bad_query'
        assert cli('replay',f'materials where id = {uuid.uuid4()}','--action-id',action,'--basis',bases[action],operator=True,expected=4)['kind']=='refused'
        assert audit()==receipt
        stale=str(uuid.uuid4());bases[stale]=bases[action]
        assert execute(stale,4,True)['kind']=='refused'
        # Observe the real public cancellation being queued before releasing the
        # in-flight transaction; no elapsed-time race determines its boundary.
        listener,path=hold('replay-staged-0','cancel-owner');request=str(uuid.uuid4())
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            running=executor.submit(execute,request,5)
            connection,frame=accept(listener,'replay-staged-0');op=frame['evidence']['operation']['operation_id']
            cancelling=executor.submit(cli,'replay-cancel',op,expected=5,operator=True)
            deadline=time.monotonic()+10
            while True:
                # Sampling a live log can include its unfinished final JSON line.
                notices=[]
                for line in (report/'cancel-owner.stderr').read_text().splitlines():
                    try:notices.append(json.loads(line))
                    except json.JSONDecodeError:pass
                if any(note.get('phase')=='replay_cancel_enqueued' and note.get('operation_id')==op for note in notices):break
                assert time.monotonic()<deadline
                time.sleep(.025)
            connection.sendall(b'c');connection.close();listener.close();path.unlink()
            stopped=cancelling.result(timeout=15)
            assert stopped['state']=='cancelled' and stopped['admitted']==64
            assert running.result(timeout=15)==stopped
        receipt=audit();assert execute(request,5)==stopped and audit()==receipt
        bindings=sql("SELECT json_build_object('operations',(SELECT json_agg(to_jsonb(o)) FROM core.operations_log o),'effects',(SELECT json_agg(to_jsonb(e)) FROM core.operation_effects e))")
        assert all(o['request_ref'] and o['request_format']=='sinex.replay.request.v1' and o['action_principal']=='foundation.operator' for o in bindings['operations'])
        assert all(o['outcome_ref'] for o in bindings['operations'])
        assert all(e['effects_ref'] and e['effect_format']=='sinex.replay.effects.v1' for e in bindings['effects'])
        guard=run(['psql',h['writer'],'-XAt','-v','ON_ERROR_STOP=1','-c',"UPDATE core.operations_log SET scope='{}'::jsonb WHERE id='"+operation+"'::uuid"],expected=1,label='operation request immutability refusal')
        assert 'operation identity and admitted request are immutable' in guard.stderr
        (report/'operation-bindings.json').write_text(json.dumps(bindings,indent=2)+'\n')
    scenario('explicit authority cancellation and request conflicts',authority)
    def malformed():
        nonlocal daemon
        stop(daemon)
        with sqlite3.connect(source) as con:
            con.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT,deleted_at INTEGER)')
            con.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',[
                ('malformed-control-good',epoch,1,0,'malformed control valid','/synthetic','control','fixture-host:user',None),
                ('malformed-control-bad','not-a-time',1,0,'malformed control invalid','/synthetic','control','fixture-host:user',None)])
        (report/'malformed-source.db').write_bytes(source.read_bytes())
        configure(True);daemon=start(config,'malformed-retention');wait_ready()
        deadline=time.monotonic()+30
        while True:
            listed=h['rpc'](endpoint,'synthetic-foundation-token','sources.list').get('result',{}).get('sources',[])
            if listed and listed[0]['last_run']['status']=='completed' and listed[0]['last_run']['rows_read']==2:break
            assert time.monotonic()<deadline
            time.sleep(.025)
        assert listed[0]['last_run']['record_errors']==1
        assert listed[0]['first_record_error']['record']['timestamp']=='not-a-time'
        good=cli('query','events where payload.command = "malformed control valid"')['rows'][0]
        traced=cli('trace',good['id'],'--bytes')['row']
        stop(daemon);source.unlink();configure(False);daemon=start(config,'malformed-retained-only');wait_ready()
        receipt=audit()
        bad_selection=f'materials where id = {traced["material"]}'
        assert cli('replay',bad_selection,'--dry-run',expected=4)['kind']=='refused'
        request=str(uuid.uuid4())
        refused=cli('replay',bad_selection,'--action-id',request,expected=4,operator=True)
        assert refused['kind']=='refused' and refused['details']['admitted']==0
        assert cli('show',good['id'])['resolved']=='live'
        now=audit();assert all(now[k]==receipt[k] for k in ['events','archives','effects'])
        (report/'malformed-refusal.json').write_text(json.dumps({'source':listed,'trace':traced,'refusal':refused,'prior_answer':good,'audit':now},indent=2)+'\n')
    scenario('malformed retained material preserves prior answers',malformed)
    stop(daemon)
