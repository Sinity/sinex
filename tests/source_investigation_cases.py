"""Synthetic acquisition outage journey through public clients, with independent facts."""
import datetime as dt
import hashlib
import json
import re
import select
import socket
import sqlite3
import subprocess
import time
import urllib.request
import uuid

PLANNED = ['initial failure and blocked first empty reading',
    'collector scheduled hold crosses its actual target with responsive clients',
    'unchanged repair and restart do not fabricate recovered interpretations',
    'offline catch-up receipts survive provider loss and damaging retained replay',
    'partial unavailable material stays owned by its original acquisition',
    'bounded history receipts and legacy unknowns preserve inspection',
    'primary diagnostic count guard and honest statement cancellation',
    'equal-evidence journal and provider comparison']
COLUMNS = ['id', 'timestamp', 'duration', 'exit', 'command', 'cwd', 'session', 'hostname', 'deleted_at']


def fixture_row(identity, command='echo synthetic acquisition'):
    return [identity, 1_700_000_000_000_000_000, 1_000_000_000, 0, command,
        '/synthetic/acquisition', 'fixture-session', 'fixture-host', None]


def create_provider(path, rows=()):
    with sqlite3.connect(path) as con:
        con.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT,deleted_at INTEGER)')
        con.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)', rows)


def exercise(h):
    work, report, env, bins = (h[k] for k in ['WORK', 'REPORT', 'ENV', 'BIN_DIR'])
    run, scenario, start, stop = (h[k] for k in ['run', 'scenario', 'start_daemon', 'stop'])
    endpoint = f'http://127.0.0.1:{h["free_port"]()}/rpc'
    source, config = work/'acquisition.db', work/'acquisition.toml'
    daemon = mcp = None
    calls, journal, comparisons = [], [], []
    native_answers = {}
    (report/'inputs').mkdir()
    fixture_inputs=[]
    identifier = 0
    token = (work/'token').read_text()
    (work/'operator-token').write_text('synthetic-acquisition-operator')
    (work/'operator-token').chmod(0o600)

    def save(name, value): (report/name).write_text(json.dumps(value, indent=2)+'\n')
    def retain_provider(path, name):
        destination=report/'inputs'/name
        with sqlite3.connect(path.as_uri()+'?mode=ro',uri=True) as source_db, sqlite3.connect(destination) as retained_db:
            source_db.backup(retained_db)
        fixture_inputs.append({'file':'inputs/'+name,'sha256':hashlib.sha256(destination.read_bytes()).hexdigest(),'kind':'independent synthetic SQLite fixture backup; no native physical-snapshot claim'})
        save('fixture-inputs.json',fixture_inputs)
    def note(kind, **facts):
        journal.append({'recorded_at':dt.datetime.now(dt.timezone.utc).isoformat(), 'kind':kind, **facts})
        save('baseline-journal.json', journal)
    def configure(path=source, interval=1000):
        config.write_text(f'''listen = "{endpoint.removeprefix('http://').removesuffix('/rpc')}"
state_dir = "{work/'acquisition-state'}"
store_dir = "{work/'acquisition-blobs'}"
writer_connection_file = "{work/'writer'}"
reader_connection_file = "{work/'reader'}"
token_file = "{work/'token'}"
operator_token_file = "{work/'operator-token'}"
sources = ["terminal.atuin-history"]
automata = []
[atuin]
path = "{path}"
poll_interval_ms = {interval}
full_interval_ms = 100 # this fixture deliberately compares the full provider
''')
    def api(method, params=None):
        req={'jsonrpc':'2.0','id':1,'method':method,'params':params or {}}
        with urllib.request.urlopen(urllib.request.Request(endpoint,data=json.dumps(req).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'}),timeout=15) as stream:
            reply=json.load(stream)
        with (report/'investigation-api.jsonl').open('a') as log:log.write(json.dumps({'request':req,'response':reply})+'\n')
        return reply
    def cli(command, *args, expected=0, operator=False):
        result=run([bins/'sinexctl','--endpoint',endpoint,'--token-file',work/('operator-token' if operator else 'token'),command,*args,'--json'],expected=expected,label='acquisition '+command)
        return json.loads(result.stdout)
    def src(path=source, **params):
        request=dict(params)
        while True:
            response=api('sources.list',request)['result']
            found=[r for r in response['sources'] if r['locator']==str(path)]
            if found:
                found[0]['_snapshot_now']=response['resolved']['now']
                return found[0]
            after=response['page']['next_after_input']
            if not after or 'input' in request:return None
            request['after_input']=after
    def record(phase, row, recovered=()):
        evidence=row['acquisition'];schedule=evidence['schedule'];state=evidence['state']
        since={'kind':'planned_next_start','value':schedule['next_start_at']} if state=='overdue' else ({'kind':'outcome_observed_at','value':schedule['settled_at']} if schedule else {'kind':'attempt_started_at','value':row['last_run']['observed_at']})
        cause=row['last_run']['reason'] if state in ('unavailable','interrupted') else ('unknown' if state=='overdue' else None)
        native_answers[phase]={'working':state,'why':cause,'since':since,'recovered_native_ids':list(recovered),'historical_coverage':'unknown'}
        note('snapshot',phase=phase,snapshot_at=row['_snapshot_now'],status=row['last_run']['status'],reason=row['last_run']['reason'],attempt_started_at=row['last_run']['observed_at'],schedule=schedule,current_scope=evidence['schedule_applicability']=='current_collector_run',retained_receipt_native_ids=list(recovered))
        save('native-journey-answers.json',native_answers)
    def observation(input_id, predicate):
        before=None
        while True:
            args={'input':input_id}
            if before is not None:args['before_seq']=before
            history=api('sources.list',args)['result']['sources'][0]['history']
            assert history['complete'] or history.get('next_before_seq'),history
            for row in history['rows']:
                if predicate(row):return row
            before=history.get('next_before_seq')
            if before is None:return None
    def wait(label, predicate, seconds=30):
        until=time.monotonic()+seconds
        while time.monotonic()<until:
            assert daemon.poll() is None, 'owned daemon stopped'
            try:
                value=predicate()
                if value:return value
            except (OSError, KeyError): pass
            time.sleep(.025)
        raise AssertionError(label)
    def invoke(method, params):
        nonlocal identifier
        identifier+=1
        req={'jsonrpc':'2.0','id':identifier,'method':method,'params':params}
        mcp.stdin.write(json.dumps(req)+'\n');mcp.stdin.flush()
        until=time.monotonic()+20
        while True:
            ready,_,_=select.select([mcp.stdout],[],[],max(0,until-time.monotonic()))
            assert ready,'MCP response timeout'
            reply=json.loads(mcp.stdout.readline())
            with (report/'investigation-mcp.jsonl').open('a') as log:log.write(json.dumps({'request':req,'response':reply})+'\n')
            if reply.get('id')==identifier:return reply
    def tool(arguments):
        reply=invoke('tools/call',{'name':'sinex_sources','arguments':arguments})['result']
        assert not reply.get('isError'),reply
        return reply.get('structuredContent') or json.loads(reply['content'][0]['text'])
    def discover():
        nonlocal mcp
        err=(report/'investigation-mcp.stderr').open('w')
        argv=[str(bins/'sinexctl'),'--endpoint',endpoint,'--token-file',str(work/'token'),'mcp']
        mcp=subprocess.Popen(argv,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,text=True,bufsize=1)
        h['CHILDREN'].append((mcp,mcp.stdout,err));h['PROCESSES'].append({'pid':mcp.pid,'argv':argv,'stderr':'investigation-mcp.stderr'})
        assert 'result' in invoke('initialize',{'protocolVersion':'2025-03-26','capabilities':{},'clientInfo':{'name':'source-investigation','version':'1'}})
        mcp.stdin.write(json.dumps({'jsonrpc':'2.0','method':'notifications/initialized'})+'\n');mcp.stdin.flush()
        tools=invoke('tools/list',{})['result']['tools'];save('investigation-tools.json',tools)
        assert 'observation' in next(t for t in tools if t['name']=='sinex_sources')['inputSchema']['properties']
    def receipt_pages(input_id, observation):
        rows=[];after=None;pages=[]
        while True:
            args=['--input',input_id,'--observation',observation]
            if after:args+=['--after-receipt',after]
            result=cli('sources',*args)['sources'][0]['receipts'];pages.append(result)
            rows+=result['rows'];after=result['next_after_receipt']
            if not after:break
        assert result['complete'] and len({r['event'] for r in rows})==len(rows)
        save('receipt-'+observation+'.json',pages)
        return rows
    def completed(path=source, after=None):
        row=src(path)
        return row if row and row['last_run']['status']=='completed' and row['acquisition']['schedule'] and (not after or row['last_run']['id']!=after) else None
    def sql(text):
        return json.loads(run(['psql',h['admin'],'--no-psqlrc','-qAt','-v','ON_ERROR_STOP=1','--command',text],label='isolated evidence audit').stdout)

    run(['python3',str(h['ROOT']/'tests/source_investigation_parser.py')],label='source investigation front-door refusals')
    configure()
    held=None;controller=None;lock=None
    try:
        def initial():
            nonlocal daemon, lock
            daemon=start(config,'acquisition-first-missing')
            first=wait('first failure',lambda:(r if (r:=src()) and r['last_run']['status']=='unavailable' and r['acquisition']['schedule'] else None))
            assert first['last_success'] is None and first['last_failure']['id']==first['last_run']['id']
            assert first['last_run']['observed_at'] < first['acquisition']['schedule']['settled_at']
            assert first['receipts']['rows']==[] and first['receipts']['complete']
            note('read_failed',reason=first['last_run']['reason'],attempt=first['last_run']['id'],outcome_observed_at=first['acquisition']['schedule']['settled_at'])
            record('first-failed',first)
            save('first-failed.json',first);stop(daemon)
            create_provider(source);retain_provider(source,'initial-empty.db')
            lock=sqlite3.connect(source);lock.execute('BEGIN EXCLUSIVE')
        scenario(PLANNED[0],initial)

        def scheduled():
            nonlocal daemon, held, controller, lock
            controller=socket.socket(socket.AF_UNIX);controller.bind(str(work/'collector.sock'));controller.listen(1);controller.settimeout(30)
            daemon=start(config,'acquisition-held','--qualification-boundary','collector-scheduled','--qualification-socket',work/'collector.sock')
            reading=wait('blocked reading',lambda:(r if (r:=src()) and r['last_run']['status']=='reading' else None))
            assert reading['acquisition']['state']=='reading' and reading['acquisition']['schedule'] is None
            assert reading['last_success'] is None and reading['last_failure']['id']
            save('blocked-reading.json',reading);lock.rollback();lock.close();lock=None
            held,_=controller.accept();frame=json.loads(held.makefile('rb').readline());save('collector-held-frame.json',frame)
            discover()
            before_packet=cli('sources');before=before_packet['sources'][0];before['_snapshot_now']=before_packet['resolved']['now']
            assert before['last_run']['status']=='completed' and before['last_run']['rows_read']==0
            assert before['acquisition']['state']=='completed',before
            assert before['last_run']['commit_seq'] > before['cursor']['commit_seq'], 'schedule-only mutation must advance source-visible read position'
            assert tool({})['sources'][0]['last_run']['id']==before['last_run']['id']
            record('first-empty',before)
            after=wait('planned start becomes overdue',lambda:(r if (r:=src())['acquisition']['state']=='overdue' else None))
            assert after['last_run']==before['last_run'] and after['receipts']['rows']==[]
            assert api('status')['result']['writer']['state']=='owned'
            note('missed_planned_start',planned=before['acquisition']['schedule']['next_start_at'],cause='unknown',attempt=before['last_run']['id'])
            record('overdue',after)
            save('deadline-before-after.json',{'before':before,'after':after,'MCP':tool({})})
            held.sendall(b'c');held.close();held=None;controller.close();controller=None
        scenario(PLANNED[1],scheduled)

        def unchanged():
            nonlocal daemon
            prior=src()['last_run']['id'];r=wait('empty repeated',lambda:completed(after=prior));assert r['last_run']['admitted']==0
            with sqlite3.connect(source) as con:con.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',fixture_row('old'))
            retain_provider(source,'old.db')
            r=wait('old row acquired',lambda:(x if (x:=completed()) and x['last_run']['rows_read']==1 else None))
            input_id=r['input'];old_observation=observation(input_id,lambda o:o['status']=='completed' and o['admitted']==1 and o['rows_read']==1);assert old_observation
            old_pass=old_observation['id'];old=receipt_pages(input_id,old_pass)
            assert len(old)==1;save('old-receipt.json',old)
            record('old-admitted',r)
            r=wait('old repeated',lambda:(x if (x:=completed(after=old_pass)) and x['last_run']['repeated']==1 else None))
            assert r['last_run']['admitted']==0 and receipt_pages(input_id,r['last_run']['id'])==[]
            record('unchanged',r)
            stop(daemon);source.rename(work/'provider-away.db');note('provider_path_removed')
            daemon=start(config,'acquisition-restart-unavailable')
            failed=wait('restart still failed',lambda:(x if (x:=src()) and x['last_run']['status']=='unavailable' else None))
            assert failed['last_success']['rows_read']==1 and failed['last_run']['admitted']==0
            record('restart-without-provider',failed)
            save('restart-no-recovery.json',failed);stop(daemon)
            (work/'provider-away.db').rename(source);daemon=start(config,'acquisition-repair-unchanged')
            repaired=wait('repair unchanged',lambda:(x if (x:=completed()) and x['last_run']['repeated']==1 else None))
            assert repaired['last_failure']['id'] and repaired['last_run']['admitted']==0
            assert receipt_pages(input_id,repaired['last_run']['id'])==[]
            record('repair-unchanged',repaired)
            save('repair-no-recovery.json',repaired)
            note('repair_read_unchanged',attempt=repaired['last_run']['id'],retained_new_interpretations=[])
        scenario(PLANNED[2],unchanged)

        def catchup():
            nonlocal daemon
            stop(daemon)
            with sqlite3.connect(source) as con:con.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',fixture_row('backfill','echo uniquely planted offline backfill'))
            retain_provider(source,'offline-catchup.db')
            note('offline_provider_append',native_id='backfill')
            daemon=start(config,'acquisition-catchup')
            r=wait('catchup',lambda:(x if (x:=completed()) and x['last_run']['rows_read']==2 else None))
            recovered_observation=observation(r['input'],lambda o:o['status']=='completed' and o['rows_read']==2 and o['admitted']==1);assert recovered_observation
            observation_id=recovered_observation['id'];receipts=receipt_pages(r['input'],observation_id);assert len(receipts)==1
            receipt=receipts[0];shown=cli('show',receipt['event']);traced=cli('trace',receipt['event'],'--bytes')
            assert shown['row']['payload']['native_id']=='backfill'
            assert traced['row']['material']==receipt['material'] and traced['row']['anchor']==receipt['start'] and traced['row']['end']==receipt['end']
            assert {k:traced['row']['record'][k] for k in COLUMNS} == dict(zip(COLUMNS,fixture_row('backfill','echo uniquely planted offline backfill')))
            record('catchup',r,[shown['row']['payload']['native_id']])
            stop(daemon);source.rename(work/'provider-away.db');daemon=start(config,'acquisition-retained-only')
            failed=wait('provider unavailable',lambda:(x if (x:=src()) and x['last_run']['status']=='unavailable' else None))
            record('provider-loss',failed)
            note('read_failed',reason=failed['last_run']['reason'],attempt=failed['last_run']['id'],attempt_started_at=failed['last_run']['observed_at'])
            assert cli('trace',receipt['event'],'--bytes')['row']['record']['id']=='backfill'
            # A real replay creates new interpretations from the old acquisition material.
            replay=cli('replay',f'materials where id = {receipt["material"]}','--action-id',str(uuid.uuid4()),operator=True)
            assert replay['state']=='completed'
            assert sql("SELECT count(*) FROM core.events WHERE created_by_operation IS NOT NULL") > 0
            after=receipt_pages(r['input'],observation_id);assert after==receipts
            assert tool({'input':r['input'],'observation':observation_id})['sources'][0]['receipts']['rows']==receipts
            save('catchup-and-replay.json',{'acquisition':r,'receipt':receipt,'show':shown,'trace':traced,'replay':replay,'after_replay':after})
            note('catchup_retained',attempt=observation_id,new_interpretations=[receipt['event']],native_id='backfill',history='unknown')
        scenario(PLANNED[3],catchup)

        def partial():
            nonlocal daemon, held, controller
            stop(daemon)
            path=work/'partial.db'
            rows=[fixture_row(f'partial-{n:03}') for n in range(65)]+[fixture_row('oversize','x'*1_100_000)]
            create_provider(path,rows);retain_provider(path,'partial-unavailable.db');configure(path,1000)
            controller=socket.socket(socket.AF_UNIX);controller.bind(str(work/'partial.sock'));controller.listen(1);controller.settimeout(30)
            daemon=start(config,'acquisition-partial','--qualification-boundary','collector-scheduled','--qualification-socket',work/'partial.sock')
            held,_=controller.accept();frame=json.loads(held.makefile('rb').readline());save('partial-held-frame.json',frame)
            r=wait('partial failed after committed prefix',lambda:(x if (x:=src(path)) and x['last_run']['status']=='unavailable' and x['last_run']['admitted']==64 else None))
            failed_observation=r['last_run']['id'];input_id=r['input']
            first=receipt_pages(input_id,failed_observation);assert len(first)==64
            stop(daemon);held.close();held=None;controller.close();controller=None
            with sqlite3.connect(path) as con:
                con.execute("DELETE FROM history WHERE id='oversize'")
                con.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',fixture_row('fresh-after-partial'))
            retain_provider(path,'partial-repair.db')
            daemon=start(config,'acquisition-partial-repair')
            repaired=wait('partial next pass',lambda:(x if (x:=completed(path)) and x['last_run']['rows_read']==66 else None))
            admitted=observation(input_id,lambda o:o['status']=='completed' and o['admitted']==2 and o['rows_read']==66);assert admitted
            assert len(receipt_pages(input_id,admitted['id']))==2
            assert receipt_pages(input_id,failed_observation)==first
            save('partial-ownership.json',{'failed':r,'original_receipts':first,'repaired':repaired})
        scenario(PLANNED[4],partial)

        def bounded():
            nonlocal daemon, held, controller
            path=work/'partial.db'
            wait('more than one history page',lambda:(x if (x:=src(path)) and not x['history']['complete'] else None))
            stop(daemon)
            controller=socket.socket(socket.AF_UNIX);controller.bind(str(work/'bounded.sock'));controller.listen(1);controller.settimeout(30)
            daemon=start(config,'acquisition-bounded-held','--qualification-boundary','collector-scheduled','--qualification-socket',work/'bounded.sock')
            held,_=controller.accept();frame=json.loads(held.makefile('rb').readline());save('bounded-held-frame.json',frame)
            first=src(path);input_id=first['input'];seen=[];before=None
            while True:
                args=['--input',input_id]
                if before:args+=['--before-seq',str(before)]
                page=cli('sources',*args)['sources'][0]['history'];assert len(page['rows'])<=8
                seen += [x['id'] for x in page['rows']];before=page['next_before_seq']
                if before is None:break
            assert len(seen)>8 and len(set(seen))==len(seen)
            assert first['last_failure']['id'], 'inherited failure remains available beyond history page'
            plans={}
            receipt_observation=json.loads((report/'partial-ownership.json').read_text())['failed']['last_run']['id']
            for name,query in {
                'latest':f"SELECT id FROM raw.observations WHERE input_id='{input_id}' ORDER BY seq DESC LIMIT 1",
                'outcome_reference':f"SELECT id FROM raw.observations WHERE id='{first['last_failure']['id']}' AND input_id='{input_id}'",
                'history':f"SELECT id FROM raw.observations WHERE input_id='{input_id}' ORDER BY seq DESC LIMIT 9",
                'receipts':f"SELECT e.id FROM core.events e JOIN raw.source_material_registry m ON m.id=e.source_material_id WHERE m.input_id='{input_id}' AND m.observation_id='{receipt_observation}' AND e.created_by_operation IS NULL ORDER BY e.id LIMIT 17",
            }.items(): plans[name]=sql('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) '+query)
            save('actual-inspection-query-plans.json',plans)
            # Lock only optional receipt evidence. Source latest_run must survive the
            # actual PostgreSQL statement budget, and inspection must then recover.
            lock_err=(report/'optional-lock.stderr').open('w')
            locker=subprocess.Popen(['psql',h['admin'],'--no-psqlrc','-qAt','-v','ON_ERROR_STOP=1'],env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=lock_err,text=True,bufsize=1)
            try:
                locker.stdin.write("BEGIN; LOCK TABLE core.events IN ACCESS EXCLUSIVE MODE; SELECT 'receipt-lock-held';\n");locker.stdin.flush()
                ready,_,_=select.select([locker.stdout],[],[],15);assert ready and locker.stdout.readline().strip()=='receipt-lock-held'
                began=time.monotonic();limited=cli('sources','--input',input_id)['sources'][0]
                assert limited['last_run']['id']==first['last_run']['id'] and limited['acquisition']['state'] in ('completed','overdue')
                assert limited['inspection']['status']=='unavailable' and limited['history']['complete'] is False
                save('optional-detail-budget.json',{'seconds':time.monotonic()-began,'response':limited})
            finally:
                locker.stdin.write('ROLLBACK;\n\\q\n');locker.stdin.flush();locker.stdin.close();locker.wait(timeout=15);lock_err.close()
                assert locker.returncode==0
            assert cli('sources','--input',input_id)['sources'][0]['inspection']['status']=='available'
            # Existing schema/rows upgrade without fabricated timing or receipt custody.
            legacy=first['last_run']['id']
            sql("WITH changed AS (UPDATE raw.observations SET metadata=metadata-'_sinex_acquisition_v1' WHERE id='"+legacy+"' RETURNING id) SELECT count(*) FROM changed")
            legacy_row=src(path,input=input_id,observation=legacy)
            assert legacy_row['acquisition']['schedule_evidence']=='unknown'
            assert legacy_row['receipts']['evidence']=='unknown_legacy_or_no_observation' and not legacy_row['receipts']['complete']
            assert 'error' in api('sources.list',{'observation':legacy})
            assert 'error' in api('sources.list',{'input':input_id,'before_seq':0})
            run([bins/'sinexd','admin','schema','--connection-file',work/'admin','--check'],label='unchanged schema accepts legacy observation metadata')
            save('bounded-legacy.json',{'history_ids':seen,'legacy':legacy_row})
            stop(daemon);held.close();held=None;controller.close();controller=None
            # Register enough actual configured inputs to exercise source paging;
            # no synthetic SQL inserts stand in for normal acquisition registration.
            extra=[]
            for n in range(17):
                missing=work/f'additional-missing-{n}.db';extra.append(str(missing));configure(missing)
                daemon=start(config,f'acquisition-additional-{n}')
                wait('additional input registration',lambda:api('status').get('result'))
                if n<16:stop(daemon)
            pages=[];after=None;inputs=[]
            while True:
                args=['--after-input',after] if after else []
                page=cli('sources',*args);pages.append(page)
                assert len(page['sources'])<=16
                inputs+=page['sources'];after=page['page']['next_after_input']
                if not after:break
            assert len(inputs)==19 and len({x['input'] for x in inputs})==19
            assert set(extra)<=set(x['locator'] for x in inputs)
            old_inputs=[x for x in inputs if x['locator']!=extra[-1]]
            assert all(x['acquisition']['state']=='unknown' and x['acquisition']['schedule_applicability'].startswith('historical_or_unknown') for x in old_inputs)
            assert inputs[-1]['acquisition']['schedule_applicability']=='current_collector_run'
            save('source-input-pages.json',pages)
        scenario(PLANNED[5],bounded)

        def primary_diagnostics():
            nonlocal daemon, held, controller
            stop(daemon)
            path=work/'diagnostic-counts.db'
            rows=[fixture_row(f'diagnostic-{n:04}') for n in range(2048)]
            for row in rows:row[2]=-2 # unsupported duration, independently malformed
            create_provider(path,rows);configure(path)
            controller=socket.socket(socket.AF_UNIX);controller.bind(str(work/'diagnostics.sock'));controller.listen(1);controller.settimeout(30)
            daemon=start(config,'diagnostics-positive','--qualification-boundary','collector-scheduled','--qualification-socket',work/'diagnostics.sock')
            held,_=controller.accept();frame=json.loads(held.makefile('rb').readline());save('diagnostics-positive-held.json',frame)
            positive=src(path);input_id=positive['input']
            assert positive['last_run']['record_errors']==2048
            error=positive['first_record_error'];assert error['evidence_status']=='available' and error['record']['id']=='diagnostic-0000'
            first_error=sql("SELECT to_jsonb(f) FROM raw.record_failures f JOIN raw.source_material_registry m ON m.id=f.material_id WHERE f.observation_id='"+positive['last_run']['id']+"' ORDER BY m.staged_at,m.id,f.anchor_byte LIMIT 1")
            for field in ['observation_id','material_id','anchor_byte','reason']:assert error[field]==first_error[field]
            stop(daemon);held.close();held=None
            with sqlite3.connect(path) as c:c.execute('UPDATE history SET duration=0')
            daemon=start(config,'diagnostics-zero','--qualification-boundary','collector-scheduled','--qualification-socket',work/'diagnostics.sock')
            held,_=controller.accept();frame=json.loads(held.makefile('rb').readline());save('diagnostics-zero-held.json',frame)
            zero=src(path);assert zero['input']==input_id and zero['last_run']['record_errors']==0 and zero['first_record_error'] is None
            assert sql('SELECT count(*) FROM raw.record_failures')>=2048
            # EXPLAIN the exact production statement; no copied approximation.
            rust=(h['ROOT']/'crate/sinex-store/src/source_inspection.rs').read_text()
            primary=re.search(r'const SOURCE_ROWS: &str = r#"(.*?)"#;',rust,re.S).group(1)
            plan=sql("PREPARE diagnostic_primary(uuid,uuid,bigint) AS "+primary+"; EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE diagnostic_primary('"+input_id+"',NULL,17)")
            def nodes(node):
                yield node
                for child in node.get('Plans',[]):yield from nodes(child)
            scans=[n for n in nodes(plan[0]['Plan']) if n.get('Relation Name')=='record_failures']
            assert scans and all(n['Actual Loops']==0 for n in scans),scans
            save('primary-diagnostic-count-guard.json',{'positive':positive,'zero':zero,'plan':plan})
            lock_err=(report/'primary-lock.stderr').open('w')
            locker=subprocess.Popen(['psql',h['admin'],'--no-psqlrc','-qAt','-v','ON_ERROR_STOP=1'],env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=lock_err,text=True,bufsize=1)
            try:
                locker.stdin.write("BEGIN; LOCK TABLE raw.inputs IN ACCESS EXCLUSIVE MODE; SELECT 'primary-lock-held';\n");locker.stdin.flush()
                ready,_,_=select.select([locker.stdout],[],[],15);assert ready and locker.stdout.readline().strip()=='primary-lock-held'
                began=time.monotonic()
                result=run([bins/'sinexctl','--endpoint',endpoint,'--token-file',work/'token','sources','--input',input_id,'--json'],expected=6,label='blocked primary source statement')
                error=json.loads(result.stderr);assert error['kind']=='budget'
                assert error['details']=={'reason':'statement_cancelled','operation':'sources.list','phase':'primary_source_rows','configured_statement_timeout_ms':2000},error
                save('primary-statement-cancelled.json',{'response':error,'seconds':time.monotonic()-began})
            finally:
                locker.stdin.write('ROLLBACK;\n\\q\n');locker.stdin.flush();locker.stdin.close();locker.wait(timeout=15);lock_err.close();assert locker.returncode==0
            after=cli('sources','--input',input_id)['sources'][0]
            assert after['last_run']['id']==zero['last_run']['id'] and after['first_record_error'] is None
            assert cli('query','events where source = terminal.atuin-history',expected=5)['total']>=2048
            save('primary-after-cancellation.json',after)
            stop(daemon);held.close();held=None;controller.close();controller=None
        scenario(PLANNED[6],primary_diagnostics)

        def compare():
            # The baseline receives the SAME retained synthetic journal observations and
            # actual direct provider availability/content. No model or private corpus.
            baseline=run(['python3',str(h['ROOT']/'tests/source_investigation_baseline.py'),str(report/'baseline-journal.json'),str(source),str(report/'inputs/offline-catchup.db')],label='competent journal plus direct provider baseline')
            result=json.loads(baseline.stdout)
            assert not result['provider']['readable'] and result['provider']['native_ids'] is None
            assert result['retained_fixture_provider']['native_ids']==['backfill','old']
            assert result['answers']['historical_coverage']=='unknown'
            assert result['answers']['recovered_native_ids']==['backfill']
            expected_states={'first-failed':'unavailable','first-empty':'completed','overdue':'overdue','old-admitted':'completed','unchanged':'completed','restart-without-provider':'unavailable','repair-unchanged':'completed','catchup':'completed','provider-loss':'unavailable'}
            assert {k:v['working'] for k,v in native_answers.items()}==expected_states
            assert result['journey']==native_answers, {'baseline':result['journey'],'native':native_answers}
            comparisons.append({'scope':'authored original Atuin outage journey, supplied timestamped journal and direct provider; original provider is currently unavailable','answer_fidelity':'tie on supplied journal facts','navigation':'native ordinary sources pages link retained interpretations; baseline uses supplied journal and direct SQLite; no measured human-effort advantage','maintenance':'both require preserved evidence; baseline journal authored by qualification driver, not a new product logging service','baseline':result,'retained_input_availability':'equal synthetic acquisition outcomes, schedule decisions, repair and recovery receipts, plus explicitly supplied consistent fixture SQLite backup; absent original provider does not invalidate retained journal facts or the supplied backup' ,'unrun':['human effort measurement','historical coverage','full M1','installed use']})
            save('baseline-comparison.json',comparisons)
            (report/'investigation-report.md').write_text('''# Native Atuin acquisition investigation

The synthetic public-client journey distinguishes observed reading outcomes, collector-selected next starts and newly admitted interpretation receipts. An explicitly held collector leaves the API responsive while its retained planned target passes. Repair without new rows and restart without a provider recover no new interpretations. Catch-up retains the planted backfill; later retained replay cannot become an original acquisition receipt. Partial failed materials retain original ownership. Pages disclose omitted detail and continue through the existing sources route. Legacy timing and receipts remain unknown.

See baseline-comparison.json for the equal-evidence comparison and its tie. API/MCP captures and enclosing command receipts preserve exact calls. This does not establish historical capture completeness, execution counts, ordinary-use superiority, installed behavior or full milestone acceptance. Earlier configured-continuation10392 remains separately bound to its original immutable package and was not rerun or requalified.
''')
        scenario(PLANNED[7],compare)
    finally:
        if lock:lock.close()
        # Stop a held collector before closing its qualification socket; EOF
        # otherwise turns fixture cleanup into a second refusal.
        if daemon and daemon.poll() is None:stop(daemon)
        if held:held.close()
        if controller:controller.close()
        if mcp and mcp.poll() is None:
            mcp.stdin.close();mcp.wait(timeout=15);assert mcp.returncode==0
