#!/usr/bin/env python3
"""Packaged guarded upgrade of a populated, explicitly pinned published predecessor."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import sqlite3
import subprocess
import tempfile
import time
import traceback
import urllib.request

ROOT=Path(__file__).resolve().parents[1]
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--predecessor-package',type=Path,required=True)
parser.add_argument('--predecessor-publication',type=Path,required=True)
args=parser.parse_args()
REPORT=Path(os.environ.get('SINEX_SCHEMA_UPGRADE_REPORT_DIR',f'/realm/project/sinex/analysis/schema-upgrade-20261016/run-{time.time_ns()}'))
REPORT.mkdir(parents=True,exist_ok=False)
WORK=Path(tempfile.mkdtemp(prefix='sinex-schema-upgrade-',dir='/realm/tmp/work'))
ENV=dict(os.environ)
for key in ['DATABASE_URL','SQLX_OFFLINE','PGHOST','PGPORT','PGDATABASE','PGUSER']:ENV.pop(key,None)
COMMANDS=[];CHILDREN=[];SCENARIOS=[];started=False

def save(name,value):
    (REPORT/name).write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')

def run(argv,expected=0,env=None,label=None):
    i=len(COMMANDS);COMMANDS.append({'argv':list(map(str,argv)),'expected':expected,'label':label,'exit':None})
    p=subprocess.run(list(map(str,argv)),cwd=ROOT,env=env or ENV,text=True,capture_output=True,timeout=1800)
    (REPORT/f'{i:03}.stdout').write_text(p.stdout);(REPORT/f'{i:03}.stderr').write_text(p.stderr)
    COMMANDS[i]['exit']=p.returncode
    assert p.returncode in ([expected] if isinstance(expected,int) else expected),f'command {i} failed: {label or argv[0]}; raw output retained'
    return p

def scenario(name,fn):
    then=time.monotonic();fn();SCENARIOS.append({'name':name,'seconds':time.monotonic()-then,'outcome':'passed'})

def sql(text,*,url=None,expected=0):
    return run(['psql',url or admin,'-XAt','-v','ON_ERROR_STOP=1','-c',text],expected=expected,label='owned database observation').stdout.strip()

def port():
    with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]

def launch(argv,name):
    out=(REPORT/(name+'.stdout')).open('w');err=(REPORT/(name+'.stderr')).open('w')
    p=subprocess.Popen(list(map(str,argv)),cwd=ROOT,env=ENV,stdout=out,stderr=err)
    CHILDREN.append((p,out,err));save(name+'-process.json',{'pid':p.pid,'argv':list(map(str,argv))});return p

def stop(p):
    if p.poll() is None:p.terminate();p.wait(timeout=20)
    assert p.returncode==0,f'process failed {p.returncode}'

def rpc(method,params=None):
    req=urllib.request.Request(endpoint,data=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params or {}}).encode(),headers={'Authorization':'Bearer synthetic-schema-token','Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=3) as response:value=json.load(response)
    with (REPORT/'rpc-observations.jsonl').open('a') as log:log.write(json.dumps({'method':method,'params':params or {},'response':value})+'\n')
    return value

def wait(fn,proc,reason,seconds=40):
    until=time.monotonic()+seconds
    while time.monotonic()<until:
        assert proc.poll() is None,f'{reason}: process exited {proc.returncode}'
        try:
            v=fn()
            if v:return v
        except (OSError,ValueError,KeyError):pass
        time.sleep(.05)
    raise AssertionError(reason)

def cli(command,*extra,expected=0):
    p=run([candidate/'bin/sinexctl','--endpoint',endpoint,'--token-file',WORK/'token',command,*extra,'--json'],expected=expected,label='packaged '+command)
    return json.loads(p.stdout or p.stderr)

def schema(binary,connection=None,*extra,expected=0,env=None):
    p=run([binary,'admin','schema','--connection-file',connection or WORK/'admin',*extra],expected=expected,env=env,label='packaged schema control')
    return json.loads(p.stdout) if p.returncode==0 else p

def snapshot():
    return sql("SELECT jsonb_build_object('events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY id) FROM core.events e),'materials',(SELECT jsonb_agg(to_jsonb(m) ORDER BY id) FROM raw.source_material_registry m),'producers',(SELECT jsonb_agg(to_jsonb(p) ORDER BY id) FROM core.producer_versions p),'inputs',(SELECT jsonb_agg(to_jsonb(i) ORDER BY id) FROM raw.inputs i),'archive',(SELECT jsonb_agg(to_jsonb(a) ORDER BY id) FROM audit.archived a),'counter',(SELECT seq FROM core.commit_counter WHERE one))")

def schema_dump():
    text=run(['pg_dump',admin,'--schema-only'],label='independent schema and grants dump').stdout
    return '\n'.join(line for line in text.splitlines() if not line.startswith(('\\restrict ','\\unrestrict ')))

def payload_catalog():
    return sql("SELECT jsonb_agg(to_jsonb(p) ORDER BY id) FROM sinex_schemas.payload_schemas p")


success=False;failure=None
try:
    predecessor=args.predecessor_package.resolve(strict=True)
    assert predecessor.parent==Path('/nix/store') and predecessor==args.predecessor_package
    publication=json.loads(args.predecessor_publication.read_text())
    assert publication['merge']=='4b5bf06197ffb1d07d57794f37f7273b634e0efd' and publication['tree']=='1a62d48cbea5da24d41497e0f1470d734fbe64bb'
    assert subprocess.check_output(['git','rev-parse',publication['source_candidate']+'^{tree}'],cwd=ROOT,text=True).strip()==publication['tree']
    save('predecessor-binding.json',{'package':str(predecessor),'publication':publication,'binaries':{n:hashlib.sha256((predecessor/'bin'/n).read_bytes()).hexdigest() for n in ['sinexd','sinexctl']}})
    built=run(['nix','build','--no-link','.#default','--print-out-paths','--max-jobs','1'],label='actual candidate Nix package')
    paths=built.stdout.splitlines();assert len(paths)==1;candidate=Path(paths[0]);binary=candidate/'bin/sinexd'
    save('candidate-binding.json',{'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),'index_tree':subprocess.check_output(['git','write-tree'],cwd=ROOT,text=True).strip(),'tracked_diff_sha256':hashlib.sha256(subprocess.check_output(['git','diff','--binary','HEAD'],cwd=ROOT)).hexdigest(),'package':str(candidate),'binaries':{n:hashlib.sha256((candidate/'bin'/n).read_bytes()).hexdigest() for n in ['sinexd','sinexctl']}})
    (WORK/'pg').mkdir();(WORK/'socket').mkdir();ENV.update(PGDATA=str(WORK/'pg'),PGHOST=str(WORK/'socket'),PGPORT=str(port()))
    run(['initdb','--no-locale','--auth=trust','--data-checksums'])
    run(['pg_ctl','-D',WORK/'pg','-l',REPORT/'postgres.log','-o',f"-k {WORK/'socket'} -c listen_addresses= -c shared_preload_libraries=timescaledb -c timescaledb.telemetry_level=off -c timescaledb.enable_chunk_skipping=on -p {ENV['PGPORT']}",'-w','start']);started=True
    for name in ['sinex_writer','sinex_reader']:run(['createuser',name])
    run(['createdb','upgrade'])
    admin=f"postgresql:///upgrade?host={WORK/'socket'}&port={ENV['PGPORT']}"
    writer=admin.replace('postgresql:///','postgresql://sinex_writer@localhost/')
    reader=admin.replace('postgresql:///','postgresql://sinex_reader@localhost/')
    read_model=reader.replace('sinex_reader','sinex_read_model')
    for name,value in [('admin',admin),('writer',writer),('reader',reader),('read-model',read_model),('token','synthetic-schema-token')]:
        (WORK/name).write_text(value);(WORK/name).chmod(0o600)
    old_binary=predecessor/'bin/sinexd'
    schema(old_binary)
    db=WORK/'atuin.db'
    fixtures=[('native-one',1700000000123456789,1000000000,0,'echo schema retained one','/synthetic/schema','session','schema-host',None),('native-two',1700000001123456789,2000000000,7,'cargo schema retained two','/synthetic/schema','session','schema-host',None)]
    with sqlite3.connect(db) as con:
        con.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER NOT NULL,duration INTEGER NOT NULL,exit INTEGER NOT NULL,command TEXT NOT NULL,cwd TEXT NOT NULL,session TEXT NOT NULL,hostname TEXT NOT NULL,deleted_at INTEGER)')
        con.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',fixtures)
    shutil.copy2(db,REPORT/'independent-predecessor-input.db');save('independent-expected-rows.json',fixtures)
    listen=port();endpoint=f'http://127.0.0.1:{listen}/rpc';config=WORK/'config.toml'
    def configure(sources=True):
        config.write_text(f'''listen="127.0.0.1:{listen}"
state_dir="{WORK/'state'}"
store_dir="{WORK/'blobs'}"
writer_connection_file="{WORK/'writer'}"
reader_connection_file="{WORK/'reader'}"
read_model_connection_file="{WORK/'read-model'}"
token_file="{WORK/'token'}"
sources={json.dumps(['terminal.atuin-history'] if sources else [])}
automata=[]
'''+(f'''[atuin]
path="{db}"
poll_interval_ms=3600000
''' if sources else ''))
    configure()
    proc=launch([old_binary,'serve','--config',config],'published-population')
    raw_query='events where source = terminal.atuin-history and kind = command.executed'
    baseline=wait(lambda:(lambda v:v.get('result') if v.get('result',{}).get('total')==2 else None)(rpc('query.run',{'query':raw_query})),proc,'published predecessor raw population')
    wait(lambda:rpc('query.run',{'query':'events where kind = command.canonical'}).get('result',{}).get('total')==2,proc,'published singleton canonical population')
    assert {r['payload']['native_id']:r['payload']['command'] for r in baseline['rows']}=={v[0]:v[4] for v in fixtures}
    save('published-baseline.json',baseline);stop(proc)
    baseline_rows=snapshot();(REPORT/'retained-before.json').write_text(baseline_rows+'\n')
    published_status=schema(old_binary,None,'--check');save('published-schema.json',published_status)
    old_payload=json.loads(sql("SELECT to_jsonb(p) FROM sinex_schemas.payload_schemas p WHERE id=3"));save('published-payload-schema3.json',old_payload)
    def pending():
        before=sql('SELECT fingerprint,catalog FROM sinex_schemas.definition');actual_schema=schema_dump()
        value=schema(binary);assert [c['number'] for c in value['pending_changes']]==[1,2]
        assert sql('SELECT fingerprint,catalog FROM sinex_schemas.definition')==before and sql("SELECT to_regclass('derivation.shell_progress') IS NULL AND to_regclass('sinex_schemas.applied_changes') IS NULL")=='t'
        proc=launch([binary,'serve','--config',config],'pending-reader')
        status=wait(lambda:rpc('status').get('result'),proc,'pending status')
        assert status['writer']['state']=='unavailable' and status['schema']['pending_changes']==[1,2] and not status['schema']['compatible']
        assert sql('SELECT pg_try_advisory_lock(73018924012)')=='t'
        assert cli('query',raw_query)['rows']==baseline['rows']
        refusal=cli('query','commands',expected=2);assert refusal['kind']=='bad_query'
        save('pending-status.json',status);stop(proc)
        assert snapshot()==baseline_rows and schema_dump()==actual_schema
    scenario('pending inspection/startup preserves catalog and admits no writer while retained raw reads work',pending)
    def dump_failure():
        before=sql('SELECT fingerprint,catalog FROM sinex_schemas.definition');actual_schema=schema_dump()
        fake=WORK/'dump-failure-bin';fake.mkdir();(fake/'pg_dump').write_text('#!/bin/sh\necho synthetic-dump-failure >&2\nexit 23\n');(fake/'pg_dump').chmod(0o700)
        failed=schema(binary,None,'--apply','--dump-file',WORK/'failed.dump',expected=1,env={**ENV,'PATH':str(fake)+':'+ENV['PATH']})
        assert 'pg_dump failed' in failed.stderr
        assert sql('SELECT fingerprint,catalog FROM sinex_schemas.definition')==before and snapshot()==baseline_rows
        assert sql("SELECT to_regclass('sinex_schemas.applied_changes') IS NULL")=='t' and schema_dump()==actual_schema
        assert (WORK/'failed.dump.stderr').read_text()=='synthetic-dump-failure\n'
    scenario('failed dump leaves populated catalog data and progress absent',dump_failure)
    def refuse_damage():
        before=sql('SELECT fingerprint FROM sinex_schemas.definition')
        sql("UPDATE sinex_schemas.definition SET fingerprint='unrecognized'")
        failure=schema(binary,None,'--apply','--dump-file',WORK/'unknown.dump',expected=1)
        assert 'unknown installed' in failure.stderr and not (WORK/'unknown.dump').exists()
        sql("UPDATE sinex_schemas.definition SET fingerprint='"+before+"'")
        sql('ALTER TABLE derivation.shell_dirty RENAME COLUMN parent_id TO damaged_parent')
        schema(binary,None,'--apply','--dump-file',WORK/'damaged.dump',expected=1)
        assert not (WORK/'damaged.dump').exists()
        sql('ALTER TABLE derivation.shell_dirty RENAME COLUMN damaged_parent TO parent_id')
        schema(binary,None,'--check')
    scenario('unknown definition and missing structure refuse before a dump or mutation',refuse_damage)
    def interrupt():
        out=(REPORT/'lock.stdout').open('w');err=(REPORT/'lock.stderr').open('w')
        locker=subprocess.Popen(['psql',admin,'-XAt','-v','ON_ERROR_STOP=1'],stdin=subprocess.PIPE,stdout=out,stderr=err,text=True,env=ENV)
        CHILDREN.append((locker,out,err))
        locker.stdin.write("BEGIN; LOCK derivation.shell_dirty IN SHARE MODE; SELECT pg_sleep(900);\n");locker.stdin.flush()
        until=time.monotonic()+15
        while sql("SELECT count(*) FROM pg_locks WHERE relation='derivation.shell_dirty'::regclass AND mode='ShareLock' AND granted")!='1':
            assert time.monotonic()<until;time.sleep(.05)
        upgrading=launch([binary,'admin','schema','--connection-file',WORK/'admin','--apply','--dump-file',WORK/'first.dump'],'interrupted-admin')
        observed=wait(lambda:sql("SELECT CASE WHEN to_regclass('sinex_schemas.applied_changes') IS NULL THEN false ELSE EXISTS(SELECT 1 FROM pg_locks WHERE relation='derivation.shell_dirty'::regclass AND mode='AccessExclusiveLock' AND NOT granted) END")=='t',upgrading,'real second entry waiting after first commit')
        progress=sql('SELECT json_agg(number ORDER BY number) FROM sinex_schemas.applied_changes');assert json.loads(progress)==[1]
        save('observed-interruption.json',{'pid':upgrading.pid,'committed_progress':json.loads(progress),'waiting_on_actual_dirty_table_alter':observed,'signal':'SIGKILL'})
        upgrading.kill();upgrading.wait(timeout=15);assert upgrading.returncode==-signal.SIGKILL
        # Terminate the actual blocker backend and client, rather than waiting on its sleep.
        sql("SELECT pg_terminate_backend(pid) FROM pg_locks WHERE relation='derivation.shell_dirty'::regclass AND mode='ShareLock' AND granted AND pid<>pg_backend_pid()")
        locker.wait(timeout=15)
        assert snapshot()==baseline_rows
        status=schema(binary,None,'--check');assert [c['number'] for c in status['pending_changes']]==[2]
        sql("UPDATE sinex_schemas.applied_changes SET number=2 WHERE number=1")
        failure=schema(binary,None,'--apply','--dump-file',WORK/'wrong-order.dump',expected=1)
        assert 'exact ordered prefix' in failure.stderr and not (WORK/'wrong-order.dump').exists()
        sql("UPDATE sinex_schemas.applied_changes SET number=1 WHERE number=2")
        status=schema(binary,None,'--apply','--dump-file',WORK/'resume.dump');assert status['pending_changes']==[]
        assert snapshot()==baseline_rows
        assert json.loads(sql("SELECT to_jsonb(p) FROM sinex_schemas.payload_schemas p WHERE id=3"))==old_payload
        registered=json.loads(payload_catalog());assert [(r['id'],r['version']) for r in registered]==[(2,'atuin-v1'),(3,'shell-command-v1-single-source'),(4,'zsh-extended-v1'),(5,'shell-command-v1-atuin-zsh')]
        save('upgraded-payload-catalog.json',registered)
        assert int(sql('SELECT count(*) FROM derivation.shell_dirty'))==2
        assert sql('SELECT bool_and(dirty_seq=(SELECT seq FROM core.commit_counter WHERE one)) FROM derivation.shell_dirty')=='t'
        run(['pg_restore','--list',WORK/'first.dump'],label='complete pre-mutation custom dump')
    scenario('observed committed real entry survives SIGKILL and ordinary explicit apply resumes exact prefix',interrupt)
    def fresh_and_repeat():
        run(['createdb','fresh']);fresh=admin.replace('/upgrade?','/fresh?');(WORK/'fresh-admin').write_text(fresh);(WORK/'fresh-admin').chmod(0o600)
        created=schema(binary,WORK/'fresh-admin');assert created['pending_changes']==[]
        assert sql('SELECT fingerprint,catalog FROM sinex_schemas.definition')==sql('SELECT fingerprint,catalog FROM sinex_schemas.definition',url=fresh)
        before=sql('SELECT fingerprint,catalog FROM sinex_schemas.definition')
        repeated=schema(binary,None,'--apply','--dump-file',WORK/'no-op.dump');assert repeated['pending_changes']==[] and not (WORK/'no-op.dump').exists()
        assert sql('SELECT fingerprint,catalog FROM sinex_schemas.definition')==before and snapshot()==baseline_rows
        sql('UPDATE sinex_schemas.applied_changes SET number=99 WHERE number=1',url=reader,expected=1)
        sql('UPDATE derivation.shell_progress SET basis=1',url=reader,expected=1)
        sql('ALTER TABLE raw.inputs ADD COLUMN forbidden integer',url=reader,expected=1)
        assert payload_catalog()==sql("SELECT jsonb_agg(to_jsonb(p) ORDER BY id) FROM sinex_schemas.payload_schemas p",url=fresh)
        columns=sql("SELECT string_agg(quote_ident(attname),',' ORDER BY attnum) FROM pg_attribute WHERE attrelid='reflection.events'::regclass AND attnum>0 AND NOT attisdropped AND attgenerated=''")
        projected=','.join('(r).'+c for c in columns.split(','))
        parents=[row['id'] for row in baseline['rows']]
        for schema_id,ids in [(3,parents),(5,parents+[parents[0]])]:
            patch=json.dumps({'source_event_ids':ids,'payload_schema_id':schema_id})
            statement="WITH source AS (SELECT jsonb_populate_record(null::reflection.events,to_jsonb(e)||'"+patch+"'::jsonb||jsonb_build_object('id',uuidv7())) AS r FROM core.events e WHERE event_type='command.canonical' LIMIT 1) INSERT INTO reflection.events("+columns+") SELECT "+projected+" FROM source"
            refused=run(['psql',admin,'-XAt','-v','ON_ERROR_STOP=1','-c',statement],expected=1,label=f'actual schema {schema_id} parent-cardinality refusal')
            assert 'registered_shell_payload' in refused.stderr
        assert snapshot()==baseline_rows
        save('fresh-and-upgraded-schema.json',created)
    scenario('fresh/upgraded definition catalogs match repeat is no-op and restricted reader cannot mutate',fresh_and_repeat)
    def after():
        configure(False);proc=launch([binary,'serve','--config',config],'upgraded-runtime')
        status=wait(lambda:(lambda v:v if v and v['writer']['state']=='owned' else None)(rpc('status').get('result')),proc,'upgraded normal writer')
        # New canonical derivation can replace old singleton interpretations; original raw IDs and bytes remain fixed.
        answer=cli('query',raw_query);assert answer['rows']==baseline['rows']
        for row in baseline['rows']:
            traced=cli('trace',row['id'],'--bytes')['row'];assert traced['record']['command']==row['payload']['command'];save('trace-'+row['payload']['native_id']+'.json',traced)
        wait(lambda:(lambda v:v and v.get('fusion',{}).get('current_now_settled') and v.get('total')==2)(rpc('query.run',{'query':'events where kind = command.canonical'}).get('result')),proc,'upgraded canonical recomputation')
        assert sql("SELECT bool_and(payload_schema_id=5) FROM core.live_events WHERE event_type='command.canonical'")=='t'
        assert sql("SELECT count(*) FROM core.events WHERE event_type='command.canonical' AND payload_schema_id=3")=='2'
        assert json.loads(sql("SELECT to_jsonb(p) FROM sinex_schemas.payload_schemas p WHERE id=3"))==old_payload
        save('upgraded-status.json',status);save('upgraded-raw.json',answer);stop(proc)
        # Actual new acquisition proves writer admission, without relying on relabelled retained parser metadata.
        with sqlite3.connect(db) as con:con.execute('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',('native-three',1700000002123456789,1000000000,0,'echo newly admitted','/synthetic/schema','session','schema-host',None))
        configure(True);proc=launch([binary,'serve','--config',config],'upgraded-admission')
        value=wait(lambda:(lambda v:v if v and v.get('total')==3 else None)(rpc('query.run',{'query':raw_query}).get('result')),proc,'real post-upgrade acquisition')
        assert any(r['payload']['native_id']=='native-three' and r['payload']['command']=='echo newly admitted' for r in value['rows'])
        stop(proc)
    scenario('upgraded actual writer queries retained traces canonical recomputation and later admission work',after)
    success=True
except Exception as error:
    failure=str(error);(REPORT/'failure.txt').write_text(traceback.format_exc())
finally:
    for child,out,err in CHILDREN:
        if child.poll() is None:
            child.kill()
            try:child.wait(timeout=15)
            except subprocess.TimeoutExpired:pass
        out.close();err.close()
    if started:
        p=subprocess.run(['pg_ctl','-D',str(WORK/'pg'),'-m','immediate','-w','stop'],env=ENV,capture_output=True,text=True)
        (REPORT/'pg-stop.stdout').write_text(p.stdout);(REPORT/'pg-stop.stderr').write_text(p.stderr)
        if p.returncode!=0:success=False;failure=(failure or '')+' PostgreSQL stop failed'
    retained_state=str(WORK)
    try:
        artifacts=REPORT/'owned-artifacts';artifacts.mkdir()
        for path in WORK.glob('*.dump*'):
            if path.is_file():shutil.copy2(path,artifacts/path.name)
        if (WORK/'blobs').is_dir():shutil.copytree(WORK/'blobs',artifacts/'retained-store')
        if (WORK/'config.toml').is_file():shutil.copy2(WORK/'config.toml',artifacts/'config.toml')
        if success:
            shutil.rmtree(WORK);retained_state='removed after private artifact preservation'
    except Exception as error:
        success=False;failure=(failure or '')+' private artifact preservation failed: '+str(error)
    save('result.json',{'success':success,'failure':failure,'scenarios':SCENARIOS,'commands':COMMANDS,'retained_owned_state':retained_state,'report':str(REPORT)})
    print(json.dumps({'success':success,'report':str(REPORT),'failure':failure}))
    if not success:sys_exit=1
    else:sys_exit=0
    raise SystemExit(sys_exit)
