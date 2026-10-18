"""Signed provider status, retained v2 failures, and current recipe controls."""
import hashlib
import json
from pathlib import Path
import sqlite3
import time
import urllib.request

PLANNED = [
    'typed prior recipe preserves source hashes reading order and stale precedence',
    'published stage-five failures retain source bytes before guarded schema-six upgrade',
    'current replay recovers signed rows without fabricating acquisition receipts',
    'signed exact query show trace and canonical fields survive repeated reads',
    'open late signed correction restart provider-loss and privacy preserve evidence',
]


def exercise(h):
    work, report, env, bins = (h[k] for k in ['WORK','REPORT','ENV','BIN_DIR'])
    run, scenario, start, stop = (h[k] for k in ['run','scenario','start_daemon','stop'])
    pin=json.loads(h['OPTIONS'].atuin_exits_predecessor_pin.read_text())
    old=Path(pin['package'])/'bin/sinexd'
    assert pin['exit_code']==0 and not pin['dirty'] and old.parent.parent.parent==Path('/nix/store')
    assert hashlib.sha256(old.read_bytes()).hexdigest()==pin['binaries']['sinexd']['sha256']
    (report/'signed-predecessor-pin.json').write_text(json.dumps(pin,indent=2))
    run(['createdb','signed_exits'],label='owned signed status database')
    for k in ['admin','writer','reader']:
        (work/('signed-'+k)).write_text(h[k].replace('/foundation?','/signed_exits?'))
    (work/'signed-read-model').write_text((work/'signed-reader').read_text().replace('sinex_reader','sinex_read_model'))
    (work/'signed-operator').write_text('synthetic-signed-operator')
    for k in ['admin','writer','reader','read-model','operator']:(work/('signed-'+k)).chmod(0o600)
    zsh=work/'signed.zsh';zsh.write_text(': '+str(1_759_276_802)+':0;echo signed 1\n')
    source=work/'signed.db';config=work/'signed.toml';endpoint=f'http://127.0.0.1:{h["free_port"]()}/rpc';token=(work/'token').read_text()
    columns=['id','timestamp','duration','exit','command','cwd','session','hostname','deleted_at']
    statuses=[-(2**63),-2,-1,0,1,2**63-1]
    epoch=1_759_276_800_000_000_000
    facts=[(f'status-{i}',epoch+i*2_000_000_000,7,x,'echo signed '+str(i),'/synthetic/signed','s','h:user',None) for i,x in enumerate(statuses)]
    facts += [('open',epoch+20_000_000_000,-1,-1,'echo open','/synthetic/signed','s','h:user',None),('signed-no-duration',epoch+22_000_000_000,-1,-2,'echo no duration','/synthetic/signed','s','h:user',None)]
    with sqlite3.connect(source) as c:
        c.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT,deleted_at INTEGER)')
        c.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',facts)
    def configure(acquire=True,privacy=''):
        config.write_text(f'''listen="{endpoint.removeprefix('http://').removesuffix('/rpc')}"
state_dir="{work/'signed-state'}"
store_dir="{work/'signed-store'}"
writer_connection_file="{work/'signed-writer'}"
reader_connection_file="{work/'signed-reader'}"
read_model_connection_file="{work/'signed-read-model'}"
token_file="{work/'token'}"
operator_token_file="{work/'signed-operator'}"
sources={json.dumps(['terminal.atuin-history','terminal.zsh-history'] if acquire else [])}
automata=[]
[[projects]]
name="signed"
roots=["/synthetic/signed"]
'''+(f'''[atuin]
path="{source}"
poll_interval_ms=500
full_interval_ms=5000
[zsh]
path="{zsh}"
execution_host="h"
protocol="append_only_extended_v1"
poll_interval_ms=500
''' if acquire else '')+privacy)
    configure();daemon=None
    def save(name,value):(report/(name+'.json')).write_text(json.dumps(value,indent=2))
    def sql(text):return run(['psql',(work/'signed-admin').read_text(),'-XAtq','-v','ON_ERROR_STOP=1','-c',text],label='signed independent SQL receipt').stdout.strip()
    def api(method,params=None):
        req=urllib.request.Request(endpoint,data=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params or {}}).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
        with urllib.request.urlopen(req,timeout=15) as response:value=json.load(response)
        with (report/'signed-rpc.jsonl').open('a') as log:log.write(json.dumps({'method':method,'params':params,'response':value})+'\n')
        return value
    def cli(command,*args,expected=(0,5),operator=False):
        r=run([bins/'sinexctl','--endpoint',endpoint,'--token-file',work/('signed-operator' if operator else 'token'),command,*args,'--json'],expected=expected,label='signed public '+command)
        return json.loads(r.stdout or r.stderr)
    def wait(label,fn):
        until=time.monotonic()+120;last=None
        while time.monotonic()<until:
            assert daemon.poll() is None,label+' daemon exited'
            try:
                last=fn()
                if last:return last
            except (OSError,KeyError):pass
            time.sleep(.08)
        raise AssertionError(label+'; last='+repr(last))
    def raw():return api('query.run',{'query':'events where source = terminal.atuin-history'}).get('result')
    def ready(n):
        q=raw()
        return q if q and q['total']==n and q['fusion']['current_now_settled'] else None
    def schema(binary,*args,expected=0):
        r=run([binary,'admin','schema','--connection-file',work/'signed-admin',*args],expected=expected,label='signed guarded schema')
        return json.loads(r.stdout) if r.returncode==0 else r
    refs={};materials=[]
    try:
        def prior_recipe():
            run(['createdb','signed_recipe'],label='owned typed prior recipe database')
            isolated=dict(env)
            for field,role in [('ADMIN','admin'),('WRITER','writer'),('READER','reader')]:
                isolated['SINEX_TEST_'+field+'_URL']=h[role].replace('/foundation?','/signed_recipe?')
            isolated['SINEX_TEST_STORE_DIR']=str(work/'signed-recipe-store')
            run(['cargo','test','--locked','-p','sinex-store','--features','product','--lib','atuin::tests::new_parser_readmits_unchanged_row_and_keeps_old_event','--','--ignored','--nocapture'],env=isolated,label='typed prior recipe equality repeat and stale control')
        scenario(PLANNED[0],prior_recipe)
        def upgrade():
            nonlocal daemon,materials
            schema(old);daemon=start(config,'signed-v2',binary=old)
            wait('v2 admits only supported statuses',lambda:ready(5))
            obs=wait('v2 retains three negative omissions',lambda:(lambda s:s if s['last_run']['status']=='completed' and s['last_run']['record_errors']==3 else None)(api('sources.list')['result']['sources'][0]))
            stop(daemon)
            materials=json.loads(sql("SELECT json_agg(id) FROM raw.source_material_registry WHERE observation_id='"+obs['last_run']['id']+"'::uuid AND durable_bytes>0"))
            assert materials
            pending=schema(bins/'sinexd','--check');assert [x['number'] for x in pending['pending_changes']]==[6,7],pending
            applied=schema(bins/'sinexd','--apply','--dump-file',work/'signed-before-six.dump')
            assert applied['checked_changes']==[{'number':6,'outcome':'applied'},{'number':7,'outcome':'applied'}],applied
            for field in ['fingerprint','catalog']:
                cold=run(['psql',h['admin'],'-XAtq','-v','ON_ERROR_STOP=1','-c','SELECT '+field+' FROM sinex_schemas.definition'],label='signed canonical cold catalog').stdout.strip()
                assert sql('SELECT '+field+' FROM sinex_schemas.definition')==cold,field
            rejected=run([old,'serve','--config',config],expected=1,label='old v2 writer refuses stage-six catalog');assert 'schema' in rejected.stderr
            save('signed-guarded-upgrade',{'omissions':obs,'materials':materials,'pending':pending,'applied':applied})
        scenario(PLANNED[1],upgrade)
        def replay():
            nonlocal daemon
            configure(False);daemon=start(config,'signed-v3-retained')
            wait('retained v2 source view',lambda:ready(5))
            for material in materials:
                outcome=cli('replay','materials where id = '+material,operator=True);assert outcome['state']=='completed',outcome
            q=wait('v3 retained signed recovery',lambda:ready(8))
            negatives=[r for r in q['rows'] if r['payload']['exit_code'] is not None and r['payload']['exit_code']<0]
            assert len(negatives)==3,negatives
            assert int(sql("SELECT count(*) FROM core.live_events WHERE source='terminal.atuin-history' AND (payload->>'exit_code')::numeric<0 AND created_by_operation IS NOT NULL"))==3
            # Replaying an old reading cannot claim a new acquisition success.
            obs=api('sources.list')['result']['sources'][0]
            assert obs['last_run']['record_errors']==3
            save('signed-retained-reprocessing',{'rows':q,'source':obs})
            stop(daemon);configure();daemon=start(config,'signed-v3-current')
            wait('actual v3 full resolves current source errors',lambda:(lambda s:s if s['last_run']['status']=='completed' and s['last_run']['record_errors']==0 else None)(api('sources.list')['result']['sources'][0]))
        scenario(PLANNED[2],replay)
        def values():
            q=wait('signed current values',lambda:ready(8));by={r['payload']['native_id']:r for r in q['rows']}
            for key,stamp,duration,status,command,*_ in facts:
                row=by[key];p=row['payload'];expected=None if status==-1 else status
                assert p['exit_code']==expected and p['duration_ns']==(None if duration==-1 else duration),p
                assert p['completion']==('end_not_recorded' if duration==-1 and status==-1 else 'ended'),p
                ref='sinex:ev/'+row['id'];shown=cli('show',ref);trace=cli('trace',ref,'--bytes')
                assert shown['row']['payload']['exit_code']==expected
                assert trace['row']['material'] and trace['row']['end']>trace['row']['anchor']
                refs[key]=(ref,trace)
            for status in statuses:
                if status==-1:continue
                result=cli('query','events where source = terminal.atuin-history and payload.exit_code = '+str(status))
                assert all(r['payload']['exit_code']==status for r in result['rows'])
                assert result['total']==(2 if status==-2 else 1),result
                indexed=cli('query','commands where project = signed and exit_code = '+str(status))
                assert indexed['total']==result['total'],indexed
                assert all(r['payload']['exit_code']==status for r in indexed['rows']),indexed
                if status==-2:
                    paired=next(r for r in indexed['rows'] if r['payload']['command']=='echo signed 1')
                    paired_trace=cli('trace','sinex:ev/'+paired['id'])
                    assert len(paired_trace['row']['parents'])==2,paired_trace
                    assert paired['payload']['observed_by']==['terminal.atuin-history','terminal.zsh-history'],paired
            before={k:r['id'] for k,r in by.items()};time.sleep(1.1)
            assert {r['payload']['native_id']:r['id'] for r in raw()['rows']}==before
            assert cli('query','commands where project = signed and exit_code = 0')['total']==1
            save('signed-public-values',{'query':q,'refs':refs})
        scenario(PLANNED[3],values)
        def late():
            nonlocal daemon
            with sqlite3.connect(source) as c:c.execute("UPDATE history SET exit=-2 WHERE id='open'")
            changed=wait('old open ordinary late signed end',lambda:next((r for r in raw()['rows'] if r['payload']['native_id']=='open' and r['payload']['exit_code']==-2),None))
            assert changed['id']!=refs['open'][0].split('/')[-1] and changed['payload']['completion']=='ended' and changed['payload']['duration_ns'] is None
            trace=cli('trace',refs['open'][0],'--bytes')
            for field in ['material','input','material_observation','anchor','end','representation','record']:
                assert trace['row'][field]==refs['open'][1]['row'][field],{'field':field,'reason':'late correction changed original retained citation'}
            assert trace['resolved']=='archived',trace
            full=wait('actual full after late correction',lambda:(lambda s:s if s['last_run']['status']=='completed' and s['last_run']['rows_read']==8 and s['last_run']['record_errors']==0 else None)(api('sources.list')['result']['sources'][0]))
            current_material=sql("SELECT id FROM raw.source_material_registry WHERE observation_id='"+full['last_run']['id']+"'::uuid AND durable_bytes>0 ORDER BY id LIMIT 1")
            assert current_material
            before=raw();stop(daemon);daemon=start(config,'signed-v3-restart');wait('restart same eight facts',lambda:ready(8))
            assert raw()['rows']==before['rows']
            source.rename(work/'signed-held.db');stop(daemon);daemon=start(config,'signed-v3-unavailable')
            wait('actual provider unavailable',lambda:any(s['last_run']['status']=='unavailable' for s in api('sources.list')['result']['sources']))
            after=cli('query','events where source = terminal.atuin-history',expected=5)
            assert after['rows']==before['rows'] and after['complete'] is False
            assert cli('trace',refs['status-0'][0],'--bytes')==refs['status-0'][1]
            save('signed-late-retained',{'changed':changed,'before':before,'unavailable':after})
            negative=next(r for r in after['rows'] if r['payload']['native_id']=='status-1')
            ref='sinex:ev/'+negative['id'];material=current_material
            cli('delete',ref,operator=True);wait('signed exact removal',lambda:ready(7))
            cli('replay','materials where id = '+material,operator=True)
            assert raw()['total']==7,'replay bypassed exact occurrence exclusion'
            wait('excluded replay projection settles',lambda:ready(7))
            cli('delete','--undo',ref,operator=True)
            cli('replay','materials where id = '+material,operator=True);wait('undo replay signed restoration',lambda:ready(8))
            restored=next(r for r in raw()['rows'] if r['payload']['native_id']=='status-1')
            assert restored['payload']['exit_code']==-2
            protected='[[privacy_rules]]\npattern="echo"\nreplacement="[protected]"\ncreated_by="operator"\n'
            stop(daemon);configure(False,protected);daemon=start(config,'signed-v3-current-privacy')
            wait('privacy restart retained signed facts',lambda:ready(8))
            protected_trace=cli('trace','sinex:ev/'+restored['id'],'--bytes')
            assert protected_trace['row']['record']['command']=='[protected] signed 1',protected_trace
            assert cli('show','sinex:ev/'+restored['id'])['row']['payload']['exit_code']==-2
            save('signed-privacy-removal',{'restored':restored,'protected_trace':protected_trace})
        scenario(PLANNED[4],late)
    finally:
        if daemon is not None:stop(daemon)
