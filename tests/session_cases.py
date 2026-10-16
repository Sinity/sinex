"""Independent finite facts; no product grouping, parser or query binder is the oracle."""
import hashlib
import json
import select
import socket
import sqlite3
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
import urllib.request

PLANNED = ['actual current published stage-2 predecessor and guarded session upgrade',
    'two initial sessions and project-scoped CLI MCP recall',
    'late bridge replaces both sessions with exact retained parents',
    'true logical-day boundary, late extension and reordered fresh baseline',
    'compatible restart and provider loss retain exact answers']
COLUMNS=['id','timestamp','duration','exit','command','cwd','session','hostname','deleted_at']
PROJECT='/synthetic/session/tide'
DAY='2026-10-13'

def stamp(clock):
    return int(datetime.fromisoformat(DAY+'T'+clock+'+02:00').timestamp())*1_000_000_000+123

def fixture():
    rows=[]
    for prefix,hour,first in [('a',3,30),('b',4,4)]:
        for n in range(7):
            minute=first+n*4
            rows.append(dict(zip(COLUMNS,[f'{prefix}{n}',stamp(f'{hour:02}:{minute:02}:00'),0,
                101 if n==1 else 0,f'cargo test shell-session {prefix}{n}',PROJECT,
                'synthetic-shell','synthetic-host:user',None])))
    for n,clock in enumerate(['05:00:00','05:04:00']):
        rows.append(dict(zip(COLUMNS,[f'd{n}',stamp(clock),0,0,f'echo distractor {n}',PROJECT+'-neighbor',
            'synthetic-shell','synthetic-host:user',None])))
    return rows

def bridge():
    return dict(zip(COLUMNS,['bridge',stamp('03:59:00'),0,0,'cargo test shell-session late bridge',PROJECT,'synthetic-shell','synthetic-host:user',None]))

def extension():
    return dict(zip(COLUMNS,['extension',stamp('04:32:00'),-1,-1,'sleep shell-session unknown end',PROJECT,'synthetic-shell','synthetic-host:user',None]))

def independent(rows):
    """Recorded intervals grouped once; no endpoint becomes another occurrence."""
    groups=[]
    for row in sorted(rows,key=lambda r:(r['timestamp'],r['id'])):
        start=row['timestamp'];end=start+max(0,row['duration'])
        if not groups or start-groups[-1]['end_ns']>300_000_000_000:
            groups.append({'start_ns':start,'end_ns':end,'parents':set()})
        groups[-1]['end_ns']=max(groups[-1]['end_ns'],end);groups[-1]['parents'].add(row['id'])
    return [dict(g,parents=sorted(g['parents'])) for g in groups]

def exercise(h):
    work,report,env,bins=(h[k] for k in ['WORK','REPORT','ENV','BIN_DIR'])
    run,scenario,start,stop=(h[k] for k in ['run','scenario','start_daemon','stop'])
    token=(work/'token').read_text();(work/'session-operator-token').write_text('synthetic-session-operator-token');(work/'session-operator-token').chmod(0o600);facts=fixture();source=work/'sessions.db';procs=[]
    observations=[];metrics=[];mcp=None;mid=0
    def save(name,value): (report/name).write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
    def sql(text,url=None):return run(['psql',url or h['admin'],'-XAt','-v','ON_ERROR_STOP=1','-c',text],label='session isolated database observation').stdout.strip()
    def fill(rows,replace=False):
        con=sqlite3.connect(source)
        if replace:con.execute('DELETE FROM history')
        con.executemany('INSERT OR REPLACE INTO history VALUES(?,?,?,?,?,?,?,?,?)',[[r[c] for c in COLUMNS] for r in rows]);con.commit();con.close()
        save('independent-inputs.json',facts)
    con=sqlite3.connect(source);con.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER NOT NULL,duration INTEGER NOT NULL,exit INTEGER NOT NULL,command TEXT NOT NULL,cwd TEXT NOT NULL,session TEXT NOT NULL,hostname TEXT NOT NULL,deleted_at INTEGER)');con.commit();con.close();fill(facts)
    pin=json.loads(h['OPTIONS'].session_predecessor_pin.read_text());old=Path(pin['package'])
    assert pin['exit_code']==0 and pin['dirty'] is False and old.parent==Path('/nix/store')
    for name in ['sinexd','sinexctl']:
        assert hashlib.sha256((old/'bin'/name).read_bytes()).hexdigest()==pin['binaries'][name]['sha256']
    # Refuse to silently treat a different runtime as the current published predecessor.
    changed=subprocess.check_output(['git','diff','--name-only',pin.get('package_candidate',pin['candidate']),'88a7a641de4517307a0e723e40ecac8ff50aa7d7'],cwd=h['ROOT'],text=True).splitlines()
    assert all(p.startswith(('tests/','docs/')) or p in ['README.md','.agentctl/project.toml'] for p in changed),changed
    save('predecessor-binding.json',dict(pin=pin,published_base='88a7a641de4517307a0e723e40ecac8ff50aa7d7',harness_docs_only_difference=changed))
    run(['createdb','session_predecessor'])
    predadmin=h['admin'].replace('/foundation?','/session_predecessor?');predwriter=h['writer'].replace('/foundation?','/session_predecessor?');predreader=h['reader'].replace('/foundation?','/session_predecessor?')
    for name,value in [('session-admin',predadmin),('session-writer',predwriter),('session-reader',predreader),('session-read-model',predreader.replace('sinex_reader','sinex_read_model')),('baseline-read-model',h['reader'].replace('sinex_reader','sinex_read_model'))]:
        (work/name).write_text(value);(work/name).chmod(0o600)
    endpoint=f'http://127.0.0.1:{h["free_port"]()}/rpc'
    def config(name,session=True,baseline=False):
        p=work/(name+'.toml');p.write_text(f'''listen = "{endpoint.removeprefix('http://').removesuffix('/rpc')}"
state_dir = "{work/('baseline-state' if baseline else 'session-state')}"
store_dir = "{work/('baseline-blobs' if baseline else 'session-blobs')}"
writer_connection_file = "{work/('writer' if baseline else 'session-writer')}"
reader_connection_file = "{work/('reader' if baseline else 'session-reader')}"
operator_token_file = "{work/'session-operator-token'}"
read_model_connection_file = "{work/('baseline-read-model' if baseline else 'session-read-model')}"
token_file = "{work/'token'}"
sources = ["terminal.atuin-history"]
automata = {('["session"]' if session else '[]')}
[[projects]]
name = "tide"
roots = ["{PROJECT}"]
[[projects]]
name = "large"
roots = ["/synthetic/session/large"]
[[projects]]
name = "long"
roots = ["/synthetic/session/long"]
[atuin]
path = "{source}"
poll_interval_ms = 500
''');return p
    def api(method='status',params=None):
        req=urllib.request.Request(endpoint,data=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params or {}}).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
        with urllib.request.urlopen(req,timeout=5) as r:value=json.load(r)
        observations.append({'method':method,'params':params,'response':value});return value
    def wait(proc,fn,label,seconds=90):
        until=time.monotonic()+seconds
        while time.monotonic()<until:
            assert proc.poll() is None,f'{label}: daemon exited {proc.returncode}'
            try:
                value=fn()
                if value:return value
            except (OSError,KeyError,ValueError):pass
            time.sleep(.05)
        save('rpc-observations.json',observations);raise AssertionError(label+' did not settle')
    def settled(sessions=True):
        s=api().get('result',{});idx=s.get('indexed_retrieval',{})
        return idx and idx.get('serving') and idx.get('position')==s['commit_seq'] and idx.get('pending_canonical')==0 and (not sessions or idx.get('pending_sessions')==0) and idx.get('failure') is None and (not sessions or idx.get('sessions',{}).get('failure') is None)
    def cli(command,*args,expected=(0,5),binary=None):
        p=run([binary or bins/'sinexctl','--endpoint',endpoint,'--token-file',work/('session-operator-token' if command=='delete' else 'token'),command,*args,'--json'],expected=expected,label='session actual CLI '+command)
        return json.loads(p.stdout or p.stderr)
    def mcp_call(method,params):
        nonlocal mid
        mid+=1;request={'jsonrpc':'2.0','id':mid,'method':method,'params':params}
        mcp.stdin.write(json.dumps(request)+'\n');mcp.stdin.flush()
        until=time.monotonic()+15
        while True:
            ready,_,_=select.select([mcp.stdout],[],[],max(0,until-time.monotonic()));assert ready,'MCP deadline exceeded'
            line=mcp.stdout.readline();assert line,'MCP ended';response=json.loads(line)
            with (report/'session-mcp.jsonl').open('a') as log:log.write(json.dumps({'request':request,'response':response})+'\n')
            if response.get('id')==mid:return response
    def tool(name,arguments):
        result=mcp_call('tools/call',{'name':name,'arguments':arguments})['result'];assert not result.get('isError'),result
        return result.get('structuredContent') or json.loads(result['content'][0]['text'])
    def discover():
        nonlocal mcp
        cli('catalog');err=(report/'session-mcp.stderr').open('w')
        argv=[str(bins/'sinexctl'),'--endpoint',endpoint,'--token-file',str(work/'token'),'mcp']
        mcp=subprocess.Popen(argv,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,text=True,bufsize=1)
        h['CHILDREN'].append((mcp,mcp.stdout,err));h['PROCESSES'].append({'pid':mcp.pid,'argv':argv,'stderr':'session-mcp.stderr'})
        assert 'result' in mcp_call('initialize',{'protocolVersion':'2025-03-26','capabilities':{},'clientInfo':{'name':'session-recovery','version':'1'}})
        mcp.stdin.write(json.dumps({'jsonrpc':'2.0','method':'notifications/initialized'})+'\n');mcp.stdin.flush()
        tools=mcp_call('tools/list',{})['result']['tools'];assert {t['name'] for t in tools}=={'sinex_search','sinex_query','sinex_recall','sinex_show','sinex_trace','sinex_sources','sinex_status'}
        save('session-discovery.json',tools)
    def query_all(text):
        rows=[];cursor=None
        while True:
            answer=cli('query',text,*(['--cursor',cursor] if cursor else []));rows+=answer['rows'];cursor=answer.get('page',{}).get('cursor')
            if not cursor:return rows
    def raw():return query_all('events where kind = command.executed')
    def current():return cli('query','events where kind = activity.session.boundary')['rows']
    def qualify(label):
        began=time.monotonic();answer=cli('recall','last','--project','tide');elapsed=time.monotonic()-began
        agent=tool('sinex_recall',{'project':'tide'});assert answer['session']==agent['session'] and answer['rows']==agent['rows']
        expected=next(g for g in reversed(independent(facts)) if any(r['cwd']==PROJECT and r['id'] in g['parents'] for r in facts))
        rows=answer['rows'];names=sorted(r['payload']['native_id'] for r in rows)
        assert names==[n for n in expected['parents'] if next(r for r in facts if r['id']==n)['cwd']==PROJECT],(names,expected)
        header=answer['session'];assert int(header['payload']['start_ns'])==expected['start_ns'] and int(header['payload']['end_ns'])==expected['end_ns']
        assert header['payload']['continuity'] is None and header['payload']['afk'] is None and header['payload']['attention'] is None and header['payload']['active_s'] is None
        allrows=current();assert len(allrows)==len(independent(facts))
        rawrows=raw();rawmap={r['payload']['native_id']:r for r in rawrows}
        canonical=query_all('commands sort when limit 10000');canonmap={r['payload']['native_id']:r for r in canonical}
        actual=[]
        for group in allrows:
            assert len(group['parents'])==len(set(group['parents']))
            members=sorted(next(n for n,r in canonmap.items() if r['id']==p) for p in group['parents'])
            actual.append({'start_ns':int(group['payload']['start_ns']),'end_ns':int(group['payload']['end_ns']),'parents':members})
            for p in group['parents']:
                row=next(r for r in canonical if r['id']==p);assert row['parents']==[rawmap[row['payload']['native_id']]['id']]
        assert sorted(actual,key=lambda x:x['start_ns'])==independent(facts)
        assert sum(len(g['parents']) for g in actual)==len({p for g in actual for p in g['parents']})==len(facts),'one canonical occurrence must belong to exactly one shell grouping'
        if 'bridge' in names:assert header['payload']['owner_day']=='2026-10-12'
        trace=cli('trace','sinex:ev/'+header['id'],'--bytes')['row'];assert trace['representation']=='shell_session'
        assert sorted(x['event'] for x in trace['parents'])==sorted(header['parents'])
        for parent in trace['parents']:
            assert len(parent['parents'])==1
            original=parent['parents'][0];assert original['material'] and original['end']>original['anchor']
            fact=next(r for r in facts if r['id']==original['record']['id'])
            assert all(original['record'][c]==fact[c] for c in COLUMNS)
        assert header['payload']['sources']==['terminal.atuin-history']
        save(label+'.json',dict(answer=answer,MCP=agent,sessions=allrows,canonical=canonical,raw=rawrows,trace=trace,independent=independent(facts)))
        metrics.append(dict(phase=label,recall_seconds=elapsed,source_rows=len(facts),commands=len(rows),database_bytes=int(sql('SELECT pg_database_size(current_database())',predadmin)),retained_bytes=sum(p.stat().st_size for p in (work/'session-blobs').rglob('*') if p.is_file())))
        return answer
    def upgrade():
        run([old/'bin/sinexd','admin','schema','--connection-file',work/'session-admin'])
        proc=start(config('predecessor',False),'session-predecessor',binary=old/'bin/sinexd');procs.append(proc)
        wait(proc,lambda:api().get('result') and len(api('query.run',{'query':'events where kind = command.executed'}).get('result',{}).get('rows',[]))==len(facts) and settled(False),'published predecessor acquisition')
        before=raw();stop(proc)
        check=run([bins/'sinexd','admin','schema','--connection-file',work/'session-admin','--check']);assert [e['number'] for e in json.loads(check.stdout)['pending_changes']]==[3]
        dump=report/'stage2-preupgrade.dump'
        run([bins/'sinexd','admin','schema','--connection-file',work/'session-admin','--apply','--dump-file',dump])
        assert dump.stat().st_size>0 and dump.stat().st_mode&0o777==0o600
        run([bins/'sinexd','admin','schema','--connection-file',work/'session-admin','--check'])
        run([bins/'sinexd','admin','schema','--connection-file',work/'session-admin','--apply','--dump-file',report/'unused-repeat.dump']);assert not (report/'unused-repeat.dump').exists()
        after=json.loads(sql("SELECT jsonb_agg(jsonb_build_object('id',id,'payload',payload,'material',source_material_id,'anchor',anchor_byte) ORDER BY id) FROM core.events WHERE event_type='command.executed'",predadmin))
        assert {r['id'] for r in before}=={r['id'] for r in after}
        for r in after:
            b=next(x for x in before if x['id']==r['id']);assert all(r[k]==b[k] for k in ['payload','material','anchor'])
        save('upgrade-retention.json',dict(before=before,after=after,dump=str(dump.name)))
    scenario(PLANNED[0],upgrade)
    control=socket.socket(socket.AF_UNIX);control.bind(str(work/'session-boundary.sock'));control.listen(1);control.settimeout(30)
    proc=start(config('sessions'),'sessions-initial','--qualification-boundary','session-replacement-committed','--qualification-socket',work/'session-boundary.sock');procs.append(proc)
    connection,_=control.accept();connection.settimeout(5);frame=json.loads(connection.makefile('rb').readline());assert frame['pid']==proc.pid and frame['evidence']['pending']
    pending=cli('recall','last','--project','tide',expected=5);assert 'partial' in json.dumps(pending)
    raw_pending=cli('query','events where kind = activity.session.boundary',expected=5);assert not raw_pending['complete'] and raw_pending['sessions']['pending_keys']>0
    save('observed-pending-boundary.json',dict(frame=frame,recall=pending,raw=raw_pending,status=api()))
    connection.close();control.close();proc.kill();proc.wait(timeout=10);assert proc.returncode==-9
    proc=start(config('sessions-after-kill'),'sessions-after-publication-kill');procs.append(proc)
    def initial():
        wait(proc,lambda:settled(),'initial session reconciliation');discover();return qualify('initial')
    scenario(PLANNED[1],initial)
    first=qualify('initial-repeat');oldids={r['id'] for r in current()}
    def merge():
        began=time.monotonic();facts.append(bridge());fill([facts[-1]])
        wait(proc,lambda:len(raw())==len(facts) and settled(),'late bridging replacement')
        answer=qualify('bridged');assert answer['session']['id'] not in oldids
        live=current();assert len(live)==2 and not oldids.intersection({r['id'] for r in live})-set(r['id'] for r in live if int(r['payload']['start_ns'])==stamp('05:00:00'))
        metrics.append({'phase':'bridge-acquisition-to-recall','seconds':time.monotonic()-began})
    scenario(PLANNED[2],merge)
    def later():
        facts.append(extension());fill([facts[-1]]);wait(proc,lambda:len(raw())==len(facts) and settled(),'late extension true boundary');qualify('extended')
        stop(proc)
        fill(list(reversed(facts)),replace=True)
        baseline=start(config('baseline',baseline=True),'sessions-baseline');procs.append(baseline)
        wait(baseline,lambda:api().get('result') and len(raw())==len(facts) and settled(),'independent fresh reordered baseline')
        qualify('fresh-baseline')
        large=[]
        for n in range(107):
            row=dict(zip(COLUMNS,[f'large-{n:03}',stamp('06:00:00')+n*1_000_000_000,0,0,f'echo bounded recall {n}','/synthetic/session/large','synthetic-shell','synthetic-host:user',None]));large.append(row)
        long=[]
        for name,clock,duration,offset in [('long-solo','07:00:00',600,0),('long-outer','03:40:00',3000,86400),('long-nested','03:45:00',300,86400),('long-overlap','04:25:00',900,86400),('long-after','04:46:00',0,86400)]:
            long.append(dict(zip(COLUMNS,[name,stamp(clock)+offset*1_000_000_000,duration*1_000_000_000,0,'synthetic interval '+name,'/synthetic/session/long','synthetic-shell','synthetic-host:user',None])))
        facts.extend(large+long);fill(large+long)
        wait(baseline,lambda:len(raw())==len(facts) and settled(),'large-session refusal population')
        refused=cli('recall','last','--project','large',expected=5);assert refused['kind']=='partial' and 'assembly exceeds' in refused['message']
        supported=query_all('commands where project = large sort when limit 200')
        assert len(supported)==107 and len({r['id'] for r in supported})==107 and {r['payload']['native_id'] for r in supported}=={r['id'] for r in large}
        fallback=[]
        for row in [supported[0],supported[-1]]:
            shown=cli('show',row['id'])['row'];traced=cli('trace',row['id'],'--bytes')['row']
            assert shown['current_eligible'] and traced['parents'][0]['record']['id']==row['payload']['native_id']
            fallback.append(dict(show=shown,trace=traced))
        save('large-session-refusal-and-retained-inspection.json',dict(refused=refused,commands=supported,fallback=fallback))
        longcommands=query_all('commands where project = long sort when limit 100')
        longmap={r['payload']['native_id']:r['id'] for r in longcommands};longgroups=current()
        actual=[g for g in longgroups if set(g['parents']) & set(longmap.values())]
        solo=next(g for g in actual if longmap['long-solo'] in g['parents']);assert solo['parents']==[longmap['long-solo']] and int(solo['payload']['end_ns'])-int(solo['payload']['start_ns'])==600_000_000_000
        crossing=next(g for g in actual if longmap['long-outer'] in g['parents']);assert set(crossing['parents'])=={longmap[k] for k in ['long-outer','long-nested','long-overlap']} and crossing['payload']['owner_day']=='2026-10-13'
        assert int(crossing['payload']['start_ns'])==stamp('03:40:00')+86400_000_000_000 and int(crossing['payload']['end_ns'])==stamp('04:40:00')+86400_000_000_000
        assert len(actual)==3 and sum(len(g['parents']) for g in actual)==len(longmap)
        save('long-overlapping-nested-cross-day-controls.json',dict(inputs=long,commands=longcommands,groups=actual))
        # Exercise only the new session-grandparent fence through the existing
        # ordinary exclusion route, held at actual bounded fusion publication.
        original=qualify('large-population')
        oldsession=original['session']['id'];removed=next(r for r in raw() if r['payload']['native_id']=='a0')
        stop(baseline)
        fence=socket.socket(socket.AF_UNIX);fence.bind(str(work/'grandparent-fence.sock'));fence.listen(1);fence.settimeout(30)
        baseline=start(config('grandparent-fence',baseline=True),'sessions-grandparent-fence','--qualification-boundary','fusion-replacement-committed','--qualification-socket',work/'grandparent-fence.sock');procs.append(baseline)
        wait(baseline,lambda:api().get('result') and settled(),'grandparent fence restart')
        deleted=cli('delete',removed['id']);assert deleted
        connection,_=fence.accept();connection.settimeout(5);frame=json.loads(connection.makefile('rb').readline());assert frame['pid']==baseline.pid
        shown=cli('show',oldsession)['row'];assert shown['current_eligible'] is False
        pending=cli('query','events where kind = activity.session.boundary',expected=5)
        assert not pending['complete'] and oldsession not in {r['id'] for r in pending['rows']}
        refused=cli('recall','last','--project','tide',expected=5);assert refused['kind']=='partial'
        save('current-grandparent-fence.json',dict(frame=frame,delete=deleted,old_session=shown,raw_pending=pending,recall=refused))
        connection.sendall(b'c');connection.close();fence.close()
        wait(baseline,lambda:settled(),'excluded session replacement')
        excluded=cli('recall','last','--project','tide');assert 'a0' not in {r['payload']['native_id'] for r in excluded['rows']} and excluded['session']['id']!=oldsession
        assert int(excluded['session']['payload']['start_ns'])==stamp('03:34:00')
        save('excluded-session-replaced.json',excluded)
        cli('delete','--undo',removed['id'])
        wait(baseline,lambda:len(raw())==len(facts) and settled(),'undo and ordinary configured readmission')
        qualify('restored-after-grandparent-fence');stop(baseline)
    scenario(PLANNED[3],later)
    def retained():
        proc=start(config('restart'),'sessions-restart');procs.append(proc);wait(proc,lambda:api().get('result') and len(raw())==len(facts) and settled(),'same-state restart');before=qualify('restart')
        stop(proc);source.rename(work/'removed-provider.db')
        proc=start(config('provider-loss'),'sessions-provider-loss');procs.append(proc)
        wait(proc,lambda:api().get('result') and settled() and any(s.get('last_run',{}).get('status')=='unavailable' for s in api('sources.list').get('result',{}).get('sources',[])),'owned provider loss')
        # Retained answers remain available, but CLI exit 5 discloses degraded source.
        answer=cli('recall','last','--project','tide',expected=5)
        assert answer['rows']==before['rows'] and answer['session']==before['session'] and not answer['complete']
        save('provider-loss.json',answer);agent=tool('sinex_recall',{'project':'tide'});assert agent['rows']==answer['rows'] and agent['complete'] is False;stop(proc)
    scenario(PLANNED[4],retained)
    save('measurements.json',dict(samples=metrics,interventions=['seed synthetic provider','append bridge','append extension','restart same owned state','remove only synthetic provider','fresh independent reversed input baseline','observed publication kill and same-state recovery','large-session refusal and retained inspection','owned raw exclusion and undo at observed fusion fence'],limits=['finite shell-only fixture; no daily capacity claim','full frontier scan per dirty logical day; component-targeted efficiency unqualified','8192 exact parents per output and 100 commands per recall; excess refuses incomplete','unknown focus/AFK/attention; grouping is not real task or continuous activity proof']))
    if mcp is not None:mcp.terminate();mcp.wait(timeout=10)
    save('rpc-observations.json',observations)
