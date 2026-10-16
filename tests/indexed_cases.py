"""Independent synthetic shell facts, actual public clients and owned process controls."""
import contextlib
import concurrent.futures
import json
import os
from pathlib import Path
import select
import signal
import socket
import sqlite3
import subprocess
import threading
import time
import urllib.request


def exercise(h):
    work,report,env=h['WORK'],h['REPORT'],h['ENV']
    run,scenario,start,stop=h['run'],h['scenario'],h['start_daemon'],h['stop']
    bins=h['BIN_DIR'];port=h['free_port']();endpoint=f'http://127.0.0.1:{port}/rpc'
    token='synthetic-foundation-token'
    read_model=h['reader'].replace('sinex_reader','sinex_read_model')
    (work/'read-model').write_text(read_model)
    (work/'operator').write_text('synthetic-replay-operator')
    db=work/'history.db';config=work/'indexed.toml'
    epoch=1_789_400_000_123_456_789
    fixture=[]
    def row(key,n,command,cwd='/synthetic/tide',exit_code=0,deleted=None):
        return (key,epoch+n*1_000_000_000,1_000_000_000,exit_code,command,cwd,'synthetic-session','synthetic-host',deleted)
    fixture=[row('old-best',0,'needle needle needle needle needle'),row('phrase',1,'cargo test needle'),row('words',2,'cargo verbose test needle'),row('reverse',3,'test cargo needle'),row('unicode',4,'echo Straße CAFE\u0301 ὈΔΥΣΣΕΎΣ'),row('plural',5,'echo offsets'),row('singular',6,'echo offset'),row('prefix',7,'echo offsetting'),row('nested',8,'needle nested','/synthetic/tide/nested'),row('path-neighbor',9,'needle neighbor','/synthetic/tidepool'),row('path-case',10,'needle case','/synthetic/Tide'),row('failure',11,'needle failed',exit_code=9),row('unknown',12,'needle running',exit_code=-1),row('secret',13,'echo ghp_'+'A'*36+' needle'),row('tie-a',14,'needle tie'),row('tie-b',14,'needle tie'),row('unicode-path',15,'needle unicodepath','/synthetic/zażółć'),row('unicode-neighbor',16,'needle neighborpath','/synthetic/zażółćdom'),row('long-url',17,'echo '+'z'*100+' https://u:'+'B'*150+'@host needle'),row('unsafe-program',18,'https://user:synthetic-secret@host')]
    fixture += [row(f'common-{n:03}',20+n,'needle') for n in range(120)]
    with sqlite3.connect(db) as con:
        con.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER NOT NULL,duration INTEGER NOT NULL,exit INTEGER NOT NULL,command TEXT NOT NULL,cwd TEXT NOT NULL,session TEXT NOT NULL,hostname TEXT NOT NULL,deleted_at INTEGER)')
        con.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)',fixture)
    (report/'independent-search-fixture.json').write_text(json.dumps(fixture,ensure_ascii=False,indent=2))
    def configuration(source=True,nested='nested',poll=500,roots=None):
        return f'''listen = "127.0.0.1:{port}"
state_dir = "{work/'indexed-state'}"
store_dir = "{work/'indexed-store'}"
writer_connection_file = "{work/'writer'}"
reader_connection_file = "{work/'reader'}"
read_model_connection_file = "{work/'read-model'}"
token_file = "{work/'token'}"
operator_token_file = "{work/'operator'}"
sources = {json.dumps(['terminal.atuin-history'] if source else [])}
automata = []
[[projects]]
name = "tide"
roots = {json.dumps(roots or ["/synthetic/tide"])}
[[projects]]
name = "{nested}"
roots = ["/synthetic/tide/nested"]
'''+(f'''[atuin]
path = "{db}"
poll_interval_ms = {poll}
''' if source else '')
    config.write_text(configuration(poll=30000))
    def profile():
        (work/'wrong-profile.toml').write_text(config.read_text().replace(str(work/'read-model'),str(work/'reader')))
        (report/'wrong-profile-config.toml').write_text((work/'wrong-profile.toml').read_text())
        rejected=run([bins/'sinexd','serve','--config',work/'wrong-profile.toml'],expected=1,label='read-model profile refuses reader role');assert 'read-model role or projection unavailable' in rejected.stderr
        run(['createdb','projection_other'],label='isolated different-store profile fixture')
        other_admin=h['admin'].replace('/foundation?','/projection_other?');(work/'other-admin').write_text(other_admin)
        run([bins/'sinexd','admin','schema','--connection-file',work/'other-admin'],label='isolated other-store schema')
        (work/'wrong-model').write_text(read_model.replace('/foundation?','/projection_other?'))
        (work/'wrong-store.toml').write_text(config.read_text().replace(str(work/'read-model'),str(work/'wrong-model')))
        (report/'wrong-store-config.toml').write_text((work/'wrong-store.toml').read_text())
        rejected=run([bins/'sinexd','serve','--config',work/'wrong-store.toml'],expected=1,label='read-model profile refuses different store');assert 'read-model role or projection unavailable' in rejected.stderr
    scenario('restricted profile and store binding',profile)
    daemon=start(config,'indexed-initial')
    observations=(report/'indexed-observations.jsonl').open('w')
    api_log=(report/'actual-api.jsonl').open('w');api_lock=threading.Lock()
    def api(method,params={},credential=token,name=None,timeout=20):
        body=json.dumps({'jsonrpc':'2.0','id':7,'method':method,'params':params}).encode()
        request=urllib.request.Request(endpoint,data=body,headers={'Authorization':'Bearer '+credential,'Content-Type':'application/json'})
        with urllib.request.urlopen(request,timeout=timeout) as response: answer=json.load(response)
        with api_lock:api_log.write(json.dumps({"method":method,"params":params,"answer":answer},ensure_ascii=False)+'\n');api_log.flush()
        if name:(report/(name+'.json')).write_text(json.dumps(answer,ensure_ascii=False,indent=2))
        return answer
    def wait(predicate,seconds=30):
        until=time.monotonic()+seconds
        while True:
            assert daemon.poll() is None,'owned indexed daemon exited; inspect stderr'
            try:
                value=api('status');observations.write(json.dumps(value)+'\n');observations.flush()
                if 'result' in value and predicate(value['result']):return value['result']
            except (OSError,ValueError):pass
            assert time.monotonic()<until,'indexed state did not reach expected frontier'
            time.sleep(.025)
    def settled():
        return wait(lambda s:s['indexed_retrieval'].get('serving') and not s['indexed_retrieval']['pending_canonical'] and s['indexed_retrieval']['position']==s['commit_seq'] and all(v['last_run']['status']=='completed' for v in api('sources.list')['result']['sources']))
    def cli(command,*args,expected=0,operator=False):
        result=run([bins/'sinexctl','--endpoint',endpoint,'--token-file',work/('operator' if operator else 'token'),command,*args,'--json'],expected=expected,label='indexed CLI '+command)
        return json.loads(result.stdout or result.stderr)
    def query(text,page=100,cursor=None):
        result=api('query.run',{'query':text,'page':page,'cursor':cursor})
        deadline=time.monotonic()+10
        while 'result' not in result and result.get('error',{}).get('kind')=='partial':
            assert time.monotonic()<deadline,result
            time.sleep(.025);result=api('query.run',{'query':text,'page':page,'cursor':cursor})
        assert 'result' in result,result
        return result['result']
    def all_rows(text):
        first=query(text);rows=first['rows'];cursor=first['page']['cursor']
        while cursor:
            page=query(text,cursor=cursor);rows+=page['rows'];cursor=page['page']['cursor']
        return first,rows
    def keys(rows):return [r['payload']['native_id'] for r in rows]
    baseline=None
    def facts():
        nonlocal baseline
        settled()
        answer,rows=all_rows('needle')
        expected=[r[0] for r in fixture if 'needle' in r[4]]
        assert answer['total']==len(expected) and set(keys(rows))==set(expected)
        assert keys(rows)[0]=='old-best','ranking substituted a newest-match subset'
        assert len({r['id'] for r in rows})==len(expected)
        assert all(r['kind']=='command.canonical' and r['origin']=='derived' and len(r['parents'])==1 for r in rows)
        assert answer['resolved']['projection_seq']==answer['resolved']['commit_seq'] and 'unknown' in answer['coverage']
        assert set(keys(query('"cargo test"')['rows']))=={'phrase'}
        assert set(keys(query('cargo test')['rows']))=={'phrase','words','reverse'}
        assert query('commands where cwd ^ /')['total']==len(fixture)
        assert keys(query('STRASSE')['rows'])==['unicode']
        assert keys(query('stras*')['rows'])==['unicode']
        ties=query('commands where command ~ tie')['rows'];assert len(ties)==2 and [r['id'] for r in ties]==sorted(r['id'] for r in ties)
        assert keys(query('CAFÉ')['rows'])==['unicode']
        assert keys(query('ὀδυσσεύσ')['rows'])==['unicode']
        assert keys(query('offset')['rows'])==['singular']
        assert keys(query('commands where needle and cwd ^ /synthetic/zażółć')['rows'])==['unicode-path']
        assert set(keys(query('commands where command ~ offset')['rows']))=={'singular','plural','prefix'}
        unsafe=query('commands where command ^ "https://"')['rows'][0];assert unsafe['payload']['canonical'] is None and 'synthetic-secret' not in json.dumps(unsafe)
        long_url=query('commands where needle and command ~ "https://"')['rows'][0];assert 'B'*30 not in json.dumps(long_url) and long_url['snippet']['highlights_unavailable']=='masked'
        assert set(keys(query('offset*')['rows']))=={'singular','plural','prefix'}
        assert set(keys(query('commands where needle and project = nested')['rows']))=={'nested'}
        assert 'nested' not in keys(query('commands where needle and project = tide')['rows'])
        assert 'path-neighbor' not in keys(query('commands where needle and cwd ^ /synthetic/tide')['rows'])
        assert 'path-case' not in keys(query('commands where needle and cwd ^ /synthetic/tide')['rows'])
        assert keys(query('commands where needle and exit_code != 0')['rows'])==['failure']
        assert set(keys(query('commands where needle and (project = nested or exit_code = 9)')['rows']))=={'nested','failure'}
        assert query('commands where needle and source = a and source = b')['total']==0
        when=next(r for r in rows if r['payload']['native_id']=='phrase')['when']
        assert keys(query(f'commands where needle and when >= {when} and when <= {when}')['rows'])==['phrase']
        assert query(f'commands where needle and when > {when} and when < {when}')['total']==0
        ordered=query('needle sort when desc limit 5');assert keys(ordered['rows'])==[f'common-{n:03}' for n in range(119,114,-1)]
        for row in rows:
            snippet=row['snippet'];raw=snippet['text'].encode()
            for a,b in snippet['spans']:assert raw[a:b].decode().casefold()=='needle'
        for text in ['commands where typo = x','commands near (events) within 1m','recall now','commands where exit_code = bad','commands where source = terminal.*','commands where project = [tide,nested]','comands since 7d','RECALL now','commands where cwd "exists"','commands where command "=" "git status"']:
            response=api('query.run',{'query':text,'page':20});assert response['error']['kind']=='bad_query' and 'result' not in response,response
        baseline={r['payload']['native_id']:r for r in rows}
        (report/'independent-search-expectation.json').write_text(json.dumps({'needle_keys':expected,'top':'old-best','phrase':['phrase'],'words':['phrase','words','reverse'],'unicode':['unicode'],'exact':['singular'],'prefix':['singular','plural','prefix']},indent=2))
    scenario('indexed fixture, independent full-population ranking and semantic controls',facts)

    mcp=None;mcp_log=[];mcp_id=1
    def mcp_start(name):
        nonlocal mcp
        args=[str(bins/'sinexctl'),'--endpoint',endpoint,'--token-file',str(work/'token'),'mcp']
        err=(report/(name+'.stderr')).open('w');mcp=subprocess.Popen(args,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,text=True,bufsize=1)
        h['CHILDREN'].append((mcp,mcp.stdout,err));h['PROCESSES'].append({'pid':mcp.pid,'argv':args,'stderr':name+'.stderr'})
        initialize=mcp_call('initialize',{'protocolVersion':'2025-03-26','capabilities':{},'clientInfo':{'name':'independent-native-qualification','version':'1'}})
        assert 'result' in initialize,initialize
        mcp.stdin.write(json.dumps({'jsonrpc':'2.0','method':'notifications/initialized'})+'\n');mcp.stdin.flush()
    def mcp_call(method,params):
        nonlocal mcp_id
        request={'jsonrpc':'2.0','id':mcp_id,'method':method,'params':params};mcp_id+=1
        mcp.stdin.write(json.dumps(request)+'\n');mcp.stdin.flush();mcp_log.append({'sent':request})
        deadline=time.monotonic()+20
        while True:
            ready,_,_=select.select([mcp.stdout],[],[],max(0,deadline-time.monotonic()));assert ready,'actual rmcp response deadline'
            line=mcp.stdout.readline();assert line,'actual rmcp transport ended'
            reply=json.loads(line);mcp_log.append({'received':reply})
            if reply.get('id')==request['id']:return reply
    def tool(name,args):
        response=mcp_call('tools/call',{'name':name,'arguments':args})
        result=response['result'];assert not result.get('isError',False),response
        return result.get('structuredContent') or json.loads(result['content'][0]['text'])
    def clients():
        nonlocal daemon
        stop(daemon);config.write_text(configuration(source=False));daemon=start(config,'stable-client-basis');settled()
        mcp_start('indexed-mcp')
        listed=mcp_call('tools/list',{})['result']['tools'];assert {t['name'] for t in listed}=={'sinex_search','sinex_query','sinex_show','sinex_trace','sinex_sources','sinex_status'}
        api_answer=query('needle',20)
        shell_answer=cli('search','needle','--page','20')
        search_answer=api('search',{'query':'needle','page':20},name='api-indexed-search')['result']
        agent_answer=tool('sinex_search',{'words':'needle','page':20})
        agent_query=tool('sinex_query',{'query':'needle','page':20})
        for actual in [shell_answer,search_answer,agent_answer,agent_query]:
            assert actual['rows']==api_answer['rows']
            assert actual['total']==api_answer['total'] and actual['coverage']==api_answer['coverage'] and actual['complete']==api_answer['complete']
        credential_query='commands where command ~ "ghp_" or command ~ "https://"'
        credentials=query(credential_query,20)
        assert tool('sinex_query',{'query':credential_query,'page':20})['rows']==credentials['rows']
        assert cli('query',credential_query,'--page','20')['rows']==credentials['rows']
        assert tool('sinex_search',{'words':'needle','query':'and command ~ "https://"'})['rows']==query('needle and command ~ "https://"',20)['rows']
        assert 'B'*30 not in json.dumps(credentials) and 'synthetic-secret' not in json.dumps(credentials)
        assert keys(cli('search','cargo','test')['rows'])==keys(query('cargo test')['rows'])
        assert keys(cli('search','cargo test')['rows'])==['phrase']
        ref='sinex:ev/'+api_answer['rows'][0]['id']
        assert tool('sinex_show',{'ref':ref})['row']==cli('show',ref)['row']
        assert tool('sinex_trace',{'ref':ref})['row']==cli('trace',ref)['row']
        assert tool('sinex_trace',{'ref':ref})['row']['parents'][0]['material']
        assert tool('sinex_status',{})['indexed_retrieval']['configured']
        (report/'actual-mcp-transport.json').write_text(json.dumps(mcp_log,ensure_ascii=False,indent=2))
    scenario('actual MCP transport and cross-client identity',clients)

    def project_boundaries():
        nonlocal daemon
        stop(daemon)
        config.write_text(configuration(source=False,roots=['/synthetic/tide*','/synthetic/tide*']))
        daemon=start(config,'project-wildcard-base');settled()
        old='commands where command = "needle needle needle needle needle"'
        assert query(old)['rows'][0]['project']=='tide'
        assert query('commands where needle and project = nested')['rows'][0]['payload']['native_id']=='nested'
        assert query('commands where command = "cargo test needle"')['rows'][0]['payload']['native_id']=='phrase'
        assert query('commands where cwd = "exists"')['total']==0
        stop(daemon)
        config.write_text(config.read_text()+'\n[[projects]]\nname = "other"\nroots = ["/synthetic/t*"]\n')
        daemon=start(config,'project-component-tie');settled()
        assert query(old)['rows'][0]['project'] is None
        assert query('commands where needle and project = nested')['rows'][0]['payload']['native_id']=='nested'
        stop(daemon)
        config.write_text(configuration(source=False,roots=['/synthetic/tide/*']).replace('roots = ["/synthetic/tide/nested"]','roots = ["/elsewhere"]'))
        daemon=start(config,'project-wildcard-present-component');settled()
        assert query(old)['rows'][0]['project'] is None
        assert query('commands where command = "needle nested"')['rows'][0]['project']=='tide'
        stop(daemon);config.write_text(configuration(source=False))
        daemon=start(config,'project-boundaries-restored');settled()
    scenario('project component-depth ambiguity and wildcard boundary',project_boundaries)

    def sql(text):
        result=run(['psql',h['admin'],'-XAtq','-v','ON_ERROR_STOP=1','-c',text],label='indexed qualification database observation')
        return json.loads(result.stdout)
    def reset_projection():
        run(['psql',read_model,'-XAtq','-v','ON_ERROR_STOP=1','-c','DELETE FROM core.search_text; DELETE FROM core.event_project; DELETE FROM derivation.read_model_position'],label='restricted read-model fixture reconstruction')
    def reconstruction():
        nonlocal daemon
        # A restricted model rebuild discards only its own rows/progress. It has no
        # event admission/archive privileges. The actual daemon rebuilds from log.
        parent=baseline['phrase']['parents'][0]
        material=cli('show',parent)['row']['material']
        replay=cli('replay','materials where id = '+material,operator=True)
        assert replay['state']=='completed' and replay['processed']==len(fixture)
        settled()
        reminted=all_rows('needle')[1]
        assert all(r['id']!=baseline[r['payload']['native_id']]['id'] for r in reminted)
        baseline.clear();baseline.update({r['payload']['native_id']:r for r in reminted})
        stop(daemon)
        config.write_text(configuration())
        reset_projection()
        boundary=work/'projection.sock'
        with socket.socket(socket.AF_UNIX) as controller:
            controller.bind(str(boundary));controller.listen(1);controller.settimeout(20)
            daemon=start(config,'projection-held','--qualification-boundary','projection-staged-1','--qualification-socket',boundary)
            conn,_=controller.accept()
            with conn:
                frame=json.loads(conn.makefile('r').readline());assert frame['pid']==daemon.pid and frame['evidence']['through']>frame['evidence']['from']
                (report/'projection-boundary.json').write_text(json.dumps(frame,indent=2))
                answer=api('search',{'query':'needle'});assert answer['error']['kind']=='partial' and 'result' not in answer
                with sqlite3.connect(db) as con:
                    con.execute("UPDATE history SET command='needle boundary updated' WHERE id='common-119'")
                    con.execute("UPDATE history SET deleted_at=? WHERE id='common-118'",(epoch,))
                deadline=time.monotonic()+20
                while True:
                    raw=api('query.run',{'query':'events where payload.command = "needle boundary updated"'})
                    if raw.get('result',{}).get('total')==1:break
                    assert time.monotonic()<deadline,'source replacement did not commit while projection was held';time.sleep(.025)
                daemon.kill();daemon.wait(timeout=10)
        daemon=start(config,'projection-resumed');settled()
        answer,rows=all_rows('needle');assert set(keys(rows))==set(baseline)-{'common-118'}
        assert {k:r['id'] for k,r in baseline.items() if k not in ('common-118','common-119')}=={r['payload']['native_id']:r['id'] for r in rows if r['payload']['native_id']!='common-119'}
        assert next(r for r in rows if r['payload']['native_id']=='common-119')['id']!=baseline['common-119']['id']
        capabilities=sql("SELECT json_build_object('canonical',(SELECT count(*) FROM core.live_events WHERE event_type='command.canonical'),'search',(SELECT count(*) FROM core.search_text),'writer_can_search',has_table_privilege('sinex_writer','core.search_text','INSERT'),'model_can_admit',has_table_privilege('sinex_read_model','core.events','INSERT'),'model_can_archive',has_table_privilege('sinex_read_model','audit.archived','INSERT'))")
        assert capabilities['canonical']==capabilities['search'] and not any(capabilities[key] for key in ['writer_can_search','model_can_admit','model_can_archive'])
    scenario('projection interruption and same-state reconstruction',reconstruction)

    def changes():
        nonlocal daemon
        with sqlite3.connect(db) as con:
            con.execute("UPDATE history SET command='needle corrected current' WHERE id='phrase'")
            con.execute("UPDATE history SET deleted_at=? WHERE id='words'",(epoch,))
        wait(lambda s:s['commit_seq']>s['indexed_retrieval'].get('position',0) or s['commit_seq']>0)
        deadline=time.monotonic()+20
        while True:
            settled();reply=api('query.run',{'query':'"cargo test"','page':100})
            if reply.get('result',{}).get('total')==0 and api('query.run',{'query':'"corrected current"','page':100}).get('result',{}).get('total')==1:break
            assert time.monotonic()<deadline;time.sleep(.05)
        assert 'words' not in keys(all_rows('needle')[1])
        corrected=query('"corrected current"')['rows'][0];assert corrected['id']!=baseline['phrase']['id']
        # Changing project labels reconstructs projections without reminting events.
        before={r['id'] for r in all_rows('needle')[1]};old_cursor=query('needle',3)['page']['cursor'];stop(daemon)
        config.write_text(configuration(nested='changed'));daemon=start(config,'project-list-rebuild');settled()
        assert {r['id'] for r in all_rows('needle')[1]}==before
        assert keys(query('commands where needle and project = changed')['rows'])==['nested']
        assert api('query.run',{'query':'needle','page':3,'cursor':old_cursor})['error']['kind']=='bad_query'
        expected={r['payload']['native_id']:r['id'] for r in all_rows('needle')[1]}
        stop(daemon);reset_projection()
        daemon=start(config,'fresh-log-reconstruction');settled()
        assert {r['payload']['native_id']:r['id'] for r in all_rows('needle')[1]}==expected
    scenario('source replacement removal replay and project rebuild',changes)

    def retained():
        nonlocal daemon
        selected=query('"corrected current"')['rows'][0];ref='sinex:ev/'+selected['id'];before=cli('trace',ref,'--bytes')
        stop(daemon);db.unlink();config.write_text(configuration(source=True,nested='changed'))
        daemon=start(config,'provider-absent-restart');wait(lambda s:s['indexed_retrieval'].get('serving') and not s['indexed_retrieval']['pending_canonical'] and s['indexed_retrieval']['position']==s['commit_seq'] and all(v['last_run']['status']=='unavailable' for v in api('sources.list')['result']['sources']))
        retained_answer=query('"corrected current"');assert retained_answer['rows'][0]['id']==selected['id'] and retained_answer['complete'] is False and not db.exists()
        assert cli('trace',ref,'--bytes')['row']==before['row']
        assert tool('sinex_search',{'words':'"corrected current"'})['rows'][0]['id']==selected['id']
        assert tool('sinex_trace',{'ref':ref})['row']['parents'][0]['material']==before['row']['parents'][0]['material']
    scenario('retained navigation without provider after restart',retained)

    @contextlib.contextmanager
    def locked(relation="core.search_text"):
        args=['psql',h['admin'],'-XAt','-v','ON_ERROR_STOP=1'];p=subprocess.Popen(args,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        h['CHILDREN'].append((p,p.stdout,p.stderr));h['PROCESSES'].append({'pid':p.pid,'argv':args})
        p.stdin.write(f"BEGIN; LOCK {relation} IN ACCESS EXCLUSIVE MODE; SELECT 'locked';\n");p.stdin.flush()
        assert [p.stdout.readline().strip() for _ in range(3)]==['BEGIN','LOCK TABLE','locked']
        try:yield
        finally:
            p.stdin.write('ROLLBACK;\n\\q\n');p.stdin.flush();p.wait(timeout=5);assert p.returncode==0
    def lifecycle():
        nonlocal daemon
        stop(daemon);config.write_text(configuration(source=False,nested='changed'));daemon=start(config,'stable-read-lifecycle');wait(lambda s:s['indexed_retrieval'].get('serving') and not s['indexed_retrieval']['pending_canonical'] and s['indexed_retrieval']['position']==s['commit_seq'])
        secret=query('needle and command ~ "ghp_"')['rows'][0]
        assert 'ghp_' not in secret['payload']['command'] and 'ghp_' not in secret['snippet']['text'] and secret['secret'] is True
        assert secret['snippet']['spans']==[] and secret['snippet']['highlights_unavailable']=='masked'
        with locked():
            response=api('search',{'query':'needle','timeout_ms':300});assert response['error']['kind']=='budget' and 'result' not in response
            args=[str(bins/'sinexctl'),'--endpoint',endpoint,'--token-file',str(work/'token'),'search','needle']
            out=(report/'disconnected-indexed.stdout').open('w');err=(report/'disconnected-indexed.stderr').open('w')
            cli_process=subprocess.Popen(args,env=env,stdout=out,stderr=err);h['CHILDREN'].append((cli_process,out,err));h['PROCESSES'].append({'pid':cli_process.pid,'argv':args})
            deadline=time.monotonic()+5
            while True:
                active=sql("SELECT coalesce(json_agg(json_build_object('pid',pid,'wait',wait_event_type)),'[]') FROM pg_stat_activity WHERE usename='sinex_reader' AND state='active' AND query LIKE 'WITH matching%'")
                if active:break
                assert time.monotonic()<deadline;time.sleep(.025)
            pids={r['pid'] for r in active};cli_process.kill();cli_process.wait(timeout=5)
            while True:
                remaining=sql('SELECT coalesce(json_agg(pid),\'[]\') FROM pg_stat_activity WHERE pid IN ('+','.join(map(str,pids))+')')
                if not remaining:break
                assert time.monotonic()<deadline;time.sleep(.025)
        with locked(),concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            pending=mcp_id
            future=executor.submit(mcp_call,'tools/call',{'name':'sinex_search','arguments':{'words':'needle'}})
            deadline=time.monotonic()+5
            while True:
                active=sql("SELECT coalesce(json_agg(pid),'[]') FROM pg_stat_activity WHERE usename='sinex_reader' AND state='active' AND query LIKE 'WITH matching%'")
                if active:break
                assert time.monotonic()<deadline;time.sleep(.025)
            cancel={'jsonrpc':'2.0','method':'notifications/cancelled','params':{'requestId':pending,'reason':'qualification cancellation'}}
            mcp.stdin.write(json.dumps(cancel)+'\n');mcp.stdin.flush();mcp_log.append({'sent':cancel})
            reply=future.result(timeout=5)
            assert 'error' in reply or reply.get('result',{}).get('isError'),reply
            while True:
                remaining=sql('SELECT coalesce(json_agg(pid),\'[]\') FROM pg_stat_activity WHERE pid IN ('+','.join(map(str,active))+')')
                if not remaining:break
                assert time.monotonic()<deadline;time.sleep(.025)
        reference='sinex:ev/'+query('"corrected current"')['rows'][0]['id']
        for name,relation,arguments in [('sinex_show','core.events',{'ref':reference}),('sinex_trace','core.events',{'ref':reference}),('sinex_sources','raw.inputs',{}),('sinex_status','core.events',{})]:
            with locked(relation),concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                pending=mcp_id;future=executor.submit(mcp_call,'tools/call',{'name':name,'arguments':arguments})
                deadline=time.monotonic()+5
                while True:
                    active=sql("SELECT coalesce(json_agg(pid),'[]') FROM pg_stat_activity WHERE usename='sinex_reader' AND state='active' AND wait_event_type='Lock'")
                    if active:break
                    assert time.monotonic()<deadline;time.sleep(.025)
                cancel={'jsonrpc':'2.0','method':'notifications/cancelled','params':{'requestId':pending,'reason':'qualification cancellation'}}
                mcp.stdin.write(json.dumps(cancel)+'\n');mcp.stdin.flush();mcp_log.append({'sent':cancel})
                reply=future.result(timeout=5);assert 'error' in reply or reply.get('result',{}).get('isError'),reply
                while True:
                    remaining=sql('SELECT coalesce(json_agg(pid),\'[]\') FROM pg_stat_activity WHERE pid IN ('+','.join(map(str,active))+')')
                    if not remaining:break
                    assert time.monotonic()<deadline;time.sleep(.025)
        with locked('core.events'):
            reply=api('object.show',{'ref':reference},timeout=15)
            assert reply['error']['kind']=='budget' and 'result' not in reply
        # Missing retained log authority cannot be advertised as complete empty.
        stop(daemon)
        sql("DELETE FROM core.search_text; DELETE FROM core.event_project; DELETE FROM derivation.read_model_position; DELETE FROM core.commit_log WHERE event_type_id=3 AND change=0; SELECT '{}'::json")
        replacement=start(config,'missing-log-refusal')
        deadline=time.monotonic()+10
        while True:
            try:
                status=api('status')
                if status.get('result',{}).get('indexed_retrieval',{}).get('failure'):
                    response=api('search',{'query':'needle'})
                    assert response.get('error',{}).get('kind')=='partial',response
                    break
            except OSError:pass
            assert time.monotonic()<deadline;time.sleep(.025)
        assert 'result' not in response
        stop(replacement)
    scenario('indexed deadlines cancellation and credential presentation',lifecycle)
    if mcp and mcp.poll() is None:mcp.stdin.close();mcp.wait(timeout=5)
    (report/'actual-mcp-transport.json').write_text(json.dumps(mcp_log,ensure_ascii=False,indent=2))
    observations.close();api_log.close()
