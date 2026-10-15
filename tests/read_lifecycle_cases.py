"""Actual packaged clients and database resources under an owned PostgreSQL lock."""
import concurrent.futures
import contextlib
import json
import signal
import socket
import subprocess
import time
import urllib.request


def exercise(h):
    work, report, env = h['WORK'], h['REPORT'], h['ENV']
    config = work/'read.toml'
    port = h['free_port']()
    endpoint = f'http://127.0.0.1:{port}/rpc'
    config.write_text(f'''listen = "127.0.0.1:{port}"
state_dir = "{work/'read-state'}"
store_dir = "{work/'read-store'}"
writer_connection_file = "{work/'writer'}"
reader_connection_file = "{work/'reader'}"
token_file = "{work/'token'}"
sources = []
automata = []
''')
    daemon = h['start_daemon'](config, 'read-lifecycle')
    observed = (report/'read-lifecycle-observations.jsonl').open('w')
    results=[]
    def sql(text):
        p = subprocess.run(['psql', h['admin'], '-XAt', '-v', 'ON_ERROR_STOP=1', '-c', text], env=env, capture_output=True, text=True, timeout=5)
        assert p.returncode == 0, p.stderr
        return json.loads(p.stdout)
    def activity():
        result=sql("SELECT coalesce(json_agg(json_build_object('pid',pid,'state',state,'xact_start',xact_start,'backend_xmin',backend_xmin,'wait_event_type',wait_event_type,'query',query)), '[]') FROM pg_stat_activity WHERE usename='sinex_reader'")
        observed.write(json.dumps({'at':time.monotonic(),'activity':result})+'\n'); observed.flush()
        return result
    def wait(predicate, seconds=10):
        deadline=time.monotonic()+seconds
        while True:
            assert daemon.poll() is None, 'owned daemon exited'
            result=activity()
            if predicate(result): return result
            assert time.monotonic()<deadline, 'expected PostgreSQL resource state not reached'
            time.sleep(.025)
    def blocked(rows):
        return [r for r in rows if r['state']=='active' and r['wait_event_type']=='Lock' and 'WITH matching' in r['query']]
    def released(pids):
        return wait(lambda rows:all(r['pid'] not in pids for r in rows),seconds=3)
    deadline=time.monotonic()+20
    while True:
        try:
            if 'result' in h['rpc'](endpoint, 'synthetic-foundation-token'): break
        except (OSError, ValueError): pass
        assert time.monotonic()<deadline
        time.sleep(.025)

    @contextlib.contextmanager
    def table_lock():
        # Real acquired lock, not a delay or a handler/test injection.
        args=['psql', h['admin'], '-XAt', '-v', 'ON_ERROR_STOP=1']
        lock=subprocess.Popen(args,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        h['CHILDREN'].append((lock,lock.stdout,lock.stderr))
        h['PROCESSES'].append({'pid':lock.pid,'argv':args,'stdout':'owned-lock.stdout','stderr':'owned-lock.stderr'})
        lock.stdin.write("BEGIN; LOCK core.events IN ACCESS EXCLUSIVE MODE; SELECT 'locked';\n"); lock.stdin.flush()
        assert [lock.stdout.readline().strip() for _ in range(3)]==['BEGIN','LOCK TABLE','locked']
        try: yield
        finally:
            if lock.poll() is None:
                lock.stdin.write('ROLLBACK;\n\\q\n'); lock.stdin.flush(); lock.wait(timeout=5)
            assert lock.returncode==0
            with (report/'owned-lock.stdout').open('a') as f: f.write('BEGIN\nLOCK TABLE\nlocked\n'+lock.stdout.read())
            with (report/'owned-lock.stderr').open('a') as f: f.write(lock.stderr.read())

    def launch(name, timeout=None):
        args=[str(h['BIN_DIR']/'sinexctl'),'--endpoint',endpoint,'--token-file',str(work/'token'),'query','events','--json']
        if timeout is not None: args+=['--query-timeout-ms',str(timeout)]
        out=(report/(name+'.stdout')).open('w'); err=(report/(name+'.stderr')).open('w')
        cli=subprocess.Popen(args,env=env,stdout=out,stderr=err)
        h['CHILDREN'].append((cli,out,err))
        h['PROCESSES'].append({'pid':cli.pid,'argv':args,'stdout':name+'.stdout','stderr':name+'.stderr'})
        return cli
    def api(params):
        request=urllib.request.Request(endpoint,data=json.dumps({'jsonrpc':'2.0','id':7,'method':'query.run','params':params}).encode(),headers={'Authorization':'Bearer synthetic-foundation-token','Content-Type':'application/json'})
        with urllib.request.urlopen(request,timeout=15) as response:
            raw=response.read()
            (report/'api-deadline.json').write_bytes(raw)
            return json.loads(raw)
    def budget(answer, timeout_ms):
        assert answer['kind']=='budget'
        details=answer.get('details',answer.get('data'))
        assert details['reason']=='execution_deadline' and details['timeout_ms']==timeout_ms
        assert details['estimate'] is None and details['suggestion']
        assert all(key not in answer for key in ['result','rows','total','page','complete'])
    def recovery():
        answer=h['run']([h['BIN_DIR']/'sinexctl','--endpoint',endpoint,'--token-file',work/'token','query','events','--json'],label='successful read after cancellation')
        result=json.loads(answer.stdout)
        assert result['complete'] is True and result['total']==0 and result['rows']==[]
        assert result['page']['cursor'] is None and result['source_status']==[]
        wait(lambda rows:all(r['xact_start'] is None and r['backend_xmin'] is None for r in rows))

    def disconnect():
        with table_lock():
            for n in range(3):
                cli=launch(f'disconnect-{n}')
                rows=wait(lambda rows:len(blocked(rows))==1)
                read=blocked(rows)[0]
                assert read['xact_start'] and read['backend_xmin']
                started=time.monotonic()
                cli.send_signal(signal.SIGKILL); cli.wait(timeout=5)
                released({read['pid']})
                assert (report/f'disconnect-{n}.stdout').read_text()==''
                results.append({'case':'disconnect','client_pid':cli.pid,'backend_pid':read['pid'],'released_seconds':time.monotonic()-started})
            # Closing an authenticated HTTP request itself uses the same lifecycle.
            raw=json.dumps({'jsonrpc':'2.0','id':8,'method':'query.run','params':{'query':'events'}}).encode()
            connection=socket.create_connection(('127.0.0.1',port),timeout=5)
            try:
                connection.sendall(f'POST /rpc HTTP/1.1\r\nHost: 127.0.0.1\r\nAuthorization: Bearer synthetic-foundation-token\r\nContent-Type: application/json\r\nContent-Length: {len(raw)}\r\n\r\n'.encode()+raw)
                rows=wait(lambda rows:len(blocked(rows))==1)
                pid=blocked(rows)[0]['pid']
            finally: connection.close()
            released({pid})
        recovery()
    h['scenario']('disconnected actual client releases blocked snapshot', disconnect)

    def api_deadline():
        with table_lock(), concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            start=time.monotonic()
            future=executor.submit(api,{'query':'events'})
            rows=wait(lambda rows:len(blocked(rows))==1)
            pid=blocked(rows)[0]['pid']
            reply=future.result(timeout=13)
            elapsed=time.monotonic()-start
            assert reply['id']==7 and 'result' not in reply
            budget(reply['error'],10_000)
            assert 9<=elapsed<13
            released({pid})
            results.append({'case':'api_default','timeout_ms':10_000,'elapsed_seconds':elapsed,'backend_pid':pid})
        recovery()
    h['scenario']('API default deadline returns budget and releases snapshot',api_deadline)

    def cli_deadline(timeout=None):
        name='cli-default' if timeout is None else 'cli-configured'
        milliseconds=60_000 if timeout is None else timeout
        with table_lock():
            start=time.monotonic()
            cli=launch(name,timeout)
            rows=wait(lambda rows:len(blocked(rows))==1)
            pid=blocked(rows)[0]['pid']
            cli.wait(timeout=milliseconds/1000+5)
            elapsed=time.monotonic()-start
            assert cli.returncode==6, f'actual CLI deadline exit {cli.returncode}'
            assert (report/(name+'.stdout')).read_text()==''
            budget(json.loads((report/(name+'.stderr')).read_text()),milliseconds)
            assert milliseconds/1000*.85<=elapsed<milliseconds/1000+4
            released({pid})
            results.append({'case':name,'timeout_ms':milliseconds,'elapsed_seconds':elapsed,'backend_pid':pid})
        recovery()
    h['scenario']('CLI default deadline returns budget and releases snapshot',cli_deadline)
    h['scenario']('CLI configured deadline returns budget and releases snapshot',lambda:cli_deadline(500))

    def exhausted():
        with table_lock():
            clients=[launch(f'saturated-{n}') for n in range(2)]
            try:
                rows=wait(lambda rows:len(blocked(rows))==2)
                pids={r['pid'] for r in blocked(rows)}
                cli=launch('queued-deadline',300)
                cli.wait(timeout=4)
                assert cli.returncode==6
                budget(json.loads((report/'queued-deadline.stderr').read_text()),300)
                assert len(blocked(activity()))==2
                for client in clients: client.kill(); client.wait(timeout=5)
                released(pids)
            finally:
                for client in clients:
                    if client.poll() is None: client.kill(); client.wait(timeout=5)
        recovery()
    h['scenario']('queued deadline and concurrent disconnect reclaim both reader slots',exhausted)

    for value in [0,3_600_001]:
        reply=api({'query':'events','timeout_ms':value})
        assert reply['error']['kind']=='bad_query' and 'result' not in reply
    h['stop'](daemon)
    observed.close()
    (report/'read-lifecycle.json').write_text(json.dumps(results,indent=2)+'\n')
