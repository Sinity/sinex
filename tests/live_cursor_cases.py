"""Independent synthetic fixture for the implemented raw-event continuation contract."""
import json
import sqlite3
import time


def exercise(h):
    work, report = h['WORK'], h['REPORT']
    run, scenario, start, stop, rpc = (h[k] for k in ['run', 'scenario', 'start_daemon', 'stop', 'rpc'])
    binaries = h['BIN_DIR']
    db = work/'cursor.db'
    epoch = 1_700_000_000_123_456_789
    # Each triple shares the exact nanosecond. A 100-row page cuts through a tie.
    original = [(f'row-{n:03}', epoch+(n//3)*2, 1, 0, f'echo synthetic {n}', '/synthetic', 'cursor', 'fixture', None) for n in range(241)]
    with sqlite3.connect(db) as con:
        con.execute('CREATE TABLE history (id TEXT PRIMARY KEY,timestamp INTEGER NOT NULL,duration INTEGER NOT NULL,exit INTEGER NOT NULL,command TEXT NOT NULL,cwd TEXT NOT NULL,session TEXT NOT NULL,hostname TEXT NOT NULL,deleted_at INTEGER)')
        con.executemany('INSERT INTO history VALUES (?,?,?,?,?,?,?,?,?)', original)
    inputs = report/'inputs'; inputs.mkdir()
    def preserve(name):
        with sqlite3.connect(db) as source, sqlite3.connect(inputs/(name+'.db')) as dest:
            source.backup(dest)
    preserve('before')
    endpoint=f"http://127.0.0.1:{h['free_port']()}/rpc"
    config=work/'cursor.toml'
    config.write_text(f'''listen = "{endpoint.removeprefix('http://').removesuffix('/rpc')}"
state_dir = "{work/'cursor-state'}"
store_dir = "{work/'cursor-store'}"
writer_connection_file = "{work/'writer'}"
reader_connection_file = "{work/'reader'}"
token_file = "{work/'token'}"
sources = ["terminal.atuin-history"]
automata = []
[atuin]
path = "{db}"
poll_interval_ms = 60000
''')
    proc = start(config, 'cursor-base')
    samples = (report/'cursor-observations.jsonl').open('w')
    def wait(predicate):
        deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            assert proc.poll() is None, 'owned daemon exited'
            try:
                answer=rpc(endpoint, 'synthetic-foundation-token', 'sources.list')
                samples.write(json.dumps(answer)+'\n'); samples.flush()
                sources=answer.get('result',{}).get('sources',[])
                if sources and sources[0]['last_run'] and predicate(sources[0]['last_run']):
                    return sources[0]['last_run']
            except (OSError,ValueError):
                pass
            time.sleep(.025)
        raise AssertionError('committed source observation not reached')
    query='events where source = terminal.atuin-history and kind = command.executed'
    restated=' events where kind = "command.executed" and source = "terminal.atuin-history" '
    def cli(text=query, cursor=None, expected=0):
        args=[binaries/'sinexctl','--endpoint',endpoint,'--token-file',work/'token','query',text,'--json']
        if cursor is not None: args += ['--cursor',cursor]
        answer=run(args,expected=expected,label='raw cursor CLI')
        return json.loads(answer.stdout if expected in (0,5) else answer.stderr)
    first=None
    second=None
    def continuation():
        nonlocal first,second,proc
        wait(lambda o:o['status']=='completed' and o['rows_read']==241)
        first=cli()
        assert first['total']==241 and len(first['rows'])==100 and first['complete'] is True
        assert all(r['payload']['timestamp_ns']==epoch+(int(r['payload']['native_id'][4:])//3)*2 for r in first['rows'])
        assert first['rows'][0]['when']=='2023-11-14T22:13:20.123456789Z'
        assert [(r['when'],r['id']) for r in first['rows']]==sorted((r['when'],r['id']) for r in first['rows'])
        unchanged=cli(restated,first['page']['cursor'])
        assert unchanged['resolved']['commit_seq']==first['resolved']['commit_seq']
        assert unchanged['page']['changed_since_first_page'] is False and unchanged['page']['change_notice'] is None
        assert unchanged['resolved']['now']==first['resolved']['now']
        assert first['rows'][-1]['when']==unchanged['rows'][0]['when']
        assert first['rows'][-1]['id']<unchanged['rows'][0]['id']
        stop(proc)
        # Remove a future row and append on both sides of the continuation key.
        with sqlite3.connect(db) as con:
            con.execute('UPDATE history SET deleted_at=? WHERE id=?', (epoch,'row-180'))
            for key, timestamp in [('append-before',epoch-100),('append-after',epoch+999)]:
                con.execute('INSERT INTO history VALUES (?,?,?,?,?,?,?,?,?)',(key,timestamp,1,0,'echo appended','/synthetic','cursor','fixture',None))
        preserve('changed')
        proc=start(config,'cursor-changed')
        wait(lambda o:o['status']=='completed' and o['rows_read']==243)
        second=cli(cursor=first['page']['cursor'])
        assert second['total']==242 and len(second['rows'])==100
        assert second['resolved']['commit_seq']>first['resolved']['commit_seq']
        assert second['page']['first_commit_seq']==first['resolved']['commit_seq'], 'continuation loses first-page commit binding'
        assert second['page']['changed_since_first_page'] is True, 'current-state changes were undisclosed'
        assert second['page']['change_notice'] and 'current' in second['page']['change_notice']
        assert second['resolved']['now']==first['resolved']['now']
        assert second['page']['expires_at']==first['page']['expires_at']
        assert first['page']['changed_since_first_page'] is False
        third=cli(restated, second['page']['cursor'])
        rows=first['rows']+second['rows']+third['rows']
        assert third['page']['cursor'] is None and third['total']==242
        native=[r['payload']['native_id'] for r in rows]
        assert len(native)==241 and len(set(native))==241
        assert 'row-180' not in native and 'append-after' in native and 'append-before' not in native
        assert [(r['when'],r['id']) for r in rows]==sorted((r['when'],r['id']) for r in rows)
        # A fresh query sees the new earlier row. Continuation never pretends to be a fixed snapshot.
        fresh=cli()
        assert fresh['rows'][0]['payload']['native_id']=='append-before' and fresh['total']==242
        (report/'cursor-expected.json').write_text(json.dumps({'first_total':241,'current_total':242,'continued_unique':241,'removed':'row-180','before_key_omitted':'append-before','after_key_included':'append-after'},indent=2)+'\n')
    scenario('raw current-state continuation and change disclosure',continuation)
    def refusals_restart():
        nonlocal proc
        cursor=first['page']['cursor']
        assert cli('events',cursor,expected=2)['kind']=='bad_query'
        assert cli(cursor='not a cursor',expected=2)['kind']=='bad_query'
        decoded=json.loads(cursor)
        for field,value in [('version',0),('order','id'),('expires_at','2000-01-01T00:00:00Z'),('first_commit_seq',-1),('last_when','bad'),('zone','Europe/Warsaw')]:
            bad=dict(decoded); bad[field]=value
            assert cli(cursor=json.dumps(bad),expected=2)['kind']=='bad_query', field
        legacy={k:decoded[k] for k in ['query','last_when','last_id']}
        assert cli(cursor=json.dumps(legacy),expected=2)['kind']=='bad_query'
        for unsupported in ['commands','events sort by id','events as of now','events where source = x or kind = x']:
            assert cli(unsupported,cursor,expected=2)['kind']=='bad_query'
        before=cli(restated,cursor)
        stop(proc); proc=start(config,'cursor-compatible-restart')
        wait(lambda o:o['status']=='completed')
        after=cli(restated,cursor)
        assert [r['id'] for r in after['rows']]==[r['id'] for r in before['rows']]
        assert after['resolved']['now']==first['resolved']['now']
        assert after['page']['first_commit_seq']==first['resolved']['commit_seq']
        # The same retained rows remain queryable when the provider becomes unavailable.
        stop(proc); db.rename(work/'provider-retired.db')
        proc=start(config,'cursor-provider-unavailable')
        wait(lambda o:o['status']=='unavailable')
        partial=cli(restated,cursor,expected=5)
        assert partial['complete'] is False and partial['source_status'][0]['status']=='unavailable'
        assert partial['total']==242 and len(partial['rows'])==100 and partial['page']['cursor']
        assert [r['id'] for r in partial['rows']]==[r['id'] for r in before['rows']]
        stop(proc)
    scenario('cursor refusal and compatible restart',refusals_restart)
    samples.close()
