"""Native presentation controls; fixtures and retained evidence are synthetic."""
import json
import sqlite3
import time
import tomllib
import urllib.request


def exercise(h, base, cli, wait_cycle, config_text):
    work, report = h['WORK'], h['REPORT']
    run = h['run']
    actual = base['actual']
    event = actual['synthetic-1']['id']
    token = 'ghp_' + 'A'*36
    expected = 'before [credential: GitHub token, masked] after'
    assert actual['synthetic-1']['secret'] is True
    assert actual['synthetic-2']['secret'] is False
    assert cli('show', event)['row']['payload']['command'] == expected
    assert cli('show', event)['row']['secret'] is True
    trace = cli('trace', event, '--bytes')['row']
    assert trace['record']['command'] == expected and trace['secret'] is True
    assert trace['record']['_from']['table'] == 'history'
    assert trace['end'] > trace['anchor'] >= 0
    def retained():
        result = run(['psql', h['admin'], '--no-psqlrc', '--tuples-only', '--no-align', '-v', 'ON_ERROR_STOP=1', '--command', f"SELECT json_build_object('payload',e.payload,'record_hash',encode(e.record_hash,'hex'),'provenance',m.parser_provenance,'blob',b.blake3) FROM core.events e JOIN raw.source_material_registry m ON m.id=e.source_material_id LEFT JOIN core.blobs b ON b.id=m.blob_id WHERE e.id='{event}'"], label='unchanged synthetic canonical hashes and provenance')
        return json.loads(result.stdout)
    before = retained()
    assert before['payload']['command'] == 'before '+token+' after'
    assert before['provenance']['privacy_rules'] == []
    # A single native logical string crosses multiple ordinary buffer widths.
    bearer = 'B'*24
    private = '-----BEGIN PRIVATE KEY-----body-----END PRIVATE KEY-----'
    command = 'prefix '+('plain '*180)+token+' Bearer '+bearer+' https://user:password@host.invalid/path --secret=x '+private+' suffix token=ast_node'
    with sqlite3.connect(work/'atuin.db') as con:
        con.execute('INSERT INTO history (id,timestamp,duration,exit,command,cwd,session,hostname,deleted_at) VALUES (?,?,?,?,?,?,?,?,?)', ('families',1_700_000_005_000_000_000,1,0,command,'/synthetic/project','fixture','host '+token,None))
    wait_cycle(lambda value:value['status']=='completed' and value['rows_read']==5)
    families = next(row for row in cli('query','events')['rows'] if row['payload']['native_id']=='families')
    rendered = families['payload']['command']
    assert rendered.startswith('prefix '+('plain '*180)) and rendered.endswith(' suffix token=ast_node')
    assert rendered.count('[credential:') == 5
    assert 'https://[credential: URL credentials, masked]@host.invalid/path' in rendered
    assert families['payload']['hostname'] == 'host [credential: GitHub token, masked]'
    assert families['secret'] is True
    for raw in [token,bearer,'user:password','--secret=x',private]: assert raw not in rendered
    for command_name,args in [('query',('events where payload.command ~ '+json.dumps(token),)),('query',('events','--cursor',token)),('show',(token,))]:
        refused=cli(command_name,*args,expected=4)
        assert refused['kind']=='refused' and token not in json.dumps(refused)
    config=work/'atuin.toml'
    port=tomllib.loads(config.read_text())['listen'].split(':')[-1]
    endpoint=f'http://127.0.0.1:{port}/rpc'
    for identity,params in [(token,{}),(17,{token:'synthetic'})]:
        request=urllib.request.Request(endpoint,data=json.dumps({'jsonrpc':'2.0','id':identity,'method':'status','params':params}).encode(),headers={'Authorization':'Bearer synthetic-foundation-token','Content-Type':'application/json'})
        with urllib.request.urlopen(request,timeout=5) as response: wire=response.read().decode()
        answer=json.loads(wire)
        assert answer['error']['kind']=='refused' and 'result' not in answer and token not in wire
    # Native identity is an opaque selector, so the entire page must refuse.
    with sqlite3.connect(work/'atuin.db') as con:
        con.execute('INSERT INTO history (id,timestamp,duration,exit,command,cwd,session,hostname,deleted_at) VALUES (?,?,?,?,?,?,?,?,?)',(token,1_700_000_006_000_000_000,1,0,'identity control','/synthetic','fixture','host',None))
    wait_cycle(lambda value:value['status']=='completed' and value['rows_read']==6)
    assert cli('query','events',expected=4)['kind']=='refused'
    assert cli('show',event)['row']['payload']['command']==expected
    assert retained()==before
    # Stop acquisition, remove the original provider, then use the same product
    # state with acquisition disabled. Native retained trace remains authoritative.
    h['stop'](h['CHILDREN'][-1][0])
    (work/'atuin.db').unlink()
    config.write_text(config_text().split('[atuin]')[0].replace('sources = ["terminal.atuin-history"]','sources = []'))
    restarted=h['start_daemon'](config,'credential-provider-deleted')
    deadline=time.monotonic()+20
    while True:
        try:
            if 'result' in h['rpc'](endpoint,'synthetic-foundation-token'): break
        except OSError: pass
        if time.monotonic()>deadline: raise AssertionError('retained restart unavailable')
        time.sleep(.05)
    assert cli('query','events where id = '+event)['rows'][0]['payload']['command']==expected
    assert cli('show',event)['row']['secret'] is True
    after_trace=cli('trace',event,'--bytes')['row']
    assert after_trace['record']==trace['record'] and after_trace['material']==trace['material']
    assert retained()==before
    h['stop'](restarted)
    (report/'credential-output-controls.json').write_text(json.dumps({'catalog':'credential-output-v1','optional_rules':[],'families':5,'provider_deleted':True,'raw_hashes_and_provenance_unchanged':True,'whole_page_identity_refusal':True},indent=2)+'\n')
