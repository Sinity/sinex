"""Exact published stage-3 predecessor through existing guarded change 4."""
import hashlib
import json
import sqlite3
import subprocess
import time
import urllib.request
from pathlib import Path

NAME='published stage-3 guarded populated upgrade preserves shell and admits Chromium'

def exercise(h,create):
    work,report,env,bins=(h[k] for k in ['WORK','REPORT','ENV','BIN_DIR'])
    run,start,stop=(h[k] for k in ['run','start_daemon','stop'])
    pin_path=h['OPTIONS'].chromium_predecessor_pin;raw=pin_path.read_bytes();pin=json.loads(raw)
    predecessor=Path(pin['package']);assert predecessor.parent==Path('/nix/store') and predecessor.resolve(strict=True)==predecessor and pin['exit_code']==0 and pin['dirty'] is False
    assert pin['candidate']=='5c69431413f5765431d38c8bf3e6c38f492d64d6'
    tree=subprocess.check_output(['git','rev-parse',pin['candidate']+'^{tree}'],cwd=h['ROOT'],text=True).strip();assert tree==pin['tree']
    for name in ['sinexd','sinexctl']:
        assert pin['binaries'][name]['path']==str(predecessor/'bin'/name)
        assert hashlib.sha256((predecessor/'bin'/name).read_bytes()).hexdigest()==pin['binaries'][name]['sha256']
    (report/'chromium-upgrade-predecessor-pin.json').write_bytes(raw)
    run(['createdb','chromium_upgrade'],label='owned browser upgrade database')
    connection={k: h[k].replace('/foundation?','/chromium_upgrade?') for k in ['admin','writer','reader']}
    for k,value in connection.items(): (work/('upgrade-'+k)).write_text(value);(work/('upgrade-'+k)).chmod(0o600)
    old=predecessor/'bin/sinexd';new=bins/'sinexd'
    def schema(binary,*extra,expected=0):
        result=run([binary,'admin','schema','--connection-file',work/'upgrade-admin',*extra],expected=expected,label='browser registered-schema upgrade')
        return json.loads(result.stdout) if result.returncode==0 else result
    applied=schema(old);assert applied['pending_changes']==[]
    provider=work/'upgrade-atuin.db'
    with sqlite3.connect(provider) as c:
        c.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT,deleted_at INTEGER)')
        c.execute("INSERT INTO history VALUES('published-command',1790856000000000001,1000,0,'cargo test --offline','/work/tide','synthetic-shell','synthetic-host',NULL)")
    endpoint=f'http://127.0.0.1:{h["free_port"]()}/rpc';config=work/'upgrade-chromium.toml';token=(work/'token').read_text()
    base=f'''listen = "{endpoint.removeprefix('http://').removesuffix('/rpc')}"
state_dir = "{work/'upgrade-state'}"
store_dir = "{work/'upgrade-store'}"
writer_connection_file = "{work/'upgrade-writer'}"
reader_connection_file = "{work/'upgrade-reader'}"
token_file = "{work/'token'}"
automata = []
'''
    atuin=f'''[atuin]
path = "{provider}"
poll_interval_ms = 1000
full_interval_ms = 86400000
'''
    config.write_text(base+'sources=["terminal.atuin-history"]\n'+atuin)
    daemon=None
    def api(method,params):
        req=urllib.request.Request(endpoint,data=json.dumps({'jsonrpc':'2.0','id':1,'method':method,'params':params}).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
        with urllib.request.urlopen(req,timeout=10) as response:return json.load(response)
    def wait(fn,reason):
        deadline=time.monotonic()+60
        while time.monotonic()<deadline:
            assert daemon.poll() is None,reason+' daemon exited'
            try:
                result=fn()
                if result:return result
            except (OSError,KeyError):pass
            time.sleep(.05)
        raise AssertionError(reason)
    query='events where source = terminal.atuin-history'
    def old_answer():
        result=api('query.run',{'query':query}).get('result')
        return result if result and result['total']==1 else None
    try:
        daemon=start(config,'published-browser-upgrade',binary=old);before=wait(old_answer,'published retained command')
        event=before['rows'][0];reference='sinex:ev/'+event['id']
        original=api('object.trace',{'ref':reference,'bytes':True})['result'];stop(daemon)
        waiting=schema(new,'--check');assert [c['number'] for c in waiting['pending_changes']]==[4,5,6]
        schema(new,'--apply',expected=2)
        dump=work/'before-browser-change.sql';upgraded=schema(new,'--apply','--dump-file',dump);assert upgraded['pending_changes']==[] and dump.stat().st_size>0
        schema(new,'--check')
        # Admission after a guarded populated upgrade uses the new source, while
        # exact old raw facts/citations survive a missing original provider.
        browser=work/'upgrade-History';create(browser,4,1);provider.rename(work/'upgrade-provider-held')
        config.write_text(base+'sources=["terminal.atuin-history","browser.history.chromium"]\n'+atuin+f'''[[chromium]]
profile="upgrade"
database_generation="22d3d942-c8a4-444e-8016-29d2e312664c"
path="{browser}"
poll_interval_ms=1000
''')
        daemon=start(config,'upgraded-browser');after=wait(old_answer,'retained command after change4')
        def new_answer():
            result=api('query.run',{'query':'events where source = browser.history.chromium'}).get('result')
            return result if result and result['total']==5 else None
        browser_answer=wait(new_answer,'new browser after change4')
        assert after['rows']==before['rows']
        retained=api('object.trace',{'ref':reference,'bytes':True})['result'];assert retained['row']['record']==original['row']['record'] and retained['row']['material']==original['row']['material']
        old_cli=run([bins/'sinexctl','--endpoint',endpoint,'--token-file',work/'token','query',query,'--json'],expected=5,label='upgraded shell public read with provider unavailable')
        new_cli=run([bins/'sinexctl','--endpoint',endpoint,'--token-file',work/'token','query','events where source = browser.history.chromium','--json'],expected=5,label='upgraded Chromium public read')
        assert json.loads(old_cli.stdout)['rows']==before['rows'] and json.loads(new_cli.stdout)['total']==5
        (report/'populated-browser-upgrade.json').write_text(json.dumps({'published_predecessor':pin,'before':before,'waiting':waiting,'applied':upgraded,'after':after,'trace':retained,'new_source':browser_answer,'dump_sha256':hashlib.sha256(dump.read_bytes()).hexdigest(),'scope':'exact published stage3 to change4 through existing guarded owner, without reset; other released upgrade controls remain separate'},indent=2)+'\n')
    finally:
        if daemon and daemon.poll() is None:stop(daemon)
