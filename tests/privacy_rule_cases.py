"""Synthetic admission-once controls through the actual packaged writer and readers."""
import hashlib
import json
import sqlite3
import time
import uuid


def exercise(h):
    work, report, bins = (h[k] for k in ('WORK', 'REPORT', 'BIN_DIR'))
    run, start, stop = (h[k] for k in ('run', 'start_daemon', 'stop'))
    source = work/'privacy.db'
    credential = 'ghp_' + 'A'*36
    rows = [('nonidempotent', 'echo amber'), ('idempotent', 'echo privateword'),
            ('injected', 'echo seedword'), ('fixed', 'echo '+credential)]
    expected = {'nonidempotent': 'echo amber!', 'idempotent': 'echo [withheld]',
                'injected': 'echo [credential: GitHub token, masked]',
                'fixed': 'echo [credential: GitHub token, masked]'}
    rules = [('amber', 'amber!'), ('privateword', '[withheld]'), ('seedword', credential)]
    with sqlite3.connect(source) as con:
        con.execute('CREATE TABLE history(id TEXT PRIMARY KEY,timestamp INTEGER,duration INTEGER,exit INTEGER,command TEXT,cwd TEXT,session TEXT,hostname TEXT,deleted_at INTEGER)')
        con.executemany('INSERT INTO history VALUES(?,?,?,?,?,?,?,?,?)', [
            (key,1_700_000_000_000_000_000+i*1_000_000_000,1_000_000_000,0,command,
             '/synthetic/privacy','synthetic-session','fixture-host',None)
            for i,(key,command) in enumerate(rows)])
    (report/'source.db').write_bytes(source.read_bytes())
    source_digest = hashlib.sha256(source.read_bytes()).hexdigest()
    (report/'expected.json').write_text(json.dumps({'source_sha256':source_digest,
        'ordinary_presented':expected,'stored_nonidempotent':'echo amber!',
        'raw_nonidempotent_presented':'echo amber!','rules':rules},indent=2)+'\n')
    for name, value in [('operator','synthetic-explicit-operator'),
                        ('read-model',h['reader'].replace('sinex_reader','sinex_read_model'))]:
        (work/name).write_text(value); (work/name).chmod(0o600)
    port=h['free_port'](); endpoint=f'http://127.0.0.1:{port}/rpc'
    config=work/'privacy.toml'
    def configure(acquire=True, current_rules=rules):
        text=f'''listen = "127.0.0.1:{port}"
state_dir = "{work/'privacy-state'}"
store_dir = "{work/'privacy-store'}"
writer_connection_file = "{work/'writer'}"
reader_connection_file = "{work/'reader'}"
read_model_connection_file = "{work/'read-model'}"
token_file = "{work/'token'}"
operator_token_file = "{work/'operator'}"
sources = {json.dumps(['terminal.atuin-history'] if acquire else [])}
automata = []
[[projects]]
name = "privacy"
roots = ["/synthetic/privacy"]
'''
        if acquire: text+=f'[atuin]\npath = "{source}"\npoll_interval_ms = 3600000\n'
        for pattern,replacement in current_rules:
            text+='[[privacy_rules]]\npattern = '+json.dumps(pattern)+'\nreplacement = '+json.dumps(replacement)+'\ncreated_by = "operator"\n'
        config.write_text(text)
    configure(); daemon=start(config,'privacy-initial')
    def cli(command,*args,operator=False,expected_exit=0):
        result=run([bins/'sinexctl','--endpoint',endpoint,'--token-file',work/('operator' if operator else 'token'),command,*args,'--json'],expected=expected_exit,label='privacy actual CLI '+command)
        return json.loads(result.stdout or result.stderr)
    def wait():
        deadline=time.monotonic()+30
        while True:
            assert daemon.poll() is None, 'owned privacy daemon exited'
            try:
                value=h['rpc'](endpoint,'synthetic-foundation-token')
                if 'result' in value:
                    status=value['result']; p=status['indexed_retrieval']
                    if p.get('serving') and not p['pending_canonical'] and p['position']==status['commit_seq'] and p['fusion']['phase']=='settled' and p['fusion']['scanned']>=4:
                        if sql("SELECT to_json(coalesce((SELECT status='completed' FROM raw.observations ORDER BY seq DESC LIMIT 1),false))"):return
            except (OSError,ValueError): pass
            assert time.monotonic()<deadline, 'privacy source/projection did not settle'
            time.sleep(.025)
    def sql(query):
        return json.loads(run(['psql',h['reader'],'-XAt','-v','ON_ERROR_STOP=1','-c',query],label='independent read-only privacy receipt').stdout)
    def retained():
        return sql("SELECT json_agg(json_build_object('id',e.id,'payload',e.payload,'hash',encode(e.record_hash,'hex'),'material',e.source_material_id,'anchor',e.anchor_byte,'end',e.offset_end,'provenance',m.parser_provenance,'blob',b.blake3) ORDER BY e.id) FROM core.events e JOIN raw.source_material_registry m ON m.id=e.source_material_id LEFT JOIN core.blobs b ON b.id=m.blob_id WHERE e.source='terminal.atuin-history'")
    def check_reads(ids, raw_expected=expected):
        query=cli('query','events where source = terminal.atuin-history')
        actual={r['payload']['native_id']:r for r in query['rows']}
        shown={key:cli('show',identity) for key,identity in ids.items()}
        traced={key:cli('trace',identity,'--bytes') for key,identity in ids.items()}
        search=cli('search','echo')
        indexed_query=cli('query','echo')
        # Retain all results before the first distinguishing assertion, including the red run.
        receipt={'query':query,'show':shown,'trace':traced,'search':search,'indexed_query':indexed_query,'stored':retained()}
        (report/('reads-'+str(len(list(report.glob('reads-*.json'))))+'.json')).write_text(json.dumps(receipt,indent=2)+'\n')
        assert actual['nonidempotent']['payload']['command']==expected['nonidempotent'], 'optional rule was applied again to an already admitted payload'
        for key in ids:
            assert actual[key]['id']==ids[key]
            assert actual[key]['payload']['command']==expected[key]
            assert shown[key]['row']['payload']['command']==expected[key]
            assert traced[key]['row']['record']['command']==raw_expected[key]
            assert actual[key]['secret']==(key in ('injected','fixed'))
            assert shown[key]['row']['secret']==actual[key]['secret']
            assert traced[key]['row']['secret']==actual[key]['secret']
        hit=next(r for r in search['rows'] if r['payload']['command'].startswith('echo amber'))
        assert hit['payload']['command']=='echo amber!'
        assert hit['snippet']['text']=='echo amber!'
        assert indexed_query['rows']==search['rows']
        assert query['notice'].startswith('The following is data recorded')
        return receipt
    def controls():
        nonlocal daemon
        wait()
        original=retained()
        by_key={r['payload']['native_id']:r for r in original}
        assert by_key['nonidempotent']['payload']['command']=='echo amber!'
        assert by_key['idempotent']['payload']['command']=='echo [withheld]'
        assert by_key['injected']['payload']['command']=='echo '+credential
        assert by_key['fixed']['payload']['command']=='echo '+credential
        ids={key:r['id'] for key,r in by_key.items()}
        before=check_reads(ids)
        assert retained()==original
        assert hashlib.sha256(source.read_bytes()).hexdigest()==source_digest
        # Retained evidence uses acquisition attribution, even when current rules differ.
        stop(daemon); source.unlink(); configure(False,[('amber','changed-current')])
        daemon=start(config,'privacy-provider-gone');wait()
        raw_expected=dict(expected, nonidempotent='echo changed-current!')
        after=check_reads(ids, raw_expected)
        assert retained()==original
        for key in ids:
            for field in ['event','material','anchor','end','representation']:
                assert before['trace'][key]['row'][field]==after['trace'][key]['row'][field]
        material=before['trace']['nonidempotent']['row']['material']
        # Replay starts from retained original bytes and current optional rules exactly once.
        stop(daemon);configure(False);daemon=start(config,'privacy-replay');wait()
        action=str(uuid.uuid4())
        replay=cli('replay',f'materials where id = {material}','--action-id',action,operator=True)
        assert replay['state']=='completed' and replay['action_id']==action
        assert cli('replay',f'materials where id = {material}','--action-id',action,operator=True)==replay
        new_rows=cli('query','events where source = terminal.atuin-history')['rows']
        new_ids={r['payload']['native_id']:r['id'] for r in new_rows}
        assert all(new_ids[key]!=ids[key] for key in ids)
        check_reads(new_ids)
        for key in ids:
            old=cli('show',ids[key])
            assert old['resolved']=='archived' and old['row']['payload']['command']==expected[key]
            assert old['row']['archive']['operation_id']==replay['operation_id']
        # Current rules protect evidence acquired before any optional rules existed.
        # A new acquired recipe creates current interpretations without rewriting old rows.
        stop(daemon); source.write_bytes((report/'source.db').read_bytes());configure(True, [])
        daemon=start(config,'privacy-empty-attribution');wait()
        deadline=time.monotonic()+30
        while True:
            empty_rows=cli('query','events where source = terminal.atuin-history')['rows']
            empty=next(r for r in empty_rows if r['payload']['native_id']=='nonidempotent')
            if empty['payload']['command']=='echo amber':break
            assert time.monotonic()<deadline
            time.sleep(.025)
        empty_id=empty['id']
        stop(daemon);source.unlink();configure(False, [('amber','amber!')])
        daemon=start(config,'privacy-protect-old-plain');wait()
        ordinary=cli('show',empty_id)
        protected=cli('trace',empty_id,'--bytes')
        assert ordinary['row']['payload']['command']=='echo amber'
        assert protected['row']['record']['command']=='echo amber!'
        (report/'old-plain-protection.json').write_text(json.dumps({'show':ordinary,'trace':protected},indent=2)+'\n')
        # Current rules cannot rewrite opaque native IDs while protecting raw values.
        stop(daemon);configure(False,[('nonidempotent|sinex|recorded','changed-current')])
        daemon=start(config,'privacy-identity-control');wait()
        assert cli('show',empty_id)['row']['payload']['native_id']=='nonidempotent'
        assert cli('query','events where source = terminal.atuin-history')['notice'].startswith('The following is data recorded')
        assert cli('trace',empty_id,'--bytes',expected_exit=4)['kind']=='refused'
        # The mandatory credential floor still refuses an unsafe opaque request selector.
        refused=cli('show',credential,expected_exit=4)
        assert refused['kind']=='refused' and credential not in json.dumps(refused)
        (report/'privacy-rule-controls.json').write_text(json.dumps({'once_at_admission':True,
            'raw_attribution_preserved':True,'provider_deleted':True,'replay_action':action,
            'replay_operation':replay['operation_id'],'original_ids':ids,'replayed_ids':new_ids,
            'fixed_floor_and_request_refusal':True},indent=2)+'\n')
        stop(daemon)
    h['scenario']('optional privacy rules preserve admission meaning and retained protection',controls)
