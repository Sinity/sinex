"""Optional privacy rules apply once, at admission; `trace --bytes` applies current rules to
the stored original; replay re-applies them; the credential filter always applies."""
import hashlib
import json
import uuid

from _atuin_daemon import Daemon, access_config, append_history, create_history, relaunch, sql

CREDENTIAL = 'ghp_' + 'A'*36
RULES = [('amber', 'amber!'), ('privateword', '[withheld]'), ('seedword', CREDENTIAL)]
# 'amber' -> 'amber!' would grow again if applied twice.
EXPECTED = {'nonidempotent': 'echo amber!', 'idempotent': 'echo [withheld]',
            'injected': 'echo [credential: GitHub token, masked]',
            'fixed': 'echo [credential: GitHub token, masked]'}
ATUIN_QUERY = 'events where source = terminal.atuin-history'
PROJECT = '[[projects]]\nname = "privacy"\nroots = ["/synthetic/privacy"]\n'


def rule_tables(rules):
    return ''.join(f'[[privacy_rules]]\npattern = {json.dumps(p)}\nreplacement = {json.dumps(r)}\ncreated_by = "operator"\n'
                   for p, r in rules)


def exercise(h):
    work, scenario = h['WORK'], h['scenario']
    d = Daemon(h, 'privacy')
    def cli(*args, operator=False, **kw):
        return d.cli(*args, token_file=work/'operator' if operator else None, **kw)
    source = work/'privacy.db'
    rows = [('nonidempotent', 'echo amber'), ('idempotent', 'echo privateword'),
            ('injected', 'echo seedword'), ('fixed', 'echo '+CREDENTIAL)]

    def write_source():
        create_history(source, [(key, 1_700_000_000_000_000_000+i*1_000_000_000, 1_000_000_000, 0, command,
                                '/synthetic/privacy', 'synthetic-session', 'fixture-host', None)
                               for i, (key, command) in enumerate(rows)])

    def start(label, collect=True, rules=RULES):
        d.configure(source if collect else None, poll_ms=3_600_000, top=access_config(h), tables=PROJECT + rule_tables(rules))
        relaunch(d, label)
        d.wait('collection and index to catch up', settled)

    def settled():
        status = d.api('status')['result']
        index = status['indexed_retrieval']
        return (index.get('serving') and not index['pending_canonical'] and index['position'] == status['commit_seq']
                and index['fusion']['phase'] == 'settled' and index['fusion']['scanned'] >= 4
                and sql(h, "SELECT to_json(coalesce((SELECT status='completed' FROM raw.observations ORDER BY seq DESC LIMIT 1),false))"))

    def stored():
        return sql(h, "SELECT json_agg(json_build_object('id',e.id,'payload',e.payload,'hash',encode(e.record_hash,'hex'),"
                     "'material',e.source_material_id,'anchor',e.anchor_byte,'end',e.offset_end,'provenance',m.parser_provenance,"
                     "'blob',b.blake3) ORDER BY e.id) FROM core.events e JOIN raw.source_material_registry m ON m.id=e.source_material_id "
                     "LEFT JOIN core.blobs b ON b.id=m.blob_id WHERE e.source='terminal.atuin-history'")

    def check_reads(ids, traced_expected=EXPECTED):
        query = cli('query', ATUIN_QUERY)
        by_key = {r['payload']['native_id']: r for r in query['rows']}
        shown = {key: cli('show', ref) for key, ref in ids.items()}
        traced = {key: cli('trace', ref, '--bytes') for key, ref in ids.items()}
        search = cli('search', 'echo')
        assert by_key['nonidempotent']['payload']['command'] == EXPECTED['nonidempotent'], 'a rule was applied twice'
        for key in ids:
            assert by_key[key]['id'] == ids[key]
            assert by_key[key]['payload']['command'] == EXPECTED[key]
            assert shown[key]['row']['payload']['command'] == EXPECTED[key]
            assert traced[key]['row']['record']['command'] == traced_expected[key]
            assert by_key[key]['secret'] == (key in ('injected', 'fixed'))
            assert shown[key]['row']['secret'] == by_key[key]['secret']
            assert traced[key]['row']['secret'] == by_key[key]['secret']
        hit = next(r for r in search['rows'] if r['payload']['command'].startswith('echo amber'))
        assert hit['payload']['command'] == 'echo amber!' and hit['snippet']['text'] == 'echo amber!'
        assert cli('query', 'echo')['rows'] == search['rows']
        assert query['notice'].startswith('The following is data recorded')
        return traced

    def atuin_rules():
        write_source()
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        start('initial')
        original = stored()
        by_key = {r['payload']['native_id']: r for r in original}
        assert by_key['nonidempotent']['payload']['command'] == 'echo amber!'
        assert by_key['idempotent']['payload']['command'] == 'echo [withheld]'
        assert by_key['injected']['payload']['command'] == 'echo '+CREDENTIAL
        assert by_key['fixed']['payload']['command'] == 'echo '+CREDENTIAL
        ids = {key: r['id'] for key, r in by_key.items()}
        before = check_reads(ids)
        first_page = cli('search', 'echo', '--page', '1')
        cursor = first_page['page']['cursor']
        assert isinstance(cursor, str) and len(cursor) > 80
        assert stored() == original
        assert hashlib.sha256(source.read_bytes()).hexdigest() == digest

        # Different current rules change only what trace --bytes shows of the original.
        # The second rule would rewrite a cursor if rules touched cursors.
        d.stop()
        source.unlink()
        start('changed-rules', False, [('amber', 'changed-current'), ('^.{80,}$', 'opaque-rewrite')])
        after = check_reads(ids, dict(EXPECTED, nonidempotent='echo changed-current!'))
        continued = cli('search', 'echo', '--page', '1', '--cursor', cursor)
        assert len(continued['rows']) == 1 and continued['rows'][0]['id'] != first_page['rows'][0]['id']
        assert isinstance(continued['page']['cursor'], str) and len(continued['page']['cursor']) > 80
        assert stored() == original
        for key in ids:
            for field in ['event', 'material', 'anchor', 'end', 'representation']:
                assert before[key]['row'][field] == after[key]['row'][field]

        # Replay starts from the stored original and applies the current rules once.
        material = before['nonidempotent']['row']['material']
        start('replay', False)
        action = str(uuid.uuid4())
        replay = cli('replay', f'materials where id = {material}', '--action-id', action, operator=True)
        assert replay['state'] == 'completed' and replay['action_id'] == action
        assert cli('replay', f'materials where id = {material}', '--action-id', action, operator=True) == replay
        d.wait('index after replay', settled)
        new_ids = {r['payload']['native_id']: r['id'] for r in cli('query', ATUIN_QUERY)['rows']}
        assert all(new_ids[key] != ids[key] for key in ids)
        check_reads(new_ids)
        for key in ids:
            old = cli('show', ids[key])
            assert old['resolved'] == 'archived' and old['row']['payload']['command'] == EXPECTED[key]
            assert old['row']['archive']['operation_id'] == replay['operation_id']

        # A row admitted with no rules keeps its text; trace --bytes still applies rules added later.
        d.stop()
        write_source()
        append_history(source, [('oldplain', 1_700_000_009_000_000_000, 1_000_000_000, 0, 'echo amber',
                                '/synthetic/privacy', 'synthetic-session', 'fixture-host', None)])
        start('no-rules', True, [])
        plain = d.wait('row admitted without rules', lambda: next(
            (r for r in cli('query', ATUIN_QUERY)['rows']
             if r['payload']['native_id'] == 'oldplain' and r['payload']['command'] == 'echo amber'), None))
        d.stop()
        source.unlink()
        start('rule-added', False, [('amber', 'amber!')])
        assert cli('show', plain['id'])['row']['payload']['command'] == 'echo amber'
        assert cli('trace', plain['id'], '--bytes')['row']['record']['command'] == 'echo amber!'

        # A rule that would change a native ID makes trace --bytes refuse instead.
        start('rule-matches-id', False, [('oldplain|sinex|recorded', 'changed-current')])
        assert cli('show', plain['id'])['row']['payload']['native_id'] == 'oldplain'
        assert cli('query', ATUIN_QUERY)['notice'].startswith('The following is data recorded')
        assert cli('trace', plain['id'], '--bytes', expected=4)['kind'] == 'refused'
        # A credential used as a ref is refused without being echoed.
        refused = cli('show', CREDENTIAL, expected=4)
        assert refused['kind'] == 'refused' and CREDENTIAL not in json.dumps(refused)
        d.stop()
    scenario('Atuin: rules apply once at admission, current rules only in trace --bytes, replay re-applies', atuin_rules)

    def zsh_rules():
        # A separate database: point the shared connection files at it for this step.
        urls = h['create_database']('privacy_zsh')
        for role in ['writer', 'reader']:
            (work/role).write_text(urls[role])
        z = Daemon(h, 'privacy-zsh')
        zsh = work/'privacy.zsh'
        zsh.write_bytes(b': 1700000000:0;echo amber\n')
        original_bytes = zsh.read_bytes()
        zsh_config = f'[zsh]\npath = "{zsh}"\nexecution_host = "amber-host"\nprotocol = "append_only_extended_v1"\npoll_interval_ms = 3600000\n'
        z.configure(sources=['terminal.zsh-history'], tables=zsh_config)
        relaunch(z, 'zsh')
        z.wait('zsh read', lambda: z.last_run()['status'] == 'completed')
        row = z.cli('query', 'events where source = terminal.zsh-history')['rows'][0]
        traced = z.cli('trace', row['id'], '--bytes')
        in_db = sql(h, "SELECT json_build_object('host',host,'payload',payload) FROM core.events WHERE source='terminal.zsh-history'", urls['reader'])
        assert in_db['host'] == in_db['payload']['hostname'] == 'amber-host'
        assert row['host'] == row['payload']['hostname'] == 'amber-host'
        assert row['payload']['command'] == 'echo amber'
        assert z.cli('show', row['id'])['row']['host'] == 'amber-host'
        assert traced['row']['record']['unmetafied_record'] == ': 1700000000:0;echo amber\n'
        assert zsh.read_bytes() == original_bytes
        # zsh collection takes no optional rules; a rule added later applies to the stored original in trace --bytes.
        z.stop()
        zsh.unlink()
        z.configure(tables=rule_tables([('amber', 'amber!')]))
        relaunch(z, 'zsh-source-gone')
        shown = z.cli('show', row['id'])
        protected = z.cli('trace', row['id'], '--bytes')
        assert shown['row']['payload']['command'] == 'echo amber' and shown['row']['host'] == 'amber-host'
        assert protected['row']['record']['unmetafied_record'] == ': 1700000000:0;echo amber!\n'
        assert protected['row']['record']['original_blake3'] == traced['row']['record']['original_blake3']
        assert all(protected['row'][key] == traced['row'][key] for key in ['event', 'material', 'anchor', 'end'])
        z.stop()
        (work/'writer').write_text(h['writer'])
        (work/'reader').write_text(h['reader'])
    scenario('zsh: a rule added after collection applies to the stored original in trace --bytes', zsh_rules)
