"""Text search over shell commands: exact words, phrases and prefixes in very long commands,
ranking, what happens when token text is unavailable, and the probe query plans."""
import json

from _atuin_daemon import Daemon, access_config, append_history, create_history, relaunch, table_lock

EPOCH = 1_759_276_800_000_000_000  # 2025-10-01 UTC
BROAD = 'commands where project = tide sort when limit 100'
DAY_3 = 'commands where project = tide and when >= 2025-10-03T00:00:00Z and when < 2025-10-04T00:00:00Z sort when'


def row(key, offset, command, cwd='/synthetic/tide', duration=0, exit_code=0):
    return (key, EPOCH+offset, duration, exit_code, command, cwd, 's', 'h', None)


LONG_ROWS = [
    row('frequency', 86_400_000_000_000, 'repeat '*290+'late neighbor'),
    row('positions', 86_400_000_000_001, ' '.join('w'+str(n) for n in range(17001))),
    row('token-bytes', 86_400_000_000_002, 'x'*1100),
    row('empty-command', 86_400_000_000_003, ''),
    row('delimiters', 86_400_000_000_004, 'first\u0001second\u0002third percent% underscore_ slash\\tail "literal"'),
    row('field-boundary', 86_400_000_000_005, 'fieldtail', cwd='/synthetic/tide/head'),
    row('wildcard-control', 86_400_000_000_006, 'underscorex slashtail percentZ'),
    row('later', 172_800_000_000_000+123, 'needle later exact', duration=9, exit_code=-1),
]


def exercise(h):
    work, scenario = h['WORK'], h['scenario']
    d = Daemon(h, 'search-text')
    def cli(*args, operator=False, **kw):
        return d.cli(*args, token_file=work/'operator' if operator else None, **kw)
    source = work/'history.db'
    create_history(source, [row('safe-'+str(n), n, 'needle safe '+str(n), duration=5) for n in range(32)])

    def start(label, collect=True):
        d.configure(source if collect else None, poll_ms=500, full_ms=1000, top=access_config(h),
                    tables='[[projects]]\nname = "tide"\nroots = ["/synthetic/tide"]\n')
        relaunch(d, label)

    def query(text):
        return d.api('query.run', {'query': text}).get('result')

    def settled(total=None, available=None):
        q = query(BROAD)
        if not q or (total is not None and q['total'] != total):
            return None
        if q['fusion']['phase'] != 'settled' or not q['fusion']['current_now_settled']:
            return None
        if available is not None and q['full_text']['available'] is not available:
            return None
        return q

    def wait_settled(what, total, available=True):
        return d.wait(what, lambda: settled(total, available), 90)

    def sql(text):
        return h['run'](['psql', h['admin'], '-XAtq', '-v', 'ON_ERROR_STOP=1', '-c', text], label='psql').stdout.strip()

    def text_unavailable_refusals():
        for text in ['commands where needle', 'commands where "needle safe"', 'commands where need*', 'commands where not needle',
                     'commands where project = tide or needle', 'commands where project = tide and needle',
                     'commands where needle sort when', 'events where needle']:
            value = cli('query', text, expected=5)
            assert value['kind'] == 'partial' and 'rows' not in value, value
        value = cli('search', 'needle', expected=5)
        assert value['kind'] == 'partial' and 'rows' not in value

    def collected():
        start('collect')
        wait_settled('32 commands indexed', 32)
    scenario('collected commands are indexed with token text', collected)

    def blocked_receipts():
        # Collection is off so that the source's last read stays the same throughout.
        start('not-collecting', False)
        wait_settled('index with collection off', 32)
        before = cli('sources')['sources'][0]
        with table_lock(h, 'core.events'):
            for _ in range(3):
                limited = cli('sources', '--input', before['input'])['sources'][0]
                assert limited['last_run'] == before['last_run'], 'a receipt timeout changed the source read'
                assert limited['acquisition'] == before['acquisition']
                assert limited['inspection']['status'] == 'unavailable'
                assert limited['history']['complete'] is False and limited['receipts']['complete'] is False
        recovered = cli('sources', '--input', before['input'])['sources'][0]
        assert recovered['last_run'] == before['last_run'] and recovered['inspection']['status'] == 'available'
        answer = cli('query', BROAD, expected=(0, 5))
        assert answer['total'] == 32 and answer['full_text']['available']
        start('collecting-again')
        wait_settled('collection restored', 32)
    scenario('blocked source receipts report unavailable and recover; the source read is unchanged', blocked_receipts)

    def long_commands():
        append_history(source, LONG_ROWS)
        wait_settled('long commands indexed', 40)
        q = d.wait('day-3 query complete', lambda: (v := cli('query', DAY_3, expected=(0, 5)))['complete'] and v, 90)
        assert q['total'] == 1 and q['full_text']['available']
        later = q['rows'][0]['payload']
        assert later['native_id'] == 'later' and later['exit_code'] is None and later['duration_ns'] == 9
        checks = {
            'repeat': {'frequency'}, '"late neighbor"': {'frequency'}, '"late repeat"': set(),
            'neigh*': {'frequency'}, 'w17000': {'positions'}, 'x'*1100: {'token-bytes'},
            '"first second third"': {'delimiters'}, json.dumps('first\u0001second\u0002third'): {'delimiters'},
            '"fieldtail synthetic"': set(), '"fieldtail head"': set(), 'fieldtail': {'field-boundary'},
            'commands where not repeat and command = "needle later exact"': {'later'},
            'commands where repeat or command = "needle later exact"': {'frequency', 'later'},
            'commands where repeat and exit_code = 0': {'frequency'},
            'wildcard': {'wildcard-control'},
            'underscore_': {'delimiters'}, 'underscore': set(), 'underscorex': {'wildcard-control'},
            json.dumps('slash\\tail'): {'delimiters'},
            '"slash tail"': {'delimiters'},
            'commands where "percent underscore_ slash tail"': {'delimiters'},
            'commands where command ~ '+json.dumps('slash\\tail'): {'delimiters'},
            'commands where command ~ '+json.dumps('slashtail'): {'wildcard-control'},
            'commands where command ~ '+json.dumps('underscore_'): {'delimiters'},
            'commands where command ~ '+json.dumps('percent% underscore_ slash\\tail "literal"'): {'delimiters'},
            'commands where command ^ '+json.dumps('first\u0001second\u0002third percent% underscore_ slash\\'): {'delimiters'},
        }
        for question, expected in checks.items():
            value = cli('query', question, expected=(0, 5))
            keys = {r['payload']['native_id'] for r in value['rows']}
            assert keys == expected and value['total'] == len(expected), (question, keys, expected)
        # The expected order comes from PostgreSQL's word_similarity over the stored tokens.
        expected_rank = json.loads(sql(
            "SELECT jsonb_agg(jsonb_build_object('native_id',key,'rank',rank) ORDER BY rank DESC,at ASC,tie ASC,id ASC) FROM ("
            "SELECT e.payload->>'native_id' AS key,word_similarity('needle repeat' COLLATE \"C\",s.tokens COLLATE \"C\")::float8 AS rank,"
            "s.ts_orig AS at,coalesce(s.ts_orig_tiebreak,0) AS tie,s.id FROM core.search_text s JOIN core.live_events e ON e.id=s.id "
            "WHERE e.payload->>'native_id' LIKE 'safe-%' OR e.payload->>'native_id' IN ('later','frequency'))expected"))
        assert len(expected_rank) == 34
        ranked, cursor = [], None
        while True:
            page = d.api('query.run', {'query': 'needle or repeat', 'page': 7, **({'cursor': cursor} if cursor else {})})['result']
            assert page['total'] == 34 and 'word_similarity' in page['read_as']
            ranked += [{'native_id': r['payload']['native_id'], 'rank': r['rank']} for r in page['rows']]
            cursor = page['page']['cursor']
            if not cursor:
                break
        assert ranked == expected_rank, (ranked, expected_rank)
        for question, total in [('commands where not repeat', 39), ('commands where project = tide', 40),
                                ('commands where not not repeat', 1), ('needle or repeat sort when', 34)]:
            unranked = d.api('query.run', {'query': question, 'page': 100})['result']
            assert unranked['total'] == total and all(r['rank'] == 0 for r in unranked['rows']), unranked
        assert cli('query', '"first\u0001second"', expected=2)['kind'] == 'bad_query'
        assert sql("SELECT count(*) FROM core.search_text WHERE tokens IS NULL") == '0'
        assert cli('search', 'repeat')['total'] == 1
    scenario('long commands match exact words, phrases and prefixes; ranking pages in order', long_commands)

    def unavailable_text():
        # Collection is off so no new commit can hide a missing check behind pending work.
        start('not-collecting-2', False)
        wait_settled('index with collection off', 40)
        reference = 'sinex:ev/'+query('commands where repeat')['rows'][0]['id']
        event = reference.removeprefix('sinex:ev/')
        encoded = sql(f"SELECT encode(convert_to(tokens,'UTF8'),'hex') FROM core.search_text WHERE id='{event}'::uuid")
        sql(f"UPDATE core.search_text SET tokens=NULL WHERE id='{event}'::uuid")
        start('text-unavailable', False)
        wait_settled('text reported unavailable', 40, False)
        text_unavailable_refusals()
        status = d.api('status')['result']['indexed_retrieval']
        assert status['full_text']['first_affected_ref'] == reference
        parent = cli('trace', reference, '--bytes')['row']['parents'][0]['event']
        # Archive the parent directly, keeping the commit position, so the index cannot catch up and hide the ref.
        sql(f"INSERT INTO audit.archived(id,archived_seq,reason) SELECT '{parent}'::uuid,seq,'supersede' FROM core.commit_counter WHERE one")
        try:
            whole = d.api('status')['result']['indexed_retrieval']
            assert whole['serving'] and whole['position'] == whole['current_commit_seq'] and whole['failure'] is None, whole
            value = whole['full_text']
            assert not value['available'] and value['first_affected_ref'] != reference, value
            assert not cli('show', reference)['row']['current_eligible']
        finally:
            sql(f"DELETE FROM audit.archived WHERE id='{parent}'::uuid")
            sql(f"UPDATE core.search_text SET tokens=convert_from(decode('{encoded}','hex'),'UTF8') WHERE id='{event}'::uuid")
            start('collecting-again-2')
            wait_settled('text available again', 40)
    scenario('unavailable token text refuses text queries and names an affected ref that is still current', unavailable_text)

    def stored_lifecycle():
        before = cli('query', DAY_3, expected=(0, 5))
        ref = 'sinex:ev/'+before['rows'][0]['id']
        trace = cli('trace', ref, '--bytes')
        start('restart')
        wait_settled('index after restart', 40)
        assert cli('query', DAY_3, expected=(0, 5))['rows'] == before['rows'] and cli('trace', ref, '--bytes') == trace
        d.stop()
        source.rename(work/'source-removed.db')
        start('source-missing')
        d.wait('missing source reported', lambda: any(s['last_run']['status'] == 'unavailable' for s in d.sources()), 90)
        assert cli('query', DAY_3, expected=5)['rows'] == before['rows']
        assert cli('trace', ref, '--bytes') == trace
        assert cli('query', 'repeat', expected=5)['total'] == 1
        rows = d.api('query.run', {'query': 'events where source = terminal.atuin-history'})['result']['rows']
        large = [r for r in rows if r['payload']['native_id'] in ['frequency', 'positions', 'token-bytes']]
        assert len(large) == 3
        sql(f"UPDATE core.search_text SET tokens=NULL WHERE id='{query('commands where repeat')['rows'][0]['id']}'::uuid")
        text_unavailable_refusals()
        raw_ref = 'sinex:ev/'+large[0]['id']
        material = cli('trace', raw_ref, '--bytes')['row']['material']
        for r in large:
            cli('delete', 'sinex:ev/'+r['id'], operator=True)
        wait_settled('text available after excluding the rows', 37)
        assert cli('query', 'commands where needle', expected=5)['total'] == 33
        cli('delete', '--undo', raw_ref, operator=True)
        assert cli('replay', 'materials where id = '+material, operator=True)['state'] == 'completed'
        wait_settled('replay restores the row with text', 38)
        assert cli('query', 'repeat', expected=5)['total'] == 1
        current = d.api('query.run', {'query': 'events where source = terminal.atuin-history'})['result']['rows']
        restored = next(r for r in current if r['payload']['native_id'] == large[0]['payload']['native_id'])
        cli('delete', 'sinex:ev/'+restored['id'], operator=True)
        wait_settled('text available after excluding the replayed row', 37)
    scenario('restart, source loss, exclusion and replay keep results and text availability right', stored_lifecycle)

    def maintenance_failure():
        d.stop()
        sql("DELETE FROM core.search_text; DELETE FROM core.event_project; DELETE FROM derivation.read_model_position; "
            "DELETE FROM core.commit_log WHERE event_type_id=3 AND change=0")
        relaunch(d, 'broken-index')
        d.wait('index failure reported', lambda: d.api('status')['result']['indexed_retrieval'].get('failure'), 90)
        for text in [DAY_3, 'commands where needle']:
            error = cli('query', text, expected=5)
            assert error['kind'] == 'partial' and 'rows' not in error
        d.stop()
    scenario('an index that cannot be rebuilt reports a failure and queries return partial', maintenance_failure)

    def probe_plans():
        h['run'](['createdb', 'fts_probe'], label='create database fts_probe')
        zero = "'00000000-0000-0000-0000-000000000000'"
        rare = "tokens COLLATE \"C\" LIKE ('%'||chr(1)||'raretoken'||chr(1)||'%')"
        explain = 'EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '
        # A copy of the search_text table and indexes: 20000 ordinary rows, one rare token, 17 unavailable.
        probe = '\n'.join([
            'CREATE EXTENSION pg_trgm;',
            'CREATE SCHEMA core;',
            'CREATE TABLE core.search_text(generation uuid NOT NULL,id uuid PRIMARY KEY,tokens text);',
            'CREATE INDEX search_text_unavailable ON core.search_text(generation,id) WHERE tokens IS NULL;',
            'CREATE INDEX search_text_tokens ON core.search_text USING gin((tokens COLLATE "C") gin_trgm_ops);',
            f"INSERT INTO core.search_text SELECT {zero},md5(n::text)::uuid,chr(1)||'ordinary'||chr(1)||n::text||chr(1)||chr(2) FROM generate_series(1,20000)n;",
            f"INSERT INTO core.search_text VALUES({zero},md5('raretoken')::uuid,chr(1)||'raretoken'||chr(1)||chr(2));",
            'ANALYZE core.search_text;',
            f"INSERT INTO core.search_text SELECT {zero},md5('null'||n)::uuid,NULL FROM generate_series(1,17)n;",
            f'{explain}SELECT EXISTS(SELECT 1 FROM core.search_text WHERE generation={zero} AND tokens IS NULL LIMIT 1);',
            f'{explain}SELECT id FROM core.search_text WHERE generation={zero} AND tokens IS NULL ORDER BY id LIMIT 16;',
            # Right after insert the GIN pending list may make a sequential scan cheaper; only the row count is checked.
            f'{explain}SELECT id FROM core.search_text WHERE {rare};',
            'VACUUM (ANALYZE) core.search_text;',
            f'{explain}SELECT id FROM core.search_text WHERE {rare};',
            f"{explain}SELECT count(*) FROM core.search_text WHERE tokens COLLATE \"C\" LIKE ('%'||chr(1)||'o%');",
        ]) + '\n'
        (work/'probe.sql').write_text(probe)
        text = h['run'](['psql', h['url']('', 'fts_probe'), '-XAtq', '-v', 'ON_ERROR_STOP=1', '--file', work/'probe.sql'],
                        label='probe query plans').stdout
        decoder, plans = json.JSONDecoder(), []
        while text.strip():
            value, end = decoder.raw_decode(text.lstrip())
            plans.append(value[0])
            text = text.lstrip()[end:]

        def nodes(plan):
            yield plan
            for child in plan.get('Plans', []):
                yield from nodes(child)
        assert len(plans) == 5
        for plan in plans[:2]:
            assert any(n.get('Index Name') == 'search_text_unavailable' for n in nodes(plan['Plan'])), plan
        assert plans[1]['Plan']['Actual Rows'] == 16
        assert all(plan['Plan']['Actual Rows'] == 1 for plan in plans[2:4]), plans[2:4]
        assert any(n.get('Index Name') == 'search_text_tokens' for n in nodes(plans[3]['Plan'])), plans[3]
        assert any(n.get('Node Type') == 'Seq Scan' for n in nodes(plans[4]['Plan'])), plans[4]
    scenario('unavailable-text probes use the partial index; a rare token uses the trigram index', probe_plans)
