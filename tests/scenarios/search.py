"""Indexed shell search through the API, CLI and MCP: ranking and query semantics,
projects, projection rebuilds and interruptions, source changes, deadlines,
cancellation and credential masking."""
import concurrent.futures
import contextlib
import json
import subprocess
import time

from _shell import Daemon, create_history, pause_listener, sql, update_history


def exercise(h):
    work, report, env, bins = h['WORK'], h['REPORT'], h['ENV'], h['BIN_DIR']
    run, scenario = h['run'], h['scenario']
    db, config = work/'history.db', work/'search.toml'
    daemon = Daemon(h, config)
    epoch = 1_789_400_000_123_456_789

    def row(key, n, command, cwd='/synthetic/tide', exit_code=0):
        return (key, epoch + n*1_000_000_000, 1_000_000_000, exit_code, command, cwd, 'synthetic-session', 'synthetic-host', None)
    fixture = [
        row('old-best', 0, 'needle needle needle needle needle'), row('phrase', 1, 'cargo test needle'),
        row('words', 2, 'cargo verbose test needle'), row('reverse', 3, 'test cargo needle'),
        row('unicode', 4, 'echo Straße CAFÉ ὈΔΥΣΣΕΎΣ'), row('plural', 5, 'echo offsets'),
        row('singular', 6, 'echo offset'), row('prefix', 7, 'echo offsetting'),
        row('nested', 8, 'needle nested', '/synthetic/tide/nested'),
        row('path-neighbor', 9, 'needle neighbor', '/synthetic/tidepool'),
        row('path-case', 10, 'needle case', '/synthetic/Tide'), row('failure', 11, 'needle failed', exit_code=9),
        row('unknown', 12, 'needle running', exit_code=-1), row('secret', 13, 'echo ghp_' + 'A'*36 + ' needle'),
        row('tie-a', 14, 'needle tie'), row('tie-b', 14, 'needle tie'),
        row('unicode-path', 15, 'needle unicodepath', '/synthetic/zażółć'),
        row('unicode-neighbor', 16, 'needle neighborpath', '/synthetic/zażółćdom'),
        row('long-url', 17, 'echo ' + 'z'*100 + ' https://u:' + 'B'*150 + '@host needle'),
        row('unsafe-program', 18, 'https://user:synthetic-secret@host'),
    ] + [row(f'common-{n:03}', 20 + n, 'needle') for n in range(120)]
    create_history(db, fixture)

    def configuration(source=True, nested='nested', poll=500, roots=None):
        return f'''listen = "{daemon.address}"
state_dir = "{work/'state'}"
store_dir = "{work/'store'}"
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
''' + (f'''[atuin]
path = "{db}"
poll_interval_ms = {poll}
full_interval_ms = 100
''' if source else '')

    def start(name, **kw):
        config.write_text(configuration(**kw))
        daemon.start(name)

    def caught_up(source_status='completed'):
        """Projection serving at the current commit, and every source's last read finished."""
        def check(s):
            indexed = s['indexed_retrieval']
            return (indexed.get('serving') and not indexed['pending_canonical'] and indexed['position'] == s['commit_seq']
                    and all(v['last_run']['status'] == source_status for v in daemon.sources()))
        return daemon.wait(check)
    query, query_all, cli, rpc = daemon.query, daemon.query_all, daemon.cli, daemon.rpc

    def keys(rows):
        return [r['payload']['native_id'] for r in rows]

    def wrong_profile():
        config.write_text(configuration(poll=30000))
        wrong_role = work/'wrong-role.toml'
        wrong_role.write_text(config.read_text().replace(str(work/'read-model'), str(work/'reader')))
        refused = run([bins/'sinexd', 'serve', '--config', wrong_role], expected=1, label='read model as sinex_reader')
        assert 'read-model role or projection unavailable' in refused.stderr
        h['create_database']('search_other')
        (work/'other-read-model').write_text(h['url']('sinex_read_model', 'search_other'))
        wrong_store = work/'wrong-store.toml'
        wrong_store.write_text(config.read_text().replace(str(work/'read-model'), str(work/'other-read-model')))
        refused = run([bins/'sinexd', 'serve', '--config', wrong_store], expected=1, label='read model on another database')
        assert 'read-model role or projection unavailable' in refused.stderr
    scenario('read model must use the sinex_read_model role on the same database', wrong_profile)

    baseline = {}

    def semantics():
        daemon.start('initial')
        caught_up()
        answer = query_all('needle')
        rows = answer['rows']
        expected = [r[0] for r in fixture if 'needle' in r[4]]
        assert answer['total'] == len(expected) and set(keys(rows)) == set(expected)
        assert keys(rows)[0] == 'old-best', 'ranking covers the whole match set, not just the newest page'
        assert len({r['id'] for r in rows}) == len(expected)
        assert all(r['kind'] == 'command.canonical' and r['origin'] == 'derived' and len(r['parents']) == 1 for r in rows)
        assert answer['resolved']['projection_seq'] == answer['resolved']['commit_seq'] and 'unknown' in answer['coverage']
        assert set(keys(query('"cargo test"')['rows'])) == {'phrase'}
        assert set(keys(query('cargo test')['rows'])) == {'phrase', 'words', 'reverse'}
        assert query('commands where cwd ^ /')['total'] == len(fixture)
        for text in ['STRASSE', 'stras*', 'CAFÉ', 'ὀδυσσεύσ']:
            assert keys(query(text)['rows']) == ['unicode'], text
        ties = query('commands where command ~ tie')['rows']
        assert len(ties) == 2 and [r['id'] for r in ties] == sorted(r['id'] for r in ties)
        assert keys(query('offset')['rows']) == ['singular']
        assert set(keys(query('offset*')['rows'])) == {'singular', 'plural', 'prefix'}
        assert set(keys(query('commands where command ~ offset')['rows'])) == {'singular', 'plural', 'prefix'}
        assert keys(query('commands where needle and cwd ^ /synthetic/zażółć')['rows']) == ['unicode-path']
        unsafe = query('commands where command ^ "https://"')['rows'][0]
        assert unsafe['payload']['canonical'] is None and 'synthetic-secret' not in json.dumps(unsafe)
        long_url = query('commands where needle and command ~ "https://"')['rows'][0]
        assert 'B'*30 not in json.dumps(long_url) and long_url['snippet']['highlights_unavailable'] == 'masked'
        assert set(keys(query('commands where needle and project = nested')['rows'])) == {'nested'}
        assert 'nested' not in keys(query('commands where needle and project = tide')['rows'])
        under_tide = keys(query('commands where needle and cwd ^ /synthetic/tide')['rows'])
        assert 'path-neighbor' not in under_tide and 'path-case' not in under_tide
        assert keys(query('commands where needle and exit_code != 0')['rows']) == ['failure']
        assert set(keys(query('commands where needle and (project = nested or exit_code = 9)')['rows'])) == {'nested', 'failure'}
        assert query('commands where needle and source = a and source = b')['total'] == 0
        when = next(r for r in rows if r['payload']['native_id'] == 'phrase')['when']
        assert keys(query(f'commands where needle and when >= {when} and when <= {when}')['rows']) == ['phrase']
        assert query(f'commands where needle and when > {when} and when < {when}')['total'] == 0
        assert keys(query('needle sort when desc limit 5')['rows']) == [f'common-{n:03}' for n in range(119, 114, -1)]
        for r in rows:
            raw = r['snippet']['text'].encode()
            for a, b in r['snippet']['spans']:
                assert raw[a:b].decode().casefold() == 'needle'
        for text in ['commands where typo = x', 'commands near (events) within 1m', 'recall now', 'commands where exit_code = bad',
                     'commands where source = terminal.*', 'commands where project = [tide,nested]', 'comands since 7d',
                     'RECALL now', 'commands where cwd "exists"', 'commands where command "=" "git status"']:
            response = rpc('query.run', {'query': text, 'page': 20})
            assert response['error']['kind'] == 'bad_query' and 'result' not in response, response
        baseline.update({r['payload']['native_id']: r for r in rows})
    scenario('ranking, phrases, prefixes, Unicode, fields, snippets and bad queries', semantics)

    mcp = None

    def clients():
        nonlocal mcp
        start('without-source', source=False)
        caught_up()
        mcp = daemon.mcp('mcp')
        api_answer = query('needle', 20)
        for answer in [cli('search', 'needle', '--page', '20'), rpc('search', {'query': 'needle', 'page': 20})['result'],
                       mcp.tool('sinex_search', {'words': 'needle', 'page': 20}),
                       mcp.tool('sinex_query', {'query': 'needle', 'page': 20})]:
            assert answer['rows'] == api_answer['rows']
            assert [answer[k] for k in ('total', 'coverage', 'complete')] == [api_answer[k] for k in ('total', 'coverage', 'complete')]
        credential_query = 'commands where command ~ "ghp_" or command ~ "https://"'
        credentials = query(credential_query, 20)
        assert mcp.tool('sinex_query', {'query': credential_query, 'page': 20})['rows'] == credentials['rows']
        assert cli('query', credential_query, '--page', '20')['rows'] == credentials['rows']
        assert mcp.tool('sinex_search', {'words': 'needle', 'query': 'and command ~ "https://"'})['rows'] == query('needle and command ~ "https://"', 20)['rows']
        assert 'B'*30 not in json.dumps(credentials) and 'synthetic-secret' not in json.dumps(credentials)
        assert keys(cli('search', 'cargo', 'test')['rows']) == keys(query('cargo test')['rows'])
        assert keys(cli('search', 'cargo test')['rows']) == ['phrase']
        ref = 'sinex:ev/' + api_answer['rows'][0]['id']
        assert mcp.tool('sinex_show', {'ref': ref})['row'] == cli('show', ref)['row']
        assert mcp.tool('sinex_trace', {'ref': ref})['row'] == cli('trace', ref)['row']
        assert mcp.tool('sinex_trace', {'ref': ref})['row']['parents'][0]['material']
        assert mcp.tool('sinex_status', {})['indexed_retrieval']['configured']
    scenario('API, CLI and MCP return the same rows', clients)

    def project_boundaries():
        old = 'commands where command = "needle needle needle needle needle"'
        start('project-wildcard', source=False, roots=['/synthetic/tide*', '/synthetic/tide*'])
        caught_up()
        assert query(old)['rows'][0]['project'] == 'tide'
        assert keys(query('commands where needle and project = nested')['rows']) == ['nested']
        assert keys(query('commands where command = "cargo test needle"')['rows']) == ['phrase']
        assert query('commands where cwd = "exists"')['total'] == 0
        daemon.stop()
        config.write_text(config.read_text() + '\n[[projects]]\nname = "other"\nroots = ["/synthetic/t*"]\n')
        daemon.start('project-tie')
        caught_up()
        assert query(old)['rows'][0]['project'] is None, 'a tie at the same depth has no project'
        assert keys(query('commands where needle and project = nested')['rows']) == ['nested']
        config.write_text(configuration(source=False, roots=['/synthetic/tide/*']).replace('roots = ["/synthetic/tide/nested"]', 'roots = ["/elsewhere"]'))
        daemon.start('project-wildcard-component')
        caught_up()
        assert query(old)['rows'][0]['project'] is None, '/synthetic/tide/* does not cover /synthetic/tide'
        assert query('commands where command = "needle nested"')['rows'][0]['project'] == 'tide'
        start('project-restored', source=False)
        caught_up()
    scenario('project roots: deepest match, ties and wildcards', project_boundaries)

    def reset_projection():
        run(['psql', h['url']('sinex_read_model'), '-XAtq', '-v', 'ON_ERROR_STOP=1', '-c',
             'DELETE FROM core.search_text; DELETE FROM core.event_project; DELETE FROM derivation.read_model_position'],
            label='clear the projection as the read-model role')

    def interruption():
        parent = baseline['phrase']['parents'][0]
        material = cli('show', parent)['row']['material']
        replay = cli('replay', 'materials where id = ' + material, operator=True)
        assert replay['state'] == 'completed' and replay['processed'] == len(fixture)
        caught_up()
        reminted = query_all('needle')['rows']
        assert all(r['id'] != baseline[r['payload']['native_id']]['id'] for r in reminted)
        baseline.clear()
        baseline.update({r['payload']['native_id']: r for r in reminted})
        daemon.stop()
        config.write_text(configuration())
        reset_projection()
        boundary = work/'projection.sock'
        with pause_listener(boundary, 20) as controller:
            proc = daemon.start('projection-paused', '--test-pause-at', 'projection-staged-1', '--test-pause-socket', boundary)
            conn, _ = controller.accept()
            with conn:
                frame = json.loads(conn.makefile('r').readline())
                assert frame['pid'] == proc.pid and frame['evidence']['through'] > frame['evidence']['from']
                answer = rpc('search', {'query': 'needle'})
                assert answer['error']['kind'] == 'partial' and 'result' not in answer
                update_history(db, "UPDATE history SET command='needle boundary updated' WHERE id='common-119'")
                update_history(db, "UPDATE history SET deleted_at=? WHERE id='common-118'", epoch)
                deadline = time.monotonic() + 20
                while rpc('query.run', {'query': 'events where payload.command = "needle boundary updated"'}).get('result', {}).get('total') != 1:
                    assert time.monotonic() < deadline, 'source change was not admitted while the projection was paused'
                    time.sleep(.025)
                daemon.kill()
        daemon.start('projection-resumed')
        caught_up()
        rows = query_all('needle')['rows']
        assert set(keys(rows)) == set(baseline) - {'common-118'}
        assert ({k: r['id'] for k, r in baseline.items() if k not in ('common-118', 'common-119')}
                == {r['payload']['native_id']: r['id'] for r in rows if r['payload']['native_id'] != 'common-119'})
        assert next(r for r in rows if r['payload']['native_id'] == 'common-119')['id'] != baseline['common-119']['id']
        roles = sql(h, "SELECT json_build_object('canonical',(SELECT count(*) FROM core.live_events WHERE event_type='command.canonical'),"
                       "'search',(SELECT count(*) FROM core.search_text),"
                       "'writer_can_search',has_table_privilege('sinex_writer','core.search_text','INSERT'),"
                       "'model_can_admit',has_table_privilege('sinex_read_model','core.events','INSERT'),"
                       "'model_can_archive',has_table_privilege('sinex_read_model','audit.archived','INSERT'))")
        assert roles['canonical'] == roles['search']
        assert not any(roles[k] for k in ['writer_can_search', 'model_can_admit', 'model_can_archive']), roles
    scenario('replay remints rows; a killed projection rebuild resumes with source changes', interruption)

    def changes():
        update_history(db, "UPDATE history SET command='needle corrected current' WHERE id='phrase'")
        update_history(db, "UPDATE history SET deleted_at=? WHERE id='words'", epoch)
        deadline = time.monotonic() + 20
        while True:
            caught_up()
            if (rpc('query.run', {'query': '"cargo test"', 'page': 100}).get('result', {}).get('total') == 0
                    and rpc('query.run', {'query': '"corrected current"', 'page': 100}).get('result', {}).get('total') == 1):
                break
            assert time.monotonic() < deadline, 'source update and removal did not reach search'
            time.sleep(.05)
        assert 'words' not in keys(query_all('needle')['rows'])
        assert query('"corrected current"')['rows'][0]['id'] != baseline['phrase']['id']
        # Renaming a project rebuilds the projection without reminting events.
        before = {r['id'] for r in query_all('needle')['rows']}
        old_cursor = query('needle', 3)['page']['cursor']
        start('project-renamed', nested='changed')
        caught_up()
        assert {r['id'] for r in query_all('needle')['rows']} == before
        assert keys(query('commands where needle and project = changed')['rows']) == ['nested']
        assert rpc('query.run', {'query': 'needle', 'page': 3, 'cursor': old_cursor})['error']['kind'] == 'bad_query'
        expected = {r['payload']['native_id']: r['id'] for r in query_all('needle')['rows']}
        daemon.stop()
        reset_projection()
        daemon.start('rebuild-from-log')
        caught_up()
        assert {r['payload']['native_id']: r['id'] for r in query_all('needle')['rows']} == expected
    scenario('source update and removal; project rename and full rebuild keep IDs', changes)

    def without_source():
        selected = query('"corrected current"')['rows'][0]
        ref = 'sinex:ev/' + selected['id']
        before = cli('trace', ref, '--bytes')
        daemon.stop()
        db.unlink()
        start('source-missing', nested='changed')
        caught_up('unavailable')
        answer = query('"corrected current"')
        assert answer['rows'][0]['id'] == selected['id'] and answer['complete'] is False and not db.exists()
        assert cli('trace', ref, '--bytes')['row'] == before['row']
        assert mcp.tool('sinex_search', {'words': '"corrected current"'})['rows'][0]['id'] == selected['id']
        assert mcp.tool('sinex_trace', {'ref': ref})['row']['parents'][0]['material'] == before['row']['parents'][0]['material']
    scenario('search, show and trace work after the source file is gone', without_source)

    @contextlib.contextmanager
    def locked(relation='core.search_text'):
        args = ['psql', h['admin'], '-XAt', '-v', 'ON_ERROR_STOP=1']
        p = subprocess.Popen(args, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        h['CHILDREN'].append((p, p.stdout, p.stderr))
        p.stdin.write(f"BEGIN; LOCK {relation} IN ACCESS EXCLUSIVE MODE; SELECT 'locked';\n")
        p.stdin.flush()
        assert [p.stdout.readline().strip() for _ in range(3)] == ['BEGIN', 'LOCK TABLE', 'locked']
        try:
            yield
        finally:
            p.stdin.write('ROLLBACK;\n\\q\n')
            p.stdin.flush()
            p.wait(timeout=5)
            assert p.returncode == 0

    def reader_backends(condition):
        """PIDs of active sinex_reader queries matching `condition`, waiting up to 5s for one."""
        deadline = time.monotonic() + 5
        while True:
            pids = sql(h, f"SELECT coalesce(json_agg(pid),'[]') FROM pg_stat_activity WHERE usename='sinex_reader' AND state='active' AND {condition}")
            if pids:
                return pids
            assert time.monotonic() < deadline, 'the reader query did not start'
            time.sleep(.025)

    def wait_gone(pids):
        deadline = time.monotonic() + 5
        while sql(h, "SELECT coalesce(json_agg(pid),'[]') FROM pg_stat_activity WHERE pid IN (" + ','.join(map(str, pids)) + ')'):
            assert time.monotonic() < deadline, f'backends {pids} kept running after their client went away'
            time.sleep(.025)

    def cancelled_tool(name, arguments, relation, condition):
        with locked(relation), concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            pending = mcp.next_id
            future = executor.submit(mcp.call, 'tools/call', {'name': name, 'arguments': arguments})
            pids = reader_backends(condition)
            mcp.cancel(pending)
            reply = future.result(timeout=5)
            assert 'error' in reply or reply.get('result', {}).get('isError'), reply
            wait_gone(pids)

    def deadlines():
        start('without-source-again', source=False, nested='changed')
        daemon.wait(lambda s: s['indexed_retrieval'].get('serving') and not s['indexed_retrieval']['pending_canonical']
                    and s['indexed_retrieval']['position'] == s['commit_seq'])
        secret = query('needle and command ~ "ghp_"')['rows'][0]
        assert 'ghp_' not in secret['payload']['command'] and 'ghp_' not in secret['snippet']['text'] and secret['secret'] is True
        assert secret['snippet']['spans'] == [] and secret['snippet']['highlights_unavailable'] == 'masked'
        blocked = "wait_event_type='Lock'"
        with locked():
            response = rpc('search', {'query': 'needle', 'timeout_ms': 300})
            assert response['error']['kind'] == 'budget' and 'result' not in response
            args = [str(bins/'sinexctl'), '--endpoint', daemon.endpoint, '--token-file', str(work/'token'), 'search', 'needle']
            out, err = (report/'killed-search.stdout').open('w'), (report/'killed-search.stderr').open('w')
            client = subprocess.Popen(args, env=env, stdout=out, stderr=err)
            h['CHILDREN'].append((client, out, err))
            pids = reader_backends(blocked)
            client.kill()
            client.wait(timeout=5)
            wait_gone(pids)
        cancelled_tool('sinex_search', {'words': 'needle'}, 'core.search_text', blocked)
        reference = 'sinex:ev/' + query('"corrected current"')['rows'][0]['id']
        for name, relation, arguments in [('sinex_show', 'core.events', {'ref': reference}), ('sinex_trace', 'core.events', {'ref': reference}),
                                          ('sinex_sources', 'raw.inputs', {}), ('sinex_status', 'core.events', {})]:
            cancelled_tool(name, arguments, relation, blocked)
        with locked('core.events'):
            reply = rpc('object.show', {'ref': reference}, timeout=15)
            assert reply['error']['kind'] == 'budget' and 'result' not in reply
        mcp.close()
    scenario('deadlines, killed clients and MCP cancellation stop the database query; masked credentials', deadlines)

    def missing_log():
        # Without the admission log, the projection cannot be rebuilt and search
        # must say partial rather than return an empty result.
        daemon.stop()
        sql(h, "DELETE FROM core.search_text; DELETE FROM core.event_project; DELETE FROM derivation.read_model_position; "
               "DELETE FROM core.commit_log WHERE event_type_id=3 AND change=0")
        daemon.start('missing-log')
        daemon.wait(lambda s: s['indexed_retrieval'].get('failure'), seconds=10)
        response = rpc('search', {'query': 'needle'})
        assert response.get('error', {}).get('kind') == 'partial' and 'result' not in response, response
        daemon.stop()
    scenario('missing admission log makes search partial, not empty', missing_log)
