"""Chromium History collection on generated databases: visits and downloads from two
profiles, late updates, full rereads, snapshots, locked databases, partial reads and the
refusals for replaced, reset or unsupported files, through CLI and MCP."""
import datetime as dt
import json
import socket
import sqlite3
import subprocess
import sys
import uuid

from _clients import Clients, wait

OFFSET = 11644473600000000  # microseconds from 1601-01-01 (Chromium's epoch) to 1970-01-01
BASE = int(dt.datetime(2026, 10, 1, tzinfo=dt.timezone.utc).timestamp()*1_000_000)
SCHEMA = '''
CREATE TABLE urls(id INTEGER PRIMARY KEY AUTOINCREMENT,url TEXT,title TEXT,visit_count INTEGER DEFAULT 0,typed_count INTEGER DEFAULT 0,last_visit_time INTEGER DEFAULT 0,hidden INTEGER DEFAULT 0);
CREATE TABLE visits(id INTEGER PRIMARY KEY AUTOINCREMENT,url INTEGER NOT NULL,visit_time INTEGER NOT NULL,from_visit INTEGER,transition INTEGER DEFAULT 0,segment_id INTEGER,visit_duration INTEGER DEFAULT 0,incremented_omnibox_typed_score BOOLEAN DEFAULT FALSE,opener_visit INTEGER,originator_cache_guid TEXT,originator_visit_id INTEGER,originator_from_visit INTEGER,originator_opener_visit INTEGER,is_known_to_sync BOOLEAN DEFAULT FALSE,consider_for_ntp_most_visited BOOLEAN DEFAULT FALSE,external_referrer_url TEXT);
CREATE INDEX visits_url_index ON visits(url);
CREATE TABLE downloads(id INTEGER PRIMARY KEY,guid VARCHAR NOT NULL,current_path LONGVARCHAR NOT NULL,target_path LONGVARCHAR NOT NULL,start_time INTEGER NOT NULL,received_bytes INTEGER NOT NULL,total_bytes INTEGER NOT NULL,state INTEGER NOT NULL,danger_type INTEGER NOT NULL,interrupt_reason INTEGER NOT NULL,hash BLOB NOT NULL,end_time INTEGER NOT NULL,opened INTEGER NOT NULL,last_access_time INTEGER NOT NULL,transient INTEGER NOT NULL,referrer VARCHAR NOT NULL,site_url VARCHAR NOT NULL,embedder_download_data VARCHAR NOT NULL,tab_url VARCHAR NOT NULL,tab_referrer_url VARCHAR NOT NULL,http_method VARCHAR NOT NULL,by_ext_id VARCHAR NOT NULL,by_ext_name VARCHAR NOT NULL,etag VARCHAR NOT NULL,last_modified VARCHAR NOT NULL,mime_type VARCHAR NOT NULL,original_mime_type VARCHAR NOT NULL);
CREATE TABLE downloads_url_chains(id INTEGER NOT NULL,chain_index INTEGER NOT NULL,url LONGVARCHAR NOT NULL,PRIMARY KEY(id,chain_index));
'''


def create(path, n=50000, downloads=320):
    """A History database with `n` visits over 1000 URLs (every 101st a redirect hop) and
    `downloads` downloads; download 1 is unfinished with unknown times."""
    with sqlite3.connect(path) as c:
        c.executescript(SCHEMA)
        c.executemany('INSERT INTO urls(id,url,title) VALUES(?,?,?)',
                      [(i, f'https://example.invalid/page/{i}', f'Synthetic page {i}') for i in range(1, 1001)])
        c.executemany('INSERT INTO visits(id,url,visit_time,from_visit,transition,visit_duration,external_referrer_url) VALUES(?,?,?,?,?,?,?)',
                      [(i, (i-1) % 1000+1, OFFSET+BASE+i*1000001, 0 if i == 1 else i-1, 0x80000000 if i % 101 == 0 else 1,
                        0 if i == 1 else 1200001, 'https://referrer.invalid/') for i in range(1, n+1)])
        for i in range(1, downloads+1):
            values = [i, str(uuid.UUID(int=i)), f'/downloads/.partial-{i}', f'/downloads/item-{i}.pdf', 0 if i == 1 else OFFSET+BASE+i*1000001,
                      123 if i == 1 else 4096, 4096, 0 if i == 1 else 1, 0, 0, b'\x00\x01', 0 if i == 1 else OFFSET+BASE+i*1000001+990001,
                      0, 0, 0, 'https://referrer.invalid/', 'https://example.invalid/', '', 'https://example.invalid/tab', '', 'GET',
                      '', '', '', '', 'application/pdf', 'application/pdf']
            c.execute('INSERT INTO downloads VALUES('+','.join('?' for _ in values)+')', values)
            c.executemany('INSERT INTO downloads_url_chains VALUES(?,?,?)',
                          [(i, 0, f'https://example.invalid/start/{i}'), (i, 1, f'https://cdn.invalid/file/{i}.pdf')])
    c.close()
    return {'visits': n, 'redirects': n//101, 'downloads': downloads}


def exercise(h):
    work, env = h['WORK'], h['ENV']
    run, scenario, start, stop = (h[k] for k in ['run', 'scenario', 'start_daemon', 'stop'])
    source, second, config, store = work/'History', work/'OtherHistory', work/'chromium.toml', work/'chromium-blobs'
    expected, other = create(source), create(second, 7, 3)
    endpoint = f'http://127.0.0.1:{h["free_port"]()}/rpc'
    clients = Clients(h, endpoint, 'chromium')
    cli, api, tool = clients.cli, clients.api, clients.tool
    generation, other_generation = '95d3d942-c8a4-444e-8016-29d2e312664c', '86d3d942-c8a4-444e-8016-29d2e312664c'
    daemon = None
    first = None
    current = {'generation': generation}

    def visit_query():
        return ('events where source = browser.history.chromium and kind = page.visited and payload.profile = main'
                f' and payload.database_generation = "{current["generation"]}"')

    def download_query():
        return ('events where source = browser.history.chromium and kind = download.observed and payload.profile = main'
                f' and payload.database_generation = "{current["generation"]}"')

    def configure(full=900000, path=source, gen=generation, profiles=True):
        current['generation'] = gen
        profile = '''[[chromium]]
profile = "{}"
database_generation = "{}"
path = "{}"
poll_interval_ms = 2500
full_interval_ms = {}
snapshot_interval_ms = 3600000
'''
        config.write_text(f'''listen = "{endpoint.removeprefix('http://').removesuffix('/rpc')}"
state_dir = "{work/'chromium-state'}"
store_dir = "{store}"
writer_connection_file = "{work/'writer'}"
reader_connection_file = "{work/'reader'}"
token_file = "{work/'token'}"
sources = ["browser.history.chromium"]
automata = []
''' + profile.format('main', gen, path, full) + (profile.format('other', other_generation, second, full) if profiles else ''))

    def status(profile='main'):
        key = [profile, current['generation'] if profile == 'main' else other_generation]
        return next((s for s in api('sources.list')['result']['sources'] if json.loads(s['locator']) == key), None)

    def settled(after=None, kind=None, profile='main', state='completed'):
        """The profile's source status once its latest read has `state` (and plan `kind`), and is not `after`."""
        s = status(profile)
        if not s:
            return None
        o = s['last_run']
        plan_kind = o['metadata'].get('browser_read_plan_v1', {}).get('kind')
        scheduled = o['metadata'].get('_sinex_acquisition_v1', {}).get('schedule')
        return s if (o['status'] == state and o['id'] != after and (kind is None or plan_kind == kind)
                     and (state != 'completed' or scheduled)) else None

    def initial_full(profile):
        """The first completed full read of a profile, searching back through its read history."""
        latest = status(profile)
        if not latest:
            return None
        page = latest
        while True:
            for o in [page['last_run']]+page.get('history', {}).get('rows', []):
                if (o['status'] == 'completed' and o['metadata'].get('browser_read_plan_v1', {}).get('kind') == 'full_value_comparison'
                        and o['metadata'].get('_sinex_acquisition_v1', {}).get('schedule')):
                    return dict(latest, last_run=o)
            before = page.get('history', {}).get('next_before_seq')
            if before is None:
                return None
            page = api('sources.list', {'input': latest['input'], 'before_seq': before})['result']['sources'][0]

    def until(label, fn, seconds=600, context=None):
        return wait(daemon, label, fn, seconds, context)

    def next_poll(s, kind=None, **kw):
        return until(f'next completed read ({kind})', lambda: settled(s['last_run']['id'], kind, **kw))

    def plan(s):
        return s['last_run']['metadata']['browser_read_plan_v1']

    def sql(text):
        return json.loads(run(['psql', h['reader'], '-XAt', '-v', 'ON_ERROR_STOP=1', '-c', text], label='Chromium database read').stdout)

    def query(text, cursor=None):
        value = api('query.run', {'query': text, **({'cursor': cursor} if cursor else {})})
        assert 'result' in value, value
        return value['result']

    def one_visit(i):
        # URLs repeat on purpose; the visit ID is what identifies a visit.
        when = dt.datetime.fromtimestamp((BASE+i*1000001)/1000000, tz=dt.timezone.utc).isoformat().replace('+00:00', 'Z')
        return next(r for r in query(visit_query()+f' and when = "{when}"')['rows'] if r['payload']['visit_id'] == i)

    def one_download(i):
        answer = query(download_query()+f' and payload.guid = "{uuid.UUID(int=i)}"')
        assert len(answer['rows']) == 1, answer
        return answer['rows'][0]

    def provider_digest(path):
        # Run in another process: closing a file descriptor here would drop the test's own SQLite locks.
        program = ('import hashlib,json,sys;from pathlib import Path;p=Path(sys.argv[1]);'
                   'print(json.dumps([hashlib.sha256(p.read_bytes()).hexdigest(),hashlib.sha256(Path(str(p)+"-journal").read_bytes()).hexdigest()]))')
        return json.loads(run([sys.executable, '-c', program, path], label='digest of the locked database').stdout)

    def assert_locked(path):
        program = 'import sqlite3,sys;c=sqlite3.connect("file:"+sys.argv[1]+"?mode=ro",uri=True,timeout=0);print(c.execute("SELECT count(*) FROM visits").fetchone())'
        result = run([sys.executable, '-c', program, path], expected=1, label='direct read of the locked database')
        assert 'database is locked' in result.stderr, result.stderr

    def same_through_clients(text):
        """CLI and MCP give the same query, show and trace answers for the first row of `text`."""
        answer = cli('query', text)
        agent = tool('sinex_query', {'query': text})
        assert answer['rows'] == agent['rows'] and answer['total'] == agent['total']
        reference = 'sinex:ev/'+answer['rows'][0]['id']
        assert cli('show', reference)['row'] == tool('sinex_show', {'ref': reference})['row']
        traced = cli('trace', reference, '--bytes')['row']
        agent_trace = tool('sinex_trace', {'ref': reference})['row']
        assert 'record' not in agent_trace, 'MCP trace returned the stored row'
        assert all(traced[k] == agent_trace[k] for k in ['material', 'anchor', 'end'])
        assert traced['record']['_from']['table'] in ('visits', 'downloads')
        return answer, traced

    def mutate(statement, parameters=()):
        with sqlite3.connect(source) as c:
            c.execute(statement, parameters)
        c.close()

    def pause_listener(path):
        listener = socket.socket(socket.AF_UNIX)
        listener.bind(str(path))
        listener.listen(1)
        listener.settimeout(30)
        return listener

    def paused(listener):
        connection, _ = listener.accept()
        message = b''
        while not message.endswith(b'\n'):
            message += connection.recv(65536)
        return connection, json.loads(message)

    try:
        configure()
        daemon = start(config, 'chromium')
        until('startup', lambda: api().get('result'))
        assert {'sinex_query', 'sinex_show', 'sinex_trace'} <= clients.connect_mcp()

        def population():
            nonlocal first
            other_first = until('first full read of the other profile', lambda: initial_full('other'))
            first = until('first full read of the main profile', lambda: initial_full('main'))
            assert first['last_run']['rows_read'] == expected['visits']+expected['downloads'], first
            assert other_first['last_run']['rows_read'] == other['visits']+other['downloads']
            answer = query(visit_query())
            assert answer['total'] == expected['visits']-expected['redirects'], answer['total']
            ids = set()
            while True:
                for r in answer['rows']:
                    assert r['payload']['profile'] == 'main' and not r['payload']['redirect']
                    assert r['id'] not in ids
                    ids.add(r['id'])
                if not answer['page']['cursor']:
                    break
                answer = query(visit_query(), answer['page']['cursor'])
            assert len(ids) == expected['visits']-expected['redirects']
            assert query(visit_query()+' and payload.redirect = true')['total'] == expected['redirects']
            assert query(download_query())['total'] == expected['downloads']
            a = one_visit(1)
            assert a['payload']['duration_us'] == 0 and a['when'] == '2026-10-01T00:00:01.000001000Z'
            assert a['payload']['duration_basis'].endswith('attention is unknown')
            assert a['payload']['referrer_url'] == 'https://referrer.invalid/' and a['payload']['referrer_basis'] == 'provider external_referrer_url'
            with sqlite3.connect(source.as_uri()+'?mode=ro', uri=True) as c:
                stored = c.execute('SELECT v.id,v.visit_time,v.visit_duration,u.url,u.title FROM visits v JOIN urls u ON u.id=v.url WHERE v.id=1').fetchone()
            c.close()
            assert tuple(a['payload'][k] for k in ['visit_id', 'visit_time_us', 'duration_us', 'url', 'title']) == stored
            d = one_download(1)
            assert d['payload']['state_label'] == 'in_progress' and d['payload']['end_unix_us'] is None
            assert d['when'] is None and d['time_quality'] == 'unknown'
            assert d['payload']['url_chain'] == ['https://example.invalid/start/1', 'https://cdn.invalid/file/1.pdf']
            same_through_clients(visit_query()+' and payload.url = "https://example.invalid/page/1"')
            same_through_clients(download_query()+f' and payload.guid = "{uuid.UUID(int=1)}"')
            # Reduced reads reread only the zero-duration visit and the unfinished download.
            reduced = next_poll(first, 'new_keys_and_prior_unfinished')
            assert reduced['last_run']['rows_read'] == 2, reduced
            assert plan(reduced)['download_keys_examined'] == expected['downloads']
            assert reduced['last_run']['metadata']['browser_checkpoint_v1']['full_due_at'] == first['last_run']['metadata']['browser_checkpoint_v1']['full_due_at']
            assert 'physical_snapshot' not in reduced['last_run']['metadata']
        scenario('two profiles: first full read, paging, redirects, downloads, and the same answers through CLI and MCP', population)

        def updates():
            nonlocal daemon

            def ordinary_values(after, label, visit_duration=None, download_fields=None, download_key=1):
                """Wait for a reduced read that shows the expected values, and check that the
                rows came from a reduced read (a poll may have started before the change)."""
                target = after['last_run']['metadata']['browser_checkpoint_v1']['full_due_at']
                context = {'after': after['last_run']['id'], 'visit_duration': visit_duration, 'download': download_fields}

                def observed():
                    latest = settled(after['last_run']['id'], 'new_keys_and_prior_unfinished')
                    if not latest:
                        return None
                    assert latest['read_checkpoint']['full_due_at'] == target, context
                    visit = one_visit(1) if visit_duration is not None else None
                    rows = query(download_query()+f' and payload.guid = "{uuid.UUID(int=download_key)}"')['rows'] if download_fields else []
                    assert len(rows) <= 1, rows
                    download = rows[0] if rows else None
                    context.update(visit=visit, actual_download=download)
                    if visit_duration is not None and visit['payload']['duration_us'] != visit_duration:
                        return None
                    if download_fields and (download is None or any(download['payload'].get(k) != v for k, v in download_fields.items())):
                        return None
                    captures = []
                    for row in (visit, download):
                        if row is None:
                            continue
                        trace = cli('trace', 'sinex:ev/'+row['id'], '--bytes')['row']
                        detail = api('sources.list', {'input': latest['input'], 'observation': trace['material_observation']})['result']['sources'][0]
                        capture = detail['receipts']['observation']
                        assert capture['id'] == trace['material_observation'] and capture['status'] == 'completed', context
                        assert capture['metadata']['browser_read_plan_v1']['kind'] == 'new_keys_and_prior_unfinished', context
                        assert capture['metadata']['browser_checkpoint_v1']['full_due_at'] == target, context
                        captures.append(capture)
                    return dict(latest, last_run=captures[-1]), visit, download
                return until(label, observed, context=context)

            before, old = one_visit(1), one_download(1)
            s = until('a reduced read', lambda: settled(kind='new_keys_and_prior_unfinished'))
            mutate('UPDATE visits SET visit_duration=98000001 WHERE id=1')
            # The numeric download ID may change; the GUID still names the same download.
            with sqlite3.connect(source) as c:
                c.execute('UPDATE downloads SET id=1001,state=4,interrupt_reason=40,end_time=? WHERE id=1', (OFFSET+BASE+90000000,))
                c.execute('UPDATE downloads_url_chains SET id=1001 WHERE id=1')
            c.close()
            s, late_visit, interrupted = ordinary_values(s, 'late visit duration and interrupted download', 98000001, {'state': 4, 'download_id': 1001})
            assert late_visit['id'] != before['id'] and late_visit['payload']['duration_us'] == 98000001
            assert interrupted['id'] != old['id'] and interrupted['payload']['state_label'] == 'interrupted'
            assert interrupted['payload']['download_id'] == 1001
            unchanged = next_poll(s, 'new_keys_and_prior_unfinished')
            assert unchanged['last_run']['rows_read'] == 1
            assert one_download(1)['id'] == interrupted['id']
            due_at = unchanged['last_run']['metadata']['browser_checkpoint_v1']['full_due_at']
            mutate('UPDATE downloads SET state=0,received_bytes=2048,end_time=0 WHERE id=1001')
            resumed, _, running = ordinary_values(unchanged, 'resumed download', download_fields={'state': 0, 'received_bytes': 2048, 'download_id': 1001})
            assert resumed['last_run']['rows_read'] == 1, resumed
            assert running['id'] != interrupted['id'] and running['payload']['state_label'] == 'in_progress' and running['payload']['received_bytes'] == 2048
            same_through_clients(download_query()+f' and payload.guid = "{uuid.UUID(int=1)}"')
            mutate('UPDATE downloads SET state=1,received_bytes=4096,end_time=? WHERE id=1001', (OFFSET+BASE+120000000,))
            completed, _, done = ordinary_values(resumed, 'completed download', download_fields={'state': 1, 'received_bytes': 4096, 'download_id': 1001})
            assert completed['last_run']['rows_read'] == 1, completed
            assert done['id'] != running['id'] and done['payload']['state_label'] == 'complete' and done['payload']['received_bytes'] == 4096
            assert query(download_query())['total'] == expected['downloads']
            assert all(v['last_run']['metadata']['browser_checkpoint_v1']['full_due_at'] == due_at for v in (resumed, completed))
            same_through_clients(download_query()+f' and payload.guid = "{uuid.UUID(int=1)}"')
            empty = next_poll(completed, 'new_keys_and_prior_unfinished')
            assert empty['last_run']['rows_read'] == 0
            # A new GUID on a reused numeric ID is a new download; the old one is not deleted.
            replacement = str(uuid.UUID(int=10001))
            mutate('UPDATE downloads SET guid=?,state=1 WHERE id=2', (replacement,))
            fresh, _, _ = ordinary_values(empty, 'new GUID on a reused ID', download_fields={'guid': replacement, 'state': 1, 'download_id': 2}, download_key=10001)
            assert fresh['last_run']['rows_read'] == 1, fresh
            assert query(download_query()+f' and payload.guid = "{replacement}"')['total'] == 1
            assert query(download_query()+f' and payload.guid = "{uuid.UUID(int=2)}"')['total'] == 1
            # A completed visit's later edit is not seen by reduced reads...
            original = one_visit(3)
            mutate('UPDATE visits SET visit_duration=5555555 WHERE id=3')
            next_poll(fresh, 'new_keys_and_prior_unfinished')
            assert one_visit(3)['id'] == original['id']
            # ...but by the full read after a restart, and by the periodic full read.
            clients.close_mcp()
            stop(daemon)
            configure(full=12000)
            daemon = start(config, 'chromium-full')
            until('full read after restart', lambda: settled(kind='full_value_comparison'))
            clients.connect_mcp()
            until('edit seen by the full read', lambda: one_visit(3)['payload']['duration_us'] == 5555555)
            original = one_visit(4)
            mutate('UPDATE visits SET visit_duration=7777777 WHERE id=4')
            until('edit seen by the periodic full read', lambda: one_visit(4)['payload']['duration_us'] == 7777777)
            assert one_visit(4)['id'] != original['id']
        scenario('late visit duration, interrupted, resumed and completed download, new GUID, and full rereads', updates)

        def stored_after_loss():
            nonlocal daemon
            baseline, trace = same_through_clients(download_query()+f' and payload.guid = "{uuid.UUID(int=1)}"')
            snapshot = first['last_run']['metadata']['physical_snapshot']
            digest = snapshot['blake3'].removeprefix('blake3:')
            blob = store/'blobs'/digest[:2]/(digest+'.zst')
            decoded = work/'decoded-History'
            with decoded.open('wb') as out:
                assert subprocess.run(['zstd', '-q', '-d', '-c', str(blob)], stdout=out, env=env).returncode == 0
            with sqlite3.connect(decoded.as_uri()+'?mode=ro', uri=True) as c:
                assert c.execute('PRAGMA quick_check').fetchall() == [('ok',)]
                assert c.execute('SELECT count(*) FROM visits').fetchone()[0] == 50000
                assert c.execute('SELECT state FROM downloads WHERE id=1').fetchone()[0] == 0, 'snapshot is not the first-read state'
            c.close()
            decoded.unlink()
            clients.close_mcp()
            stop(daemon)
            source.rename(work/'History-moved')
            second.rename(work/'OtherHistory-moved')
            daemon = start(config, 'chromium-source-missing')
            until('source reported unavailable', lambda: settled(state='unavailable'))
            clients.connect_mcp()
            missing, missing_trace = same_through_clients(download_query()+f' and payload.guid = "{uuid.UUID(int=1)}"')
            assert missing['rows'] == baseline['rows'] and missing['complete'] is False
            assert missing_trace['record'] == trace['record']
            clients.close_mcp()
            stop(daemon)
            (work/'History-moved').rename(source)
            (work/'OtherHistory-moved').rename(second)
        scenario('the first-read snapshot decodes to the original database; stored rows answer when the file is gone', stored_after_loss)

        def locked():
            nonlocal daemon
            locked_path = work/'LockedHistory'
            create(locked_path, 120, 4)
            con = sqlite3.connect(locked_path)
            con.execute('PRAGMA locking_mode=EXCLUSIVE')
            con.execute('PRAGMA cache_size=1')
            con.execute('BEGIN EXCLUSIVE')
            con.execute('UPDATE visits SET visit_duration=999999999')
            assert_locked(locked_path)
            before = provider_digest(locked_path)
            assert_locked(locked_path)
            configure(path=locked_path, gen='77d3d942-c8a4-444e-8016-29d2e312664c', profiles=False)
            daemon = start(config, 'chromium-locked')
            try:
                s = until('full read of the locked database', lambda: settled(kind='full_value_comparison'))
                assert plan(s)['custody']['kind'] == 'verified_locked_rollback_copy', s
                assert query(visit_query())['rows'][0]['payload']['duration_us'] == 0, 'read the uncommitted change'
                quiet = next_poll(s, 'new_keys_and_prior_unfinished')
                assert plan(quiet)['custody']['kind'] == 'verified_locked_copy_reused'
                assert provider_digest(locked_path) == before, 'the locked database or its journal changed'
                assert_locked(locked_path)
            finally:
                stop(daemon)
                con.rollback()
                con.close()
            # The journal disappears while the daemon copies: the copy must be discarded and retried.
            con = sqlite3.connect(locked_path)
            con.execute('PRAGMA locking_mode=EXCLUSIVE')
            con.execute('BEGIN EXCLUSIVE')
            con.execute('UPDATE visits SET visit_duration=888888888')
            boundary = work/'browser-copy.sock'
            listener = pause_listener(boundary)
            daemon = start(config, 'chromium-copy-race', '--test-pause-at', 'browser-copy-before-check', '--test-pause-socket', boundary)
            try:
                connection, notice = paused(listener)
                assert notice['pause_point'] == 'browser-copy-before-check'
                con.rollback()
                con.close()
                connection.sendall(b'c')
                connection.close()
                s = until('retry after the journal disappeared', lambda: settled(kind='full_value_comparison'))
                assert plan(s)['custody']['attempts'] >= 2, plan(s)
                assert query(visit_query())['rows'][0]['payload']['duration_us'] == 0
            finally:
                listener.close()
                stop(daemon)
            # A journal appears while the daemon copies.
            con = sqlite3.connect(locked_path)
            con.execute('PRAGMA locking_mode=EXCLUSIVE')
            con.execute('BEGIN EXCLUSIVE')
            boundary.unlink()
            listener = pause_listener(boundary)
            daemon = start(config, 'chromium-journal-appears', '--test-pause-at', 'browser-copy-before-check', '--test-pause-socket', boundary)
            try:
                connection, notice = paused(listener)
                assert notice['evidence']['source_pair']['journal'] is None
                con.execute("UPDATE urls SET title='uncommitted journal appearance'")
                connection.sendall(b'c')
                connection.close()
                s = until('retry after a journal appeared', lambda: settled(kind='full_value_comparison'))
                assert plan(s)['custody']['attempts'] >= 2
                assert query(visit_query())['rows'][0]['payload']['title'] == 'Synthetic page 1'
            finally:
                listener.close()
                boundary.unlink()
                stop(daemon)
                con.rollback()
                con.close()
        scenario('a locked database is read through a checked copy; a copy that changes underneath is retried', locked)

        def partial():
            nonlocal daemon
            path = work/'PartialHistory'
            create(path, 192, 4)
            with sqlite3.connect(path) as c:
                c.execute('UPDATE urls SET title=? WHERE id=130', ('x'*1100000,))
            c.close()
            configure(path=path, gen='33d3d942-c8a4-444e-8016-29d2e312664c', profiles=False)
            daemon = start(config, 'chromium-partial')
            failed = until('read fails on the oversized row', lambda: settled(state='unavailable'))
            assert failed['last_run']['rows_read'] == 128 and 'before provider transfer' in failed['last_run']['reason'], failed
            assert 'browser_checkpoint_v1' not in failed['last_run']['metadata']
            carry = sql(f"SELECT carry FROM raw.source_cursors WHERE input='{failed['input']}' AND binding='browser.history.chromium'")
            assert carry == {}, 'a failed read advanced the cursor'
            old = failed['last_run']['id']
            visits_of_old = (f"SELECT coalesce(jsonb_agg(e.payload->'visit_id' ORDER BY (e.payload->>'visit_id')::bigint),'[]') FROM core.events e "
                             f"JOIN raw.source_material_registry m ON m.id=e.source_material_id WHERE m.observation_id='{old}'")
            assert sql(visits_of_old) == list(range(1, 129))
            with sqlite3.connect(path) as c:
                c.execute("UPDATE urls SET title='repaired title' WHERE id=130")
            c.close()
            recovered = until('full read after the fix', lambda: settled(kind='full_value_comparison'))
            assert recovered['last_run']['rows_read'] == 196 and recovered['last_run']['repeated'] == 128, recovered
            assert sql(visits_of_old) == list(range(1, 129)), 'the later read changed what the failed read admitted'
            seen, after = [], None
            while True:
                page = api('sources.list', {'input': failed['input'], 'observation': old, **({'after_receipt': after} if after else {})})['result']['sources'][0]['receipts']
                seen += [r['event'] for r in page['rows']]
                if page['complete']:
                    break
                after = page['next_after_receipt']
                assert after
            assert len(seen) == len(set(seen)) == 128
            stop(daemon)
        scenario('a read that fails partway keeps the rows it admitted, and the next full read repeats them', partial)

        def refusals():
            nonlocal daemon
            path = work/'GuardHistory'
            create(path, 30, 2)
            configure(path=path, gen='66d3d942-c8a4-444e-8016-29d2e312664c', profiles=False)
            daemon = start(config, 'chromium-guard')
            s = until('first full read', lambda: settled(kind='full_value_comparison'))
            initial_schema = s['read_checkpoint']['schema_version']
            with sqlite3.connect(path) as c:
                c.execute('ALTER TABLE visits ADD COLUMN extra_column INTEGER DEFAULT 9')
            c.close()
            altered = until('full read after a schema change', lambda: settled(s['last_run']['id'], 'full_value_comparison'))
            assert altered['read_checkpoint']['schema_version'] != initial_schema and altered['last_run']['rows_read'] == 32
            with sqlite3.connect(path) as c:
                c.execute('VACUUM')
            c.close()
            vacuum = until('full read after VACUUM', lambda: settled(altered['last_run']['id'], 'full_value_comparison'))
            assert vacuum['last_run']['rows_read'] == 32 and vacuum['read_checkpoint']['identity'] == altered['read_checkpoint']['identity']
            with sqlite3.connect(path) as c:
                c.execute('UPDATE downloads SET guid=? WHERE id=2', (str(uuid.UUID(int=1)),))
            c.close()
            duplicate = until('duplicate GUID refused', lambda: settled(state='unavailable'))
            assert duplicate['last_run']['rows_read'] == 0 and 'duplicate' in duplicate['last_run']['reason']
            assert duplicate['read_checkpoint'] == vacuum['read_checkpoint']
            with sqlite3.connect(path) as c:
                c.execute('UPDATE downloads SET guid=? WHERE id=2', (str(uuid.UUID(int=2)),))
            c.close()
            until('full read after the GUID is fixed', lambda: settled(duplicate['last_run']['id'], 'full_value_comparison'))
            replacement = work/'ReplacementHistory'
            create(replacement, 30, 2)
            path.rename(work/'OldGuardHistory')
            replacement.rename(path)
            failed = until('replaced file refused', lambda: settled(state='unavailable'))
            assert 'reconciliation required' in failed['last_run']['reason'], failed
            stop(daemon)
            daemon = start(config, 'chromium-guard-restart')
            fenced = until('replaced file still refused after restart', lambda: settled(state='unavailable'))
            assert 'fenced' in fenced['last_run']['reason'], fenced
            stop(daemon)
            # Declaring a new generation is how the replacement gets read.
            configure(path=path, gen='55d3d942-c8a4-444e-8016-29d2e312664c', profiles=False)
            daemon = start(config, 'chromium-new-generation')
            until('full read of the new generation', lambda: settled(kind='full_value_comparison'))
            with sqlite3.connect(path) as c:
                c.execute("UPDATE sqlite_sequence SET seq=0 WHERE name='visits'")
            c.close()
            reset = until('visit ID sequence reset refused', lambda: settled(state='unavailable'))
            assert 'reset' in reset['last_run']['reason'], reset
            stop(daemon)
            invalid = work/'UnsupportedHistory'
            with sqlite3.connect(invalid) as c:
                c.execute('CREATE TABLE visits(id INTEGER PRIMARY KEY)')
            c.close()
            configure(path=invalid, gen='44d3d942-c8a4-444e-8016-29d2e312664c', profiles=False)
            daemon = start(config, 'chromium-unsupported-schema')
            unsupported = until('unsupported schema refused', lambda: settled(state='unavailable'))
            assert unsupported['last_run']['rows_read'] == 0
        scenario('schema change and VACUUM reread; duplicate GUID, replaced file, ID reset and unsupported schema are refused', refusals)
    finally:
        if clients.mcp is not None and clients.mcp.proc.poll() is None:
            clients.close_mcp()
        if daemon is not None and daemon.poll() is None:
            stop(daemon)
