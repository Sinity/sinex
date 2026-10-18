"""Grouping of Atuin and zsh records into canonical shell commands, checked
against hand-written expected answers (fixtures/fusion/selected-journey.json)
through late rows, exclusion, replay, matching rules, restarts, zsh decoding and
privacy rules."""
import concurrent.futures
import copy
from datetime import datetime, timezone
import http.client
import json
from pathlib import Path
import sqlite3
import subprocess
import time
import uuid

from _shell import HISTORY_COLUMNS, Daemon, add_history, create_history, pause_listener, sql, update_history

# Copied from the spec repository, sinex-spec/prototype/fusion/selected-journey.json
# at b2373e95f054b327bb13fad357b74b9b06a207c4.
JOURNEY = json.loads((Path(__file__).parent/'fixtures/fusion/selected-journey.json').read_text())
RAW_ATUIN = 'events where source = terminal.atuin-history and kind = command.executed'
RAW_ZSH = 'events where source = terminal.zsh-history and kind = command.imported'
TIME_FIELDS = ['when', 'time_quality', 'time_precision', 'time_clock', 'time_until', 'time_suspect']


def expected_capture(stage_name):
    """The journey's literal expectation for one stage, with interpretation aliases filled in."""
    stage = next(s for s in JOURNEY['stages'] if s['name'] == stage_name)
    answer = copy.deepcopy(stage['expected'])
    answer['rows'] = [copy.deepcopy(JOURNEY['expected_rows'][key]) for key in stage['expected']['rows']]
    for row in answer['rows']:
        row['parent_interpretations'] = [stage['interpretations'][key] for key in row['parents']]
        for trace, key in zip(row['trace'], row['parents']):
            trace['interpretation'] = stage['interpretations'][key]
    return {'stage': stage_name, 'live_interpretations': stage['interpretations'],
            'excluded_occurrences': stage['excluded_occurrences'],
            'archived_interpretations': stage['archived_interpretations'], 'answer': answer}


def difference(expected, actual, path='capture'):
    """The first difference, with types compared strictly (1 is not True)."""
    if type(expected) is not type(actual):
        return f'{path}: expected {type(expected).__name__}, got {type(actual).__name__}'
    if isinstance(expected, dict):
        if expected.keys() != actual.keys():
            return f'{path}: missing {sorted(expected.keys() - actual.keys())}, extra {sorted(actual.keys() - expected.keys())}'
        return next((d for k in expected if (d := difference(expected[k], actual[k], f'{path}.{k}'))), None)
    if isinstance(expected, list):
        if len(expected) != len(actual):
            return f'{path}: expected {len(expected)} entries, got {len(actual)}'
        return next((d for i, (a, b) in enumerate(zip(expected, actual)) if (d := difference(a, b, f'{path}[{i}]'))), None)
    return None if expected == actual else f'{path}: expected {expected!r}, got {actual!r}'


def rfc3339(ns):
    seconds, nanos = divmod(ns, 1_000_000_000)
    return datetime.fromtimestamp(seconds, timezone.utc).strftime('%Y-%m-%dT%H:%M:%S') + f'.{nanos:09}Z'


def zsh_record(at_ns, command):
    """One extended-history record as zsh writes it: escaped newlines, sentinel space, metafied bytes."""
    wire = command.replace('\n', '\\\n')
    if wire.rstrip(' ').endswith('\\'):
        wire += ' '
    out = bytearray()
    for c in f': {at_ns // 1_000_000_000}:0;'.encode() + wire.encode() + b'\n':
        out.extend([0x83, c ^ 0x20] if c == 0 or c >= 0x80 else [c])
    return bytes(out)


def exercise(h):
    work, run, scenario = h['WORK'], h['run'], h['scenario']
    source, zsh, config = work/'atuin.db', work/'zsh-history', work/'grouping.toml'
    store = work/'store'
    daemon = Daemon(h, config)
    query, query_all, rpc = daemon.query, daemon.query_all, daemon.rpc
    observations = {o['key']: o for o in JOURNEY['observations']}
    question = JOURNEY['question']
    failed_question = (f'commands where project = harbor and exit_code != 0 and when >= {rfc3339(question["start_ns"])}'
                       f' and when < {rfc3339(question["end_ns"])}')
    zsh_written = {}  # observation key -> (native_id, record bytes)
    aliases, versions, first_trace = {}, {}, {}

    def cli(command, *args, expected=(0, 5), op=False):
        return daemon.cli(command, *args, expected=expected, operator=op)

    def atuin_row(o):
        return (o['native_key'], o['start_ns'], o['duration_ns'] or 1_000_000_000, -1 if o['exit'] is None else o['exit'],
                o['command'], o['cwd'] or '', 'synthetic-fusion-session', o['host'], None)

    def configure(collect=True, poll=3600000, rules='', zpath=None, urls=None, operator=True, extra_roots=()):
        paths = urls or {n: work/n for n in ['writer', 'reader', 'read-model']}
        config.write_text(f'''listen = "{daemon.address}"
state_dir = "{work/'state'}"
store_dir = "{store}"
writer_connection_file = "{paths['writer']}"
reader_connection_file = "{paths['reader']}"
read_model_connection_file = "{paths['read-model']}"
token_file = "{work/'token'}"
''' + (f'operator_token_file = "{work/"operator"}"\n' if operator else '') + f'''sources = {json.dumps(['terminal.atuin-history', 'terminal.zsh-history'] if collect else [])}
automata = []
[[projects]]
name = "harbor"
roots = {json.dumps(['/work/harbor', *extra_roots])}
''' + (f'''[atuin]
path = "{source}"
poll_interval_ms = {poll}
full_interval_ms = 100
[zsh]
path = "{zpath or zsh}"
execution_host = "fixture-host"
protocol = "append_only_extended_v1"
poll_interval_ms = {poll}
''' if collect else '') + rules)

    def launch(name, collect=True, **kw):
        daemon.stop()
        configure(collect, **kw)
        daemon.start(name)
        daemon.wait()

    def settled():
        def check(s):
            indexed = s['indexed_retrieval']
            return (indexed.get('serving') and indexed['position'] == s['commit_seq'] and indexed['pending_canonical'] == 0
                    and indexed['fusion']['phase'] == 'settled' and indexed['fusion']['failure'] is None)
        return daemon.wait(check, seconds=180)

    def collected():
        return daemon.wait(lambda _: len(sources := daemon.sources()) == 2
                           and all(x['last_run'] and x['last_run']['status'] == 'completed' for x in sources))

    def raw():
        return query_all(RAW_ATUIN)['rows'] + query_all(RAW_ZSH)['rows']

    def trace(ref, with_bytes=True):
        return cli('trace', ref, *(['--bytes'] if with_bytes else []))['row']

    def native_id(key):
        o = observations[key]
        return o['native_key'] if o['source'].endswith('atuin-history') else zsh_written[key][0]

    def material_bytes(material):
        """The stored source material, decompressed from the blob store."""
        info = sql(h, "SELECT jsonb_build_object('hash',b.blake3,'bytes',b.size_bytes) FROM raw.source_material_registry m "
                      f"JOIN core.blobs b ON b.id=m.blob_id WHERE m.id='{uuid.UUID(material)}'")
        digest = info['hash'].removeprefix('blake3:')
        data = subprocess.run(['zstd', '-dc', store/'blobs'/digest[:2]/(digest + '.zst')], capture_output=True, check=True, timeout=30).stdout
        assert len(data) == info['bytes']
        return data

    def check_raw(key, row):
        """A raw row matches its fixture observation and traces to the bytes that were written."""
        o = observations[key]
        assert row['source'] == o['source'] and row['payload']['command'] == o['command'] and row['payload']['timestamp_ns'] == o['start_ns']
        assert row['payload']['native_id'] == native_id(key)
        if row['id'] not in aliases:
            versions[key] = versions.get(key, 0) + 1
            aliases[row['id']] = f'{key}.v{versions[key]}'
        t = trace(row['id'])
        assert t['event'] == row['id'] and t['material'] == row['material'] and t['anchor'] == row['anchor']
        record = material_bytes(t['material'])[t['anchor']:t['end']]
        if row['source'].endswith('zsh-history'):
            assert record == zsh_written[key][1]
            assert t['record']['unmetafied_record'] == f": {o['start_ns'] // 1_000_000_000}:0;{o['command']}\n"
        else:
            decoded = json.loads(record)
            assert {c: decoded[c] for c in HISTORY_COLUMNS} == dict(zip(HISTORY_COLUMNS, atuin_row(o))), decoded
            assert decoded['_from']['table'] == 'history' and decoded['_from']['rowid'] > 0
        first_trace.setdefault(row['id'], t)

    def time_of(row):
        until = row['time_until']
        return {'at_ns': row['payload']['timestamp_ns'], 'quality': row['time_quality'], 'precision': row['time_precision'],
                'clock': row['time_clock'].title() if row['time_quality'] == 'captured' else None,
                'until_ns': None if until is None else int(datetime.fromisoformat(until).timestamp() * 1_000_000_000)}

    def capture(stage):
        """Read raw and grouped rows through the API, describe them in the journey's terms and compare."""
        raw_rows = raw()
        by_key = {}
        for r in raw_rows:
            key = next(k for k, o in observations.items() if r['source'] == o['source'] and r['payload']['native_id'] == native_id(k))
            by_key[key] = r
            check_raw(key, r)
        key_by_id = {r['id']: k for k, r in by_key.items()}
        answer = query_all('commands')
        assert answer['complete'], answer
        assert sql(h, "SELECT jsonb_agg(DISTINCT payload_schema_id) FROM core.live_events WHERE event_type='command.canonical'") == [5]
        rows = []
        for r in answer['rows']:
            p = r['payload']
            keys = sorted((key_by_id[i] for i in r['parents']), key=lambda k: (observations[k]['source'] != 'terminal.atuin-history', k))
            evidence, association = p['field_evidence'], p['association']
            for field in TIME_FIELDS:
                assert r[field] == by_key[key_by_id[evidence['time']]][field], field
            parents = trace(r['id'])['parents']
            assert {v['event'] for v in parents} == set(r['parents'])
            for v in parents:
                assert v == {k: x for k, x in first_trace[v['event']].items() if k != 'secret'}
            rows.append({
                'group': '+'.join(keys), 'parents': keys, 'owner_hour': p['owner_hour'], 'command': p['command'], 'cwd': p['cwd'],
                'exit': p['exit_code'],
                'field_origins': {name: key_by_id[evidence[field]] for name, field in [('command', 'command'), ('cwd', 'cwd'), ('exit', 'exit_code'), ('time', 'time')]},
                'association': {'basis': association['basis'], 'strategy': association['strategy'], 'ambiguous': association['ambiguous'],
                                'candidate_counts': {key_by_id[i]: n for i, n in association['candidate_counts'].items()}},
                'trace': [{'record': observations[k]['record'], 'source': by_key[k]['source'], 'native_key': observations[k]['native_key'],
                           'start_ns': by_key[k]['payload']['timestamp_ns'], 'time': time_of(by_key[k]),
                           'interpretation': aliases[by_key[k]['id']]} for k in keys],
                'time': time_of(r), 'parent_interpretations': [aliases[by_key[k]['id']] for k in keys]})
        rows.sort(key=lambda r: (r['owner_hour'], r['group']))
        archived = sql(h, "SELECT coalesce(jsonb_agg(a.id ORDER BY a.id),'[]') FROM audit.archived a JOIN core.events e ON e.id=a.id "
                          "WHERE e.source IN ('terminal.atuin-history','terminal.zsh-history')")
        deleted = sql(h, "SELECT coalesce(jsonb_agg(equivalence_key),'[]') FROM core.deleted_occurrences WHERE kind='exclude'")
        excluded = [k for k, o in observations.items()
                    if o['source'].endswith('atuin-history') and any(json.loads(d)[1] == o['native_key'] for d in deleted)]
        failed = query(failed_question)['rows']
        actual = {
            'stage': stage, 'live_interpretations': {k: aliases[r['id']] for k, r in by_key.items()},
            'excluded_occurrences': sorted(excluded), 'archived_interpretations': sorted(aliases[i] for i in archived),
            'answer': {
                'complete': answer['complete'], 'pending_owner_hours': [],
                'coverage': 'continuous-capture-unknown' if all(r['payload']['capture_coverage'] == 'unknown' for r in answer['rows']) else 'unexpected',
                'claim_scope': 'selected-policy-grouping' if all(r['payload']['claim_scope'] == 'selected-policy-grouping' for r in answer['rows']) else 'unexpected',
                'exact_execution_count': None if all(r['payload']['exact_execution_count'] is None for r in answer['rows']) else -1,
                'rows': rows, 'failed_groups': sorted('+'.join(key_by_id[i] for i in r['parents']) for r in failed),
                'failed_groups_exact': answer['complete']}}
        (h['REPORT']/f'{stage}.json').write_text(json.dumps(actual, ensure_ascii=False, indent=2) + '\n')
        mismatch = difference(expected_capture(stage), actual)
        assert mismatch is None, f'{stage}: {mismatch}'
        return by_key, answer

    create_history(source, [atuin_row(observations[k]) for k in ['a1', 'f1']], wal=True)
    content = b''
    for key in ['z1', 'fz']:
        record = zsh_record(observations[key]['start_ns'], observations[key]['command'])
        zsh_written[key] = (f'chain-record:{len(content)}', record)
        content += record
    zsh.write_bytes(content)

    def first_groups():
        launch('initial')
        collected()
        settled()
        capture('before-late')
    scenario('Atuin and zsh records group as expected', first_groups)

    def late_row():
        daemon.stop()
        add_history(source, atuin_row(observations['a2']))
        address = work/'late.sock'
        with pause_listener(address, 40) as listener:
            configure()
            proc = daemon.start('late-paused', '--test-pause-at', 'fusion-replacement-committed', '--test-pause-socket', address)
            conn, _ = listener.accept()
            conn.settimeout(10)
            with conn:
                notice = json.loads(conn.makefile('r').readline())
                assert notice['pid'] == proc.pid and notice['pause_point'] == 'fusion-replacement-committed'
                # The projection keeps up while the writer is paused.
                daemon.wait(lambda s: s['indexed_retrieval']['position'] == s['commit_seq'])
                pending = query_all('commands')
                assert pending['complete'] is False and pending['fusion']['current_now_settled'] is False
                assert pending['rows'], 'rows stay available while regrouping is pending'
                daemon.kill()
        launch('late-restart')
        collected()
        settled()
        capture('late-settled')
    scenario('a late Atuin row: answers stay available while pending; kill and restart finish regrouping', late_row)

    def clients():
        launch('without-sources', False)
        settled()
        _, answer = capture('late-settled')
        assert cli('query', 'commands')['rows'] == answer['rows']
        ref = 'sinex:ev/' + answer['rows'][0]['id']
        with daemon.mcp('mcp') as mcp:
            assert mcp.tool('sinex_query', {'query': 'commands', 'page': 100})['rows'] == answer['rows']
            assert mcp.tool('sinex_query', {'query': failed_question, 'page': 100})['rows'] == cli('query', failed_question)['rows'] == query(failed_question)['rows']
            assert mcp.tool('sinex_show', {'ref': ref})['row'] == cli('show', ref)['row']
            assert mcp.tool('sinex_trace', {'ref': ref})['row'] == trace(ref, False)
            found = mcp.tool('sinex_search', {'words': 'cargo', 'page': 100})
            assert found['rows'] == cli('search', 'cargo', '--page', '100')['rows']
            assert len(found['rows']) == 2 and all(r['snippet']['spans'] for r in found['rows'])
    scenario('API, CLI and MCP agree on grouped commands and the failed-command question', clients)

    def exclusion():
        members, answer = capture('late-settled')
        a1 = members['a1']
        material = trace(members['a2']['id'])['material']
        assert cli('delete', a1['id'], '--action-id', str(uuid.uuid4()), expected=4)['kind'] == 'refused'
        pair = next(r for r in answer['rows'] if len(r['parents']) == 2)
        refused = cli('delete', pair['id'], '--action-id', str(uuid.uuid4()), op=True, expected=4)
        assert 'canonical references are not followed' in refused['details']['reason']
        action = str(uuid.uuid4())
        deleted = cli('delete', a1['id'], '--action-id', action, op=True, expected=0)
        assert deleted['state'] == 'completed'
        settled()
        capture('excluded')
        assert cli('delete', a1['id'], '--action-id', action, op=True, expected=0) == deleted, 'repeating an action returns its outcome'
        archived = sql(h, "SELECT jsonb_agg(jsonb_build_object('event',e.id,'schema',e.payload_schema_id,'logged_type',l.event_type_id,"
                          "'registered_type',t.id)) FROM core.commit_log l JOIN core.events e ON e.id=l.event_id "
                          "JOIN core.event_types t ON t.name=e.event_type WHERE l.change=1 AND e.id IN ("
                          + ','.join(f"'{i}'" for i in deleted['archived']) + ')')
        assert {r['event'] for r in archived} == set(deleted['archived'])
        assert all(r['logged_type'] == r['registered_type'] for r in archived)
        assert any(r['schema'] == 5 and r['logged_type'] == 3 for r in archived), archived
        # With the source files gone, stored material still replays.
        source.rename(work/'atuin-moved.db')
        zsh.rename(work/'zsh-moved')
        launch('without-source-files', False)
        settled()
        selection = f'materials where id = {material}'
        replay = cli('replay', selection, '--action-id', str(uuid.uuid4()), op=True, expected=0)
        assert replay['state'] == 'completed' and replay['admitted'] == 2 and replay['processed'] == 3, replay
        settled()
        capture('replayed-excluded')
        undone = cli('delete', a1['id'], '--undo', '--action-id', str(uuid.uuid4()), op=True, expected=0)
        assert undone['state'] == 'completed'
        settled()
        capture('undo-only')
        assert cli('replay', selection, '--action-id', str(uuid.uuid4()), op=True, expected=0)['admitted'] == 3
        settled()
        capture('replayed-restored')
        for identity, old in first_trace.items():
            assert trace(identity) == (old if old.get('archive') else {**old, 'archive': trace(identity)['archive']})
        (work/'atuin-moved.db').rename(source)
        (work/'zsh-moved').rename(zsh)
    scenario('exclude an Atuin row, replay, undo and replay again without the source files', exclusion)

    def replay_checks():
        add_history(source, ('replay-binding', 10_000_000_000_000, 1, 0, 'echo replay-binding-original', '/work/harbor',
                             'replay-binding', 'fixture-host', None))
        launch('replay-initial')
        collected()
        settled()

        def current():
            return next(r for r in query_all(RAW_ATUIN)['rows'] if r['payload']['native_id'] == 'replay-binding')
        row = current()
        selected = trace(row['id'])['material']
        selection = f'materials where id = {selected}'
        launch('replay-without-sources', False)
        settled()
        preview = cli('replay', selection, '--dry-run', expected=0)
        cli('replay', selection, '--action-id', str(uuid.uuid4()), op=True, expected=0)
        settled()
        replayed = current()
        assert replayed['id'] != row['id']
        stale = cli('replay', selection, '--basis', preview['basis'], '--action-id', str(uuid.uuid4()), op=True, expected=4)
        assert stale['details']['admitted'] == 0 and 'preview basis changed' in stale['details']['reason']
        live_atuin = "SELECT jsonb_agg(id ORDER BY id) FROM core.live_events WHERE source='terminal.atuin-history' AND event_type='command.executed'"
        daemon.stop()
        update_history(source, "UPDATE history SET command='echo replay-binding-divergent' WHERE id='replay-binding'")
        launch('replay-source-changed')
        collected()
        settled()
        before = sql(h, live_atuin)
        refused = cli('replay', selection, '--action-id', str(uuid.uuid4()), op=True, expected=4)
        assert refused['details']['admitted'] == 0 and 'differs from the current occurrence reading' in refused['details']['reason'], refused
        assert sql(h, live_atuin) == before
        daemon.stop()
        update_history(source, "UPDATE history SET command='echo replay-binding-original' WHERE id='replay-binding'")
        launch('replay-source-restored')
        collected()
        settled()
        other = current()
        assert other['material'] != selected
        cli('replay', selection, '--action-id', str(uuid.uuid4()), op=True, expected=0)
        settled()
        replacement = current()
        assert replacement['material'] == selected and replacement['id'] != other['id']
        stored = sql(h, "SELECT jsonb_agg(jsonb_build_object('material',source_material_id,'key',equivalence_key,'hash',encode(record_hash,'hex'))) "
                        "FROM core.events WHERE id IN (" + ','.join(f"'{r['id']}'" for r in [row, replayed, other, replacement]) + ')')
        assert len({r['key'] for r in stored}) == 1 and len({r['hash'] for r in stored}) == 1 and len({r['material'] for r in stored}) == 2
    scenario('replay refuses a stale preview and a changed source row, and replays an equal row from older material', replay_checks)

    controls, next_start = {}, 20_000

    def control(name, atuin, zsh_rows, expected, host='fixture-host'):
        """Add Atuin and zsh records of one command at the given offsets; `expected` lists the groups by label."""
        nonlocal next_start
        base = next_start * 1_000_000_000
        next_start += 2000
        command = 'echo fusioncontrol-' + name
        add_history(source, *[(f'{name}-{label}', base + offset, 1, 0, command, '/work/harbor', 'control-session', host, None)
                              for label, offset in atuin])
        labels = {f'{name}-{label}': label for label, _ in atuin}
        with zsh.open('ab') as out:
            for label, offset in zsh_rows:
                labels[f'chain-record:{out.tell()}'] = label
                out.write(zsh_record(base + offset, command))
        controls[name] = {'labels': labels, 'expected': expected}

    def check_control(name):
        rows = query_all('commands where command ~ ' + json.dumps('fusioncontrol-' + name))['rows']
        native = {r['id']: r['payload']['native_id'] for r in raw()}
        labels = controls[name]['labels']
        actual = sorted(sorted(labels[native[i]] for i in r['parents']) for r in rows)
        expected = sorted(sorted(group) for group in controls[name]['expected'])
        assert actual == expected, (name, expected, actual)
        return rows

    def matching():
        control('global', [('a', 0), ('b', 1_600_000_000)], [('x', 1_000_000_000), ('y', 2_000_000_000)], [['a', 'x'], ['b', 'y']])
        control('one-two', [('a', 0)], [('x', 30_000_000_000), ('y', 40_000_000_000)], [['a'], ['x'], ['y']])
        control('two-one', [('a', 0), ('b', 10_000_000_000)], [('x', 40_000_000_000)], [['a'], ['b'], ['x']])
        control('one-one', [('a', 0)], [('x', 30_000_000_000)], [['a', 'x']])
        control('strong-first', [('a', 0), ('b', 35_000_000_000)], [('x', 0), ('y', 65_000_000_000)], [['a', 'x'], ['b', 'y']])
        control('two-inclusive', [('a', 2_000_000_000)], [('x', 0)], [['a', 'x']])
        control('two-plus-nano', [('a', 2_000_000_001)], [('x', 0)], [['a', 'x']])
        control('sixty-inclusive', [('a', 60_000_000_000)], [('x', 0)], [['a', 'x']])
        control('sixty-plus-nano', [('a', 60_000_000_001)], [('x', 0)], [['a'], ['x']])
        control('host', [('a', 0)], [('x', 0)], [['a'], ['x']], host='other-host')
        control('duplicates', [('a', 0)], [('x', 0), ('y', 0)], [['a', 'x'], ['y']])
        control('outer-trim', [('a', 0)], [('x', 0)], [['a', 'x']])
        control('inner-space', [('a', 0)], [('x', 0)], [['a'], ['x']])
        update_history(source, "UPDATE history SET command='  echo fusioncontrol-outer-trim  ' WHERE id='outer-trim-a'")
        update_history(source, "UPDATE history SET command='echo  fusioncontrol-inner-space' WHERE id='inner-space-a'")
        launch('matching')
        collected()
        settled()
        results = {name: check_control(name) for name in controls}
        assert results['two-inclusive'][0]['payload']['association']['strategy'] == 'window-2s'
        assert results['two-plus-nano'][0]['payload']['association']['strategy'] == 'window-60s-unique'
        # Identical zsh records at the same time stay distinct events.
        duplicates = [r for r in raw() if controls['duplicates']['labels'].get(r['payload']['native_id']) in ('x', 'y')
                      and r['source'] == 'terminal.zsh-history']
        assert len(duplicates) == 2 and len({r['id'] for r in duplicates}) == 2
    scenario('matching: shortest pairs, 2s and 60s windows, host, duplicates and whitespace', matching)

    def long_chain():
        # Alternating records 59s apart over more than three hours: every record
        # but the two ends has two candidates, so the unique 60s pass pairs none.
        count = 190
        atuin = [(f'a{i}', i * 118_000_000_000) for i in range(count // 2)]
        zsh_rows = [(f'z{i}', (i * 118 + 59) * 1_000_000_000) for i in range(count // 2)]
        control('hours', atuin, zsh_rows, [[label] for label, _ in atuin + zsh_rows])
        launch('long-chain')
        collected()
        settled()
        assert len(check_control('hours')) == count
        update_history(source, "UPDATE history SET deleted_at=1 WHERE id='hours-a40'")
        launch('long-chain-removal')
        collected()
        settled()
        controls['hours']['expected'].remove(['a40'])
        assert len(check_control('hours')) == count - 1
    scenario('a long chain of competing records stays unpaired; removing one regroups', long_chain)

    def progress():
        return sql(h, "SELECT jsonb_build_object('seed',(SELECT max(commit_seq) FROM core.events WHERE source='terminal.atuin-history' "
                      "AND payload->>'native_id'='fair-later'),'progress',(SELECT to_jsonb(p) FROM derivation.shell_progress p WHERE one),"
                      "'dirty',(SELECT count(*) FROM derivation.shell_dirty))")

    def fairness():
        daemon.stop()
        add_history(source, *[(f'fair-{n}', 900_000_000_000_000 + n * 1_000_000_000, 1, 0, f'echo fair-{n}', '/work/harbor',
                               'fair-session', 'fixture-host', None) for n in range(150)])
        address = work/'fair.sock'
        with pause_listener(address, 60) as listener:
            configure(poll=100)
            proc = daemon.start('fair', '--test-pause-at', 'fusion-replacement-committed', '--test-pause-socket', address)
            conn, _ = listener.accept()
            conn.settimeout(15)
            with conn:
                notice = json.loads(conn.makefile('r').readline())
                basis = notice['evidence']['basis']
                assert notice['pid'] == proc.pid, notice
                add_history(source, ('fair-later', 901_000_000_000_000, 1, 0, 'echo fair-later', '/work/harbor', 'fair-session', 'fixture-host', None))
                with sqlite3.connect(source) as con:
                    later_rowid = con.execute("SELECT rowid FROM history WHERE id='fair-later'").fetchone()[0]
                conn.sendall(b'c')

        def later_enqueued(_):
            for line in (h['REPORT']/'fair.stderr').read_text().splitlines():
                try:
                    message = json.loads(line)
                except ValueError:
                    continue
                if message.get('test_phase') == 'atuin-step-enqueued' and later_rowid in message['sqlite_rowids']:
                    return True
            return False
        # The new row is admitted while the regrouping pass that started before it is still running.
        status = daemon.wait(later_enqueued)
        fusion = status['indexed_retrieval']['fusion']
        assert fusion['basis'] == basis and fusion['phase'] != 'settled', status
        state = progress()
        assert state['progress']['basis'] == basis and state['progress']['phase'] != 'settled', state
        deadline = time.monotonic() + 30
        while state['seed'] is None:
            assert time.monotonic() < deadline, 'the late row was not admitted'
            time.sleep(.01)
            state = progress()
        assert state['seed'] > basis and state['progress']['basis'] == basis and state['progress']['phase'] != 'settled', state
        seed = state['seed']
        daemon.wait(lambda s: s['indexed_retrieval']['fusion']['last_evaluated'] >= seed)
        settled()
        state = progress()
        assert state['dirty'] == 0 and state['progress']['last_evaluated'] >= seed
        canonical = "SELECT jsonb_agg(id ORDER BY id) FROM core.live_events WHERE event_type='command.canonical'"
        before = sql(h, canonical)
        running = sql(h, "SELECT jsonb_agg(id ORDER BY id) FROM core.runs WHERE status='running'")
        assert running
        daemon.stop()
        shutdown = sql(h, "SELECT jsonb_build_object('runs',(SELECT jsonb_agg(jsonb_build_object('status',status,'ended',ended_at IS NOT NULL)) "
                          "FROM core.runs WHERE id IN (" + ','.join(f"'{i}'" for i in running) + ")),"
                          "'open_materials',(SELECT count(*) FROM raw.source_material_registry WHERE status='capturing'))")
        assert all(r['status'] == 'completed' and r['ended'] for r in shutdown['runs']) and shutdown['open_materials'] == 0, shutdown
        launch('fair-restart', False)
        settled()
        assert sql(h, canonical) == before
    scenario('new source rows are admitted while regrouping runs; clean shutdown and restart keep IDs', fairness)

    def head_race():
        raw_before = query_all(RAW_ATUIN)['rows']
        original = next(r for r in raw_before if r['payload']['native_id'] == 'hours-a90')
        selected = trace(next(r for r in raw_before if r['payload']['native_id'] == 'fair-later')['id'])['material']
        selection = f'materials where id = {selected}'
        assert original['material'] != selected
        preview = cli('replay', selection, '--dry-run', expected=0)
        assert preview['selected'] > 128
        before = query_all('commands')['rows']
        action = str(uuid.uuid4())
        daemon.stop()
        address = work/'head-race.sock'
        lost = {}

        def request():
            try:
                return rpc('replay.start', {'query': selection, 'action_id': action}, operator=True)
            except (OSError, http.client.HTTPException) as error:
                lost['error'] = repr(error)
                return None
        with pause_listener(address, 45) as listener:
            configure(False)
            proc = daemon.start('head-race', '--test-pause-at', 'replay-committed-0', '--test-pause-socket', address)
            daemon.wait()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(request)
                conn, _ = listener.accept()
                conn.settimeout(10)
                with conn:
                    frame = json.loads(conn.makefile('r').readline())
                    operation = frame['evidence']['operation']
                    assert frame['pid'] == proc.pid and operation['processed'] == 64 and operation['state'] == 'running'
                    daemon.wait(lambda s: s['indexed_retrieval']['serving'] and s['indexed_retrieval']['position'] == s['commit_seq']
                                and s['indexed_retrieval']['failure'] is None)
                    archived = {r['old'] for r in frame['evidence']['effects']['replacements'] if r['old']}
                    expected_ids = {r['id'] for r in before if not archived.intersection(r['parents'])}
                    pending = query_all('commands')
                    assert pending['total'] == len(expected_ids) and {r['id'] for r in pending['rows']} == expected_ids and pending['complete'] is False
                    # A command whose parent was just replaced is hidden from every query path but still navigable.
                    inactive = next(r for r in before if archived.intersection(r['parents']))
                    ref = inactive['id']
                    by_id = 'commands where id = ' + ref
                    hidden = query(by_id)
                    assert hidden['rows'] == [] and hidden['total'] == 0 and hidden['complete'] is False
                    direct = cli('query', by_id, expected=5)
                    assert direct['rows'] == [] and direct['total'] == 0
                    shown, navigation = cli('show', ref), trace(ref, False)
                    assert shown['row']['current_eligible'] is False and navigation['current_eligible'] is False
                    assert any(p['archive'] for p in navigation['parents'])
                    word = inactive['payload']['command'].strip().split()[0]
                    searched = rpc('search', {'query': word + ' and id = ' + ref, 'page': 20})['result']
                    assert searched['rows'] == [] and searched['total'] == 0 and searched['complete'] is False
                    searched = cli('search', word, '--query', 'and id = ' + ref, expected=5)
                    assert searched['rows'] == [] and searched['total'] == 0
                    with daemon.mcp('pending-mcp') as mcp:
                        mq = mcp.tool('sinex_query', {'query': by_id, 'page': 20})
                        ms = mcp.tool('sinex_search', {'words': word, 'query': 'and id = ' + ref, 'page': 20})
                        assert mq['rows'] == ms['rows'] == [] and mq['total'] == ms['total'] == 0 and mq['complete'] is ms['complete'] is False
                        assert mcp.tool('sinex_show', {'ref': ref})['row'] == shown['row'] and mcp.tool('sinex_trace', {'ref': ref})['row'] == navigation
                    stored = sql(h, "SELECT jsonb_build_object('live',EXISTS(SELECT 1 FROM core.live_events WHERE id='" + ref + "'),"
                                    "'indexed',EXISTS(SELECT 1 FROM core.search_text s JOIN derivation.read_model_position p ON p.generation=s.generation WHERE s.id='" + ref + "'),"
                                    "'replaced_parents',(SELECT count(*) FROM core.events e,unnest(e.source_event_ids) parent(id) WHERE e.id='" + ref + "' "
                                    "AND NOT EXISTS(SELECT 1 FROM core.live_events raw WHERE raw.id=parent.id AND raw.record_state='present')))")
                    assert stored['live'] and stored['indexed'] and stored['replaced_parents'] > 0, stored
                    daemon.kill()
                assert future.result(timeout=20) is None and lost, 'the replay request should lose its connection'
        # Without the operator token the paused replay stays parked while the
        # source changes the row and changes it back.
        for phase, command in [('changed', 'echo head-race-temporary'), ('equal', original['payload']['command'])]:
            update_history(source, 'UPDATE history SET command=? WHERE id=?', command, 'hours-a90')
            launch('without-operator-' + phase, operator=False)
            collected()
            settled()
            assert rpc('status')['result']['replay_authorized'] is False
            parked = sql(h, f"SELECT progress FROM core.operations_log WHERE id='{operation['operation_id']}'")
            assert parked['processed'] == 64 and parked['state'] == 'running'
        launch('operator-again', False)

        def refused(_):
            reply = rpc('replay.status', {'operation_id': operation['operation_id']})
            return (reply.get('result') or reply.get('error', {}).get('data', {})).get('state') == 'refused'
        daemon.wait(refused)
        settled()
        outcome = cli('replay', selection, '--action-id', action, op=True, expected=4)['details']
        assert 64 <= outcome['processed'] < preview['selected'] and outcome['admitted'] >= 64 and 'head changed' in outcome['reason'], outcome
        current = next(r for r in query_all(RAW_ATUIN)['rows'] if r['payload']['native_id'] == 'hours-a90')
        assert current['id'] != original['id']
        rows = sql(h, "SELECT jsonb_agg(jsonb_build_object('id',id,'hash',encode(record_hash,'hex'),'archived',EXISTS(SELECT 1 FROM audit.archived a WHERE a.id=e.id))) "
                      f"FROM core.events e WHERE id IN ('{original['id']}','{current['id']}')")
        assert len({r['hash'] for r in rows}) == 1 and next(r for r in rows if r['id'] == current['id'])['archived'] is False
        kept = sql(h, f"SELECT jsonb_build_object('effects',(SELECT count(*) FROM core.operation_effects WHERE operation_id='{operation['operation_id']}'),"
                      f"'progress',(SELECT progress FROM core.operations_log WHERE id='{operation['operation_id']}'))")
        assert kept['effects'] == outcome['steps'] and kept['progress'] == outcome
        fresh = cli('replay', selection, '--action-id', str(uuid.uuid4()), op=True, expected=0)
        assert fresh['state'] == 'completed' and fresh['processed'] == preview['selected']
        settled()
    scenario('a paused replay is refused after its source row changes and keeps its committed part', head_race)

    def rebuild():
        launch('rebuild-before', False)
        settled()
        before = query_all('commands')
        generation = rpc('status')['result']['indexed_retrieval']['generation']
        launch('rebuild', False, extra_roots=['/synthetic/rebuild-one'])
        settled()
        after = query_all('commands')
        assert rpc('status')['result']['indexed_retrieval']['generation'] != generation
        assert before['rows'] == after['rows'] and before['total'] == after['total']
        log = sql(h, f"SELECT to_jsonb(l) FROM core.commit_log l WHERE l.event_id='{before['rows'][0]['id']}' AND l.change=0")
        daemon.stop()
        sql(h, f"DELETE FROM core.commit_log WHERE commit_seq={log['commit_seq']}")
        configure(False, extra_roots=['/synthetic/rebuild-two'])
        daemon.start('missing-log')
        daemon.wait(lambda s: s['indexed_retrieval']['failure'] is not None)
        refused = rpc('query.run', {'query': 'commands'})
        assert refused['error']['kind'] == 'partial' and 'result' not in refused
        refused = cli('query', 'commands', expected=5)
        assert refused['kind'] == 'partial' and 'rows' not in refused
        raw_answer = query_all(RAW_ATUIN)
        assert raw_answer['total'] > 128 and raw_answer['complete'] is True, 'raw reads do not need the log'
        daemon.stop()
        sql(h, "INSERT INTO core.commit_log(commit_seq,event_id,change,store,event_type_id) VALUES("
               f"{log['commit_seq']},'{log['event_id']}',{log['change']},{log['store']},{log['event_type_id']})")
        launch('log-restored', False)
        settled()
        restored = query_all('commands')
        assert restored['rows'] == before['rows'] and restored['total'] == before['total']
    scenario('a projection rebuild keeps rows; a missing log row makes answers partial', rebuild)

    def zsh_records():
        unicode_command = 'printf zażółć\nnext'
        original = zsh.read_bytes()
        offset = len(original)
        record = zsh_record(950_000_000_000_000, unicode_command)
        # What zsh 5.9 `fc -W` writes, and `fc -R` reads back: a trailing
        # backslash, with or without spaces, gets one sentinel space.
        codec = [('echo slash\\', b'echo slash\\ '), ('echo slashspace\\ ', b'echo slashspace\\  '),
                 ('echo slashspaces\\  ', b'echo slashspaces\\   '), ('echo slashtab\\\t', b'echo slashtab\\\t'),
                 ('echo slashtabspace\\\t ', b'echo slashtabspace\\\t '), ('echo slashnewline\\\nnext', b'echo slashnewline\\\\\nnext'),
                 ('echo plainspace  ', b'echo plainspace  ')]
        codec_bytes = b''.join(b': 954000:0;' + wire + b'\n' for _, wire in codec)
        full = original + record + zsh_record(951_000_000_000_000, 'echo ghp_' + 'A'*36) + codec_bytes
        zsh.write_bytes(full)
        launch('zsh-unicode')
        collected()
        settled()
        row = sql(h, "SELECT jsonb_build_object('id',id,'material',source_material_id,'anchor',anchor_byte,'end',offset_end,'payload',payload) "
                     f"FROM core.live_events WHERE source='terminal.zsh-history' AND payload->>'native_id'='chain-record:{offset}'")
        assert row['payload']['command'] == unicode_command
        unicode_trace = trace(row['id'])
        assert material_bytes(row['material'])[row['anchor']:row['end']] == record
        stored = sql(h, "SELECT jsonb_agg(jsonb_build_object('id',id,'command',payload->>'command','anchor',anchor_byte,'end',offset_end,"
                        "'material',source_material_id) ORDER BY anchor_byte) FROM core.live_events "
                        "WHERE source='terminal.zsh-history' AND ts_orig=to_timestamp(954000)")
        assert [r['command'] for r in stored] == [command for command, _ in codec], stored
        for r, (_, wire) in zip(stored, codec):
            assert material_bytes(r['material'])[r['anchor']:r['end']] == b': 954000:0;' + wire + b'\n'
        # Raw zsh reads support time equality, not ranges.
        time_range = RAW_ZSH + f' and when >= {rfc3339(954_000_000_000_000)} and when < {rfc3339(954_001_000_000_000)}'
        refused = rpc('query.run', {'query': time_range})
        assert refused['error']['kind'] == 'bad_query' and 'result' not in refused
        same_time = RAW_ZSH + ' and when = ' + rfc3339(954_000_000_000_000)
        for answer in [query_all(same_time), cli('query', same_time)]:
            assert answer['total'] == len(codec)
            rows = sorted(answer['rows'], key=lambda r: r['anchor'])
            assert [r['id'] for r in rows] == [r['id'] for r in stored]
            assert [r['payload']['command'] for r in rows] == [command for command, _ in codec]
        assert 'A'*30 not in json.dumps(cli('query', RAW_ZSH))
        secret = sql(h, "SELECT to_jsonb(id) FROM core.live_events WHERE source='terminal.zsh-history' AND payload->>'command' LIKE 'echo ghp_%' LIMIT 1")
        assert 'A'*30 not in json.dumps(trace(secret))

        def zsh_unavailable(path=zsh):
            daemon.wait(lambda _: any(x['locator'] == str(path) and x['last_run']['status'] == 'unavailable' for x in daemon.sources()))
        # A rewritten file stops reading for good, even when the old bytes come back.
        daemon.stop()
        zsh.write_bytes(b': 1:0;rewrite\n')
        launch('zsh-rewritten')
        zsh_unavailable()
        assert trace(row['id']) == unicode_trace
        daemon.stop()
        zsh.write_bytes(full)
        launch('zsh-restored')
        zsh_unavailable()
        positions = sql(h, "SELECT jsonb_agg(position) FROM raw.source_cursors WHERE binding='terminal.zsh-history'")
        assert any(p.get('chain_broken') for p in positions)
        assert trace(row['id']) == unicode_trace
        daemon.stop()
        bad = work/'zsh-malformed'
        bad_bytes = b'plain-command\n' + zsh_record(952_000_000_000_000, 'valid-after-malformed')
        bad.write_bytes(bad_bytes)

        def bad_source(status):
            return next((x for x in daemon.sources() if x['locator'] == str(bad) and x['last_run']['status'] == status), None)
        launch('zsh-malformed', zpath=bad)
        daemon.wait(lambda _: bad_source('completed'))
        malformed = bad_source('completed')
        assert malformed['last_run']['record_errors'] == 1 and malformed['last_run']['admitted'] == 1
        assert material_bytes(malformed['first_record_error']['material_id']).startswith(bad_bytes)
        daemon.stop()
        bad.write_bytes(bad_bytes + b': 953000:0;unfinished')
        launch('zsh-partial-record', zpath=bad)
        daemon.wait(lambda _: bad_source('unavailable'))
        assert bad_source('unavailable')['cursor']['offset'] == len(bad_bytes), 'a trailing partial record waits'
        daemon.stop()
        bad.write_bytes(bad.read_bytes() + b'\n')
        launch('zsh-record-finished', zpath=bad)
        daemon.wait(lambda _: bad_source('completed'))
        finished = bad_source('completed')
        assert finished['last_run']['record_errors'] == 0 and finished['cursor']['record_errors'] == 1
        assert finished['cursor']['offset'] == bad.stat().st_size
        answer = query_all(RAW_ZSH)
        status = next(x for x in answer['source_status'] if x['input'] == finished['input'])
        assert status['record_errors'] == 0 and status['retained_record_errors'] == 1 and answer['complete'] is False, 'old record errors keep coverage incomplete'
    scenario('zsh: stored bytes, Unicode, escapes, masking, rewrites and malformed records', zsh_records)

    def privacy():
        nonlocal store
        daemon.stop()
        urls = h['create_database']('shell_grouping_privacy')
        paths = {}
        for name, url in [('writer', urls['writer']), ('reader', urls['reader']),
                          ('read-model', h['url']('sinex_read_model', 'shell_grouping_privacy'))]:
            paths[name] = work/('privacy-' + name)
            paths[name].write_text(url)
        store = work/'privacy-store'
        with sqlite3.connect(source) as con:
            con.execute('DELETE FROM history')
        add_history(source, *[(f'private-{i}', 1_000_000_000_000_000 + i * 10_000_000_000, 1, 0, 'echo tok_' + s, '/work/harbor',
                               'privacy', 'fixture-host', None) for i, s in enumerate(['first', 'second'])])
        privacy_zsh = work/'privacy-zsh'
        privacy_zsh.write_bytes(zsh_record(1_000_000_000_000_000, 'echo [withheld]'))
        rules = '[[privacy_rules]]\npattern = "tok_[a-z]+"\nreplacement = "[withheld]"\ncreated_by = "operator"\n'
        configure(rules=rules, zpath=privacy_zsh, urls=paths)
        refused = run([h['BIN_DIR']/'sinexd', 'serve', '--config', config], expected=1, label='privacy rules with the zsh source')
        assert 'privacy' in refused.stderr
        # Admit Atuin rows with a privacy rule, Atuin only; a later config without
        # rules cannot group them with zsh records.
        text = config.read_text().replace('"terminal.atuin-history", "terminal.zsh-history"', '"terminal.atuin-history"')
        config.write_text(text[:text.index('[zsh]')] + rules)
        daemon.start('privacy-atuin-only')
        daemon.wait(lambda _: any(x['last_run'] and x['last_run']['status'] == 'completed' for x in daemon.sources()))
        masked = query_all(RAW_ATUIN)
        assert masked['total'] == 2 and {r['payload']['command'] for r in masked['rows']} == {'echo [withheld]'}
        material = trace(masked['rows'][0]['id'])['material']
        launch('privacy-without-rules', zpath=privacy_zsh, urls=paths)
        blocked = daemon.wait(lambda s: s['indexed_retrieval']['fusion']['failure'] is not None)
        unavailable = query_all('commands')
        assert unavailable['complete'] is False and 'comparison unavailable' in unavailable['fusion']['failure']
        time.sleep(.2)
        assert rpc('status')['result']['indexed_retrieval']['fusion']['failure'] == blocked['indexed_retrieval']['fusion']['failure'], 'later polls keep the failure'
        restored = cli('replay', f'materials where id = {material}', '--action-id', str(uuid.uuid4()), op=True, expected=0)
        assert restored['admitted'] == 2
        settled()
        answer = query_all('commands')
        assert answer['total'] == 3 and all(len(r['parents']) == 1 for r in answer['rows']), answer
        daemon.stop()
    scenario('Atuin rows admitted under privacy rules do not group until replayed without them', privacy)
