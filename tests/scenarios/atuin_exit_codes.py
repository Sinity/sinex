"""Atuin exit codes across the signed 64-bit range: query, show, trace and indexed
filters agree; a late exit code, restart, source loss, delete/undo with replay and a
privacy rule keep the stored values and their citations."""
import json
import sqlite3
import time

from _atuin_daemon import Daemon, create_history, sql

STATUSES = [-(2**63), -2, -1, 0, 1, 2**63-1]
EPOCH = 1_759_276_800_000_000_000
RAW = 'events where source = terminal.atuin-history'


def exercise(h):
    work, scenario = h['WORK'], h['scenario']
    read_model, operator = work/'read-model', work/'operator-token'
    read_model.write_text(h['url']('sinex_read_model'))
    operator.write_text('synthetic-operator-token')
    for path in (read_model, operator):
        path.chmod(0o600)
    # The zsh line matches status-1 (2s after EPOCH), so that command is seen by both sources.
    zsh = work/'history.zsh'
    zsh.write_text(f': {EPOCH//1_000_000_000 + 2}:0;echo signed 1\n')
    source = work/'atuin.db'
    facts = [(f'status-{i}', EPOCH + i*2_000_000_000, 7, x, f'echo signed {i}', '/synthetic/signed', 's', 'h:user', None)
             for i, x in enumerate(STATUSES)]
    facts += [('open', EPOCH + 20_000_000_000, -1, -1, 'echo open', '/synthetic/signed', 's', 'h:user', None),
              ('signed-no-duration', EPOCH + 22_000_000_000, -1, -2, 'echo no duration', '/synthetic/signed', 's', 'h:user', None)]
    create_history(source, facts)
    daemon = Daemon(h, 'exit-codes')
    top = f'read_model_connection_file = "{read_model}"\noperator_token_file = "{operator}"\n'
    projects = '[[projects]]\nname = "signed"\nroots = ["/synthetic/signed"]\n'

    def configure(collect=True, privacy=''):
        if collect:
            daemon.configure(source, poll_ms=500, full_ms=5000, sources=['terminal.atuin-history', 'terminal.zsh-history'], top=top,
                             tables=projects + f'[zsh]\npath = "{zsh}"\nexecution_host = "h"\nprotocol = "append_only_extended_v1"\npoll_interval_ms = 500\n')
        else:
            daemon.configure(top=top, tables=projects + privacy)

    def cli(command, *args, operator_token=False):
        return daemon.cli(command, *args, expected=(0, 5), token_file=operator if operator_token else None)

    def wait(label, predicate):
        return daemon.wait(label, predicate, 120)

    def raw():
        return daemon.api('query.run', {'query': RAW}).get('result')

    def settled(n):
        q = raw()
        return q if q and q['total'] == n and q['fusion']['current_now_settled'] else None

    def latest_full():
        s = daemon.sources()
        run = next(x for x in s if x['source'] == 'terminal.atuin-history')['last_run']
        return run if run['status'] == 'completed' and run['rows_read'] == 8 and run['record_errors'] == 0 else None

    refs = {}
    configure()
    daemon.start('exit-codes')
    try:
        def values():
            wait('first full read without record errors', latest_full)
            q = wait('eight events', lambda: settled(8))
            by_id = {r['payload']['native_id']: r for r in q['rows']}
            for key, _, duration, status, *_ in facts:
                payload = by_id[key]['payload']
                expected = None if status == -1 else status
                assert payload['exit_code'] == expected and payload['duration_ns'] == (None if duration == -1 else duration), payload
                assert payload['completion'] == ('end_not_recorded' if duration == -1 and status == -1 else 'ended'), payload
                ref = 'sinex:ev/' + by_id[key]['id']
                assert cli('show', ref)['row']['payload']['exit_code'] == expected
                trace = cli('trace', ref, '--bytes')
                assert trace['row']['material'] and trace['row']['end'] > trace['row']['anchor']
                refs[key] = (ref, trace)
            for status in STATUSES:
                if status == -1:
                    continue
                result = cli('query', f'{RAW} and payload.exit_code = {status}')
                assert all(r['payload']['exit_code'] == status for r in result['rows'])
                assert result['total'] == (2 if status == -2 else 1), result
                indexed = cli('query', f'commands where project = signed and exit_code = {status}')
                assert indexed['total'] == result['total'], indexed
                assert all(r['payload']['exit_code'] == status for r in indexed['rows']), indexed
                if status == -2:
                    paired = next(r for r in indexed['rows'] if r['payload']['command'] == 'echo signed 1')
                    assert len(cli('trace', 'sinex:ev/' + paired['id'])['row']['parents']) == 2
                    assert paired['payload']['observed_by'] == ['terminal.atuin-history', 'terminal.zsh-history'], paired
            before = {k: r['id'] for k, r in by_id.items()}
            time.sleep(1.1)
            assert {r['payload']['native_id']: r['id'] for r in raw()['rows']} == before, 'repeated reads changed event ids'
            assert cli('query', 'commands where project = signed and exit_code = 0')['total'] == 1
        scenario('exit codes from -2^63 to 2^63-1 read back the same through query, show, trace and commands', values)

        def late():
            with sqlite3.connect(source) as c:
                c.execute("UPDATE history SET exit=-2 WHERE id='open'")
            changed = wait('late exit code on the open row', lambda: next(
                (r for r in raw()['rows'] if r['payload']['native_id'] == 'open' and r['payload']['exit_code'] == -2), None))
            assert changed['id'] != refs['open'][0].split('/')[-1]
            assert changed['payload']['completion'] == 'ended' and changed['payload']['duration_ns'] is None
            trace = cli('trace', refs['open'][0], '--bytes')
            for field in ['material', 'input', 'material_observation', 'anchor', 'end', 'representation', 'record']:
                assert trace['row'][field] == refs['open'][1]['row'][field], f'late change altered the old citation: {field}'
            assert trace['resolved'] == 'archived', trace
            full = wait('full read after the late change', latest_full)
            material = sql(h, f"SELECT to_json(id) FROM raw.source_material_registry WHERE observation_id='{full['id']}'::uuid "
                              "AND durable_bytes>0 ORDER BY id LIMIT 1")
            assert material
            before = raw()
            daemon.restart('exit-codes-restart')
            wait('eight events after restart', lambda: settled(8))
            assert raw()['rows'] == before['rows']
            source.rename(work/'moved-away.db')
            daemon.restart('exit-codes-source-unavailable')
            wait('source unavailable', lambda: any(s['last_run']['status'] == 'unavailable' for s in daemon.sources()))
            after = daemon.cli('query', RAW, expected=5)
            assert after['rows'] == before['rows'] and after['complete'] is False
            assert cli('trace', refs['status-0'][0], '--bytes') == refs['status-0'][1]
            # Deleting one event survives replay of its material until the delete is undone.
            negative = next(r for r in after['rows'] if r['payload']['native_id'] == 'status-1')
            ref = 'sinex:ev/' + negative['id']
            cli('delete', ref, operator_token=True)
            wait('seven events after delete', lambda: settled(7))
            cli('replay', 'materials where id = ' + material, operator_token=True)
            assert raw()['total'] == 7, 'replay brought back a deleted event'
            wait('seven events after replay', lambda: settled(7))
            cli('delete', '--undo', ref, operator_token=True)
            cli('replay', 'materials where id = ' + material, operator_token=True)
            wait('eight events after undo and replay', lambda: settled(8))
            restored = next(r for r in raw()['rows'] if r['payload']['native_id'] == 'status-1')
            assert restored['payload']['exit_code'] == -2
            daemon.stop()
            configure(False, '[[privacy_rules]]\npattern = "echo"\nreplacement = "[protected]"\ncreated_by = "operator"\n')
            daemon.start('exit-codes-privacy')
            wait('eight events with a privacy rule', lambda: settled(8))
            protected = cli('trace', 'sinex:ev/' + restored['id'], '--bytes')
            assert protected['row']['record']['command'] == '[protected] signed 1', protected
            assert cli('show', 'sinex:ev/' + restored['id'])['row']['payload']['exit_code'] == -2
        scenario('a late exit code, restart, source loss, delete/undo with replay and privacy keep stored values', late)
    finally:
        if daemon.proc.poll() is None:
            daemon.stop()
