"""A shell grouping publication turn cancelled mid-statement rolls back as a
whole, and after a restart without the source the published commands keep their
payloads and trace to the original Atuin rows.

The middle part is the ignored Rust test shell::publication_tests::..., which
needs rows admitted by a running daemon, so it runs here via cargo (development
build only).
"""
import json
import os

from _shell import HISTORY_COLUMNS, Daemon, create_history

PUBLICATION_TEST = 'shell::publication_tests::cancelled_publish_batch_rolls_back_and_republishes_with_same_ids'


def exercise(h):
    if h['OPTIONS'].packaged:
        print('  skip shell_publication: it runs a Rust test via cargo, so it needs the development build', flush=True)
        return
    work, run, scenario = h['WORK'], h['run'], h['scenario']
    source, config = work/'history.db', work/'publication.toml'
    daemon = Daemon(h, config)
    facts = [(f'turn-{n}', 1_700_000_000_000_000_000 + n, 9, n % 2, f'echo publication {n}', '/synthetic/harbor',
              'synthetic-session', 'fixture-host', None) for n in range(6)]
    create_history(source, facts)
    admitted, proof = {}, {}

    def configure(collect):
        config.write_text(f'''listen = "{daemon.address}"
state_dir = "{work/'state'}"
store_dir = "{work/'store'}"
writer_connection_file = "{work/'writer'}"
reader_connection_file = "{work/'reader'}"
token_file = "{work/'token'}"
sources = {json.dumps(['terminal.atuin-history'] if collect else [])}
automata = []
''' + (f'''[atuin]
path = "{source}"
poll_interval_ms = 3600000
''' if collect else f'read_model_connection_file = "{work/"read-model"}"\n'))

    def rollback():
        configure(True)
        daemon.start('collect')
        daemon.wait(lambda _: any(s['last_run'] and s['last_run']['status'] == 'completed' for s in daemon.sources()))
        rows = daemon.cli('query', 'events where source = terminal.atuin-history and kind = command.executed')['rows']
        assert len(rows) == 6
        admitted.update({r['id']: r for r in rows})
        daemon.stop()
        env = {**h['ENV'], 'DATABASE_URL': os.environ['DATABASE_URL'],
               'SINEX_TEST_ADMIN_URL': h['admin'], 'SINEX_TEST_WRITER_URL': h['writer']}
        result = run(['cargo', 'test', '--locked', '-p', 'sinex-store', '--features', 'product', '--lib', PUBLICATION_TEST,
                      '--', '--exact', '--ignored', '--nocapture'], env=env, label='publication turn rollback test', timeout=1800)
        lines = [x.removeprefix('PUBLICATION_TURN_PROOF ') for x in result.stdout.splitlines() if x.startswith('PUBLICATION_TURN_PROOF ')]
        assert len(lines) == 1, 'the Rust test did not print its result line'
        proof.update(json.loads(lines[0]))
        assert proof['whole_turn_rollback'] and proof['restart_stable'] and proof['mixed'] == {'skip': 1, 'reuse': 1, 'new': 4}
    scenario('a cancelled publication turn rolls back whole and reuses committed IDs', rollback)

    def restart():
        source.rename(work/'history-moved.db')
        configure(False)
        daemon.start('restart-without-source')
        daemon.wait(lambda s: s['indexed_retrieval'].get('serving') and s['indexed_retrieval'].get('pending_canonical') == 0
                    and s['indexed_retrieval']['fusion']['phase'] == 'settled' and s['indexed_retrieval']['position'] == s['commit_seq'],
                    seconds=90)
        rows = daemon.cli('query', 'commands')['rows']
        assert len(rows) == 5 and {r['id'] for r in rows} == set(proof['canonical_ids'])
        published = {e['id']: e for e in proof['canonical']}
        for row in rows:
            event = published[row['id']]
            assert row['payload'] == event['payload'] and row['parents'] == event['source_event_ids']
            parent = admitted[row['parents'][0]]
            for field in ['when', 'time_quality', 'time_precision', 'time_clock', 'time_until', 'time_suspect']:
                assert row[field] == parent[field], field
            traced = daemon.cli('trace', 'sinex:ev/' + row['id'], '--bytes')['row']['parents']
            assert len(traced) == 1 and traced[0]['event'] == row['parents'][0]
            record = traced[0]['record']
            record = json.loads(record) if isinstance(record, str) else record
            expected = dict(zip(HISTORY_COLUMNS, facts[int(row['payload']['native_id'].removeprefix('turn-'))]))
            assert {key: record[key] for key in HISTORY_COLUMNS} == expected
            assert row['payload']['field_evidence']['time'] == row['parents'][0]
            assert daemon.cli('show', 'sinex:ev/' + row['id'])['row']['payload'] == row['payload']
        daemon.stop()
    scenario('after a restart without the source, commands keep payloads and trace to their rows', restart)
