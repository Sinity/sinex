"""Paging raw `events` queries with `--cursor`: continuation across source changes,
change notices, bad cursors, restart and an unavailable source."""
import json
import sqlite3

from _atuin_daemon import ATUIN_QUERY, INSERT, Daemon, create_history

EPOCH = 1_700_000_000_123_456_789
# The same query written differently; a cursor from one must continue the other.
RESTATED = ' events where kind = "command.executed" and source = "terminal.atuin-history" '


def exercise(h):
    work, scenario = h['WORK'], h['scenario']
    db = work/'cursor.db'
    # Every three rows share a nanosecond, so the 100-row page boundary cuts through a tie.
    create_history(db, [(f'row-{n:03}', EPOCH + (n//3)*2, 1, 0, f'echo synthetic {n}', '/synthetic', 'cursor', 'fixture', None)
                        for n in range(241)])
    daemon = Daemon(h, 'cursor')
    daemon.configure(db, poll_ms=60000)
    daemon.start('cursor-base')

    def query(text=ATUIN_QUERY, cursor=None, expected=0):
        return daemon.cli('query', text, *(['--cursor', cursor] if cursor is not None else []), expected=expected)

    def sorted_by_time(rows):
        return [(r['when'], r['id']) for r in rows] == sorted((r['when'], r['id']) for r in rows)

    first = None

    def continuation():
        nonlocal first
        daemon.wait('first read', lambda: (r := daemon.last_run())['status'] == 'completed' and r['rows_read'] == 241)
        first = query()
        assert first['total'] == 241 and len(first['rows']) == 100 and first['complete'] is True
        assert all(r['payload']['timestamp_ns'] == EPOCH + (int(r['payload']['native_id'][4:])//3)*2 for r in first['rows'])
        assert first['rows'][0]['when'] == '2023-11-14T22:13:20.123456789Z'
        assert sorted_by_time(first['rows'])
        unchanged = query(RESTATED, first['page']['cursor'])
        assert unchanged['resolved']['commit_seq'] == first['resolved']['commit_seq']
        assert unchanged['page']['changed_since_first_page'] is False and unchanged['page']['change_notice'] is None
        assert unchanged['resolved']['now'] == first['resolved']['now']
        assert first['rows'][-1]['when'] == unchanged['rows'][0]['when']
        assert first['rows'][-1]['id'] < unchanged['rows'][0]['id']
        daemon.stop()
        # Remove a row on a later page and add rows before and after the continuation point.
        with sqlite3.connect(db) as con:
            con.execute('UPDATE history SET deleted_at=? WHERE id=?', (EPOCH, 'row-180'))
            for key, timestamp in [('append-before', EPOCH-100), ('append-after', EPOCH+999)]:
                con.execute(INSERT, (key, timestamp, 1, 0, 'echo appended', '/synthetic', 'cursor', 'fixture', None))
        daemon.start('cursor-changed')
        daemon.wait('read after change', lambda: (r := daemon.last_run())['status'] == 'completed' and r['rows_read'] == 243)
        second = query(cursor=first['page']['cursor'])
        assert second['total'] == 242 and len(second['rows']) == 100
        assert second['resolved']['commit_seq'] > first['resolved']['commit_seq']
        assert second['page']['first_commit_seq'] == first['resolved']['commit_seq'], 'continuation lost the first page commit'
        assert second['page']['changed_since_first_page'] is True, 'changes since the first page were not reported'
        assert second['page']['change_notice'] and 'current' in second['page']['change_notice']
        assert second['resolved']['now'] == first['resolved']['now']
        assert second['page']['expires_at'] == first['page']['expires_at']
        third = query(RESTATED, second['page']['cursor'])
        rows = first['rows'] + second['rows'] + third['rows']
        assert third['page']['cursor'] is None and third['total'] == 242
        native = [r['payload']['native_id'] for r in rows]
        assert len(native) == 241 and len(set(native)) == 241
        assert 'row-180' not in native and 'append-after' in native and 'append-before' not in native
        assert sorted_by_time(rows)
        # A fresh query sees the new earlier row: continuation reads current state, not a snapshot.
        fresh = query()
        assert fresh['rows'][0]['payload']['native_id'] == 'append-before' and fresh['total'] == 242
    scenario('continuation reads current rows after the cursor and reports changes', continuation)

    def bad_cursors():
        cursor = first['page']['cursor']
        assert query('events', cursor, expected=2)['kind'] == 'bad_query'
        assert query(cursor='not a cursor', expected=2)['kind'] == 'bad_query'
        decoded = json.loads(cursor)
        for field, value in [('version', 0), ('order', 'id'), ('expires_at', '2000-01-01T00:00:00Z'),
                             ('first_commit_seq', -1), ('last_when', 'bad'), ('zone', 'Europe/Warsaw')]:
            assert query(cursor=json.dumps({**decoded, field: value}), expected=2)['kind'] == 'bad_query', field
        old_format = {k: decoded[k] for k in ['query', 'last_when', 'last_id']}
        assert query(cursor=json.dumps(old_format), expected=2)['kind'] == 'bad_query'
    scenario('malformed, altered and old-format cursors are bad queries', bad_cursors)

    def restart_and_source_loss():
        cursor = first['page']['cursor']
        before = query(RESTATED, cursor)
        daemon.restart('cursor-restart')
        daemon.wait('read after restart', lambda: daemon.last_run()['status'] == 'completed')
        after = query(RESTATED, cursor)
        assert [r['id'] for r in after['rows']] == [r['id'] for r in before['rows']]
        assert after['resolved']['now'] == first['resolved']['now']
        assert after['page']['first_commit_seq'] == first['resolved']['commit_seq']
        # Stored rows stay queryable when the source becomes unavailable.
        daemon.stop()
        db.rename(work/'moved-away.db')
        daemon.start('cursor-source-unavailable')
        daemon.wait('source unavailable', lambda: daemon.last_run()['status'] == 'unavailable')
        partial = query(RESTATED, cursor, expected=5)
        assert partial['complete'] is False and partial['source_status'][0]['status'] == 'unavailable'
        assert partial['total'] == 242 and len(partial['rows']) == 100 and partial['page']['cursor']
        assert [r['id'] for r in partial['rows']] == [r['id'] for r in before['rows']]
    scenario('a cursor survives restart and an unavailable source', restart_and_source_loss)

    def other_query_forms():
        cursor = first['page']['cursor']
        for other in ['events sort by id', 'events as of now', 'events where source = x or kind = x']:
            assert query(other, cursor, expected=2)['kind'] == 'bad_query', other
        # Indexed retrieval is not configured here, so that is reported before the cursor is read.
        assert query('commands', cursor, expected=5)['kind'] == 'partial'
        daemon.stop()
    scenario('unsupported forms are refused with a cursor; commands report indexed search unavailable', other_query_forms)
