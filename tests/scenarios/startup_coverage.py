"""Query coverage before the first Atuin read finishes: a configured source that has
not been read (or is missing) makes answers incomplete, never falsely complete."""
import sqlite3

from _atuin_daemon import ATUIN_QUERY, Daemon, create_history


def exercise(h):
    work, scenario = h['WORK'], h['scenario']
    path = work/'startup.db'
    create_history(path, [('startup', 1700000000123456789, 1, 0, 'synthetic startup', '/synthetic', 'startup', 'fixture', None)])
    daemon = Daemon(h, 'startup')

    def current(status, total):
        """Wait for the source's latest run to reach `status`, then check the query total."""
        def check():
            if daemon.cli('sources')['sources'][0]['last_run']['status'] != status:
                return None
            answer = daemon.cli('query', ATUIN_QUERY, expected=0 if status == 'completed' else 5)
            assert answer['total'] == total and answer['complete'] == (status == 'completed')
            return answer
        daemon.wait(f'source {status} with {total} rows', check, 20)

    def no_source():
        daemon.configure()
        daemon.start('unconfigured')
        daemon.ready()
        answer = daemon.cli('query', ATUIN_QUERY)
        assert answer['complete'] is True and answer['total'] == 0 and answer['source_status'] == []
        assert daemon.cli('sources')['sources'] == []
        daemon.stop()
    scenario('with no source configured, an empty answer is complete', no_source)

    def blocked_read(empty=False):
        if empty:
            # A separate database, so rows stored from the populated run cannot hide an empty source.
            urls = h['create_database']('startup_empty')
            (work/'writer').write_text(urls['writer'])
            (work/'reader').write_text(urls['reader'])
            daemon.name = 'startup-empty'
            with sqlite3.connect(path) as con:
                con.execute('DELETE FROM history')
        # An EXCLUSIVE transaction in DELETE journal mode blocks the collector's read-only open.
        lock = sqlite3.connect(path)
        lock.execute('PRAGMA journal_mode=DELETE')
        lock.execute('BEGIN EXCLUSIVE')
        daemon.configure(path)
        daemon.start('blocked-empty' if empty else 'blocked-first')
        try:
            daemon.ready()
            status = daemon.cli('status')
            answer = daemon.cli('query', ATUIN_QUERY, expected=(0, 5))
            sources = daemon.cli('sources')['sources']
            assert answer['complete'] is False, 'a configured source that was never read reported complete'
            assert answer['total'] == 0
            assert len(sources) == 1 and sources[0]['locator'] == str(path)
            assert sources[0]['last_run']['rows_read'] == 0
            assert sources[0]['last_run']['status'] in ('reading', 'unavailable')
            assert len(answer['source_status']) == 1 and answer['source_status'][0]['status'] in ('reading', 'unavailable')
            assert status['supported_sources'] == ['terminal.atuin-history']
        finally:
            lock.rollback()
            lock.close()
        current('completed', 0 if empty else 1)
        daemon.stop()
        if not empty:
            return
        # After restart, the source is incomplete again until it is reread, even though
        # the previous read completed with zero rows.
        lock = sqlite3.connect(path)
        lock.execute('BEGIN EXCLUSIVE')
        daemon.start('blocked-restart')
        try:
            daemon.ready()
            assert daemon.cli('query', ATUIN_QUERY, expected=5)['complete'] is False
            assert daemon.cli('sources')['sources'][0]['last_run']['status'] in ('reading', 'unavailable')
            daemon.stop()
        finally:
            lock.rollback()
            lock.close()
        daemon.start('released-restart')
        daemon.ready()
        current('completed', 0)
        daemon.stop()
    scenario('a blocked first read is incomplete until it finishes', blocked_read)
    scenario('an empty source is incomplete after restart until it is reread', lambda: blocked_read(True))

    def missing():
        absent = work/'never-created.db'
        daemon.configure(absent)
        daemon.start('missing')
        daemon.ready()
        source = daemon.wait('missing source unavailable', lambda: next(
            (s for s in daemon.cli('sources')['sources'] if s['locator'] == str(absent) and s['last_run']['status'] == 'unavailable'), None), 20)
        assert source['last_run']['reason'] and source['last_run']['rows_read'] == 0
        assert daemon.cli('query', ATUIN_QUERY, expected=5)['complete'] is False
        assert not absent.exists(), 'the daemon created the missing source file'
        daemon.stop()
    scenario('a missing source stays unavailable and is not created', missing)
