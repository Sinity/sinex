"""Synthetic Atuin history and an independent session grouping to compare against."""
import sqlite3

from _atuin_daemon import COLUMNS

GAP_NS = 300_000_000_000


def write(path, rows, replace=False):
    """Insert or replace rows (dicts keyed by COLUMNS); with `replace`, drop all others first."""
    con = sqlite3.connect(path)
    with con:
        if replace:
            con.execute('DELETE FROM history')
        con.executemany('INSERT OR REPLACE INTO history VALUES(?,?,?,?,?,?,?,?,?)',
                        [[r[c] for c in COLUMNS] for r in rows])
    con.close()


def independent(rows):
    """Group rows by the session rule (a gap over 300 s after the latest end starts a new
    session; an unknown duration counts as a point), without using the product's code."""
    groups = []
    for row in sorted(rows, key=lambda r: (r['timestamp'], r['id'])):
        start = row['timestamp']
        end = start+max(0, row['duration'])
        if not groups or start-groups[-1]['end_ns'] > GAP_NS:
            groups.append({'start_ns': start, 'end_ns': end, 'parents': set()})
        groups[-1]['end_ns'] = max(groups[-1]['end_ns'], end)
        groups[-1]['parents'].add(row['id'])
    return [dict(g, parents=sorted(g['parents'])) for g in groups]


def query_all(cli, text):
    """Every row of a query, following cursors."""
    rows, cursor = [], None
    while True:
        answer = cli('query', text, *(['--cursor', cursor] if cursor else []))
        rows += answer['rows']
        cursor = answer.get('page', {}).get('cursor')
        if not cursor:
            return rows


def check_trace(cli, header, facts):
    """`trace` of a session leads to its commands and from each to the source row it came from."""
    trace = cli('trace', 'sinex:ev/'+header['id'], '--bytes')['row']
    assert sorted(p['event'] for p in trace['parents']) == sorted(header['parents'])
    for parent in trace['parents']:
        assert len(parent['parents']) == 1, parent
        original = parent['parents'][0]
        assert original['material'] and original['end'] > original['anchor']
        fact = next(r for r in facts if r['id'] == original['record']['id'])
        assert all(original['record'][c] == fact[c] for c in COLUMNS), (original['record'], fact)
    return trace
