"""Disposable SQL probe, invoked by the consumer benchmark before cluster stop.

Call run(psql, output_directory, source_root). psql accepts SQL and returns its
unaligned output or None on failure. This collects actual wall times without
EXPLAIN ANALYZE on writes. It only touches core.published and synthetic
core.event_tombstones in the caller's disposable benchmark database.
"""
import json
import pathlib
import re
import time


def run(psql, output_directory, source_root):
    root = pathlib.Path(source_root)
    source = (root / "crates/sinex-store/src/commit_log.rs").read_text()
    page_storage = "event_ids" in source[source.index("pub async fn record_published"):]
    # Run the exact checked statements from this source revision.
    statements = {}
    for function in ("record_published", "excised_on_stream", "forget_published"):
        body = source[source.index("pub async fn " + function):]
        match = re.search(r'r#"(.*?)"#', body, re.S)
        if match:
            sql = match.group(1).replace(' AS "event_id!"', '').replace(' AS "stream_seq!"', '')
        elif function == "forget_published":
            sql = 'DELETE FROM core.published WHERE event_id = ANY($1)'
        else:
            raise RuntimeError("statement not found: " + function)
        types = {"record_published": "uuid, uuid, uuid[], bigint[]" if page_storage else "uuid[], bigint[]", "excised_on_stream": "bigint", "forget_published": "uuid[], bigint[]" if page_storage else "uuid[]"}[function]
        statements[function] = (types, sql)

    def query(sql):
        result = psql(sql)
        if result is None:
            raise RuntimeError("disposable publication probe failed: " + sql[:160])
        return result.strip()

    def timed(sql):
        started = time.monotonic()
        result = query(sql)
        return {"wall_ms": (time.monotonic() - started) * 1000, "result": result}

    mapping = "core.published p CROSS JOIN LATERAL unnest(p.event_ids,p.stream_seqs) m(event_id,stream_seq)" if page_storage else "core.published m"
    audit = json.loads(query(f"SELECT json_build_object('pairs', count(*), 'event_ids', count(DISTINCT m.event_id), 'stream_seqs', count(DISTINCT m.stream_seq), 'wrong_pairs', count(*) FILTER (WHERE c.event_id IS NULL OR c.commit_seq <> m.stream_seq), 'expected', (SELECT count(*) FROM core.commit_log WHERE event_id IS NOT NULL)) FROM {mapping} LEFT JOIN core.commit_log c ON c.event_id=m.event_id"))
    pathlib.Path(output_directory, 'publication-audit.json').write_text(json.dumps(audit, indent=2))
    if not (audit['pairs'] == audit['event_ids'] == audit['stream_seqs'] == audit['expected'] and audit['wrong_pairs'] == 0):
        raise RuntimeError('published pairs differ from the generated stream: ' + json.dumps(audit))
    results = {"page_storage": page_storage, "source": str(root), "cases": []}
    # 3.85 million mappings approximate the inspected confirmed-stream map.
    for overlap in (False, True):
        query("TRUNCATE core.published; DELETE FROM core.event_tombstones")
        id_expr = "md5(i::text)::uuid" if overlap else "lpad(to_hex(i),32,'0')::uuid"
        if page_storage:
            insert = f"""INSERT INTO core.published(first_event,last_event,event_ids,stream_seqs)
              SELECT min_id[1], max_id[1], ids, seqs FROM (
                SELECT array_agg(event_id ORDER BY event_id) min_id,
                  array_agg(event_id ORDER BY event_id DESC) max_id,
                  array_agg(event_id ORDER BY event_id) ids,
                  array_agg(stream_seq ORDER BY event_id) seqs
                FROM (SELECT {id_expr} event_id, i*3 stream_seq, (i-1)/1000 page
                      FROM generate_series(1,3851000) i) e GROUP BY page) pages"""
        else:
            insert = f"INSERT INTO core.published SELECT {id_expr}, i*3 FROM generate_series(1,3851000) i"
        fill = timed(insert)
        query("ANALYZE core.published")
        sizes = json.loads(query("SELECT json_build_object('total_bytes', pg_total_relation_size('core.published'), 'index_bytes', pg_indexes_size('core.published'), 'rows', (SELECT count(*) FROM core.published))"))
        # A recent absent tombstone overlaps all random pages; then an exact
        # match is added. Preserve both lookup shapes and unmatched bounds.
        target = "md5('3850500')::uuid" if overlap else "lpad(to_hex(3850500),32,'0')::uuid"
        query(f"INSERT INTO core.event_tombstones(event_id,source,event_type,operation_id) VALUES ({target},'bench.synthetic','bench.event','00000000-0000-0000-0000-000000000001')")
        candidates = int(query(f"SELECT count(*) FROM core.published WHERE {target} BETWEEN first_event AND last_event")) if page_storage else None
        types, sql = statements["excised_on_stream"]
        lookup = [timed(f"PREPARE lookup({types}) AS {sql}; EXECUTE lookup(1000)") for _ in range(3)]
        # Rewriting uses the same pair equality as the checked store code.
        types, sql = statements["forget_published"]
        args = f"ARRAY[{target}]" + (",ARRAY[11551500::bigint]" if page_storage else "")
        rewrite = timed(f"PREPARE forget({types}) AS {sql}; EXECUTE forget({args})")
        for sample in lookup:
            if '|11551500|' not in sample['result']:
                raise RuntimeError('lookup did not return the exact target pair')
        remaining = int(query("SELECT coalesce(sum(cardinality(event_ids)),0) FROM core.published" if page_storage else "SELECT count(*) FROM core.published"))
        if remaining != 3850999:
            raise RuntimeError(f'wrong remaining pair count: {remaining}')
        import hashlib
        numbers = [500 + 1000 * page for page in range(1000)]
        ids = [hashlib.md5(str(i).encode()).hexdigest() if overlap else f'{i:032x}' for i in numbers]
        id_array = 'ARRAY[' + ','.join("'" + value + "'" for value in ids) + ']::uuid[]'
        seq_array = 'ARRAY[' + ','.join(str(i * 3) for i in numbers) + ']::bigint[]'
        query(f"INSERT INTO core.event_tombstones(event_id,source,event_type,operation_id) SELECT id,'bench.synthetic','bench.event','00000000-0000-0000-0000-000000000001'::uuid FROM unnest({id_array}) t(id); ANALYZE core.event_tombstones")
        types, sql = statements['excised_on_stream']
        batch_lookup = [timed(f'PREPARE lookup({types}) AS {sql}; EXECUTE lookup(1000)') for _ in range(3)]
        for sample in batch_lookup:
            found = {int(line.split('|')[1]) for line in sample['result'].splitlines() if '|' in line}
            if found != {i * 3 for i in numbers}:
                raise RuntimeError('batch lookup did not return the exact 1000 scattered pairs')
        types, sql = statements['forget_published']
        args = id_array + (',' + seq_array if page_storage else '')
        batch_rewrite = timed(f'PREPARE forget({types}) AS {sql}; EXECUTE forget({args})')
        batch_remaining = int(query('SELECT coalesce(sum(cardinality(event_ids)),0) FROM core.published' if page_storage else 'SELECT count(*) FROM core.published'))
        if batch_remaining != 3849999:
            raise RuntimeError(f'wrong pair count after scattered erasures: {batch_remaining}')
        types, sql = statements['excised_on_stream']
        empty_lookup = [timed(f'PREPARE lookup({types}) AS {sql}; EXECUTE lookup(1000)') for _ in range(3)]
        if any('|' in sample['result'] for sample in empty_lookup):
            raise RuntimeError('erased pairs were returned again')
        results["cases"].append({"overlapping_uuid_bounds": overlap, "candidate_pages": candidates, "remaining_pairs": remaining, "fill": fill, "sizes": sizes, "lookup": lookup, "rewrite": rewrite,
            'scattered_1000': {'lookup': batch_lookup, 'rewrite': batch_rewrite, 'remaining_pairs': batch_remaining, 'post_erasure_lookup': empty_lookup}})
    pathlib.Path(output_directory, 'publication-probe.json').write_text(json.dumps(results, indent=2))
    return results

if __name__ == '__main__':
    import subprocess
    import sys
    url, output, source = sys.argv[1:]
    from urllib.parse import parse_qs, urlparse
    socket = pathlib.Path(parse_qs(urlparse(url).query)['host'][0]).resolve()
    if socket != pathlib.Path(output).resolve() / 'pg':
        raise RuntimeError('publication probe requires the disposable output-directory cluster')
    def psql(sql):
        result = subprocess.run(['psql', url, '-v', 'ON_ERROR_STOP=1', '-Atc', sql], text=True, capture_output=True)
        if result.returncode:
            sys.stderr.write(result.stderr)
            return None
        return result.stdout
    import threading
    stop = threading.Event()
    samples = []
    started = time.monotonic()
    def observe():
        while not stop.is_set():
            paths = ['/proc/pressure/cpu', '/proc/pressure/memory', '/proc/pressure/io', '/proc/stat', '/proc/diskstats']
            samples.append({'seconds': time.monotonic() - started, 'host': {path: pathlib.Path(path).read_text() for path in paths}})
            stop.wait(1)
    observer = threading.Thread(target=observe, daemon=True)
    observer.start()
    try:
        run(psql, output, source)
    finally:
        stop.set()
        observer.join()
        pathlib.Path(output, 'publication-probe-load.json').write_text(json.dumps(samples, indent=2))
