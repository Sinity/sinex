"""Finite plain-blob accounting using the owner's stock-zstd-readable layout."""
import hashlib
import json
from pathlib import Path


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def manifest(root):
    return [{'path': str(p.relative_to(root)), 'bytes': p.stat().st_size,
        'sha256': hashlib.sha256(p.read_bytes()).hexdigest(),
        'category': 'open' if p.relative_to(root).parts[0] == 'open' else
            ('temporary' if p.relative_to(root).parts[:2] == ('blobs', 'tmp') else 'closed')}
        for p in sorted(root.rglob('*')) if p.is_file()]


def account(root, destination, materials, observation, facts, run, decoder):
    """Read a held collector's closed material, preserving exact file and row evidence."""
    before = manifest(root)
    assert all(f['category'] == 'closed' for f in before), 'held EOF left open or temporary custody'
    assert all(m['status'] == 'completed' and m['durable_bytes'] == m['total_bytes'] == m['size_bytes']
        and m['blob_status'] == 'present' for m in materials), 'material custody not closed'
    by_path = {f['path']: f for f in before}
    accounted = []
    expected_rows = sorted(facts, key=lambda r: r['id'])
    for material in materials:
        digest = material['blake3'].removeprefix('blake3:')
        path = Path('blobs') / digest[:2] / (digest+'.zst')
        file = by_path.pop(str(path))
        retained = destination / (material['id']+'.zst')
        retained.write_bytes((root/path).read_bytes())
        decoded = run([decoder, '-d', '-q', '-c', retained], label='owner-prescribed plain zstd decoding').stdout.encode()
        assert len(decoded) == material['durable_bytes'] and decoded.endswith(b'\n')
        envelopes = [json.loads(line) for line in decoded.splitlines()]
        assert all(set(r['_from']) == {'observation', 'rowid', 'table'} and
            r['_from']['observation'] == material['observation_id'] and
            r['_from']['table'] == 'history' and r['_from']['rowid'] == i+1
            for i, r in enumerate(envelopes)), 'acquisition coordinates differ'
        rows = [{k: v for k, v in row.items() if k != '_from'} for row in envelopes]
        assert sorted(rows, key=lambda r: r['id']) == expected_rows, 'source fields changed'
        source_bytes = sum(len(canonical(row))+1 for row in rows)
        assert decoded == b''.join(canonical(row)+b'\n' for row in envelopes), 'unexpected canonical encoding'
        (destination/(material['id']+'.jsonl')).write_bytes(decoded)
        owner_layout = run([decoder, '--list', '--verbose', retained], label='owner decoder representation inspection').stdout
        accounted.append({'material': material, 'file': file, 'owner_decoder_layout': owner_layout,
            'encoding_metadata_bytes': None, 'encoding_metadata_reason': 'owner CLI does not expose a verified separate seek-table length',
            'logical_envelope_bytes': len(decoded), 'logical_source_rows_jsonl_bytes': source_bytes,
            'acquisition_envelope_extra_bytes': len(decoded)-source_bytes,
            'source_rows_sha256': hashlib.sha256(b''.join(canonical(r)+b'\n' for r in rows)).hexdigest(),
            'envelopes_sha256': hashlib.sha256(decoded).hexdigest(),
            'new_this_observation': material['observation_id'] == observation})
    assert not by_path, 'unattributed store files'
    assert manifest(root) == before, 'store changed while collector held'
    return {'files': before, 'file_bytes': sum(f['bytes'] for f in before),
        'closed_bytes': sum(f['bytes'] for f in before if f['category'] == 'closed'),
        'open_bytes': 0, 'temporary_bytes': 0, 'materials': accounted}
