#!/usr/bin/env python3
"""Small source-delta and byte-attribution controls without native execution."""
import hashlib
from pathlib import Path
import shutil
import subprocess
import tempfile
from package_binding import require_harness_only
from retention_accounting import account, canonical


def refuses(fn, *args):
    try: fn(*args)
    except AssertionError: return
    raise AssertionError('negative control was accepted')


require_harness_only(['tests/recovery_context_cases.py', 'docs/rewrite/atuin.md', 'README.md', '.agentctl/project.toml'])
for path in ['crate/sinex-store/src/atuin.rs', 'Cargo.lock', 'Cargo.toml', 'flake.nix', 'flake.lock', 'xtask/src/main.rs', 'rust-toolchain.toml']:
    refuses(require_harness_only, ['tests/retention_accounting.py', path])

with tempfile.TemporaryDirectory(prefix='sinex-accounting-check-',dir='/realm/tmp/work') as scratch:
    root=Path(scratch);store=root/'store';blob=store/'blobs/aa'/(64*'a'+'.zst');blob.parent.mkdir(parents=True)
    row={'id':'synthetic','command':'echo synthetic'}
    envelope=dict(row,_from={'observation':'synthetic-observation','table':'history','rowid':1})
    decoded=canonical(envelope)+b'\n'
    decoder=shutil.which('zstd');assert decoder
    encoded=subprocess.run([decoder,'-q','-c'],input=decoded,capture_output=True,check=True).stdout
    blob.write_bytes(encoded)
    material={'id':'synthetic-material','observation_id':'synthetic-observation','blake3':64*'a',
        'status':'completed','blob_status':'present','durable_bytes':len(decoded),'total_bytes':len(decoded),'size_bytes':len(decoded)}
    def run(args,**kwargs):return subprocess.run(args,text=True,capture_output=True,check=True)
    def measure(name):
        destination=root/name;destination.mkdir()
        return account(store,destination,[material],'synthetic-observation',[row],run,decoder)
    proof=measure('positive');m=proof['materials'][0]
    assert m['logical_source_rows_jsonl_bytes']==len(canonical(row))+1
    assert m['acquisition_envelope_extra_bytes']==len(decoded)-len(canonical(row))-1
    assert m['envelopes_sha256']==hashlib.sha256(decoded).hexdigest()
    assert proof['file_bytes']==len(encoded) and m['encoding_metadata_bytes'] is None
    material['durable_bytes']+=1;refuses(measure,'wrong-length');material['durable_bytes']-=1
    material['observation_id']='different';refuses(measure,'wrong-observation');material['observation_id']='synthetic-observation'
    (store/'open').mkdir();(store/'open/transient').write_bytes(b'copy')
    refuses(measure,'transient')
print('7 runtime/build refusals, exact source/envelope accounting and length/observation/transient controls passed')
