"""Actual Source intake/outbox/HTTP producer for isolated native bridge proof."""
import argparse
import asyncio
import json
import os
import sqlite3
from pathlib import Path

from polylogue.sinex.models import PublicationMode
from polylogue.sinex.obligations import list_obligations, load_payload
from polylogue.sinex.network_transport import SinexHTTPTransport
from polylogue.sinex.service import PublicationService
from polylogue.storage.blob_store import BlobStore
from tests.infra.sinex_publication import acquired_publication, drain_publication

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--redrive", action="store_true")
    parser.add_argument("--extra-grok-responses", type=int, default=0)
    args = parser.parse_args()
    if args.redrive:
        with sqlite3.connect(args.root / "source.db") as conn:
            conn.row_factory = sqlite3.Row
            obligation = list_obligations(conn)[0]
            payload = load_payload(conn, obligation)
    else:
        if args.extra_grok_responses:
            obligation, payload = asyncio.run(acquired_publication(args.root, extra_grok_responses=args.extra_grok_responses))
        else:
            obligation, payload = asyncio.run(acquired_publication(args.root))
    manifest = json.loads(payload.manifest_bytes)
    prepared = {"request_id": obligation.request_id, "anchor_count": len(manifest["anchors"]),
        "expected_record_ids": list(manifest["anchors"])}
    if not args.redrive:
        # This neutral fixture marker observes a committed Source obligation before
        # HTTP. It allows the native recovery test to queue the existing request lock.
        marker = args.root / "source-obligation-ready.json"
        temporary = marker.with_suffix(".tmp")
        temporary.write_text(json.dumps(prepared), encoding="utf-8")
        temporary.replace(marker)
    transport = SinexHTTPTransport(args.endpoint, os.environ["SINEX_API_TOKEN"], BlobStore(args.root / "blob"), args.root / "rpc-files")
    service = PublicationService(args.root / "source.db", PublicationMode.MIRROR, transport=transport)
    # The actual coordinator/ThreadBridge owns each short Source writer section;
    # HTTP is outside that lease. This is the service attempt owner, not adapter-only.
    summary = asyncio.run(drain_publication(service, redrive=args.redrive))
    status = service.status()
    print(json.dumps({**prepared,"request_id":obligation.request_id,"outbox_status":status.as_dict(),
        "attempted":summary.attempted,"confirmed":summary.confirmed,
        "object_id":payload.object_id,"revision_id":payload.revision_id,
        "normalizer_version":payload.normalizer_version,
        "originals":[{"role":o.role,"source_ref":o.source_ref,"sha256":o.blob_hash,
            "size_bytes":o.size_bytes,"media_type":o.media_type,"acquisition":o.acquisition.to_dict()}
            for o in payload.originals]}, ensure_ascii=False))

if __name__ == "__main__":
    main()
