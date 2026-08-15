# CAS architecture decision

Status: selected architecture, pending operational proof and final wipe-gate review.

## Decision

Sinex-owned `MaterialManifestV1` and the exact encoded source-material bytes addressed by their local BLAKE3 digest are the canonical source-material authority. PostgreSQL owns the material registry and provenance relationships. The manifest is immutable once published. Replay, retention, fsck, purge, and source-removal recovery must resolve authority through the registry, manifest, and exact-byte CAS object together.

The BLAKE3 digest and encoded byte count identify the exact stored representation. A logical source may have several material parts, parser ranges, or container members, but those relationships are manifest metadata. They do not replace the exact-byte identity and they do not authorize fsck to invent child objects.

Third-party systems are subordinate adapters:

- `casync` is eligible for chunking, packing, compression, and replication behind an export or backup adapter. Its chunk identity must never replace the manifest's exact-byte digest.
- Perkeep is eligible for interoperability with immutable blobs and metadata claims. Its permanode or claim model is not the Sinex replay authority.
- git-annex remains a legacy compatibility and migration backend. Its git-tracked filename and metadata model does not define Sinex material identity.
- IPFS is eligible for export or replication when the original BLAKE3 digest and manifest accompany the DAG. A CID is not the source-material identity.
- Borg and restic are backup transports. An archive hash is evidence about a backup artifact, not a material identity.

No third-party backend is currently selected as the canonical replay store. Selecting one later requires a portable round trip that preserves exact bytes, canonical manifest bytes, source-material identity, ranges, unknown metadata, and replay-after-source-removal behavior.

## Non-negotiable invariants

1. Ingest stores or durably stages the exact encoded bytes before the registry claims the material is complete.
2. A manifest records unavailable information explicitly as `unknown`, `not_applicable`, or `withheld`; it never fabricates metadata.
3. Replay reads the manifest and exact bytes from CAS. It never depends on the original source path after material finalization.
4. Deduplication may reuse an identical byte object, but it must preserve every material registry and manifest reference.
5. Compression, chunking, packing, repacking, defragmentation, splitting, and merging are reversible optimizations. They require export/import proofs before activation and cannot change canonical digest, encoded size, or manifest identity.
6. Ordinary tombstone, registry cleanup, and CAS GC must retain manifest-backed material until an explicit, reviewed purge removes the complete authority graph.
7. Destructive fsck and purge stop on incomplete traversal, missing authority, conflicting references, or a page that makes no progress. They must leave an operator-retryable record.
8. Backup and restore cover PostgreSQL, CAS objects, manifests, NATS state where required, and Beads/Dolt state. A successful archive command is not restore evidence.

## Proof required before wipe

- Route-level source-removal replay for every source family in the frozen import manifest.
- CAS write, rename, fsync, lease, quarantine, delete, reference-reappearance, and crash/response-ambiguity tests.
- Portable export/import round trip through each selected adapter, with byte and manifest hashes compared.
- CAS-inclusive restore of PostgreSQL, CAS, NATS, and Beads/Dolt against isolated state, followed by normal Sinex startup and replay.
- Scale measurements for large manifests, compressed database chunks, fsck traversal, replay archive/restore, admission, and storage headroom.
- Explicit purge tests covering manifests, material registry rows, blobs, projections, archives, retention roots, and external backup references.

Until these proofs are recorded in the wipe gate, this document is an architecture selection and proof plan, not a claim that the CAS is wipe-ready.
