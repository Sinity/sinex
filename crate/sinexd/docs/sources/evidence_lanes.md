# Source-Material Evidence Lanes

Status: design record for #1207.

Sinex source material has two complementary evidence lanes:

1. **Occurrence lane**: rotating acquisition-payload material that anchors
   observed records with offsets, anchors, timestamps, and parser semantics.
2. **Snapshot lane**: immutable source-of-truth artefacts plus change logs that
   can re-derive occurrences after the live source mutates or disappears.

The occurrence lane is efficient for normal ingestion. For polling sources, one
row-stream material spans many poll cycles and rotates by policy; a registry row
per poll or per source row is historical bad shape, not the target model. The
snapshot lane is the replay-correctness backstop: it preserves what the source
looked like around the time the occurrence was interpreted.

## Roles

Each source-material record should declare its evidence role in metadata until a
dedicated typed column exists.

| Role | Meaning | Examples |
|---|---|---|
| `epistemic_evidence` | Immutable evidence about source-of-truth state. | SQLite snapshot, exported JSONL archive, compressed directory snapshot. |
| `acquisition_payload` | Bytes staged to move records through the ingestion path. | Row-stream frame, material slice, append-only segment. |
| `storage_substrate` | Internal persistence/storage support, not itself a domain observation. | CAS object, staged temp file, assembler WAL segment. |
| `derived_semantic` | Generated interpretation from prior evidence. | Summary, embedding input projection, parser-derived report. |

`epistemic_evidence` is the replay anchor. `acquisition_payload` may be enough
for immediate ingestion, but it does not prove the wider source state by itself.

## Source-Class Policy

| Source class | Occurrence lane | Snapshot lane | Trigger policy |
|---|---|---|---|
| Growing SQLite DB | Long-lived row-stream material with stable row identity. | SQLite backup/snapshot file. | Row stream rotates by size/time; snapshots on startup, periodic interval, clean shutdown, and before destructive replay. |
| Append-only log | Byte-range segment with offset anchors. | Rotated log copy or compressed segment set. | On rotation, size threshold, and periodic seal. |
| Static export file | File material with byte offsets. | Same file may serve as both occurrence and evidence. | Content hash change. |
| Mutable directory tree | File/drop occurrence records. | Directory snapshot or manifest+content bundle. | Periodic interval or significant content change. |
| Ephemeral IPC stream | Frame/window material. | Usually unavailable. | Emit explicit continuity gaps; do not claim full replayability. |
| External canonical mirror | Metadata mirror event. | External IDs/hashes and optional staged export. | Per external update or scheduled export snapshot. |

## Replay Semantics

When replaying a material-provenance event:

1. Prefer an `epistemic_evidence` snapshot whose validity window covers the
   occurrence timestamp or source offset.
2. Apply WAL/change-log material between the snapshot and occurrence when
   available.
3. Re-run the parser against the reconstructed source view.
4. If only an `acquisition_payload` exists, replay may reinterpret that payload
   but must report reduced evidence strength.
5. If neither lane is available, replay must surface a continuity gap rather
   than silently producing an empty or best-effort result.

Replay output should record which evidence lane was used so query, readiness,
and context-pack surfaces can distinguish exact reinterpretation from degraded
or non-replayable interpretation.

## Metadata Shape

Minimum metadata fields for the lane contract:

```json
{
  "evidence_role": "epistemic_evidence",
  "source_identity": "desktop.activitywatch",
  "source_path": "/home/user/.local/share/activitywatch/aw-server-rust/sqlite.db",
  "captured_at": "2026-05-17T00:00:00Z",
  "valid_from": "2026-05-16T23:00:00Z",
  "valid_until": "2026-05-17T00:00:00Z",
  "covers_material_ids": ["..."],
  "wal_from": "optional-sequence-or-offset",
  "wal_until": "optional-sequence-or-offset",
  "content_hash": "blake3:..."
}
```

`covers_material_ids` links occurrence-lane material to snapshot-lane evidence
without adding a second provenance column to events. Events keep exactly one
material provenance root; the replay planner follows material metadata to find
supporting evidence.

## First Proof

The SQLite evidence lane is the first practical proof target:

- row-stream materials keep current ingestion efficient;
- periodic SQLite snapshots become `epistemic_evidence`;
- metadata links row-stream materials to the nearest snapshot;
- replay can state whether it used snapshot-backed evidence or only the row
  acquisition payload.

This aligns with `crate/sinexd/docs/sources/sqlite_evidence_lane.md`; that document owns
SQLite-specific retention and implementation details.

## Guardrails

- Do not model dual evidence by adding a second material column to events.
- Do not call acquisition payloads full replay evidence unless they contain the
  relevant source-of-truth state.
- Do not require ephemeral streams to become replayable; emit explicit gaps.
- Do not retain every snapshot forever; retention policy belongs to the source
  class and operator storage budget.
