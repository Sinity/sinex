# Historical Backfill Runtime Plane

Historical backfill is a node/runtime operation. It is not a direct database
import and it must not bypass source-material registration, NATS batching, or
`sinex-ingestd` persistence.

## Runtime Path

The expected path is:

1. A node receives a historical scan request with an input checkpoint and a
   bounded `TimeHorizon::Historical`.
2. The node reads external source records from its normal source adapter.
3. Each interpreted record is appended to SDK-managed source material.
4. The node emits material-provenance events through its `EventEmitter`.
5. `NodeRunner` batches and publishes those events to NATS.
6. `sinex-ingestd` consumes the batch and persists rows into `core.events`.
7. Queries observe the events with non-null `source_material_id` and valid
   anchors.

Tests that call `scan_historical` directly are useful for node logic and
checkpoint edge cases, but they are not sufficient proof of historical
backfill. The runtime-plane proof must include `NodeRunner`, NATS, ingestd, and
database assertions.

## Current Proven Sources

The checked-in runtime proofs cover:

- Terminal Atuin SQLite rows: `shell.atuin / command.executed`
- Terminal line-oriented shell history: `shell.history / command.imported`
- Desktop ActivityWatch SQLite rows:
  `activitywatch / window.active`,
  `activitywatch / browser.tab.active`, and
  `activitywatch / afk.changed`
- Browser history rows:
  `webhistory / page.visited` from qutebrowser native SQLite, Chromium
  `History` SQLite, and browser exports in `json`, `jsonl`, `ndjson`, and
  `csv` formats

These sources all flow through `NodeRunner<IngestorNodeAdapter<_>>` in tests and
assert persisted `core.events` rows with material provenance.

The browser runtime proof intentionally keeps the established
`webhistory / page.visited` event namespace. Renaming this into a broader
`browser.*` taxonomy is event-taxonomy work, not part of the backfill proof.

## Checkpoints

Historical scans return an external checkpoint owned by the node. Re-running a
scan from that returned checkpoint should process no already-covered source
records and should not create duplicate persisted events.

For terminal history, the checkpoint is per source. Text sources track byte and
line progress; SQLite sources track row IDs. For ActivityWatch, the checkpoint
tracks the last observed SQLite row ID. For browser history, the checkpoint is a
node-owned external source-progress object containing dump-import fingerprints,
line progress for append-only JSONL/NDJSON dumps, SQLite row cursors, and SQLite
snapshot evidence state.

## Explicit Rewind And Rescan

An explicit rewind is a caller decision to pass an older checkpoint, commonly
`Checkpoint::None`, instead of the latest returned checkpoint. Rewind means
"read those source records again through the normal runtime path." It is not a
mutation of existing events.

When a rewind is intended to replace earlier interpretations, use the replay
archive/invalidation flow around the scan. The node scan itself only emits fresh
material-provenance events from the selected source window; archive/supersession
policy belongs to replay control, not to the source node.

## Verification

The useful local proof shape is:

```bash
xtask build -p sinex-ingestd
xtask test -p sinex-terminal-ingestor -E 'test(scan_historical_persists_terminal_history_through_node_runtime)'
xtask test -p sinex-desktop-ingestor -E 'test(scan_historical_persists_activitywatch_through_node_runtime)'
xtask test -p sinex-browser-ingestor -E 'test(scan_historical_persists_browser_history_through_node_runtime)'
```

The first command is required when tests spawn `sinex-ingestd`; the sandbox
refuses stale runtime binaries.

Host proof is separate from the fixture proof. On a live host, run the same node
scan mode against real configured paths, then query `core.events` for the source
and event types above. The query result must include material provenance.
