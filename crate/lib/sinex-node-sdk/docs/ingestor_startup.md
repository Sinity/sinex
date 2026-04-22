# Ingestor Startup — The Three-Phase Lifecycle

Every ingestor in continuous-service mode executes three mandatory phases. This contract is encoded in the `IngestorNode` trait — the three required methods map directly onto the phases.

## Phases

1. **Snapshot** — capture instantaneous current state of the source. For file-backed sources: list all current files; for DB-backed: current row count or equivalent. Implemented as `scan_snapshot(&mut state, args)`.
2. **Gap-fill (historical scan)** — load the last checkpoint, process data from checkpoint to now. Implemented as `scan_historical(&mut state, from, until, args)`. Large and small gaps use the same code path — a 6-month history file import and a 30-second post-restart gap-fill are the same operation at different scales.
3. **Continuous sensing** — only after the gap is filled, begin live monitoring. Implemented as `run_continuous(&mut state, start, ...)` where `start: ContinuousStart` is a live-tail cursor set by the SDK startup runner after snapshot + bounded gap-fill completes.

**Critical invariant:** an ingestor that skips gap-fill and jumps straight to continuous creates a permanent gap in the historical record. The SDK ordering enforces snapshot → historical → continuous; don't bypass.

## Why continuous is typed separately

`run_continuous` receives `ContinuousStart` rather than a plain `Checkpoint`. This prevents a specific failure mode: continuous startup widening itself into a historical import.

Historical incident (system journal node): with `import_on_startup=true` and `import_hours=0`, "continuous mode" meant "import all journal history on every service start." Host memory-pressure incident followed. The fix was typing the continuous-phase entry so the cursor is *known-live-tail*, not *open-ended-from-past*.

## Crash recovery

The checkpoint stores `blob_id` and current byte offset. On restart:

1. Ingestor finalizes the in-flight material as `status: 'recovered_partial'`.
2. Normal three-phase startup proceeds.
3. The gap between crash and restart becomes auditable via `source_material_registry`.

## Source-material input shapes

The SDK provides *output-shape* infrastructure (`AcquisitionManager` for begin→append→finalize, `StageAsYouGoContext` for emit-events-with-provenance-as-material-grows). All ingestors use these. The *input-shape* layer is newer and partial:

| Shape | Examples | SDK adapter | Status |
|---|---|---|---|
| **SQLite database** (growing, mutable) | Atuin, ActivityWatch, browser history | `sqlite_source.rs` + `input_shapes.rs` — read-only open, row-ID checkpoint, strict/lenient checkpointed runners | ✅ Shared by terminal + desktop + browser |
| **Append-only file** (tail with inotify) | scribe-tap JSONL, weechat logs, any log file | `file_tailer.rs` + `input_shapes.rs` — UTF-8 tail poller with tracked file state and rotation/truncation detection | ✅ First shared surface exists. Live watcher orchestration beyond polling is still ingestor-specific. |
| **Ephemeral IPC stream** (no persistent source) | Hyprland socket, DBus signals, MQTT | — | ❌ Not built. Desktop ingestor implements locally. Key difference from files: no gap-fill possible; missed messages are lost. |
| **One-time dump** (static file, process once) | GDPR exports, Takeout archives | `batch_importer.rs` + `input_shapes.rs` root discovery | ✅ Discovery/progress substrate exists. Parser/record semantics remain domain-specific. |
| **Incremental dump** (new file appears periodically) | Chrome history export, periodic snapshots | `batch_importer.rs` + `input_shapes.rs` root discovery | ✅ Discovery/progress substrate exists. Dedup/parse policy remains domain-specific. |
| **API-backed fetch** (poll external API) | Spotify API, Reddit API, GitHub API | — | ❌ Not built. Needs rate limiting, pagination, cursor checkpoint. |
| **File-drop directory** (watch for new files) | Screenshots, recordings, document intake | — | ❌ Not built. Closest to inotify file watcher but per-file (process each once) not per-line. |

### How the SQLite adapter works

- Opens the DB read-only with a 5-second busy timeout (handles WAL contention from the source app).
- Queries `WHERE rowid > ?checkpoint`.
- Maps rows via a caller-provided closure.
- Checkpoints the max `rowid` seen.

Result: node code specifies source-query shape + row-to-event mapping; SDK owns cursor management and checkpointing.

### Historical-import transparency

Importing a zsh history file from 3 years ago and watching live shell history are the same code path. `scan_historical(from=None, until=now)` processes the entire file. The ingestor reads rows, stages source material, emits events. Gap-fill and historical import are the same operation.

When historical data and realtime data cover overlapping time ranges (e.g., Spotify GDPR export + API feed), dedup happens at the event level via natural keys. Both paths emit `spotify.track.played` with the same `(track_id, played_at)` natural key. Idempotency prevents duplicates.

## Storage: growing stream materials

As of 2026-04-21, high-volume row and metadata-only sources now use **growing stream materials**. The earlier per-row `stage_material()` path produced one tiny source material per row and thrashed the assembler. Stream materials accumulate in a single append-only material and rotate on size or time boundary, matching the ingestor output shape to the assembler's single-stream consumer.

`AppendStreamAcquirer` returns byte anchors for appended records; terminal, browser, and ActivityWatch imports append JSONL-style records into rotating source materials. The same SDK shape covers filesystem metadata-only observations — deleted, moved, and empty-file create/modify events append JSONL observation records through `BufferedAppendStreamWriter` instead of minting one zero-byte material per transient file event.

## Current acquisition path — status (2026-04-21)

The SDK/runtime acquisition path went through four convergent improvements after the initial host deploy exposed specific failure modes. Understanding these matters for extending the path without re-introducing the classes they closed.

### Transport: ordered stream boundary

The earlier runtime split source-material begin/slice/end frames across `SOURCE_MATERIAL_BEGIN`, `SOURCE_MATERIAL_SLICES`, and `SOURCE_MATERIAL_END`. Awaiting publish acks in the SDK did not create a cross-stream ordering contract for ingestd, so the assembler could observe an end frame before local begin state and churn placeholder/buffer state under live load.

Current shape: `AcquisitionManager` publishes `source_material.frames.>` into one `SOURCE_MATERIAL` stream, and ingestd consumes one ordered `ingestd_material_frames` durable consumer. Out-of-order recovery remains for WAL restore, redelivery, and non-SDK publishers; normal SDK traffic no longer depends on it.

### Hot path: record batching before material frames

Tiny journal and DBus records were becoming one physical source-material slice each, forcing multiple durability syncs per slice. The proper boundary is **preserving logical-record anchors while reducing physical frame/write amplification** — not terminal throttling.

Current shape: `AcquisitionManager::append_record_batch` and `AppendStreamAcquirer::append_many_with_anchors` append a bounded batch as one material slice and return exact byte anchors for every logical record. `sinex-system-ingestor` coalesces sequential producer appends for 20 ms then drains queued material appends into 64-record / 128 KiB batches. A coalescing window is required because high-volume watchers enqueue through awaited event decoration; without it, "batch queued payloads" still degenerates into one NATS frame per logical event under sequential scheduling.

### Rotation: hot-watcher materials use SDK rotation

Long-lived system watcher materials (`system.unified_journal`, `system.dbus`, `system.udev`, `system.node`) stayed open for the watcher's lifetime and bypassed `AppendStreamAcquirer`'s size/age rotation policy — the SDK had the right abstraction, but the highest-volume caller still owned a fixed `SourceMaterialHandle`.

Current shape: `AppendStreamAcquirer::from_active_handle` lets callers expose the initial material id while delegating all subsequent appends and rotation to the SDK stream path. `RealWatcherMaterialContext` uses this rotating stream and sets event provenance from the returned `SourceRecordAnchor.material_id`, so records after a rotation cite the new material rather than the initial one.

### Storage boundary: local CAS for small finalized materials

Live cgroup inspection exposed two misplaced optimizations: `sinex-ingestd.service` retained a resident `git-annex add --json --batch` helper; then direct `git-annex add --json` helper trees took 10-28 s for tiny finalized materials.

Current shape — hybrid backend:
- Finalized materials **up to 16 MiB** → SDK **local CAS** with `SINEXBLAKE3-s<size>--<hash>` keys.
- Larger → bounded short-lived git-annex.

`BlobManager` retrieval and verification are backend-aware: local CAS verifies through BLAKE3 instead of treating the stored content hash as a git-annex SHA digest. Idempotency closure: the DB repository upserts by BLAKE3 checksum when present, falling back to `(annex_backend, content_hash)` only for legacy/no-checksum rows. First host rollout proof: 0 outstanding / 0 redelivered / 0 unprocessed frames; only the Rust daemon in ingestd's cgroup; no `git-annex` processes found; new-PID logs clean; recent `core.blobs` rows overwhelmingly `SINEXBLAKE3`.

### Four-role reframe: acquisition vs evidence lanes

Row-stream materials are the immediate **acquisition-payload source material** lane. The design reserves a second lane — **complementary epistemic-evidence** — for periodic immutable DB snapshots or WAL-frame capture, providing stronger long-term anchoring for historical reinterpretation. Per `sinex-target-vision/reference/design-intent.md`:

- **`db_snapshot`**: safe snapshot of DB state, registered as a single source material. Events can additionally point into it with `offset_kind = "rowid"` + `anchor_byte = row_id` for stronger epistemic anchoring.
- **`db_wal`**: capture SQLite WAL frames as source material chunks — content-addressable diffs between checkpoints capturing exactly what changed.

Target state: *run both lanes concurrently*, not replace row capture with snapshots. The complementary lane is not yet built; tracked as [sinex#323](https://github.com/Sinity/sinex/issues/323).

## Related

- [`stream_runtime.md`](stream_runtime.md) — derived-node runtime (transducer/windowed/scope-reconciler) + shared state-persistence + cooperative-shutdown mechanics
- [`trait-selection.md`](trait-selection.md) — decision flowchart; includes `IngestorNode` selection criteria
- [`provenance.md`](provenance.md) — sensor/ingestor separation + Stage-as-You-Go rules
- [`stage_as_you_go.md`](stage_as_you_go.md) — real-time provenance tracking during material growth
