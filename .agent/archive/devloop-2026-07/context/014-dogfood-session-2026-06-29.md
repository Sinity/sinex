---
created: 2026-06-29T04:00+02:00
purpose: Operating log + current-run + handoff for the 2026-06-29 dogfood dev-loop session
status: active
project: sinex
---

# Dogfood session 2026-06-29 (resumed from 5b7f07f1)

Read first: [001-standing-goal.md](001-standing-goal.md). Prior thread:
[012-dogfood-operating-log.md](012-dogfood-operating-log.md),
memory `dogfood_dev_loop_recipe.md`.

## Branch / head
- Checkout: `/realm/project/sinex` (main checkout, NOT a worktree).
- Branch: `feature/dev/automaton-confirmed-delivery`.
- This session's commit: `d22532ba0 fix(runtime): let StaticFileAdapter emit a directory as logical_path`.
- Branch carries the confirmed-delivery Option-C work (cherry-picked, verified).
  NOT merged to master yet.

## STRUCTURAL dev-loop fixes (the central work — a jamming loop is not a feedback engine)
Reconstruction-honesty audit (querying "what was I doing tonight" on the real
store) exposed that the dev loop was NOT honestly reconstructing my work, and was
structurally fragile. Root-caused + fixed:

- **`5a2e804cb` confirmed-events stream `discard: New`→`Old`.** Confirmed events are
  already persisted in PG, but the durability gate acks raw only after the confirmed
  publish succeeds — so a full stream with discard:New REJECTED the publish → raw
  never acked → JetStream redelivery STORM → whole pipeline wedged (12,661 "maximum
  messages exceeded", stalled every ~30min). discard:Old = bounded ring, never jams.
  Verified: `nats stream info DEV_SINEX_RAW_EVENTS_CONFIRMED` shows Discard:Old,
  pipeline runs under firehose with 0 saturation. Follow-ups in **#2204** (Interest
  retention; raw-events WorkQueue; self-obs volume).
- **entity-extractor FEEDBACK LOOP fixed** (`input_provenance_filter Any→MaterialOnly`).
  It consumed its OWN `entity.extracted` output (`*` + Any) and re-extracted entities
  from the entity text → runaway: 46-57K self-parented entity-extractor events PER 2-3
  MIN, the dominant firehose (NOT self-obs/journald). MaterialOnly = extract from
  observed source data only (commands/files/journal/docs), never from interpretations.
  Sole loop (other Any automata don't self-feed). [verifying volume collapse]

## Reconstruction-honesty gaps still open (make "what was I doing" honest)
- **git snapshot staleness**: git source is `RuntimeShape::OnDemand`; doesn't catch
  commits made after its scan → MY OWN commits invisible in recall. Needs re-scan.
- **agent commands invisible**: my Bash-tool commands bypass atuin/zsh history; the
  clearest record of agent work isn't captured. Needs an agent-activity source.
- **journald salience**: 12.5K priority-6 routine vs 2 notable (≤4) in 5h; recall must
  summarize routine + surface notable, not sample random info lines.

## What landed this session (real artifacts)
-1. **fs source FIXED end-to-end** (`51747ef13 fix(sources): keep file-drop notify
   watcher alive`). Root cause: `build_file_drop_stream`'s notify watcher was an
   unreferenced `_watcher` param → `async_stream` dropped it when the fn returned
   (not when the stream ended) → notify thread died → rx closed → stream ended
   after the first buffered event → continuous source reopened a fresh watcher
   every 30s, losing all changes in the gap. Silent zero-ingestion. Fix: own the
   watcher inside the generator (`let _watcher = watcher;` + `Send` bound) and make
   `run_continuous` select the blocking drain against shutdown. Found via live
   instrumentation (watch targets re-registered exactly every 30s = smoking gun).
   **Verified end-to-end:** writes under a watched root produce real
   `fs-watcher`/`file.created`+`file.modified` events in core.events (with paths),
   shown by the recall lens as live occurrence activity. #2203 closed.
   NOTE: event source label is `fs-watcher`, NOT the binding id `fs` — query
   `source='fs-watcher'`. Two follow-ups noted in #2203 (dev-NATS jetstream
   saturation under restart churn; materialize_file_content_record nil-material
   early returns).

0. **Recall lens tiered by provenance** (`4a27b2efb feat(sinexctl): tier recall
   pack by provenance`). `events context` now partitions the window via the
   query API's `has_lineage` filter into **occurrence** (material — the headline)
   vs **derived signals** (de-emphasized "system interpretations") vs
   self-observation; each tier gets its own coverage + detail budget so
   occurrence families are never starved. Proven on the live store anchored at
   2026-06-28T22:00: before = "1477 activity events" with entity-extractor (1465)
   as headline and `git 9 latest ?`/`shell 3 latest ?`; after = "12 activity
   events" (9 commits + 3 commands) WITH real timestamps + samples
   (just chisel / z lyn / nvim), 2997 derived demoted, 75106 self-obs excluded.
   Tests: recall_pack_* + source_family_buckets (3 passed, --allow-contended-host).
   This is the first concrete step toward the §6 EvidenceWindow/ContextPack
   keystone (occurrence-vs-derived altitude + explicit coverage).

1. **Git ingestion works end-to-end.** `StaticFileAdapter` now emits a directory
   as `logical_path` with no bytes (was rejecting dirs with "Is a directory, os
   error 21" — the blocker the recipe memory recorded). git-commit-history source
   now starts healthy and ingested **1461 `git/commit.created` events** from the
   sinex repo, ts_orig = real author dates 2025-05-30 → 2026-06-29. Verified via
   `psql core.events where source='git'`. Test:
   `test_static_file_directory_yields_logical_path_without_bytes` (3 passed).

## BREAKTHROUGH — my commits are NOW visible in Sinex (git capture works)
- `7001e2b4a` fix(sources): StaticFileAdapter re-reads DIRECTORIES (read-once gate
  now files-only). Root cause of git-staleness: `cursor.processed` returned an
  empty stream after first scan; correct for static files, WRONG for the git repo
  *directory* (re-scannable). Now git continuous re-scan works: `seen_records=1
  emitted=1469`, git events 1461→1469 (all tonight's commits, max ts_orig 03:46→
  10:25), dedup holds 1469/1469, settles to 300s poll (no firehose). `sinexctl
  events context` lists my commits as occurrence activity. **#2205 CLOSED.**
  The "what was I doing" lens now reconstructs my actual git work.
- Debugging that cracked it: `src_drain` traces in drain_adapter showed
  `seen_records=0` → StaticFileAdapter empty stream → the `cursor.processed` gate.
- Remaining recall-honesty: journald firehose buries git/shell in the lens
  (salience needed); agent Bash-tool commands still bypass atuin (need agent
  source); ~1.97M historical entity-extractor loop garbage frozen in store.

## (prior) END-OF-SESSION STATE
- **Dev loop RECOVERED and live** (job 2000195, new binary 10:36). The build
  stalls were caused by a **stale `postmaster.pid`** left by a killed postmaster
  (NOT the port — postgres is socket-only, `listen_addresses=''`). Fix: remove
  `<checkout>/dev-state/data/postgres/postmaster.pid` then start. (#2206 partly
  misdiagnosed as port conflict — the real blocker was the stale pidfile +
  bootstrap not cleaning it / not failing fast.)
- **git-continuous (`2ab88bc9f`): mechanism WORKS, scan does NOT capture new
  commits.** git now re-polls every 300s (verified two cycles) but each re-scan
  replays the SAME 1461 (`candidate_processed_count=1461`, "Checkpoint KV is ahead;
  skipping") — the AppendStream checkpoint is wedged; tonight's commits never
  land. **My commits still invisible in recall.** Remaining work = incremental
  `<last_sha>..HEAD` cursor scan + checkpoint-progress fix (#2205, now core not
  optional).

## Current run (was live, now down)
- Dev sinexd: `xtask run core --bg`, job `2000186` (full manifest, fresh NATS) as of 05:26.
  Was 2000171/289760 earlier; cleared dev-NATS jetstream store mid-session to drain
  `maximum messages exceeded` saturation (Postgres data preserved).
  (re-check: `ps -eo pid,comm | awk '$2=="sinexd"'`). 4 source drivers healthy:
  terminal.zsh-history, terminal.atuin-history, git-commit-history, fs.
- Logs: `.sinex/state/jobs/2000171/{stdout,stderr}.log`.
- WARNING: `pkill -f 'target/debug/sinexd'` / `-f 'xtask run core'` matches the
  agent's OWN shell command line → SIGTERMs the shell (exit 144). Kill sinexd by
  PID (`ps -eo pid,comm | awk '$2=="sinexd"{print $1}'`), never by `-f` pattern.
- Checkout hash: `566e9bf8d5e8`. Dev DB:
  `postgresql:///sinex_dev?host=/var/cache/sinex/sinity/566e9bf8d5e8/dev-state/run`.
- Gateway: `https://127.0.0.1:19086`, token `dev-token-sinnix-prime:admin`,
  mTLS `.sinex/tls/{ca,client,client-key}.pem`. (Read live values from
  `/proc/<sinexd-pid>/environ`.)
- Source bindings manifest: `.agent/dev/dev-source-bindings.json` — now lists
  terminal.zsh-history, terminal.atuin-history, git-commit-history, fs.

## Store population (real vs self-obs), this checkout
| family | count | note |
|---|---|---|
| sinex / sinexd | ~677K | self-observation (~77%) — EXCLUDE from recall |
| shell | ~81K | REAL: atuin/zsh commands (source label `shell.atuin`, type `command.executed`) |
| entity-extractor / derived | ~149K | derivations off the above |
| git | 1461 | real commit history |
| fs-watcher | flowing | FIXED — file.created/file.modified live (source label `fs-watcher`) |
| journald | flowing | WIRED — `system.journald` binding, live `entry.written` from system journal (uid 1000 can read it) |
| health-aggregator | 26 | |

## OPEN FINDING — fs source: watcher establishes but delivers 0 events
- fs binding added (source_id `fs`, `FileContentDropConfig`: watch_paths
  `["/realm/project/sinex"]`, recursive, ignored dirs target/.git/.sinex/.claude/...,
  ignored suffixes -wal/-shm/.tmp/..., max_capture_bytes 1MiB).
- Driver starts **healthy**: `AdapterBackedSource initialized adapter_kind=file_drop`,
  "entering continuous poll loop". No open/watch errors logged.
- Adapter is `notify`-based inotify (`file_drop.rs::open` → `recommended_watcher`).
  `drain_adapter` blocks on the infinite stream (correct — watcher stays alive).
- BUT: writing files under watched roots (root-level AND `.agent/dev/`) produced
  **0 events**; `events_processed=0`; no `fs%` rows in `raw.source_material_registry`;
  no new PG errors; nothing in `records_from_file_drop_event` path observed.
- Because `ignored_directory_names` is set, `choose_file_drop_watch_plan` takes the
  `NativeFiltered` branch (`survey.ignored_directories > 0`) → per-directory
  NonRecursive watches via `planned_file_drop_watch_targets` (file_drop.rs:613).
- Hypotheses to test with live instrumentation (a temp trace in
  `records_from_file_drop_event` / after `watcher.watch`):
  1. NativeFiltered target enumeration omits the root and/or `.agent/dev` dirs.
  2. inotify events arrive but map to `event_kind()=None` and are silently dropped
     (Create/Modify map fine in code — would need Access-only delivery, unlikely).
  3. notify delivery issue on the `/realm` filesystem for this watch shape.
- Cannot cleanly parallelize a debug subagent here: it needs the live dev sinexd,
  and a second sinexd would collide on the dev DB/NATS. Debug inline or in a
  separate dedicated session. Binding left in manifest — harmless (driver idle).
- Tracked as **#2203** (filed this session, with repro/diagnostic plan).

## Next three actions
1. Build the recall keystone on the now-multi-source store (terminal+git): inspect
   `sinexctl events context` current shape, drive a real "what was I doing around
   T" pack on a real anchor, judge whether it already honors the §6 EvidenceWindow
   shape (coverage/absence explicit, family-aware) or still silos. Produce a real
   ContextPack artifact (md+json) on real data.
2. Root-cause the fs watcher-no-events finding (live instrumentation) OR file it.
3. Keep this log + 001 current; write handoff before quota.

## Handoff (resume here)
**Read first:** [001-standing-goal.md](001-standing-goal.md), this file.
**State:** branch `feature/dev/automaton-confirmed-delivery`, 2 commits this
session (`d22532ba0` git-dir adapter, `4a27b2efb` provenance-tiered recall).
Live dev loop running (job 2000171). Store: git 1461, shell ~81K, derived ~149K,
fs 0. NOT merged to master.
**Populated-steady-state goal MET:** terminal + fs + git + journald all flowing
(commits c9709dc25, 7b894ec72, 51747ef13, d22532ba0). Dev loop job 2000187.
**Next actions:**
1. **§6 EvidenceWindow keystone** — the recall-lens provenance tiering is step 1;
   next is the shared `EvidenceWindow(anchor, scope, sources, relation_policy,
   coverage_policy) → ContextPack(md+json)` that context/incident/agent-brief
   collapse onto. Big architectural thread — scope as its own issue first.
3. **journald source** — more substrate; needs systemd-journal group access for
   uid 1000 (may be blocked; check before wiring).
**Commands to resume:** dev-loop bringup in memory `dogfood_dev_loop_recipe.md`.
Recall demo: `sinexctl events context --at <T> --since 2h` with mTLS flags.

## Do not repeat
- Don't re-derive the dev-loop bringup — it's in memory `dogfood_dev_loop_recipe.md`.
- Don't conflate worktree branches a4fb3e6b (replay/schema crash-recovery) and
  ab8420f7 (finalize-decouple + reflection 1-byte-material fix) with THIS dev
  branch — they target prod keystones #2187/#2182, a different lane. abb78a3c /
  ac74836b are the already-cherry-picked confirmed-delivery source worktrees (stale).
- zsh does NOT word-split `$VAR`; wrap multi-flag invocations in `bash -c`.
