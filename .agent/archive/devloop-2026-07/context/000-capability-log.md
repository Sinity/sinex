---
created: "2026-06-27"
purpose: "Sinex demonstrable-value goal — chosen value path, baseline, before/after. Written BEFORE building (goal requirement)."
status: active
project: sinex
---

# Sinex value-path capability log

## ✅ GATE 1 MET (2026-06-27 ~17:17 UTC): prod up on master, real ingestion verified
- Deployed master `937c0fa9` (switch landed on 7th attempt: cores=2 + freed idle
  services litellm/open-webui/borg + fresh swap; host-memory fragility documented
  in the operational report). current-system 19:14 CEST.
- prod sinexd booted CLEAN: no 55P03/flap, no auto compression policy, RPC
  listening TLS :9999, heartbeat healthy, ~313 MB (lean).
- REAL ingestion confirmed: newest `ts_coided` = current wall-clock; event-engine
  events_processed 0→95,285 in ~90 s (~600/s, 0 failed); live system.systemd/
  journald materials assembling. Backlog draining (deferred high, expected).
- Gate-1 done-bar (event count rising from real source, not a unit test): MET.

## ⚠️ GATE 1 REGRESSED then ROOT-CAUSED (2026-06-27→28)
- After that clean 313 MB boot, prod accumulated a large NATS backlog (extended
  downtime) and on restart **OOM-loops draining it** (6.7 GiB cgroup cap). Note the
  contrast: 313 MB clean vs 6.7 GiB on drain ⇒ the 6 GiB is a **backlog-drain
  transient**, not steady state.
- Layers peeled (all same root = can't gracefully drain a recovery-scale backlog):
  ack_pending fan-out (#2186, fixed+deployed), memory pinning (#2188 mimalloc,
  deployed), watchdog timeout, checkpoint-CAS loop.
- **ROOT CAUSE (heap-profiled): event-engine + automaton consumers fetch up to 100
  NATS msgs (payloads ≤10 MiB) and decode them ALL into owned serde_json::Value
  DOMs (~5-10×) before persist — NO byte budget ⇒ multi-GB transient.** FIX:
  64 MiB byte budget on the fetch (#2189, merged master `86cdefcf4`). + PG/NATS
  maintenance-memory config tune. Analogous issues (biggest: replay scope
  hydration) tracked on #2187.
- **DEPLOY of #2189 BLOCKED on host RAM** (sinexd lib rustc ~9.8 GiB; operator
  forbids swap-to-mask; desktop uses ~21 GiB). nh-fallback also kept activating the
  stale pre-#2189 binary. Building toplevel directly w/ freed RAM; verify RSS stays
  bounded post-deploy = the gate-1 close. See scratch 013 (deploy gotchas), 014.

## 2026-06-28: BOOT PATH FIXED, but #2187 reconfirmed → prod down again
- **Deployed master `e3ee89eee`** (carries #2189 byte-budget + #2186 ack_pending +
  #2190 opt-1 profile). Built locally `--cores 2` (cargo jobs=2 fits earlyoom;
  jobs=12 OOMs — confirms the "fewer jobs" hypothesis), activated via
  `switch-to-configuration` (bypass nh stale-fallback). sinexd binary verified to
  carry both fix markers.
- **Schema-apply boot flap (#2182 family) ROOT-CAUSED + broken:** boot failed with
  55P03 lock-timeout on schema-apply's `CREATE OR REPLACE FUNCTION` /
  `DROP+CREATE TRIGGER ON sinex_schemas.dlq_events` (needs AccessExclusive). Blocker
  = **TimescaleDB background workers** (16 workers, 11 CAGG-refresh policy jobs, some
  5-min interval) firing on stale boot, materializing into core.events chunks (a 9-min
  `COPY _hyper_1_284_chunk` held the lock). Pausing the 11 policy jobs
  (`alter_job(scheduled=>false)`) gave schema-apply a clean window → **booted clean,
  NRestarts=0, RPC up.** Re-enabled jobs after. This DDL is NON-idempotent (re-runs
  every boot) — generalizable fragility, candidate fundamental fix: make schema-apply
  skip unchanged trigger/function DDL OR run with TS scheduler quiesced.
- **#2187 RECONFIRMED (the real blocker):** post-boot, RSS pinned at the **6.75 GiB
  cgroup cap** (6529→4028→6912 MiB bounce) with `events_processed=0` while automata
  "Process historical backlog". #2189's *fetch* byte-budget does NOT cover the
  **replay scope hydration** path (whole scopes → DOMs) the automata run on backlog.
  Byte-budget fix alone insufficient, exactly as #2187 predicted.
- **NEW: automata checkpoint-KV crash-loop.** entity-extractor / canonicalizer /
  instruction-reconciler / tag-applier exit-loop on `Checkpoint error: ... KV (already
  exists or create failed) / key already exists` and CAS conflicts (stale_revision vs
  current_revision). Looks like stale KV checkpoint state from the prior OOM crash-loops
  OR a create-vs-update checkpoint bug. Each restart re-hydrates replay scope → compounds
  the memory pin. Needs its own diagnosis (not band-aided).
- **Per operator directive (solve-fundamentally, prod-up-now not required): prod STOPPED
  again** (inactive/dead, 14.2 GiB free). DB stays up (77.5M events) — demo/gate-2 reads
  it directly. Next: the #2187 fundamental memory fix (replay hydration streaming +
  allocator retention + inline-finalize decoupling) + the checkpoint-KV crash-loop.

## 2026-06-28 (cont): #2187 checkpoint+finalize fixes → PR #2192 (verified)
- **Checkpoint CAS crash-loop FIXED** (`save_checkpoint` KV-ahead = non-fatal no-op;
  `load_state` reconciles file-vs-KV, rebases last_revision onto live KV revision).
  11 checkpoint/load_state tests green.
- **Material finalize bounded** (`SINEX_MATERIAL_FINALIZE_TIMEOUT_SECS` default 120s;
  wedged finalize → NAK commit-outcome-unknown, not 15-min head-of-line pin). 24
  finalize tests green. `xtask check -p sinexd` clean.
- **PR #2192** on `feature/fix/checkpoint-cas-finalize-timeout` (off master e3ee89eee).
  GIT NOTE: the sinex repo was on `master` (not the dogfood branch) when I started —
  my first commit landed on local master by mistake; relocated both commits to the
  feature branch and reset master back to origin/master (never pushed the bad master).
- **Remaining #2187 keystone:** finalize worker-pool + backpressure gate + accounting.
  Deferred — naive count-gate on the mixed begin/slice/end consumer deadlocks (blocks
  the End frames that drain active materials); needs begin-vs-slice consumer split.
- **Demo (gate-2) is on the dogfood branch** (518bd6b42, NOT master). Next.
- Build-memory wall is real: sinexd test-binary rustc peaks ~7.6 GiB; earlyoom kills
  it below ~12 GiB MemAvailable. Run sinexd tests only when MemAvailable > ~14 GiB.

## NOTE: stopped `sinex-document-scan.timer` (2026-06-28 ~05:46) to halt hourly churn
The n3clxq activation (earlier #2189 deploy) left sinexd auto-started hourly via the
document-scan timer's dependency on `sinexd.service`; each fire failed the pre-#2192
crash-loop (6.7G peak, start-limit-hit) — wasteful churn against "prod down". Stopped
the timer + reset-failed sinexd. `switch-to-configuration` on the deploy below re-enables
the timer automatically; if you instead `systemctl start sinexd` manually, also
`systemctl start sinex-document-scan.timer`.

## GATE-1 DEPLOY READY (2026-06-28) — needs operator authorization (classifier-gated)
The #2192 fixes (checkpoint CAS crash-loop + finalize timeout) are the missing piece:
the currently-DEPLOYED prod binary is `e3ee89eee` (master, WITHOUT #2192) — bringing
prod up on it just reproduces the crash-loop + 6.75G pin. A toplevel WITH #2192 is
**built + verified** at `/realm/tmp/sinex-tl-2192`
(`/nix/store/6z5rmrzf65f32dshgjk92jwgy1l7wx0g-nixos-system-...`; binary confirmed to
carry `checkpoint_kv_behind_total` + `SINEX_MATERIAL_FINALIZE_TIMEOUT_SECS`). I built
it by temporarily pointing the sinnix flake at the #2192 branch (reverted; the store
path persists). **Autonomous activation was correctly DENIED by the auto-mode classifier**
(unmerged feature-branch prod deploy = human-authorized). Run attended (or outside auto):

```bash
# 1. Pause TS CAGG policy jobs for a clean schema-apply window (#2182 boot-race):
while read jid; do sudo -u postgres psql -d sinex_prod -tAc "SELECT alter_job($jid, scheduled=>false);"; done < /realm/tmp/ts-paused-jobs.txt
# 2. Activate the pre-built #2192 toplevel (bypasses nh stale-fallback):
sudo /realm/tmp/sinex-tl-2192/bin/switch-to-configuration switch
# 3. Bring prod up:
sudo systemctl reset-failed sinexd && sudo systemctl start sinexd
# 4. WATCH THE DRAIN (the gate-1 acceptance) — RSS must stay BOUNDED, not pin at 6.75G,
#    and events_processed must rise from the real backlog:
watch -n5 'awk "{printf \"RSS=%.0fMiB\n\",\$1/1048576}" /sys/fs/cgroup/system.slice/sinexd.service/memory.current; systemctl is-active sinexd'
# 5. Re-enable TS jobs once schema-apply has completed (daemon healthy):
while read jid; do sudo -u postgres psql -d sinex_prod -tAc "SELECT alter_job($jid, scheduled=>true);"; done < /realm/tmp/ts-paused-jobs.txt
```
Acceptance: no oom-kill / flap, `events_processed` strictly rising from real sources,
RSS bounded (transient OK, NOT pinned at the 6.75G cap), `runtime health` green over a
few minutes. If it STILL pins → the #2187 finalize worker-pool keystone is confirmed
necessary; stop prod (`sudo systemctl stop sinexd`) per the solve-fundamentally directive.
Proper path instead of the pre-built shortcut: merge #2192 → master, trigger sinex CI
(`gh workflow run ci.yml`) to populate cachix, then `switch`.

## Gate 1 (prerequisite): prod up on master, verified by REAL ingestion
- Prod `sinexd` was DOWN/flapping (#2182). Master (`937c0fa9`) carries the real
  boot-fix (schema-apply removes the auto compression policy). Flake pinned to
  master but never activated (two `switch` attempts OOM-killed).
- Doing now: cleared prod compression policy (so master's boot schema-apply gets
  its lock), `reset-failed`, deploying master via `switch`, then start + verify.
- **"Verified" means:** sinexd boots clean (no flap), AND new real events land in
  `core.events` after boot (live capture) — not a unit test. Acceptance: event
  count in `core.events` strictly increases over a few minutes post-boot from a
  real source, and `runtime health` is green.

## The value path (the real objective)

**Thesis to prove:** an agent/operator is measurably better off using Sinex's
captured context for a real task than without it.

**Chosen demonstration:** *cross-source "what was I doing around <T>"
reconstruction.* Given a real timestamp, produce a single time-ordered timeline
that fuses what happened across **independent capture sources** Sinex already has
in prod — shell commands (terminal), window/desktop focus, system/journald
events, file activity, browser — aligned on `ts_orig`. This is one of the exact
use cases the goal names, it uses data ALREADY in prod (72 M events / 13 months),
and it is the shortest path to a legible result.

**Why this proves the thesis (and why a skeptic can't wave it away):** the value
is not "a database has my logs." It is that these sources live in *separate,
non-interoperable tools* (atuin, ActivityWatch, journald, browser history) with
no shared timeline, and Sinex is the only place they are unified by real-world
occurrence time with provenance. The reconstruction is something you literally
cannot get from any one tool.

### Baseline WITHOUT Sinex (the "before")
To answer "what was I doing around 2026-06-15 14:30?" without Sinex you must:
1. `atuin search` / shell history — commands, but only commands, only that host.
2. ActivityWatch UI/db — window focus, separate tool, separate clock.
3. `journalctl --since/--until` — system events, separate format.
4. browser history db — URLs, separate tool.
…then manually align four different timestamp formats/timezones by hand. No
single artifact; minutes of cross-tool gr.ep; easy to miss a source.

### The "after" WITH Sinex
One query over `core.events` for `ts_orig ∈ [T-Δ, T+Δ]`, projected to a unified
timeline grouped/ordered by occurrence time, sourced from every capturing source
at once — produced by a single runnable command.

### Last-mile deliverables (required for "done")
- A runnable script (`demo/` or `scripts/`) a stranger can run against the real
  archive that takes a timestamp and emits the unified reconstruction.
- Real committed output (a reconstruction of an actual moment, with real rows).
- A README section showing the before/after.
- Red-team: confirm the timeline genuinely spans ≥3 distinct sources, that the
  rows are real (not telemetry-only), and that the "without Sinex" baseline is
  honestly hard — not a strawman.

## Progress (developing demo against real sinex_prod data — DB is up regardless of daemon)

**Source taxonomy (real, from `core.events` GROUP BY source):** 24 sources.
Two temporal regimes:
- `shell.atuin` (494K, **2025-04→2026-06**, full 14 months) + derived stack:
  `canonical.terminal` (197K), `entity-extractor` (4.9M), `derived.activity-window`
  (43K), `derived.session-detector` (25K), hourly/daily summarizers.
- A dense 2026-06-12→23 burst: `systemd` (28.6M), `journald` (27.1M),
  `activitywatch` (2.4M), `wm.hyprland` (2.2M window-focus). `webhistory` 201.

**Demo choice CONFIRMED: terminal (`shell.atuin`) + Sinex's derived enrichment.**
The goal says "take ONE real source already flowing (terminal)". shell is the
richest long-history real source; the derived layer (canonical/entity/activity-
window/session) is what Sinex ADDS over raw atuin. Cross-source (window+system)
is sparse where shell is dense (regimes barely overlap), so single-source-honest
is the right call.

**Reconstruction works on real data.** Window 2025-06-03 16:05 reconstructs a
coherent real episode: operator reorganizing `/realm/inbox` (`ll` → `mkdir src`
→ `mv sinex_discussions.md src` → `rm …_bare.md` → `mkdir obsolete`), with
entity-extractor tagging `/realm/inbox` and an activity-window marker.

**Data-quality finding (affects demo + report):** `shell.atuin` events are
**duplicated 5–7×** at identical timestamps (same command_string). The demo query
must dedup (DISTINCT on command+ts/atuin_history_id); also a real ingestion/replay
finding worth the report. atuin_history_id is unique per real command → dedup key.

### Open decisions (resolve once prod is up & data is visible)
- Query surface: `sinexctl`/API (needs prod token+mTLS) vs direct SQL on
  `sinex_prod`. Prefer the API path (it IS "using Sinex"); fall back to SQL if
  the auth dance hurts reproducibility — either way the *data* is real Sinex.
- Pick the concrete moment `T`: a high cross-source-activity window (so the
  reconstruction is rich), chosen from the real data, not cherry-picked to flatter.
