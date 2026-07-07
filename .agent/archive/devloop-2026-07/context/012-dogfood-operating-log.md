---
created: "2026-06-27"
purpose: "Operating log — dogfood sinex from the operator's perspective; find & fix architectural/coherence/observability/UX breakage. Long-term branch feature/dogfood/operator-coherence."
status: active
project: sinex
---

# Dogfood operating log (operator-coherence)

Goal (operator, 2026-06-27): straighten out architecturally broken things in
sinex — coherence, observability, UX. Dogfood thoroughly from the operator's
perspective: verify what works and what doesn't, fix the breakage, be nimble.
Work on long-term branch `feature/dogfood/operator-coherence`, independent of the
issue-set. PRs/merge happen later in a concentrated pass.

## Constraints
- Host memory-constrained (swap 100%, polylogue rebuild running) → heavy `sinexd`
  builds get earlyoom-killed. Lighter builds (sinexctl) may work. Runtime
  dogfooding against a live daemon is gated on a build + infra.
- Prod sinexd is DOWN (stale rev 33ad318; do not start until concentrated deploy).
- `sinexctl` on PATH = prod binary (stale rev). Offline/UX surfaces still
  representative for UX dogfooding; behavior-fixes target current source.
- NEVER falsify provenance/clocks (see feedback_never_falsify_clocks). Evidence
  over stale plans.

## Method
operator-surface sweep → note breakage w/ severity → reproduce → fix on branch →
re-verify. Sev: P1 broken/misleading, P2 confusing/incoherent, P3 polish.

## Findings
- **F1 (P2, UX) sinexctl operator errors are developer-noisy.** Expected operator
  errors — missing token, gateway `Connection refused` — render via color-eyre
  default with `Location: crate/sinexctl/src/.../*.rs:NNN` + `Backtrace omitted.
  Run with RUST_BACKTRACE=1` boilerplate, and no actionable hint. `main.rs:160` =
  bare `color_eyre::install()`. FIX: HookBuilder dropping location/env sections by
  default, keeping them when RUST_BACKTRACE set. (exit code is correctly 1 — my
  first read of exit=0 was a `head` pipe artifact; re-verified.)
- **F2 (P3, docs) root command set drift.** `sinexctl --help` exposes 14 top-level
  commands incl. `query` and `show`; README "Command Groups" documents 12 and
  omits `query`/`show`. Also command ordering in help is arbitrary. (README fix.)
- Note: `config show` (resolves even w/o file) and `config path` (clear "create
  with config init" hint) have GOOD UX — keep as reference patterns.

- **F3 (P2, coherence) `events inspect` is an exact duplicate of `events explain`.**
  `events.rs:33,36` both wrap `ExplainCommand`; dispatch `events.rs:57` is
  `Inspect(cmd) | Explain(cmd) => cmd.execute(...)` — identical type, identical
  exec, identical help. `format_registry.rs` + `tui.rs` also treat them as
  equivalent. FIX: remove `inspect` (keep `explain`, tied to the type/file).
  [pending — needs all refs updated: events.rs, format_registry.rs, tui.rs, mcp?]
- **F4 (P3) `runtime list` vs `runtime modules` confusingly named** but NOT
  duplicates: `list` = registered modules (optional role filter), `modules` =
  live presence/health/uptime. Clarify help wording only. [deferred]
- **F5 (P1, BROKEN — dev pipeline dead) dev NATS JetStream cap 256MB << streams'
  2GiB reservations** → `xtask run core` fails every stream create with
  "insufficient storage resources" (10047); material assembler + self-observation
  materializer dead. sinexd File streams reserve up to 2GiB each (events, dlq,
  processing_failures, source_material) + confirmations 512M + KV; JetStream
  reserves max_bytes vs account max_file at create. Sandbox tests use small
  streams → hid it from CI; only real `xtask run core` trips it. FIXED:
  `xtask/src/infra/services/nats.rs` NATS_JETSTREAM_MAX_FILE 256MB→16GB.

## Runtime bring-up notes (dogfooding)
- Dev stack: `xtask infra start` (postgres :5432-ish socket + nats :4308),
  `xtask run core` runs sinexd (API TLS 127.0.0.1:19086). System NATS is :4222
  (32G, separate). Dev nats pid is xtask-managed.
- sinexd boots OK ("RPC server listening on TLS 127.0.0.1:19086") even with the
  NATS storage error (degraded — no event persistence).
- TODO token: gateway auth — need to determine dev token provisioning
  (SINEX_API_TOKEN_FILE / SINEX_API_ADMIN_TOKEN_FILE) to exercise API commands.

## Fixes applied
- `46dcdb78f` fix(sinexctl): quiet operator error reports unless RUST_BACKTRACE (F1, verified).
- (committed) fix(xtask): dev NATS JetStream 256MB→16GB (F5).

## Live dogfood results (dev stack brought up, verified)
Recipe (was non-obvious — see F6): URL https://127.0.0.1:19086, token
`dev-token-<hostname>:admin` (preflight.rs default_dev_rpc_token), mTLS
`--ca-cert .sinex/tls/ca.pem --client-cert .sinex/tls/client.pem --client-key
.sinex/tls/client-key.pem`. Run daemon: `xtask run core --bg` (NOT nohup/fg —
fg dies with the launching shell).
- WORKS: runtime gateway ping→pong, runtime health (all green), runtime automata
  (rich table, 14 automata), sources readiness, sources cockpit, events recent,
  events explain <id> (full details), metrics throughput, ops list, ops dlq list,
  privacy policy (needs subcmd), tasks list, semantic epoch.
- **#2184 CONFIRMED LIVE**: `sources readiness` shows ~30 self-observation
  `#material=<uuid>` rows each MATERIALS=1, caveats `material.staged_unparsed,
  binding.not_in_db, parser.operation_evidence_unjoined`. Degenerate granularity
  is real and operator-visible.
- NON-findings (verified, avoided false reports): `semantic epochs`→ correct cmd
  is `epoch` (singular); `events explain` 404 was my id-extraction grabbing
  view_id not payload.cards[].ref.id; the broad "all commands broken" sweep was
  zsh word-split (`${=cmd}`).
- F6 (P2, UX): dev connection needs 3 separate cert flags + token + URL, all
  undocumented for the local-dev case. A `sinexctl config`/runtime-target preset
  or a `xtask`-emitted dev descriptor would remove this friction. [deferred]

## Fixes applied (branch feature/dogfood/operator-coherence)
- `46dcdb78f` fix(sinexctl): quiet operator error reports unless RUST_BACKTRACE (F1) — verified A/B on fresh binary.
- `868ff5256` fix(xtask): dev NATS JetStream 256MB→16GB (F5) — verified: "Material streams bootstrapped successfully", 0 errors (was many 10047).
- `8b51b473b` refactor(sinexctl): `events inspect`→visible alias of `explain` (F3) — verified: help shows `[aliases: inspect]`, identical dispatch, build clean.
- `055f29ba1` fix(sinexctl): **F8 (P1) `sources stage` panicked on EVERY call** — its
  `--format: SourceMaterialFormat` collided with global `--format: OutputFormat`
  (clap arg-id `format`, type-downcast panic, sources.rs:170). Renamed to
  `--material-format`. VERIFIED: staging now works end-to-end (staged a 35B file,
  status completed, blob stored). Checked no other cmd has a conflicting-type
  `format` arg. Ingestion learning: `sources stage` registers material + CAS blob
  but event_count=0 — parsing needs a binding; server rejects /tmp staging.

## #2184 reflection materials — CORRECTED by dogfooding (don't fix a hypothesis)
- Dev (current master, ~30min steady + 1 graceful restart): **0 one-byte
  self-obs materials**, 75 total, ~5/automaton, batching healthily.
- Prod (rev 33ad318, 14mo): 9,394/9,714 entity-resolver one-byte. REAL.
- `git log 33ad318..937c0fa9 -- self_observation.rs acquisition_manager.rs` = **0
  commits** → code identical → difference is RUNTIME PATTERN not code.
- Could NOT reproduce in dev: entity-resolver is IDLE in dev (no real entity data).
  Trigger correlates with real automaton load, unconfirmed mechanism.
- → REPORT updated: recommend reproducing #2184 under representative load / targeted
  unit test BEFORE fixing. My initial report §6 over-stated it ("1 material/event");
  corrected. Same discipline as the ts_orig-minting retraction: don't fix unseen.
- Dev sinexd currently DOWN (killed during repro); infra (pg/nats) still up.
  Restart for more dogfood: `xtask run core --bg`.

## Report
- /realm/inbox/sinex-operational-report-2026-06-27.md — living operational report
  (what works/how/caveats/perf/UX). Seeded from this session; expand via more dogfood.

## Open / deferred
- F2 (README documents 12 groups, omits top-level `query`/`show`; arbitrary order) — doc fix.
- F4 (`runtime list` vs `modules` vs `automata` naming clarity) — help wording.
- F6 (dev connection ergonomics) — runtime-target/config preset.
- #2184 reflection materialization lifecycle fix + rename (needs build/test; mechanism mapped in issue).
- Verification owed at concentrated pass: `xtask check --full` + `xtask test` over all branch commits.
