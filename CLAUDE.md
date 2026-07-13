# Sinex Agent Memory

> Local-first event-driven capture platform. Rust nightly / edition 2024,
> NATS JetStream, PostgreSQL (TimescaleDB + pgvector). One workspace, one daemon (`sinexd`),
> one CLI (`sinexctl`), one automation plane (`xtask`). Pre-release, single-operator
> deployment with no external backwards-compatibility obligations.
> Deployed on `sinnix-prime` via the sinnix NixOS flake (`services.sinex.enable`).
>
> This file is the complete always-loaded agent surface. `AGENTS.md` is a symlink to it.
> Deep-dives live in `docs/architecture.md`, `docs/glossary.md`, and `crate/**/docs/`
> (map at the bottom). Keep this file dense; move detail to owned docs.

## Operator Contract

- **Full autonomy is granted**: diagnose → fix → build → deploy → verify solo. Merge PRs,
  deploy, restart prod without asking. Destructive operations (data deletion, history
  rewrite, wipes) still get stated intent first.
- **Solve fundamentally, not fast-unlock.** Prod-being-up is NOT required; take it down and
  build the root-cause fix rather than cgroup bumps / temp swap / let-it-grind.
- **Finish end-to-end.** "Foundation for X" means build X now. Don't commit stubs silently;
  finish or declare incomplete. Don't halt mid-plan to ask "continue?" — take the next step
  and record assumptions.
- **Cleanliness beats entropy**: modify directly, delete replaced paths in the same change.
  No deprecation shims, no parallel old/new paths, no "removed in a follow-up".
- **Facts beat assumptions**: act on code and live systems, not memory or invented
  constraints ("time", "safety", "won't run in CI" are not reasons to skip building).
- **Use general mechanisms.** Before inventing a per-source field/policy/scope, map the need
  onto the existing privacy / disclosure / lifecycle / coverage / session-state planes.

## Public Repository Boundary

Treat every tracked file, commit message, branch, tag, Beads issue, CI log, and
GitHub discussion as public.

- Capture code, schemas, deployment interfaces, and unmistakably synthetic
  fixtures belong in Git. Secrets, real encrypted secret payloads, private
  datasets or exports, transcripts, narratives, identity profiles, and
  unrelated personal information do not.
- Operator-specific identities, account names, private source roots, and
  deployment credentials come from external configuration. Fixtures must not
  encode real-looking personal finance, health, employment, or activity data.
- Before committing, review the complete staged diff as public content. Path
  and regex checks cannot decide whether prose, fixtures, or commit messages
  are appropriate.
- If there is any doubt whether material belongs in the public repository,
  confirm with the operator before committing it.
- Publish product changes through the normal PR flow. Review every additional
  branch or tag independently; never use `--mirror`, `--all`, or `--tags` as a
  publication shortcut.
- If private material enters history, stop publication and rewrite the allowed
  ref. A later deletion does not remove the historical blob or message.

### Doctrine tripwires (each of these has burned a session)

- **Never falsify provenance clocks.** No backdating `ts_coided`, no minting UUIDv7 from
  `ts_orig`. The clocks are SUPPOSED to differ for imports — that difference is the point.
- **State is durable.** Sinex state should never need wiping; fix forward via archive
  cascade + replay. A wipe proposal is a design-failure signal.
- **Privacy/redaction is a presentation feature**, not a security boundary. Source access
  and deployment isolation own confidentiality; don't treat display redaction as a
  substitute for either.
- **sqlx compiles against the LIVE dev DB only.** No `.sqlx/` offline cache, no
  `SQLX_OFFLINE`. On connection-refused, fix the dev DB (`xtask doctor`), never work around.
- **No id-based idempotency, no `UNIQUE(material_id, anchor_byte)`, no content-derived
  event ids.** If you reach for any of these you've misread the identity model (below).
- **Don't trust closing comments / issue text over `master`.** Verify landing claims with
  `git log`/`gh pr view --json state,mergedAt` before building on them.

## Session Orientation (do this before substantive work)

1. `bd prime` — Beads is the task substrate AND the devloop: `bd ready` → claim
   (`bd update <id> --claim`) → work → PR → close with reasons + verification commands;
   create linked beads for discovered follow-ups. No markdown TODO lists as shared truth.
   The former bespoke conductor packet is archived at `.agent/archive/devloop-2026-07/`
   (evidence corpus, never scaffold — do not resurrect it or `devloop-*` scripts).
2. The **current code-grok entry point** is recorded in the bd memory `code-grok-*` key and
   lives at `.agent/scratch/NNN-grok-*.md` (highest NNN wins; each note says what it
   supersedes). Read it before trusting older scratch/audit claims — external audit line
   numbers are usually stale against master.
3. `.agent/scratch/` is the session-notes plane (gitignored thinking space);
   `.agent/scratch/new/` is an inbox for external analysis batches (verify before trusting).
   `.agent/CONVENTIONS.md` holds the execution-grade bead bar and repo agent conventions.
4. Reconcile-on-claim: when claiming a bead, re-verify its cited file:line facts against
   current master before coding.

## Architecture Core

### The provenance model (the one load-bearing idea)

Every event has exactly one provenance:

- **Material** (`source_material_id` set, `source_event_ids` NULL): "I interpreted this byte
  range of this registered source material." Replay = re-read the material.
- **Derived** (`source_event_ids` set, material NULL): "I derived this from these parent
  events." Replay = re-run the automaton on the parents.

Enforced at four levels: `EventBuilder` typestate (no `.build()` without provenance), serde
wire format (rejects both/neither), DB XOR CHECK, `NonEmptyVec` for parents.

**Three clocks** on every event:

| Clock | Meaning | Across replay |
|---|---|---|
| `ts_orig` | when it happened in the world (quality-ranked from `raw.temporal_ledger`; may arrive `None` and resolve at persistence) | stable |
| `ts_coided` | when sinex minted THIS interpretation — generated column `uuid_extract_timestamp(id)`, not independent | new |
| `ts_persisted` | row write time (column DEFAULT) | new |

Query by `ts_orig` for "what happened", `ts_coided` for "what did sinex interpret when".
Continuous aggregates bucket on `ts_coided` — historical imports are invisible to them
without explicit refresh.

**Identity**: the event `id` (random UUIDv7) is *interpretation* identity — replay mints new
ids. *Occurrence* identity is the `(source_material_id, anchor_byte)` columns — stable
across replay, never the PK. One live interpretation per occurrence is upheld by replay
archiving the old row first (hypertables can't UNIQUE on it). Occurrence dedup, where it
exists, is the admission-time `equivalence_key` check (fail-open) — downstream and
object-level, never the PK.

**Replay is not idempotent by design**: archive cascade → scope invalidation → NATS scan
command → source re-reads → fresh events through the NORMAL pipeline with current rules.
New `id`, new `ts_coided`, same occurrence coordinates.

### Storage/authority split (interpretation-plane doctrine)

```
raw material / raw events     durable witness layer
projection rows               rebuildable scoped read models
candidate claims              evidence-carrying interpretations
operator judgments            explicit promotion/rejection decisions
presentation/context packs    consumers, never authorities
```

A derivation that cannot say which layer it writes is not ready to implement. Canonical
derived events and accepted claims are the only default inputs to further derivation.
Candidate confidence defaults must be unknown/low — never 1.0 with empty evidence refs.

### Pipeline (source → query)

```
adapter drains records → materialize (anchor into raw.source_material_registry)
→ parse → EventBuilder.from_material() → EventEmitter (mpsc)
→ EventBatcher (100/1s) → NATS raw stream (per-lane subjects)
→ sinexd::event_engine: admission (parse/schema/plausibility/equivalence-key dedup)
→ MaterialReadySet FK gate (NAK+retry → DLQ after budget)
→ ts_orig resolution from temporal ledger → central redact_batch chokepoint
→ persist (derived → REPEATABLE READ QueryBuilder; big material batches → COPY)
→ confirmed publish GATES the ack (durability gap = redelivery)
→ per-automaton durable consumers on the confirmed stream → derived events re-enter
→ SSE bus / sinexctl / MCP read surfaces
```

Two storage lanes: `core.events` (activity) and `reflection.events` (self-observation),
routed by `SourceRole`, each with its own JetStream consumer and retention. Self-observation
must not pollute activity surfaces.

Failure routing: JSON/schema failures → DLQ; FK-not-ready → NAK+delay; poison rows in a COPY
batch → bisect halves, isolate → DLQ; retryable → NAK; terminal delivery count → DLQ.

Sources of truth that drift-proof this section: automata census = `AutomatonSpec` registry
(`crate/sinexd/src/automata/registry.rs`); stream/consumer shapes =
`event_engine/jetstream_consumer/bootstrap.rs` + `nixos/modules/nats.nix`; telemetry
relations = `TELEMETRY_*` constants in `crate/sinex-schema/src/apply.rs`.

### Known open correctness seams (verified; tracked in bd epic r6d)

- Emission is an in-memory handoff end-to-end until NATS publish: source cursors and
  automaton checkpoints can durably advance before outputs are durable (loss window on
  crash). Repair frame = one shared durable-emission receipt (beads r6d.4, vxu, r6d.7, w4i).
- Recovery spool caps preserved lines and permanently discards the rest.
- Health defaults to Healthy (missing reporter counts as healthy).
- Do not "fix" these ad-hoc in passing — they are sequenced campaign work with a shared
  primitive; check bd state first.

### Schema map

| Schema | Holds |
|---|---|
| `core` | `events` (hypertable, partition by UUIDv7 `id`), `blobs`, entities/relations/tags, embeddings, tombstones, operations log, `source_session_state`, `model_effects` |
| `reflection` | `events` — self-observation lane, own retention |
| `raw` | `source_material_registry`, `temporal_ledger` — provenance roots |
| `audit` | `archived_events` — replay target, immutable |
| `sinex_schemas` | payload schema registry, validation cache, DLQ, backfill runs |
| `sinex_telemetry` | continuous aggregates + views (constants in apply.rs) |

Schema evolution = declarative convergence (`sinex-schema apply`), NOT migrations. Drift the
apply engine doesn't reconcile → `xtask schema strict-diff`. Explicit data repairs →
`xtask schema backfill`.

### Type-enforcement ladder

When adding an invariant, pick the strongest affordable level — and never leave a
correctness invariant at convention-only:

1. compile-time (typestate, phantom-typed `Id<T>`, newtypes, `NonEmptyVec`)
2. lint / forbidden-pattern gate (`xtask check --forbidden`, ast-grep catalog)
3. DB constraint (CHECK / FK / trigger)
4. runtime validation at boundaries (`validate_path`, `validate_json`, admission)
5. startup/lazy contract check (e.g. COPY column contract)
6. convention only (danger zone — document why if something must live here)

## Workspace Map

```
crate/sinex-primitives   types, errors, Id<T>, Timestamp, events+builder, privacy engine,
                         domain enums, validation, transport taxonomy, authority/llm scaffolding
crate/sinex-schema       schema defs + declarative convergence (apply/converge/strict_diff)
crate/sinex-db           pools, repositories (DbPoolExt), COPY protocol, PKM orchestration
crate/sinex-macros       #[derive(EventPayload)]
crate/sinexd             the daemon: event_engine / api (JSON-RPC+SSE+MCP-backing) / sources /
                         runtime (drivers, automaton adapter, checkpoints, replay) / automata /
                         supervisor
crate/sinexctl           operator CLI: events, query, recall, show, sources, runtime, ops,
                         privacy, semantic, docs, metrics, tui, mcp
tests/e2e, tests/vm-suite, tests/workspace
xtask                    build/test/infra/docs/history automation (the only cargo frontend)
nixos/                   deployment modules (canonical deployment surface)
```

Import decisions: types/errors/ids from `sinex_primitives::prelude::*`; DB via
`sinex_db::DbPoolExt` repositories (`pool.events()`, `.source_materials()`, …) — never raw
`sqlx::query!` on a pool outside repositories; runtime traits from `crate::runtime::*`
inside sinexd.

## Code Patterns

Event creation — typed payload + provenance, nothing else:

```rust
let ev = FileCreatedPayload { .. }.from_material(material_id).build()?;      // ingestor
let ev = SummaryPayload { .. }.from_parents(parent_ids)?.build()?;           // automaton
// escape hatch: EventBuilder::dynamic("source", "type", json!(..)).from_material(m, anchor)
```

Errors — always `SinexError` with context; `public_payload()` for API/CLI surfaces
(Display/Debug are internal and may leak paths/SQL). `with_error_source(&e)` at typed
boundaries. xtask uses `color_eyre`.

| Situation | Use | Never |
|---|---|---|
| Event id / timestamps / source+type | `Id<Event>`, `Timestamp`, `EventSource`/`EventType` newtypes | raw Uuid/String/OffsetDateTime |
| Status/tier/field values | domain enums (`sinex_primitives::domain`, `events::enums`) | string comparison |
| DB queries | `sqlx::query!` (compile-checked) inside repositories | bare-string `sqlx::query` |
| Lazy/once | `std::sync::LazyLock` / `OnceLock` | lazy_static / once_cell |
| Async closures (single call) | `F: AsyncFnOnce() -> T` | boxed future gymnastics |
| Async closures (polling) | `F: Fn() -> Fut` + `\|\| async {..}` | `async \|\| {..}` (breaks Send) |
| Tests | `#[sinex_test]`, per-crate `tests/`, `Timeouts::*`, `wait_for_condition` | `#[tokio::test]`, sleeps, magic numbers |
| Test events needing DB | `ctx.publish(payload)` | manual insert (FK violation) |

Edition 2024: `std::env::set_var` is `unsafe`; let-chains are in; RPIT capture via
`+ use<'a>`.

Runtime shapes (registration: `register_source_contract!` / `register_source_runtime_binding!`
for sources, `AutomatonSpec` in `automata/registry.rs` for automata):

| Building | Implement | Model |
|---|---|---|
| raw capture from the world | `SourceDriver` (+ adapter/parser split: `InputShapeAdapter` yields anchored records, `MaterialParser` yields intents) | snapshot / historical / continuous scans |
| 1:1 derived transform | `Transducer` | stateless |
| accumulate-then-emit | `Windowed` | window_complete + periodic flush |
| per-scope reconciliation | `ScopeReconciler` | scope state |

Every automaton emits ONE `output_event_type()`. Derived events carry `automaton_model`,
`semantics_version`, `equivalence_key` — set them.

## Toolchain: xtask is the only cargo frontend

**Never bare `cargo` — no exceptions, a hook blocks it.** If xtask lacks a surface, extend
xtask (`feat(xtask)`), don't bypass. `cargo run -p xtask` is also wrong (recompiles xtask;
the binary is on PATH).

| Task | Command |
|---|---|
| fast verify | `xtask check` (`--lint`, `--full` for broad) |
| autofix | `xtask fix` / `xtask fix --smart` |
| tests | `xtask test` (impact-planned) · `-p <pkg>` · `-E 'test(name)'` · `--heavy` · `--impact-mode=off --all` for deliberate full pass |
| list tests | `xtask test --list -p <pkg>` |
| build | `xtask build -p <pkg>` |
| local stack | `xtask infra start/status/stop`, `xtask doctor`, `xtask run core --bg` |
| background | append `--bg`; poll `xtask jobs active/output/wait <id>` |
| failure forensics | `xtask history diagnostics --level error`, `xtask history tests analyze` |
| generated surfaces | `xtask docs sync` / `xtask docs check` |
| schema | `xtask schema strict-diff`, `xtask schema backfill` |
| VM coverage | `xtask test vm --category smoke\|integration` |

Async-first: spawn `--bg`, keep working, poll. One plain `--bg` call — don't nest it in
shell background (duplicate runs collide on the target lock). Read the printed job id; exit
code at `.sinex/state/jobs/<id>/exit_code`. Never pipe xtask through `head`/`tail` (hook
blocks it). Never combine `--workspace` with `-p`.

`$SINEX_STATE_DIR` = durable checkout state (`<checkout>/.sinex/state`, holds
`xtask-history.db` — evidence, never delete); `$SINEX_CACHE_DIR`/`CARGO_TARGET_DIR` =
disposable, relocated to `/var/cache/sinex/<user>/<hash>/` by the devshell.

## Verification & Git

- Verification cadence: narrow command for the changed surface while iterating; broad gate
  (`xtask check --full`, `xtask test --impact-mode=off --all`) once per publishable phase.
  Canonical matrix: `TESTING.md`.
- **PR flow to master, squash-merge, title = permanent history line ending `(#N)`.** PR body
  needs Summary / Problem / Solution / Verification (exact commands + the line that
  matters). No resolver keywords next to issue numbers — `Ref #N` only.
- Pre-push drift guard (`.githooks/pre-push`): schema-bundle check + `--changed-strict` when
  Rust changed. Bypass only in emergencies with `SINEX_SKIP_DRIFT_GUARD=1`, documented.
- **Closure honesty**: Bead `close_reason` text includes a Closure Evidence Manifest;
  `xtask verify closure <bead-id>` checks every AC disposition and executes its commands.
  Deferred rows name follow-up Beads. Never claim "closed by PRs #X–#Y" without checking
  each merge.
- No hosted PR-blocking CI: **the local gate is the gate.**

## Traps (verified the hard way)

- **Worktrees**: put compile-heavy worktrees under `/realm/tmp/worktrees/` (never `/tmp` —
  wear-limited disk). An inherited `CARGO_TARGET_DIR` pointing at the main checkout makes
  worktree checks false-pass in <1s; xtask self-corrects with a WARNING — a real check takes
  minutes. Worktree devshells inherit a broken `DATABASE_URL`; read the real one from the
  main checkout's devshell and `env DATABASE_URL=... ` explicitly (also for `git push`).
- **Memory pressure**: earlyoom SIGTERMs rustc when free <15% (exit 144 ≠ code error). One
  heavy compile at a time; don't run two heavy `xtask test --bg` (target-lock collision).
  Clippy on the full workspace can exceed the 600s cargo timeout under load —
  `SINEX_CARGO_TIMEOUT=1800`, not lock-contention theories.
- **Dev runtime**: `xtask run core --bg` (foreground self-times-out); kill sinexd by PID,
  never `pkill -f`. Dev gateway/token/TLS coordinates come from `xtask run core --dry-run`.
- **Prod**: sinexd is a SYSTEM service (`sudo systemctl stop/start sinexd`); prod DB is the
  system PostgreSQL on TCP 5432 (`sinex_prod`), not the dev socket. Deploy = pin sinex rev
  in the sinnix flake, then `nix develop --command switch` from `/realm/project/sinnix`.
- **git**: `git branch --show-current` before committing (sessions have committed to the
  wrong branch). Stage by path. Agent-authored text never puts resolver keywords next to
  issue numbers.
- **zsh**: no word-splitting — `bash -c` for scripts that assume it; `${=var}` to split.

## Docs Map (content owned elsewhere)

| Topic | Location |
|---|---|
| Architecture deep-dive (provenance long-form, 23-step lifecycle, trust boundaries, thresholds, enforcement tables, NATS topology) | `docs/architecture.md` |
| Glossary | `docs/glossary.md` |
| Contributor workflow / PR norms / closure verification | `CONTRIBUTING.md` |
| Test matrix | `TESTING.md` |
| xtask command guide + reference | `xtask/docs/command-guide.md`, `xtask/docs/command-reference.md` |
| sinexctl CLI | `crate/sinexctl/README.md`, `crate/sinexctl/docs/` |
| Event engine / API / sources / automata / replay | `crate/sinexd/docs/**` |
| DB schema design, repositories, lifecycle, backup | `crate/sinex-db/docs/**` |
| Type system, errors, newtypes, transport, knowledge boundaries | `crate/sinex-primitives/docs/**` |
| Deployment modules, TLS, env vars, resource scoping, threat model | `nixos/modules/**` |
| Issue/PR operating model, CI policy, authority surfaces, claim ledger | `.github/**` |
| Vision / roadmap | `/realm/project/sinex-target-vision/` |
| Agent conventions, scratch, bead bar, graph lints | `.agent/CONVENTIONS.md`, `.agent/README.md` |
