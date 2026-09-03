# Sinex Agent Contract

> Local-first event-driven capture platform. Rust nightly / edition 2024,
> NATS JetStream, PostgreSQL (TimescaleDB + pgvector). One workspace, one daemon (`sinexd`),
> one CLI (`sinexctl`), one automation plane (`xtask`). Pre-release, single-operator
> deployment with no external backwards-compatibility obligations.
> Deployed on `sinnix-prime` via the sinnix NixOS flake (`services.sinex.enable`).
>
> This file is the complete always-loaded agent surface. `AGENTS.md` is a symlink to it.
> Deep-dives live in `docs/architecture.md`, `docs/glossary.md`, and `crate/**/docs/`
> (map at the bottom). Keep this file dense; move detail to owned docs.

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

## Toolchain: xtask is the only cargo frontend

**Never bare `cargo` — no exceptions, a hook blocks it.** If xtask lacks a surface, extend
xtask (`feat(xtask)`), don't bypass. `cargo run -p xtask` is also wrong (recompiles xtask;
the binary is on PATH).

| Task | Command |
|---|---|
| fast verify | `xtask check` locally or AgentCTL `check_default` |
| autofix | `xtask fix` locally or AgentCTL `fix_default` |
| tests | `xtask test` locally or AgentCTL `test_default`; use `--impact-mode=off --all` for a deliberate full pass |
| list tests | `xtask test --list -p <pkg>` |
| build | `xtask build -p <pkg>` locally or AgentCTL `build_default` |
| local stack | `agentctl job start sinex dev_services --workspace <workspace-id>`, `xtask doctor`, `agentctl job start sinex run_core --workspace <workspace-id>` |
| lifecycle | Start declared `check_default`, `build_default`, `fix_default`, `test_default`, `run_core`, `run_all_automatons`, `run_all_sources`, `vm_smoke`, and `vm_validate` operations through AgentCTL; use `agentctl job get/logs/result/cancel` with its returned ID |
| failure forensics | `xtask history diagnostics --level error`, `xtask history tests analyze` |
| generated surfaces | `xtask docs sync` / `xtask docs check` |
| schema | `xtask schema strict-diff`, `xtask schema backfill` |
| VM coverage | `xtask test vm --category smoke\|integration` |

AgentCTL owns launch, logs, cancellation, results, checkout identity, service leases, and
process trees for declared operations. The descriptor supports bounded booleans, strings,
string lists, integers, and enums; add typed parameters when a recurring semantic command
needs them. Keep one-off custom source/module selectors as foreground `xtask` runs. Never
pipe xtask through `head` or `tail` because the hook blocks it.

`$SINEX_STATE_DIR` = durable checkout state (`<checkout>/.sinex/state`, holds
`xtask-history.db` — evidence, never delete); `$SINEX_CACHE_DIR`/`CARGO_TARGET_DIR` =
disposable, relocated to `/var/cache/sinex/<user>/<hash>/` by the devshell.

## Verification & Git

- Verification cadence: narrow command for the changed surface while iterating; broad gate
  (`xtask check --full`, `xtask test --impact-mode=off --all`) once per publishable phase.
  Canonical matrix: `TESTING.md`.
- **PR flow to master, ready for review, squash-merge, title = permanent history line ending `(#N)`.** Do not open draft PRs in this repository. A branch is pushed only after its scoped checks and PR body are ready for review. PR body needs Summary / Problem / Solution / Verification (exact commands + the line that matters). No resolver keywords next to issue numbers — `Ref #N` only.
- Pre-push drift guard (`.githooks/pre-push`): schema-bundle check + `--changed-strict` when
  Rust changed. Bypass only in emergencies with `SINEX_SKIP_DRIFT_GUARD=1`, documented.
- **Closure honesty**: Bead `close_reason` text includes a Closure Evidence Manifest;
  `xtask verify closure <bead-id>` checks every AC disposition and executes its commands.
  Deferred rows name follow-up Beads. Never claim "closed by PRs #X–#Y" without checking
  each merge.
- No hosted PR-blocking CI: **the local gate is the gate.**

## Runtime and worktree traps

- Compile-heavy worktrees live under `/realm/worktrees/`. Start `dev_services`
  for that workspace before any database-backed check; the devshell derives one
  PostgreSQL and NATS port pair per checkout, so worktrees never contend.
- SQLx compiles against the live dev database. Never create an offline cache or
  set `SQLX_OFFLINE`; repair the declared development service instead.
- Start runtime operations through AgentCTL and use the returned job ID for
  logs, results, and cancellation. Do not infer ownership from process names.
- Production `sinexd` is a system service. A Sinnix `switch` does not restart
  it because `runtime.restartOnSwitch = false`; when deployment is intended to
  take effect immediately, explicitly restart it and verify the active binary,
  unit generation, and schema convergence.

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
