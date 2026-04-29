# Source-unit declaration & promotion contract

The `source_unit` module formalizes the contract every ingestor must
satisfy before it can be considered promoted. Closes the keystone of
issue #690 and folds in #691 (horizons), #699 (retention), and #700
(`MutableSnapshot` checkpoint family).

## Why this exists

Before the descriptor, what an ingestor was — its identity, what it
emits, how it captures, what privacy tier it occupies, what proof
obligations gate its merge — was implicit knowledge spread across
neighbour ingestors, target-vision prose, scratch notes, and CLAUDE.md
conventions. Each new ingestor re-derived the contract from whatever was
nearby, which is how silent drift accumulated between privacy
expectations, replay obligations, and per-source idioms.

The descriptor makes the contract executable. It is a typed promise
collected through `inventory`, walkable from any binary, and
inspectable by tooling without affecting the runtime path.

## Shape

A `SourceUnitDescriptor` declares:

| Field | Meaning |
|---|---|
| `id` | Canonical short name; matches an `EventSource` value the binary uses. |
| `namespace` | Logical grouping (`"shell"`, `"filesystem"`, `"desktop"`). |
| `checkpoint_family` | Which SDK checkpoint adapter (`AppendStream`, `MutableSnapshot {…}`, `Journal`, `Polling`, `LiveObservation`). |
| `event_types` | `(source, event_type)` pairs the binary promises to emit. |
| `privacy_tier` | `Public`, `Sensitive`, or `Secret` (placeholder pending #455/#460). |
| `runtime_shape` | `Continuous`, `OnDemand`, or `Scheduled`. |
| `horizons` | Which time horizons the binary serves: `Continuous`, `Historical`, or both. |
| `retention` | `Forever`, `Days`, or `Tiered` retention policy. |
| `proof_obligations` | Proof-catalog scenarios that must pass for promotion. |
| `occurrence_identity` | `Uuid5From(…)`, `Natural`, or `Anchor`. |

## Folded sub-issues

**#691 (horizons).** `horizons` formalizes that historical and
continuous are *two horizons of one source-unit contract*, not two
persistence planes. Both flow through the same NATS → ingestd → DB
pipeline. There is no historical persistence backdoor; if a future case
ever justifies bulk-COPY backfill it must be ingestd-owned, gated behind
an explicit benchmark + scenario, and clearly labeled an exception.

**#699 (retention).** `retention` belongs on the descriptor because the
policy is declared by the source. The maintenance runtime (separate
follow-up) reads descriptors at startup, evaluates policies against
event timestamps, and runs cascade-aware archival. Default is
`Forever` — for personal-history sources, archiving is opt-in.

**#700 (MutableSnapshot).** `CheckpointFamily::MutableSnapshot {
backing_store_kind, occurrence_anchor }` lifts the SQLite-snapshot
pattern from a node-sdk special case into a named primitive. The same
shape unblocks ActivityWatch, Fish history, Reddit/Spotify exports, and
future Messenger / Chrome backing stores. The snapshot becomes linked
evidence; the row stream remains canonical event provenance.

## Registration

```rust
use sinex_primitives::register_source_unit;
use sinex_primitives::source_unit::*;

register_source_unit! {
    SourceUnitDescriptor {
        id: "terminal",
        namespace: "shell",
        checkpoint_family: CheckpointFamily::MutableSnapshot {
            backing_store_kind: "sqlite",
            occurrence_anchor: "atuin_history_id",
        },
        event_types: &[("shell.atuin", "command.executed"), …],
        privacy_tier: PrivacyTier::Sensitive,
        runtime_shape: RuntimeShape::Continuous,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &["terminal_smoke", "terminal_history_replay"],
        occurrence_identity: OccurrenceIdentity::Uuid5From(
            "(source_unit, atuin_history_id)"),
    }
}
```

`register_source_unit!` is a thin wrapper over `inventory::submit!`.
Walk the registry through `sinex_primitives::source_unit::all_source_units()`
or `find_source_unit(id)`.

## Promotion gate

A new ingestor must not merge until:

1. Its `SourceUnitDescriptor` is filled in (every field, no defaults).
2. The declared `event_types` resolve to live `EventPayload` constants.
3. The declared `proof_obligations` resolve to scenarios in the proof
   catalog.
4. Those scenarios pass.

Cross-reference enforcement is the job of the `xtask issue-drift` /
`xtask docs` tooling layer (issue #694, separate PR). The descriptor
itself is the source of truth.

## Status of acceptance criteria

The descriptor type and macro land here. Terminal is backfilled as the
proven case. The remaining acceptance criteria from #690 are tracked as
explicit follow-ups:

- *Cross-reference verification (xtask warnings on missing
  descriptors):* deferred to the issue-drift detector work (#694).
- *Backfill remaining ingestors:* mechanical follow-up; one PR per
  ingestor crate.
- *CLAUDE.md / CONTRIBUTING references the descriptor as the promotion
  gate:* updated in this PR.
- *At least one new ingestor lands using the descriptor as primary
  contract:* deferred — gated on a real new-ingestor PR. Will be cited
  retroactively when the first such PR lands.
