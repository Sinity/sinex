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
| `proof_obligations` | Catalog-backed obligation IDs plus descriptor-local verification tags; only `obligation:*` entries are hard catalog references. |
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

`SourceUnitDescriptor` is **semantic-only** — it describes the (source, event_type)
contract, privacy tier, retention policy, and verification tags/catalog
obligations. Deployment-shape fields (`runner_pack`, `runtime_shape`,
`checkpoint_family`, `package_impact`, `implementation_mode`, `build_impact`)
live on a paired `SourceUnitBinding`, keyed by `source_unit_id` (FK to the
descriptor's `id`). See `proof.rs` and `docs/design/event-taxonomy-v2.md`
Section 9 for the split rationale.

```rust
use sinex_primitives::{register_source_unit, register_source_unit_binding};
use sinex_primitives::proof::{
    SourceUnitDescriptor, SourceUnitBinding, SourceUnitBuildImpact,
    PrivacyTier, Horizon, RetentionPolicy, OccurrenceIdentity,
    CheckpointFamily, RuntimeShape, SubjectRef,
};

register_source_unit! {
    SourceUnitDescriptor {
        id: "terminal.atuin-history",
        namespace: "terminal",
        event_types: &[("shell.atuin", "command.executed")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        proof_obligations: &[
            "obligation:source_unit.material_provenance",
            "obligation:source_unit.package_impact_rationale",
        ],
        occurrence_identity: OccurrenceIdentity::Natural,
        access_policy: "target_home_read:.local/share/atuin/history.db",
    }
}

register_source_unit_binding! {
    SourceUnitBinding::builder(
        SubjectRef::from_static("source_unit:terminal.atuin-history"),
        "terminal.atuin-history",
        "terminal",
    )
    .implementation("sinex-terminal-ingestor::atuin")
    .adapter("sqlite_row_stream")
    .output_event_type("command.executed")
    .privacy_context("command")
    .material_policy("canonical_json_lines")
    .checkpoint_policy("sqlite_row_id")
    .resource_shape("linear_rows_bounded_memory")
    .source_unit_id("terminal.atuin-history")
    .runner_pack("terminal")
    .checkpoint_family(CheckpointFamily::MutableSnapshot {
        backing_store_kind: "sqlite",
        occurrence_anchor: "atuin_history_id",
    })
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("no_new_output")
    .implementation_mode("rust_in_pack:terminal")
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build()
}
```

Both macros are thin wrappers over `inventory::submit!`. Walk the registries via
`sinex_primitives::proof::all_source_units()` and
`sinex_primitives::proof::source_unit_bindings()`.

## Promotion gate

A new ingestor must not merge until:

1. Its `SourceUnitDescriptor` is filled in (every field, no defaults).
2. The declared `event_types` resolve to live `EventPayload` constants.
3. Declared `obligation:*` entries resolve to known proof-catalog obligations;
   descriptor-local tags remain advisory verification metadata.
4. Any `required` obligation has a runner binding and a checkable command.
5. Scenario metadata separates catalog-backed `claim:*` IDs from free-form
   assertion IDs.
6. Passing evidence is required only where a concrete runner/check has been
   wired; advisory/deferred obligations are visible backlog, not a promotion
   gate.

Cross-reference enforcement is the job of `xtask docs proof-catalog --check`
and `xtask docs check`. The descriptor itself is the source of truth for the
source-unit contract, while the generated proof catalog is the cross-reference
surface that prevents required catalog obligations from becoming unchecked prose.

## Status of acceptance criteria

The descriptor type and macro land here. Terminal is backfilled as the
proven case. The remaining acceptance criteria from #690 are tracked as
explicit follow-ups:

- *Cross-reference verification (xtask warnings on missing
  descriptors):* superseded by the proof-catalog/source-unit validation
  work (#1129/#1099).
- *Backfill remaining ingestors:* mechanical follow-up; one PR per
  ingestor crate.
- *CLAUDE.md / CONTRIBUTING references the descriptor as the promotion
  gate:* updated in this PR.
- *At least one new ingestor lands using the descriptor as primary
  contract:* deferred — gated on a real new-ingestor PR. Will be cited
  retroactively when the first such PR lands.
