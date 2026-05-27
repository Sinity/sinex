# Event Taxonomy v2 — EventSource Demotion

Status: design record for #1082. Implementation deferred to post-Wave-4 per #1126.

## 1. Current State

98 `(source, event_type)` pairs across 16 payload domain files. 30 distinct `source` values, 88 distinct `event_type` values. Full inventory at `.agent/scratch/recon-wave1-lane4-taxonomy-descriptors.md`.

### What `source` conflates (6 overloaded semantics)

| # | Semantics | Current location | Examples |
|---|-----------|-----------------|----------|
| 1 | Schema namespace | Registry key `(source, event_type)` | `fs-watcher:file.created` |
| 2 | Source-unit identity | `SourceUnitDescriptor.id` | `terminal.atuin`, `wm.hyprland` |
| 3 | Runtime producer | `Event<T>.source` column | `sinex.ingestd`, `sinex.gateway` |
| 4 | NATS routing token | Subject template | `events.raw.{source}.{event_type}` |
| 5 | Query filter dimension | `EventQuery.sources` | `--source fs-watcher` |
| 6 | Domain/material family | Implicit grouping | desktop, system, terminal |

## 2. Collision Analysis

88 of 98 pairs are already globally unique by event_type alone. 10 collisions:

| event_type | Sources | Resolution |
|-----------|---------|------------|
| `command.executed` | shell.kitty, shell.atuin, shell.history.{bash,zsh,fish} | Prefix with source domain |
| `device.connected` | dbus, udev | Prefix with source domain |
| `monitoring.started` | system, desktop, terminal | Prefix with source domain |

## 3. Proposed Taxonomy

**Rename rules:**
1. Dot-namespaced by domain: `{domain}.{kind}.{action}`
2. Source-specific prefixes for shared kinds
3. 88 already-unique types stay as-is
4. New types use deep dot notation from the start

**Target fields on `core.events`:**

| Field | Type | Replaces |
|-------|------|----------|
| `event_type` | `EventType` | (source, event_type) — globally unique semantic kind |
| `source_unit_id` | `SourceUnitId` | source-as-identity |
| `producer_id` | `ProducerId` (new) | source-as-runtime |
| `source` | `EventSource` | Retained as compatibility alias, then renamed |

## 4. Migration Stages

1. Add `source_unit_id`, `producer_id` columns (nullable), populate from descriptor lookup
2. Migrate schema registry key to `(event_type, schema_version)`
3. Migrate NATS subjects to `events.raw.{event_type}`
4. Migrate query surfaces: `--source` → `--source-unit`
5. Drop `source` column compatibility

## 5. First Implementation Slice

`sinexctl verify --source-units` (implemented in PR #1142) cross-checks descriptor declarations against payload inventory. This is the first non-doc consumer. Next: add `source_unit_id` to `core.events`.

## 6. Non-Goals

- Do not perform schema migration (design only)
- Do not rename event types casually
- Do not blur material vs derived provenance

Refs: #1054, #1081, #1058, #1059, #1064, #1126.

## 7. Descriptor / Binding Split — Implementation Status

Earlier drafts of this design described a future split between a host- and
deployment-agnostic *semantic descriptor* (`SourceUnitDescriptor`) and a
deployment-shaped *binding* (`SourceUnitBinding`) that records unit name,
target user, paths, and runtime knobs for a particular host. As of #1184 the
structural prep for that split is in place, and #1175 (Phase 4) ties off the
remaining items so the catalog matches the v2 taxonomy.

### Landed in #1184 (structural prep)

- `RuntimeUnitDescriptor` was renamed to `SourceUnitBinding`. There is no
  third type; the binding is the only deployment-shaped declaration.
- `SourceUnitBinding` gained a `proposed: bool` marker so future-state
  bindings can be registered with `proposed: true` without being treated as
  live deployments.
- `SourceUnitBinding` gained a `source_unit_id: &'static str` foreign-key
  field that points at the descriptor it deploys. An empty string is
  permitted for legacy/pre-FK bindings; a non-empty value that does not
  resolve to a registered descriptor is a build-equivalent failure.
- `xtask source-units check` enforces the FK at validation time
  (`unresolved_binding_source_unit_ids` in the validation report) and the
  rendered manifest carries a new `proposed_bindings` array so future-state
  bindings are visible in tooling without being mistaken for live units.

### Landed in #1175 Phase 4 (this slice)

- Infra source-unit descriptors are registered for `blob.*` payloads
  (`blob-storage`) and for `sinex.*` self-observation payloads
  (`sinex-process-lifecycle`, `sinex-automaton-error`, `sinex-metrics`,
  `sinex-ingestd-telemetry`, `sinex-gateway-telemetry`,
  `sinex-node-telemetry`). These are descriptor-only — they have no
  `SourceUnitBinding` because the events are produced from inside binaries
  that already have their own pack bindings (no dedicated `sinex-infra`
  systemd unit). The `infra` runner pack maps to a `<embedded>` sentinel
  binary in `xtask source-units check`, and the same units are exempted
  from the static-emitter-backing rule because their producers are
  dispersed across multiple call sites in multiple binaries.
- `sinexctl verify --source-units` cross-checks every
  `SourceUnitDescriptor.event_types` pair against the `EventPayload`
  inventory. It reports two gap classes: orphan descriptor pairs (a
  descriptor declares a `(source, event_type)` with no matching payload)
  and unclaimed payloads (a payload has no `register_source_unit!` claim).
  The CLI prefers the xtask-rendered `docs/source-units.json` manifest as
  its descriptor source because the CLI binary does not link the node
  crates at compile time; when the manifest is unavailable it falls back
  to the in-binary descriptor inventory and emits a coverage caveat.

### Future migration: descriptor-field split

The current `SourceUnitDescriptor` still carries a small amount of
deployment-shaped state — `runner_pack`, `runtime_shape`, `access_policy`,
`implementation_mode`, and the `package_impact` / `build_impact` block.
These are still legitimately *descriptor* fields today because they
describe the unit's intrinsic shape (it is a continuous service, it lives
in this runner pack, it is a Rust source unit, etc.) rather than a
particular host's wiring. The next slice (deferred) will do one more
round of separation:

- Move `runner_pack`, `runtime_shape`, `access_policy`, and
  `implementation_mode` onto `SourceUnitBinding` so a single semantic
  descriptor can be deployed under different runner packs (e.g. the
  filesystem source unit deployed as a continuous watcher *or* as a
  scheduled scan unit).
- Keep `package_impact` and `build_impact` on the descriptor — they are
  promotion-gate evidence about the unit's intrinsic shape, not a
  deployment knob.
- Add the missing NixOS-module generation step so adding a binding adds a
  systemd unit without manual Nix edits. PR #1184 does the type-shape
  prep; the actual Nix generation is the deferred slice.

### Manifest fields visible to tooling

`xtask source-units render` writes `docs/source-units.json`. After #1184
the manifest also surfaces:

- `proposed_bindings: Vec<ProposedBindingManifest>` — every
  `SourceUnitBinding` registered with `proposed: true`, sorted by
  subject. Useful for "show me what is on the roadmap but not yet a
  live unit."
- `validation.unresolved_binding_source_unit_ids` — every binding whose
  `source_unit_id` is non-empty but does not resolve to a registered
  descriptor. This is a hard failure in `xtask source-units check`.

### Invariants

The following invariants are enforced by `xtask source-units check`,
checked by `sinexctl verify --source-units`, and described in this
section so future agents can recognise the design intent:

1. **Every binding has a descriptor.** A binding's `source_unit_id` must
   either be empty (legacy) or resolve to a registered descriptor.
   Build-equivalent failure: `unresolved_binding_source_unit_ids` is
   non-empty.
2. **A descriptor without a binding is allowed.** Descriptor-only
   registrations are legitimate when (a) the unit is a roadmap proposal
   (covered separately by `proposed: true` bindings) or (b) the unit is
   an *infra source unit* whose runtime owners are existing pack
   bindings — see the seven `infra` descriptors registered by
   `crate/lib/sinex-primitives/src/events/payloads/{blob,process,metrics}.rs`.
3. **Every declared `(source, event_type)` payload pair is claimed by
   exactly one descriptor.** `sinexctl verify --source-units` reports
   orphan descriptor pairs and unclaimed payloads; both must be empty
   for the check to pass.
4. **Every descriptor's `(source, event_type)` pair must resolve to a
   registered `EventPayload`.** Removing a payload without removing the
   descriptor is a build-equivalent failure
   (`invalid_output_event_pairs`).

## 8. Consumer Surfaces

The descriptor / binding split exists because there are real consumers
that benefit from each side. Without these the split would be
ceremony-only and #1129 would block it.

| Surface | Reads | Notes |
|---------|-------|-------|
| `xtask source-units render` / `check` | descriptors + bindings | Source of truth for the manifest committed at `docs/source-units.json` |
| `sinexctl verify --source-units` | descriptors (via manifest) + payload inventory | First runtime consumer of the split — checks coverage on a deployed binary |
| Future NixOS module generator | bindings | Will read the binding catalog to emit systemd units; deferred slice |
| `core.events` schema (target) | binding `source_unit_id` | The v2 taxonomy stores `source_unit_id` on every event; the descriptor + binding catalog is the resolution surface |

## 9. Open Items

The descriptor / binding split has landed structurally and is wired into
two consumers (`xtask source-units check` and
`sinexctl verify --source-units`). Remaining items, in priority order:

1. **NixOS module generation from bindings.** PR #1184 prepared the
   binding shape; the actual Nix generator that emits a systemd unit
   per binding is deferred. Until it lands, adding a binding still
   requires manual `nixos/modules/sinex/*.nix` edits.
2. **Descriptor-field split for `runner_pack` / `runtime_shape` /
   `access_policy` / `implementation_mode`.** These currently live on
   the descriptor but are properly deployment-shaped. The next slice
   moves them onto `SourceUnitBinding` so a single descriptor can be
   deployed in different shapes on different hosts.
3. **`source_unit_id` column on `core.events`.** The v2 taxonomy stores
   `source_unit_id` per event so queries can resolve back to the
   semantic descriptor without a `(source, event_type)` lookup. The
   descriptor catalog in #1175 Phase 4 is the prerequisite; the column
   add is the next logical migration.
4. **Replace `(source, event_type)` schema-registry key with
   `(event_type, schema_version)`** once `source` is fully redundant
   with `source_unit_id`.

Refs: #1054, #1058, #1059, #1064, #1081, #1126, #1129, #1175, #1184.
