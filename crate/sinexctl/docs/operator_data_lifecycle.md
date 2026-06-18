# Operator Data Lifecycle

Status: partial CLI/operator contract for #1072.

This document defines the operator-facing controls that exercise the underlying
archive, tombstone, source-material, and audit primitives. The goal is
meaningful control over a personal lifelogging archive, not GDPR compliance
theater.

Implemented today:

- `sinexctl privacy audit` as a read-only posture report.
- `sinexctl privacy export` as scoped, metadata-shaped export that deliberately
  omits raw payloads/snippets.
- `sinexctl sources list` and `sinexctl sources show` for source-material
  inspection.
- lifecycle/archive/tombstone primitives in `sinexd::api` and `sinex-db`.

Still open under #1072:

- privacy-specific delete/forget workflow;
- retroactive redact workflow;
- privacy-policy replay/index invalidation UX;
- explicit retention policy preview/confirmation UX.

## What This Doc Owns

- The operator-visible surface for export, delete, audit, retroactive
  redaction, source-material manifest, and retention scheduling.
- The interaction model with the cascade archive/tombstone primitive.
- The interaction model with evidence-lane material (occurrence vs.
  snapshot) and with audited semantic renames.

## What This Doc Does Not Own

- Threat model: `nixos/modules/security-threat-model.md`.
- Tombstone schema / cascade primitive: implemented in `core.events`,
  `audit.archived_events`, `core.event_tombstones`. This doc consumes those
  primitives; it does not redefine them.
- Per-source sensitivity classification: per-source policy (vision §2) is
  consumed here as default retention input.
- Label-only rename mechanics: issue #1101 owns the alias catalog; this doc
  references aliases as a separate path from purge.

## Five Operator Verbs

The lifecycle surface collapses to five verbs. Each lands on the cascade
archive / tombstone primitive or on the alias catalog. None mutate
`core.events` payloads in place except a future retroactive redaction workflow,
which must be explicit and audited.

### Export

```
sinexctl privacy export --source <src> --since <t> --until <t> \
  --format json --output <path>
```

Current export is metadata-shaped. It includes event identity, source/type,
timestamp, host, provenance shape, associated blob count, schema/run/operation
IDs, cursor metadata, and explicit `payload_redacted` / `snippet_redacted`
flags. It does not dump raw payloads, snippets, source-material blobs, or
decrypted content.

CLI must warn whenever an unencrypted export of a CRITICAL- or HIGH-tier
source lands on disk if future raw-content export modes are added.

### Delete

```
sinexctl privacy delete --source <src> --before <t> [--cascade] \
  --dry-run | --confirm | --permanent --yes-i-understand-data-is-gone
```

Not implemented as a privacy command today. The honest backend primitive is
the lifecycle archive/tombstone path. A future privacy delete/forget command
must preview and then call that lifecycle surface rather than bypassing it.

`--dry-run` reports affected event count, chain depth, source-material
blobs that would lose their last reference, and estimated runtime.

### Audit

```
sinexctl privacy audit --source <src> --since <t> [--context <ctx>] [--show-privacy-rules]
```

Read-only operator inspection of what was captured and what the
PrivacyEngine did with it. Use cases: did the engine actually redact what I
thought it would? Did private mode (`crate/sinexctl/docs/private_mode.md`) really
suppress capture over a window? Was a continuity gap real or a capture
outage?

### Retroactive redact

```
sinexctl privacy redact --event <id> --confirm
sinexctl privacy redact --source <src> --payload-contains <pat> --confirm
```

Not implemented today. A future retroactive redaction flow replaces an event's payload content with a marker
(`⌜RETROACTIVELY_REDACTED⌝`), updates `updated_at`, and writes an annotation
to `core.event_annotations`. The event id, type, source, and timestamps
survive. The annotation records who, when, and why — never the redacted
content.

Use when the operator wants to keep the fact-of-event but remove specific
content. Use _delete_ when the event should be gone entirely.

### Source-material manifest

```
sinexctl sources list [--source <src>] [--orphaned] [--blob-missing]
sinexctl sources show <material-id>
```

Window into raw inputs (vs. derived events). Surfaces orphaned blobs (no
event references — safe to clean) and broken provenance (blob missing from
the CAS — needs operator attention).

## Retention Scheduler

Retention is the same primitive (archive -> tombstone) executed on a schedule
instead of on demand. Today, schema-level `retention_seconds` can drive
`sinexd::api::lifecycle_ttl`; broad operator retention UX remains open under
#1072/#1172.

```
sinexctl ops lifecycle retention status
sinexctl ops lifecycle retention apply --dry-run
sinexctl ops lifecycle retention apply --confirm
```

Do not add broad auto-pruning defaults without an explicit policy and preview.

## Provenance Constraint

Cascade tombstone respects the provenance chain. An event cannot be
tombstoned while any live or archived event references it as a source.
Concretely:

- Retention dates are effectively determined by the _youngest_ event in a
  chain. A 3-year-old root with a 6-month-old derivative is not eligible
  for tombstone until the derivative also ages out, or until the operator
  explicitly cascades.
- A CAS blob is not deleted from local storage while any event references
  it. Cascade tombstone signals CAS cleanup only after the last reference
  is gone. (Reference-count tracking is implementation; the contract is
  "blob outlives last referencing event for at least one cleanup pass".)
- See `crate/sinexd/docs/sources/evidence_lanes.md` for how snapshot-lane material interacts: when
  the operator purges an event, only the occurrence-lane material owned by
  that event is candidate for cleanup. Shared snapshot-lane evidence is
  reference-counted across many events.

## Interaction with Semantic Renames

The rights surface and the alias catalog from issue #1101
operate on different layers and must not be confused:

- **Alias rename**: the stored event is unchanged. The canonical
  name shown by queries and exports is a function of the alias catalog.
  Aliases are never a substitute for purge — alias rows do not remove
  content.
- **Retroactive redact**: the stored payload is rewritten to a marker.
  Suitable when the label is fine but the content is sensitive.
- **Delete (cascade)**: the event is moved to archive, then to tombstone.
  The event no longer exists as a live/archived row; only the tombstone
  metadata remains.

Operator surfaces export must honor alias canonicalization for label-only
renames but must always report stored names alongside canonical names so
audit cannot be hidden behind a rename.

## Tombstone Contract

`core.event_tombstones` retains only what is needed to truthfully report
the past existence of an event:

| Field | Meaning |
|---|---|
| `id` | UUIDv7 of the original event (timestamp-encoded) |
| `source` | Stored source value |
| `event_type` | Stored event type |
| `ts_orig` | When the event occurred |
| `ts_purged` | When tombstoned |
| `purge_reason` | Free text, conventionally `retention_policy:<source>:<period>` or operator-provided |
| `purge_operation_id` | Foreign key into `core.operations_log` |

Tombstones do not retain payloads, source-material references, or lineage
details. Once tombstoned, content is irrecoverable. This is by design.

## Audit Trail

Every verb above writes a `core.operations_log` row that captures actor,
timestamp, dry-run report hash, and parameters. Operator-driven purges and
retention-policy purges look the same in the audit log; `purge_reason`
discriminates.

## Verification Targets

A first-class proof of this surface needs to show:

- Export of a small range produces a JSONL file that round-trips through
  re-import (where re-import is meaningful) or is byte-stable.
- Dry-run delete reports correct counts and correct blob reference impact.
- Cascade tombstone refuses to advance when a chain has a younger live
  event, and the refusal is reported (not silent).
- Retroactive redact updates the payload, writes the annotation, and
  leaves the operations log entry intact.
- Source-material manifest correctly distinguishes orphaned blobs from
  blob-missing rows.
- Retention `--dry-run` and `--confirm` produce identical reports modulo
  state mutation.

## Related

- `nixos/modules/security-threat-model.md` (T6 future-self regret)
- `crate/sinexd/docs/sources/evidence_lanes.md` (occurrence vs snapshot lane,
  reference-count contract for snapshot material)
- Issue #1101 (alias vs purge boundary)
- `crate/sinexctl/docs/private_mode.md` (capture-time suppression is
  the complement to retention-time purge)
- Issues: #1072, #1042, #1101, #1442
