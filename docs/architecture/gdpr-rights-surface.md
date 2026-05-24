# Operator Rights Surface

Status: design record for #1072. Supersedes target-vision/reference/privacy-and-operations.md §6 and §4.

The user is both the data subject and the data controller. This document
defines the operator-facing controls that exercise the underlying archive,
tombstone, and audit primitives — what we call the "rights surface" by
analogy with GDPR vocabulary, not by legal compliance. The goal is
meaningful operator control over a personal lifelogging archive, not a
regulator-facing reporting interface.

## What This Doc Owns

- The operator-visible surface for export, delete, audit, retroactive
  redaction, source-material manifest, and retention scheduling.
- The interaction model with the cascade archive/tombstone primitive.
- The interaction model with evidence-lane material (occurrence vs.
  snapshot) and with audited semantic renames.

## What This Doc Does Not Own

- Threat model: `threat-model.md`.
- Tombstone schema / cascade primitive: implemented in `core.events`,
  `audit.archived_events`, `core.event_tombstones`. This doc consumes those
  primitives; it does not redefine them.
- Per-source sensitivity classification: per-source policy (vision §2) is
  consumed here as default retention input.
- Label-only rename mechanics: `audited-semantic-renames.md` owns the alias
  catalog; this doc references aliases as a separate path from purge.

## Five Operator Verbs

The rights surface collapses to five verbs. Each lands on the cascade
archive / tombstone primitive or on the alias catalog. None mutate
`core.events` payloads in place (except retroactive redaction, which is
explicit and audited).

### Export

```
sinexctl privacy export --source <src> --since <t> --until <t> \
  --format jsonl|csv|html|blobs --output <path> [--decrypt] [--encrypt-output <age-key>]
```

- `jsonl` / `csv`: tabular event metadata + payload.
- `blobs`: dumps referenced source-material blobs to a directory.
- `html`: human-readable "what does Sinex know about me?" report.
- `--decrypt` materializes `Strategy::Encrypt` tokens and decrypts referenced
  encrypted blobs at output time. The privacy key is consumed in-process and
  never written to the export.
- `--encrypt-output <age-key>` wraps the output in `age` so the export does
  not become a fresh unencrypted copy at rest.

CLI must warn whenever an unencrypted export of a CRITICAL- or HIGH-tier
source lands on disk.

### Delete

```
sinexctl privacy delete --source <src> --before <t> [--cascade] \
  --dry-run | --confirm | --permanent --yes-i-understand-data-is-gone
```

Default path: archive then tombstone. `--permanent` skips archive (tombstone
direct from live). Cascade follows provenance: derived events go along with
roots; see _Provenance constraint_ below.

`--dry-run` reports affected event count, chain depth, source-material
blobs that would lose their last reference, and estimated runtime.

### Audit

```
sinexctl privacy audit --source <src> --since <t> [--context <ctx>] [--show-privacy-rules]
```

Read-only operator inspection of what was captured and what the
PrivacyEngine did with it. Use cases: did the engine actually redact what I
thought it would? Did private mode (`runtime-private-mode.md`) really
suppress capture over a window? Was a continuity gap real or a capture
outage?

### Retroactive redact

```
sinexctl privacy redact --event <id> --confirm
sinexctl privacy redact --source <src> --payload-contains <pat> --confirm
```

Retroactive redaction replaces an event's payload content with a marker
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

Retention is the same primitive (archive → tombstone) executed on a
schedule instead of on demand. The NixOS module renders rules; a timer unit
runs them.

```
sinexctl lifecycle retention status
sinexctl lifecycle retention apply --dry-run
sinexctl lifecycle retention apply --confirm
```

Default retention periods are source-differentiated. The reference defaults
in vision §4.1 are a starting table, not an implementation contract — they
must be revisited per-source as part of the retention work in #1072. The
operator-facing tunable is `services.sinex.retention.rules` (NixOS module).

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
- See `evidence-lanes.md` for how snapshot-lane material interacts: when
  the operator purges an event, only the occurrence-lane material owned by
  that event is candidate for cleanup. Shared snapshot-lane evidence is
  reference-counted across many events.

## Interaction with Audited Semantic Renames

The rights surface and the alias catalog (`audited-semantic-renames.md`)
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

## Verification

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

- `docs/architecture/threat-model.md` (T6 future-self regret)
- `docs/architecture/evidence-lanes.md` (occurrence vs snapshot lane,
  reference-count contract for snapshot material)
- `docs/architecture/audited-semantic-renames.md` (alias vs purge boundary)
- `docs/architecture/runtime-private-mode.md` (capture-time suppression is
  the complement to retention-time purge)
- Issues: #1072, #1042, #1101, #1442
