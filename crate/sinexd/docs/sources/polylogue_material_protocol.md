# Polylogue material protocol v1

Polylogue is an external producer. It stages and confirms exact provider
artifacts and normalized NDJSON segments as source material before publishing
an `EventIntent` to JetStream. Sinex events contain typed facts and anchors;
transcript and tool bytes stay in the registered material.

## Revision and material

The producer publishes one immutable `PolylogueRevisionManifest` alongside the
material. It must declare `polylogue.material-protocol/v1`, semantics version
2, the Origin vocabulary version and digest, stable session/native/revision
identifiers, segment SHA-256 and byte sizes, expected counts, and exact record
anchors. The head segment has index `-1` and contains one `session` record;
transcript segments contain `message`, `block`, `attachment`, and
`session_event` records. `line_index` is a manifest coordinate; the event's
`source_material_id` and `anchor_byte` are the durable occurrence coordinate.

Before publishing, the producer must verify every segment, the joined content
digest, manifest counts, record digests, sequence bounds, and message/block
counts. Missing material, an unknown vocabulary digest, a changed digest, a
bad anchor, or an undeclared segment is a rejected revision.

## EventIntent

Publish JSON `EventIntent` to
`{env}.sinex.events.raw.integration.polylogue.<kind>.observed` with the normal
raw-event headers:

```json
{
  "envelope_version": "1",
  "source_id": "integration.polylogue",
  "parser_id": "polylogue-material-producer",
  "parser_version": "1.0.0",
  "events": [{
    "id": "<random UUIDv7>",
    "source": "integration.polylogue",
    "event_type": "integration.polylogue.message.observed",
    "payload": {
      "revision_id": "<manifest revision_id>",
      "record_id": "<manifest record id>",
      "anchor": {"segment_index": 0, "line_index": 0, "seq": 0, "kind": "message", "sha256": "<hex>"},
      "kind": "message",
      "session_id": "<stable session id>",
      "native_id": "<provider id>",
      "seq": 0
    },
    "source_material_id": "<confirmed registered material UUID>",
    "anchor_byte": 0,
    "host": "<producer host>"
  }],
  "admitted_at": "<RFC3339>",
  "admitted_by": "<producer host>"
}
```

The event ID is a new random interpretation ID on replay. It is not derived
from the source or content. `Nats-Msg-Id` is the physical envelope's dedupe
header. A multi-event envelope has one aggregate raw-message settlement and
cannot be acknowledged until every child has a terminal durable outcome.

## Progress and retry

Use the shared DurableEmissionReceipt with a stable `request_id` for the
manifest and each progress atom. Producer progress advances only after the
receipt unlocks progress. On restart, reconcile that request ID before
re-emitting. The manifest's expected counts and digests are evidence for
reconciliation, not a second transport transaction coordinator.

The source material must be confirmed before the EventIntent is sent. A
producer must not invent a virtual material ID, acknowledge after an mpsc send,
or treat a partial child result as revision completion.
