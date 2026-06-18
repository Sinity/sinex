# Source Identity Demotion Map (#1881)

This map classifies the main coordinates currently named `source` or
`source_id`. It intentionally does not rename every existing field. The goal is
to state which invariant each coordinate owns so future source, package,
admission, and privacy work does not reuse one source-shaped value as material
origin, producer binding, parser identity, event contract, admission policy,
privacy authority, occurrence scope, deployment grouping, and UI grouping.

## Doctrine

`source` may remain an event namespace, display grouping, transport routing
segment, operator grouping, or legacy query/index coordinate. It must not be
the semantic authority for material origin, parser identity, schema identity,
admission behavior, privacy/disclosure behavior, occurrence identity, or
deployment shape.

The target spine is:

```text
RawMaterial
  -> ProducerRun / Parser
  -> EventIntent / Candidate
  -> Admission
  -> AdmittedEvent
  -> Projection / Artifact / Proposal / Judgment / Operation / View
```

## Classification

| Category | Current coordinates | Actual invariant | Decision |
| --- | --- | --- | --- |
| Material origin | `raw.source_material_registry.id`, `source_identifier`, `SourceMaterial`, `MaterialAnchor`, `source_material_id` | Identifies preserved bytes, records, snapshots, rows, and material anchors. | Keep separate from event namespace and source package id. Prefer material-origin wording in docs/views. |
| Producer/runtime binding | `SourceRuntimeBinding`, runtime subject, runner pack, `EventIntent.source_id` | Describes the configured producer/parser binding that emitted a candidate or intent. | Treat `source_id` here as source package / producer-binding provenance, not event meaning. |
| Parser binding | `SourceId`, `ParserId`, `ParserManifest.source_id`, `ParserContext.source_id`, parser registry lookup | Selects parser implementation and records parser provenance for interpreted material. | Keep as parser/package dispatch key. Do not use it as event contract identity. |
| Event namespace | `EventSource`, `EventType`, `Event.source`, `Event.event_type`, transport subjects, `core.events.source` | Names the event family for routing, query, display, and compatibility indexes. | Keep. Document as namespace/index coordinate. |
| Event contract/schema identity | `EventContractId`, `EventContract.payload_schema`, `payload_schema_id`, legacy `(source,event_type)` schema lookup | Names the semantic event contract and payload validation shape. | New code should prefer EventContract / payload schema ids. `(source,event_type)` remains a compatibility lookup. |
| Admission policy scope | `AdmissionPolicyId`, `AdmissionPolicyScope`, `accepted_event_contracts`, `AdmissionOutcome` | Owns accept/reject/quarantine/defer/propose behavior. | Do not add source-keyed admission authority. Policies reference event contracts, not source packages or event namespaces. |
| Privacy/disclosure scope | `SourceContract.privacy_tier`, parser field metadata, `privacy.field_rules(event_source,event_type,field_path)`, disclosure refs | Current field rules are selectors; source privacy is a posture/default. | Runtime disclosure authority belongs to explicit operator-controlled policy over fields, materials, surfaces, logs, exports, DLQ, telemetry, and completions. |
| Occurrence/equivalence scope | `OccurrenceIdentity`, parser `OccurrenceKey.source_id`, `ScopeKey`, `EquivalenceKey` | Owns natural occurrence and replacement/dedup boundaries. | Keep explicit keys. Parser source id is only a scoping prefix until occurrence policy is contract-aware. |
| UI/deployment grouping | `SourceCoverageView.source_id`, `SourceReadiness.source_id`, source catalog export, Nix source bindings | Groups operator/deployment rows and readiness/coverage views. | Keep as view/deployment grouping with caveats/actions. It is not semantic authority. |
| Legacy denormalized index | `core.events.source`, event query source filters, schema fallback pair, privacy selector pair, relation source labels | Provides lookup/display compatibility and performance. | Keep as legacy namespace/index while contract ids and policy ids take authority. |

## Current Code Invariants

- `Event.source` is a namespace. Admitted rows also carry payload schema,
  provenance, scope key, equivalence key, material/derived provenance, and
  operation provenance beside `source` and `event_type`.
- `EventIntent.source_id` is producer/package provenance for an admission
  envelope. It is not the event namespace and not admission policy authority.
- `EventContract.id` is the semantic event coordinate. The current shell
  history contract allows multiple source packages to emit the same
  `shell.history / command.imported` event namespace.
- `AdmissionPolicy.accepted_event_contracts` references event contract ids, not
  package ids and not event source namespaces.
- Parser `SourceId` groups parser/package dispatch. Parser outputs carry
  separate `EventSource` / `EventType` coordinates and material/provenance
  fields.
- `SourceContract.id` and `SourceRuntimeBinding.source_id` are package/catalog
  coordinates. They group declarations and runtime bindings; they do not define
  payload schema, admission policy, or field disclosure behavior by themselves.
- `raw.source_material_registry.source_identifier` is a material-origin
  locator. It may be a path, URI, database identity, or acquisition key, and it
  is deliberately more permissive than `EventSource`.
- `SourceCoverageView` is an operator view over contracts, runtime bindings,
  event counts, material counts, caveats, privacy posture, and actions. It is a
  coverage grouping, not a semantic registry.

## Package Issue References

Source and capture package issues should cite this map by naming the coordinate
they need, not by saying "source id" generically.

Use these references in package specs:

| Package spec need | Coordinate to cite |
| --- | --- |
| Preserved bytes, records, snapshots, rows, exports, or stream segments | Material origin / source material coordinate |
| Live watcher, import job, adapter process, native host, or configured runner | Producer/runtime binding coordinate |
| Parser implementation, parser version, accepted material shape, parse context | Parser binding coordinate |
| Canonical event family such as `shell.history / command.imported` | Event namespace plus EventContract id |
| Payload validation shape and schema semantics | EventContract / payload schema coordinate |
| Accept, reject, quarantine, defer, duplicate, or propose behavior | AdmissionPolicy / AdmissionOutcome coordinate |
| Field/material/view/export/log/DLQ/telemetry/completion disclosure behavior | Operator-controlled privacy/disclosure policy coordinate |
| Natural occurrence, duplicate slot, replacement, or scope invalidation | Occurrence/equivalence/scope coordinate |
| Operator readiness, deployment binding, generated catalog, package mode row | UI/deployment/package grouping coordinate |

This keeps comprehensive package specs concrete: a browser, email, terminal,
desktop, audio/OCR, or system-log issue can still be one domain capability, but
each mode must state which material origin, producer binding, parser binding,
event contract, admission policy, disclosure policy, occurrence/equivalence
scope, operations, and coverage row it owns.

## Follow-Up Owners

- Event contract and payload-schema authority: #1902 landed the primitive
  registry; runtime/catalog consumers must keep using contract ids rather than
  package ids or event namespaces as authority.
- Admission policy and outcome authority: #1900 landed the primitive
  vocabulary; runtime consumers must keep mapping decisions through explicit
  policy/outcome coordinates.
- Privacy/disclosure enforcement: #1693.
- Package/mode completeness checks: #1792.
- Capture/admission/projection debt visibility: #1901.
- Occurrence/equivalence authority: #1448/#1692 for duplicate authority and
  finalization, with #1792 package checks ensuring source packages declare the
  occurrence/equivalence coordinate they rely on.
- Source coverage vs identity separation: #1685 is closed; residual ambiguity is
  now this map plus #1792/#1901 coverage/debt consumers.

## Verification Slice

The first executable split is pinned by
`event_contract_id_decouples_package_ids_from_event_namespace` in
`crate/sinex-primitives/tests/admission_contracts_test.rs`: the shell-history
event contract is emitted by package ids such as `terminal.bash-history` and
`terminal.zsh-history`, while admission policy accepts the contract id rather
than either package id or the `shell.history` event namespace.
