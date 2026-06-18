# Output Kinds

Sinex classifies durable and operator-visible outputs before adding new write
paths. The shared vocabulary lives in `sinex_primitives::output_kind`.

| Kind | Meaning | Current examples |
| --- | --- | --- |
| `CanonicalEvent` | Immutable admitted fact in `core.events`. | `core.events` |
| `ProjectionRow` | Rebuildable state computed from events or material. | `domain.current_objects`, `source.coverage` |
| `Artifact` | Persisted generated report, bundle, catalog, or export. | `artifacts.source_catalog` |
| `Proposal` | Candidate change or truth claim requiring authority. | `curation.proposal` |
| `Judgment` | Explicit authority decision over a proposal. | `curation.judgment` |
| `OperationRecord` | Intentional control-plane activity or finalization record. | `operations_log` |
| `EphemeralView` | Read result delivered to CLI, API, TUI, MCP, or another view surface. | `relations.evidence_window`, `views.view_envelope` |

New output-producing work should add or reference an
`OutputKindDeclaration`. A derived status, report, proposal, operation, or view
needs an explicit reason before it is admitted as a canonical event.
