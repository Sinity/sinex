# View DTO spine

The design should be implemented through shared view DTOs before it becomes a complex screen hierarchy. This is the leverage point that makes CLI, TUI, MCP, and future SinexFS/web stay coherent.

## ViewEnvelope

Every generated view should include:

- `schema_version`
- `view_id`
- `generated_at`
- `source_surface`
- `runtime_target`
- `freshness`
- `query_echo`
- `filters`
- `caveats`
- `privacy_state`
- `actions`

## SinexObjectRef

Stable reference types should cover:

- event
- source_unit
- source_material
- material_anchor
- document
- document_chunk
- task
- semantic_lane
- semantic_entity
- semantic_relation
- operation
- replay_run
- snapshot
- dlq_message
- context_pack
- moment_candidate
- privacy_session
- caveat
- rpc_method
- command

The UI should pass references, not raw blobs, between views.

## ActionAvailability

Actions must be described in data, not implied by clickable decoration.

Core fields:

- `id`
- `label`
- `state`: enabled, disabled, target, loading, dangerous, partial, unavailable
- `reason`
- `command_equivalent`
- `rpc_method`
- `side_effect`: read, compose, write, admin, destructive
- `requires_confirmation`
- `dry_run_available`
- `audit_output_ref`

## EventCardView

The first high-value projection. It should flatten an event into a readable object:

- event id and stable short id
- timestamp triad: original, ingested, material anchor time if known
- source family and raw source string
- event type
- summary/snippet
- payload preview and raw availability
- material/source refs
- caveats and privacy state
- trace/provenance refs
- domain/semantic projections
- action availability

## Why this matters

Without these view DTOs, each command and surface will invent its own rendering and disabled-action logic. With them, the TUI can be built incrementally while MCP, CLI JSON, visual smoke, and future web views all observe the same semantics.
