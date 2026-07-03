# Read-Only MCP Server

Status: implemented read-only stdio surface for
[#1105](https://github.com/Sinity/sinex/issues/1105).

The **live tool inventory** is owned by `crate/sinexctl/src/mcp.rs` and
validated by `crate/sinexctl/tests/validation_test.rs`. This record is the
substrate-invariant contract; the table below is a docs-facing mirror and is
checked against the live tool list.

Sinex exposes a local MCP server for coding agents and analysis tools.
The first server is read-only: an evidence access surface, not a
control plane and not an actuator.

## Tools

| Tool |
| --- |
| `sinex_assembly_stats` |
| `sinex_audit_trail` |
| `sinex_automata_status` |
| `sinex_command_frequency` |
| `sinex_context_pack` |
| `sinex_coordination_instance_health` |
| `sinex_coordination_instances` |
| `sinex_coordination_leader` |
| `sinex_curation_proposals` |
| `sinex_current_device_state` |
| `sinex_current_health` |
| `sinex_dlq_peek` |
| `sinex_dlq_stats` |
| `sinex_documents_chunks` |
| `sinex_documents_get` |
| `sinex_documents_search` |
| `sinex_event_engine_batch_stats` |
| `sinex_event_engine_validation` |
| `sinex_file_activity` |
| `sinex_gateway_stats` |
| `sinex_lifecycle_status` |
| `sinex_llm_budget_report` |
| `sinex_llm_prompts` |
| `sinex_llm_route_explain` |
| `sinex_metric_counters` |
| `sinex_ops_get` |
| `sinex_ops_list` |
| `sinex_orient` |
| `sinex_privacy_status` |
| `sinex_query` |
| `sinex_recent_activity` |
| `sinex_relation_evidence` |
| `sinex_replay_operations` |
| `sinex_replay_status` |
| `sinex_search_events` |
| `sinex_semantic_epochs` |
| `sinex_semantic_lane_diffs` |
| `sinex_semantic_lane_outputs` |
| `sinex_semantic_lanes` |
| `sinex_shadow_consumers` |
| `sinex_source_bindings` |
| `sinex_source_continuity` |
| `sinex_source_coverage` |
| `sinex_source_drift` |
| `sinex_source_gap_explain` |
| `sinex_source_health` |
| `sinex_source_identifier_continuity` |
| `sinex_source_material` |
| `sinex_source_materials` |
| `sinex_source_presets` |
| `sinex_source_readiness` |
| `sinex_source_stats` |
| `sinex_sources_active` |
| `sinex_sources_registry` |
| `sinex_sources_status` |
| `sinex_sources_status_view` |
| `sinex_stream_stats` |
| `sinex_system_health` |
| `sinex_system_ping` |
| `sinex_system_state` |
| `sinex_system_version` |
| `sinex_task_state` |
| `sinex_tasks_list` |
| `sinex_throughput` |
| `sinex_trace_lineage` |
| `sinex_window_focus` |

## Authority Boundary

The server may read from existing `sinexd::api` RPC and CLI query surfaces. It
must not open a direct mutation-capable database session, publish
events, stage source material, edit Nix configuration, or execute
instruction loops.

Allowed v1 behavior:

- orient cold agents to the evidence model, refs, provenance, query shape, and
  caveat semantics from the shared agent-orientation document;
- search events and source materials;
- execute descriptor-backed query-unit selections over events, sources,
  debt, operations, and runtime health;
- trace provenance and material evidence links;
- return source readiness and continuity caveats;
- expose runtime privacy, health, source, automata, replay,
  task, document, semantic-lane, and event-engine telemetry read models;
- fetch document metadata and ranked document search metadata with raw
  text and side data redacted.

Forbidden v1 behavior:

- event publishing;
- source staging;
- archive / tombstone / delete;
- proposal finalization;
- code / Nix writes;
- actuator commands;
- raw secret / private samples by default.

If a future tool looks write-like, it belongs behind `#1086`
proposal/judgment or an existing authenticated API command with
dry-run and explicit approval semantics. It must not be smuggled into
the read-only MCP binary.

## Transport And Versioning

Initial transport: stdio for local agents on the same host.

Current implementation pin: MCP protocol `2025-06-18`, implemented as a
local JSON-RPC stdio subset in `sinex-mcp-server` without an MCP SDK
dependency. The compatibility test lists tools, validates each tool's
JSON schema shape, and asserts the protocol-version constant. The
initialize handler accepts `2025-03-26` and `2024-11-05` clients as
older stdio clients. Do not track protocol drafts by assumption.

HTTP/SSE transport is a follow-up only when there is a real consumer.

## Deliberate Tool Omissions (load-bearing)

These exclusions are part of the contract — adding any of them needs
explicit policy work, not a quiet addition to `crate/sinexctl/src/mcp.rs`:

- no replay preview tool yet — the shared descriptor is mutating/write
  shaped even when used as a dry-run planner;
- no raw blob retrieval tool — `content.retrieve_blob` returns raw
  material content and needs a redacted/policy-shaped variant before
  MCP exposure;
- no raw document chunk-text tool — `documents.get_chunks` returns
  raw text by design; MCP uses `documents.get_chunks_redacted`;
- no workbench-inspect tool until its API read surface can enforce
  the same redaction contract as the source-material detail tool.

## Common Response Shape

Every tool response is a `ViewEnvelope`-shaped structured JSON object:

- `source_surface` names the MCP tool that produced the view;
- `query_echo` records the query parameters that shaped the result;
- `payload` contains the typed or JSON result object;
- `caveats` uses stable machine-readable IDs when available;
- `privacy_state` records redaction state when fields are suppressed or
  summarized;
- `generated_at` and `freshness` timestamp the view.

Payload snippets default to summaries or redacted samples. Returning raw
material bytes or private text requires an explicit future policy gate.

## Tool Schema Requirements

Tool schemas are part of the public contract. Small and stable:

```json
{
  "name": "sinex_source_readiness",
  "inputSchema": {
    "type": "object",
    "properties": {
      "source_family": { "type": "string" },
      "source_id": { "type": "string" },
      "include_caveats": { "type": "boolean", "default": true }
    },
    "additionalProperties": false
  }
}
```

Use nullable/optional fields instead of overloading strings. Prefer
exact IDs over natural-language selectors when the caller already has
an ID.

## Verification Requirements

MCP changes are complete only when:

- `list_tools` returns the expected tool names and JSON schemas;
- each new tool has one fixture-backed call test;
- the server starts over stdio without requiring a writable DB
  connection;
- a grep or unit test proves no v1 tool registers write verbs such as
  `stage`, `publish`, `delete`, `archive`, `tombstone`, `finalize`, or
  `actuate`;
- sensitive sample fixtures return redaction/suppression metadata
  rather than raw secret text;
- the MCP catalog maps every tool to typed read-only RPC descriptors,
  except local static orientation content, and tests reject untyped raw-RPC
  usage in the MCP module.

## Follow-Ups

Open or link follow-up issues when a desired tool lacks an API/RPC
read surface. Do not silently fall back to ad-hoc SQL as the stable
backend.

**Related:** `crate/sinexctl/docs/operator_surfaces.md`,
`xtask/docs/runtime-target-boundaries.md`, issue `#1105`.
