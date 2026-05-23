# Read-Only MCP Server

Status: implemented read-only stdio surface for #1105, with the current tool
inventory owned by `crate/cli/src/mcp.rs` and validated by
`crate/cli/tests/validation_test.rs`.

Sinex should expose a local MCP server for coding agents and analysis tools, but
the first server is read-only. It is an evidence access surface, not a control
plane and not an actuator.

## Authority Boundary

The server may read from existing gateway/RPC/CLI query surfaces. It must not
open a direct mutation-capable database session, publish events, stage source
material, edit Nix configuration, or execute instruction loops.

Allowed v1 behavior:

- search events and source materials;
- trace provenance and material evidence links;
- return source readiness and continuity caveats;
- expose runtime privacy, health, node, ingestor, automata, replay, task,
  document, semantic-lane, and ingestd telemetry read models;
- fetch document metadata and ranked document search metadata with raw text and
  side data redacted.

Forbidden v1 behavior:

- event publishing;
- source staging;
- archive/tombstone/delete;
- proposal finalization;
- code/Nix writes;
- actuator commands;
- raw secret/private samples by default.

If a future tool looks write-like, it belongs behind #1086 proposal/judgment or
an existing authenticated gateway command with dry-run and explicit approval
semantics. It should not be smuggled into the read-only MCP binary.

## Transport And Versioning

Initial transport: stdio for local agents on the same host.

First implementation pin: MCP protocol `2024-11-05`, implemented as a local
JSON-RPC stdio subset in `sinex-mcp-server` without an MCP SDK dependency. The
compatibility test lists tools, validates each tool's JSON schema shape, and
asserts the protocol-version constant. Do not track protocol drafts by
assumption.

HTTP/SSE transport is a follow-up only when there is a real consumer.

## Implemented Tool Set

| Tool | Backend | Output contract |
|------|---------|-----------------|
| `sinex.search_events` | `events.query` | events with ids, source/type, timestamps, and redacted payload/snippet summaries |
| `sinex.trace_lineage` | `events.lineage` | event id, material/synthesis provenance, parent ids, redacted material links |
| `sinex.source_readiness` | `sources.readiness.*` gateway methods | source family/unit status, caveat codes, stale/missing/error evidence |
| `sinex.source_continuity` | `sources.continuity.*` | source-family continuity, gaps, seams, and replayability |
| `sinex.source_gap_explain` | `sources.continuity.explain_gap` | source-family coverage-gap attribution at a timestamp |
| `sinex.source_identifier_continuity` | `sources.continuity` | continuity and replayability for one source identifier |
| `sinex.privacy_status` | `privacy.private_mode.status` | runtime private-mode state |
| `sinex.system_health` | `system.health` | gateway and confirmation-path health |
| `sinex.tasks_list` | `tasks.list` | current task workflow search/filter results |
| `sinex.task_state` | `tasks.state.get` | exact task workflow state by id |
| `sinex.replay_operations` | `replay.list_operations` | replay operation list with filters |
| `sinex.replay_status` | `replay.operation_status` | one replay operation state |
| `sinex.documents_search` | `documents.search` | ranked document metadata with text/headline/side-data redacted |
| `sinex.documents_get` | `documents.get` | document metadata with side-data redacted |
| `sinex.semantic_epochs` | `semantic.epochs.list` | semantic epoch registry listing |
| `sinex.semantic_lanes` | `semantic.lanes.list` | semantic lane registry listing |
| `sinex.semantic_lane_outputs` | `semantic.lane_outputs.list` | isolated semantic lane output records |
| `sinex.semantic_lane_diffs` | `semantic.lane_diffs.list` | semantic lane diff reports |
| `sinex.automata_status` | `automata.status` | derived-node liveness, checkpoint, lag, and throughput |
| `sinex.ingestors_status` | `ingestors.status` | source-ingestor liveness, health, and emission status |
| `sinex.nodes_health` | `nodes.health` | aggregate runtime node health |
| `sinex.nodes_active` | `nodes.list_active` | active runtime node presence |
| `sinex.nodes_registry` | `nodes.list` | persisted node state registry |
| `sinex.ingestd_validation` | `telemetry.ingestd_validation` | latest ingestd admission and validation snapshot |
| `sinex.ingestd_batch_stats` | `telemetry.ingestd_batch_stats` | ingestd batch, latency, and validation telemetry buckets |
| `sinex.throughput` | `telemetry.throughput` | per-source and per-component event/request throughput summary |
| `sinex.recent_activity` | `telemetry.recent_activity` | recent activity summary for local agent context |
| `sinex.command_frequency` | `telemetry.command_frequency` | command-frequency telemetry for shell context |
| `sinex.file_activity` | `telemetry.file_activity` | file-activity telemetry for project context |
| `sinex.system_state` | `telemetry.system_state` | CPU, memory, disk, and unit telemetry buckets |
| `sinex.window_focus` | `telemetry.window_focus` | desktop window focus telemetry buckets |
| `sinex.current_health` | `telemetry.current_health` | current health telemetry rows |
| `sinex.current_device_state` | `telemetry.current_device_state` | current device-state telemetry rows |
| `sinex.gateway_stats` | `telemetry.gateway_stats` | gateway request and latency telemetry buckets |
| `sinex.stream_stats` | `telemetry.stream_stats` | JetStream fill and message telemetry buckets |
| `sinex.assembly_stats` | `telemetry.assembly_stats` | material assembly telemetry buckets |
| `sinex.node_stats` | `telemetry.node_stats` | node processing telemetry buckets |
| `sinex.metric_counters` | `telemetry.metric_counters` | named metric counter telemetry buckets |
| `sinex.llm_prompts` | `llm.prompts.list` | LLM prompt-template registry events |
| `sinex.llm_route_explain` | `llm.route.explain` | deterministic LLM routing explanation |
| `sinex.llm_budget_report` | `llm.budget.report` | LLM budget-ledger usage report |
| `sinex.curation_proposals` | `curation.proposals.list` | curation proposal event listing |
| `sinex.dlq_stats` | `dlq.list` | raw-ingest DLQ stream statistics |
| `sinex.dlq_peek` | `dlq.peek` | sanitized raw-ingest DLQ message previews |
| `sinex.source_materials` | `sources.list` | staged source-material catalog listing |
| `sinex.source_material` | `sources.show` | staged source-material detail with metadata redacted |
| `sinex.source_coverage` | `sources.coverage` | source-material coverage buckets |
| `sinex.source_presets` | `sources.presets.list` | built-in source resolver preset catalog |
| `sinex.source_bindings` | `sources.bindings.list` | configured source binding listing |
| `sinex.ops_list` | `ops.list` | operations log listing |
| `sinex.ops_get` | `ops.get` | operation detail lookup |
| `sinex.lifecycle_status` | `lifecycle.status` | data lifecycle tier status |
| `sinex.gitops_sources` | `gitops.list_sources` | GitOps schema source listing |
| `sinex.audit_trail` | `audit.get` | audit trail for one operation |
| `sinex.coordination_instances` | `coordination.list_instances` | coordination instance listing |
| `sinex.coordination_leader` | `coordination.get_leader` | coordination leader lookup |
| `sinex.coordination_instance_health` | `coordination.instance_health` | coordination instance health lookup |
| `sinex.shadow_consumers` | `shadow.list` | shadow consumer listing |
| `sinex.system_ping` | `system.ping` | gateway ping |
| `sinex.system_version` | `system.version` | gateway package version |

Deliberate omissions:

- no replay preview tool yet, because the shared descriptor is mutating/write
  shaped even when used as a dry-run planner;
- no raw blob retrieval tool, because `content.retrieve_blob` returns raw
  material content and needs a redacted/policy-shaped variant before MCP
  exposure;
- no document chunk-text tool, because `documents.get_chunks` returns raw text
  by design and needs a separate redaction/policy shape before MCP exposure;
- no workbench-inspect tool until its gateway read surface can enforce the same
  redaction contract as the source-material detail tool;
- no context-pack tools until #1095 provides a stable read model.

## Common Response Shape

Every tool response should be structured JSON with:

- `items` or a named result object, never opaque prose only;
- `ids` for events, source materials, runs, operations, or evidence records;
- `provenance_refs` when the result depends on events or materials;
- `caveats` using stable machine-readable codes when available;
- `redaction` metadata when fields are suppressed or summarized;
- `generated_at` and the query parameters that shaped the result.

Payload snippets must default to summaries or redacted samples. Returning raw
material bytes or private text requires an explicit future policy gate.

## Tool Schema Requirements

Tool schemas are part of the public contract. Keep them small and stable:

```json
{
  "name": "sinex.source_readiness",
  "inputSchema": {
    "type": "object",
    "properties": {
      "source_family": { "type": "string" },
      "source_unit_id": { "type": "string" },
      "include_caveats": { "type": "boolean", "default": true }
    },
    "additionalProperties": false
  }
}
```

Use nullable/optional fields instead of overloading strings. Prefer exact IDs
over natural-language selectors when the caller already has an ID.

## Verification

Required for MCP changes:

- `list_tools` returns the expected tool names and JSON schemas.
- Each tool has one fixture-backed call test when it is added.
- The server starts over stdio without requiring a writable DB connection.
- A grep or unit test proves no v1 tool registers write verbs such as `stage`,
  `publish`, `delete`, `archive`, `tombstone`, `finalize`, or `actuate`.
- Sensitive sample fixtures return redaction/suppression metadata rather than
  raw secret text.
- The MCP catalog maps every tool to typed read-only RPC descriptors, and tests
  reject untyped raw-RPC usage in the MCP module.

## Follow-Ups

Open or link follow-up issues when a desired tool lacks a gateway/RPC read
surface. Do not silently fall back to ad-hoc SQL as the stable backend.
