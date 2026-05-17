# Read-Only MCP Server

Status: architecture contract for #1105. Implementation first slice is tracked
in #1351.

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
- build or fetch context-pack-like JSON once #1095 provides a read surface;
- preview replay/workbench plans when the backing API is dry-run/read-only.

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

The implementation must pin the exact MCP protocol and library version in the
binary docs and in a compatibility test. Do not track protocol drafts by
assumption. The compatibility test should list tools and validate each tool's
JSON schema shape.

HTTP/SSE transport is a follow-up only when there is a real consumer.

## Tool Set

First slice:

| Tool | Backend | Output contract |
|------|---------|-----------------|
| `sinex.search_events` | gateway event query or `sinexctl query --json` | events with ids, source/type, timestamps, payload summaries, caveats |
| `sinex.trace_lineage` | gateway trace or `sinexctl trace --json` | event id, material/synthesis provenance, parent ids, material links |
| `sinex.source_readiness` | `sources.readiness.*` gateway methods | source family/unit status, caveat codes, stale/missing/error evidence |

Second slice, after backing surfaces exist:

| Tool | Backend |
|------|---------|
| `sinex.source_continuity` | #1085 continuity reports |
| `sinex.get_recent_context` | `sinexctl context --json` or gateway equivalent |
| `sinex.build_context_pack` | #1095 context-pack builder |
| `sinex.material_show` | source-material show with policy-filtered samples |
| `sinex.replay_preview` | #1060 dry-run replay planner |
| `sinex.workbench_inspect` | #1062 read-only staged-material inspection |

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

Required for the first implementation PR:

- `list_tools` returns the expected tool names and JSON schemas.
- Each first-slice tool has one fixture-backed call test.
- The server starts over stdio without requiring a writable DB connection.
- A grep or unit test proves no v1 tool registers write verbs such as `stage`,
  `publish`, `delete`, `archive`, `tombstone`, `finalize`, or `actuate`.
- Sensitive sample fixtures return redaction/suppression metadata rather than
  raw secret text.

## Follow-Ups

Open or link follow-up issues when a desired tool lacks a gateway/RPC read
surface. Do not silently fall back to ad-hoc SQL as the stable backend.
