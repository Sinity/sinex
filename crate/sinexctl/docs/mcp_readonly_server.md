# Read-Only MCP Server

Status: implemented read-only stdio surface for
[#1105](https://github.com/Sinity/sinex/issues/1105).

The **live tool inventory** is owned by `crate/sinexctl/src/mcp.rs` and
validated by `crate/sinexctl/tests/validation_test.rs`. This record is the
substrate-invariant contract; the tool list is intentionally not
duplicated here (it drifts).

Sinex exposes a local MCP server for coding agents and analysis tools.
The first server is read-only: an evidence access surface, not a
control plane and not an actuator.

## Authority Boundary

The server may read from existing `sinexd::api` RPC and CLI query surfaces. It
must not open a direct mutation-capable database session, publish
events, stage source material, edit Nix configuration, or execute
instruction loops.

Allowed v1 behavior:

- search events and source materials;
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

First implementation pin: MCP protocol `2024-11-05`, implemented as a
local JSON-RPC stdio subset in `sinex-mcp-server` without an MCP SDK
dependency. The compatibility test lists tools, validates each tool's
JSON schema shape, and asserts the protocol-version constant. Do not
track protocol drafts by assumption.

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
  the same redaction contract as the source-material detail tool;
- no context-pack tools until `#1095` provides a stable read model.

## Common Response Shape

Every tool response is structured JSON with:

- `items` or a named result object, never opaque prose only;
- `ids` for events, source materials, runs, operations, evidence;
- `provenance_refs` when the result depends on events or materials;
- `caveats` using stable machine-readable codes when available;
- `redaction` metadata when fields are suppressed or summarized;
- `generated_at` plus the query parameters that shaped the result.

Payload snippets default to summaries or redacted samples. Returning
raw material bytes or private text requires an explicit future policy
gate.

## Tool Schema Requirements

Tool schemas are part of the public contract. Small and stable:

```json
{
  "name": "sinex.source_readiness",
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
  and tests reject untyped raw-RPC usage in the MCP module.

## Follow-Ups

Open or link follow-up issues when a desired tool lacks an API/RPC
read surface. Do not silently fall back to ad-hoc SQL as the stable
backend.

**Related:** `crate/sinexctl/docs/operator_surfaces.md`,
`xtask/docs/runtime-target-boundaries.md`, issue `#1105`.
