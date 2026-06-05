# Operator UX Convergence

Status: `sinexctl` implementation plan for the UI/UX convergence wave.

Sinex operator surfaces should feel like projections of one runtime model, not
separate tools that happen to call the same API. The canonical live control
boundary remains `sinexd` API plus `sinexctl`; TUI, MCP, and future SinexFS
surfaces are read or interaction projections over the same DTOs.

## Current Duplication Targets

| Concern | Current shape | Target shape |
| --- | --- | --- |
| Command metadata | Clap leaf tree, `command_path()` match, and format registry all need manual updates. | A command catalog owns UX metadata; format validation and future projections read it. |
| RPC methods | Command paths call typed `GatewayClient` methods, but method metadata still lives across DTO modules, registry setup, CLI wrappers, and docs. | Typed RPC descriptors remain the single contract for method names, request/response DTOs, role, stability, and catalog metadata. |
| Output | Many commands hand-match JSON/YAML/table and print their own sections. | Commands return through shared output helpers and view DTOs; bespoke renderers are limited to genuinely interactive surfaces. |
| Runtime summary | `status`, `now`, `runtime`, `recent`, `watch`, and `tui` each assemble overlapping status/event views. | Shared runtime/event view models feed shortcut commands, TUI, and agent-facing projections. |
| Projection surfaces | CLI, MCP, and future SinexFS risk defining separate JSON shapes. | MCP/SinexFS use `sinexd` API or CLI JSON read models with IDs, caveats, provenance refs, redaction metadata, and `generated_at`. |

## Authority Rules

- `sinexctl` is the canonical operator UX for live runtime control.
- `xtask` remains development-plane tooling and must not become the production
  control surface.
- TUI, MCP, and SinexFS do not own mutation semantics. They wrap `sinexd` API
  reads or explicit API mutations with the same approval/dry-run rules as CLI.
- Manual declarations remain event-native: command/form input is source
  material, and admitted facts flow through normal provenance.

## Implementation Slices

1. Command catalog: add consolidated command metadata and make format matrix
   rendering read it.
2. RPC catalog promotion: keep CLI/MCP on typed client calls, then collapse
   duplicated method metadata into typed descriptors that can feed registry,
   docs, command catalog, and validation checks.
3. Output spine: move table/JSON/YAML handling for common commands behind shared
   output helpers and typed view DTOs.
4. Runtime views: make `status`, `now`, and TUI consume the same runtime snapshot
   builder.
5. Projection alignment: MCP and SinexFS expose the same read models, with
   stable caveat and redaction fields.

## Verification

- Exact command-leaf coverage between Clap and the command catalog.
- Grep or unit tests preventing command modules from adding new raw RPC calls.
- Golden output tests for high-traffic operator views.
- Fixture-backed MCP/projection tests that assert IDs, caveats, provenance refs,
  redaction metadata, and generated timestamps are present where applicable.
