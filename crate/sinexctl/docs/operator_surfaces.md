# Operator surfaces

Sinex has several ways to inspect or control one runtime. They share one
authority model rather than becoming independent tools with overlapping state.

## Surface map

| Surface | Role | Authority |
| --- | --- | --- |
| `sinexctl` | Canonical command-line query and control surface | Reads and explicit mutations through the typed `sinexd` API client. |
| `sinexctl tui` | Interactive dashboard for runtime, operations, modules, sources, events, and DLQ state | Read-only API views plus copyable canonical commands; it does not implement a second mutation path. |
| MCP server | Agent-facing evidence reader | Read-only tools over the same query and view contracts. |
| Shell completions and prompt helpers | Discovery and status adornment | Derived metadata or bounded reads only. |
| Desktop launchers | Start a canonical operator surface | No embedded runtime or persistence logic. |
| `xtask` | Checkout development and verification | Development plane, not the deployed control plane. |

## Authority rules

1. **Runtime mutations terminate at the `sinexd` API.** A CLI action uses the
   typed client and the runtime's authorization, validation, audit, and
   operation-recording path.
2. **Interactive views do not own truth.** The TUI renders API view models and
   offers canonical command hints. It does not write directly to PostgreSQL,
   NATS, source material, or local caches.
3. **Agent reads remain reads.** MCP exposes bounded query and evidence views.
   Write-like suggestions enter the proposal and judgment model described in
   [curation authority](../../sinex-primitives/docs/curation_authority.md).
4. **Privacy is enforced below the view.** CLI, TUI, and MCP consume redacted
   responses and do not bypass [private mode](private_mode.md) or disclosure
   policy through local side channels.
5. **Runtime target identity stays visible.** Every live view follows the
   [runtime-target contract](../../../xtask/docs/runtime-target-boundaries.md)
   so checkout-local and deployed-host evidence are not silently combined.

## Shared view model

The CLI, TUI, and MCP surfaces converge on typed views such as
`ViewEnvelope`, `EventCardView`, `OperationControlCardView`,
`SourceCoverageView`, and `ActionAvailability`. A view carries stable object
references, caveats, provenance or material references where relevant,
redaction state, and generation time. Human tables and interactive panels are
renderings of those contracts, not parallel schemas.

The TUI currently provides six tabs: dashboard, operations, modules, sources,
events, and DLQ. It refreshes those views through `GatewayClient`, preserves
failure state instead of displaying missing data as empty, and can copy
bounded values or suggested `sinexctl` commands through OSC52.

## Adding or changing a surface

An operator-surface change should demonstrate:

- typed API-client use rather than raw transport calls;
- the correct finite or streaming output contract;
- JSON/YAML/table agreement for non-interactive commands;
- redaction, unavailable, partial, empty, loading, and error states where the
  surface can encounter them;
- runtime-target attribution for live evidence;
- command-catalog and MCP-schema synchronization when public leaves change.

The repository-wide ownership map lives in
[`.github/authority-surfaces.md`](../../../.github/authority-surfaces.md).
CLI-specific convergence work is documented in
[`operator_ux_convergence.md`](operator_ux_convergence.md).
