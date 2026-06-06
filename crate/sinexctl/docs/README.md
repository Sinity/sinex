# sinexctl Documentation

This directory is the crate-local documentation surface for `sinexctl`.

Current local owners:

- `../README.md` — operator entrypoint and command overview
- `../DESIGN.md` — command structure and architecture notes
- `demo_walkthrough.md` — generated `sinexctl verify --demo` walkthrough
- `mcp_readonly_server.md` — read-only MCP surface contract and live-tool docs table
- `operator_data_lifecycle.md` — privacy audit/export and lifecycle operation contract
- `operator_ux_convergence.md` — command/RPC/output projection convergence plan
- `operator_surfaces.md` — operator-surface authority classes and boundaries
- `private_mode.md` — operator-controlled runtime capture suppression
- `state_snapshot.md` — `sinexctl state snapshot/inspect/restore` runbook

This crate should own:

- CLI-specific transport/auth UX
- command-surface behavior
- local configuration semantics
- any query/interaction docs moved down from top-level `README.md#architecture`
