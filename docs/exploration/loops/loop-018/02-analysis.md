# Loop 018 - Registry-only Schemas vs JSON Files

Scope
- `schemas/v1/registry.json` entries without matching `#[event_payload]` in code.
- Corresponding JSON schema files under `schemas/v1`.

Registry-only Entries
- Total registry-only entries: 16.
- All registry-only entries have corresponding JSON schema files on disk.
- Sample entries include:
  - `shell.kitty/command.executed`
  - `terminal.kitty/session.started`
  - `shell.bash_histfile/command.historical`
  - `system/journald.historical`
  - `atuin/entry.imported`

Spot-check Findings
- `schemas/v1/shell.kitty/command.executed.json` describes Kitty shell integration events (command, window/tab IDs).
- `schemas/v1/terminal.kitty/session.started.json` describes terminal session metadata (shell_type, env_vars, working_directory).
- These schemas appear specific to integrations that may be defined outside the current Rust `EventPayload` inventory.

Findings
- Registry-only schemas are present and appear intentional (integration/legacy sources), but they are not represented by `#[event_payload]` types in the current codebase.
- This suggests schemas may be maintained externally (GitOps or legacy) and are not tied to Rust payload types.

Risks
- If these events are still emitted, the lack of typed payloads in Rust may reduce compile-time safety and schema-gen coverage.
- If they are deprecated, the registry may contain obsolete schemas that should be pruned.

Opportunities
- Confirm the intended source of these schemas (external GitOps vs legacy in-repo).
- Decide whether to reintroduce typed payloads or explicitly document these as external-only schemas.
