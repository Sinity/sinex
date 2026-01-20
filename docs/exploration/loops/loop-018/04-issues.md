# Loop 018 - Concrete Issues

1) Registry-only schemas are not represented by typed `EventPayload` definitions.
- 16 registry entries (e.g., `shell.kitty/command.executed`, `terminal.kitty/session.started`) have JSON schemas but no corresponding `#[event_payload]` in code.
- This implies schema artifacts include external or legacy sources without typed Rust payloads.
