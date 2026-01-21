# Loop 019 - Dynamic Event Emission for Registry-only Schemas

Goal
- Determine whether registry-only schema sources are emitted via dynamic event creation (e.g., `Event::dynamic`, JSON payloads, or `test_event`).
- Identify components that emit shell/terminal/journald events without typed payloads.

Process
1) Search for `Event::dynamic`, `test_event`, or raw JSON event creation in the codebase.
2) Filter results for registry-only sources (`shell.*`, `terminal.kitty`, `atuin`, `journald`, `system.*`).
3) Map call sites to emitting components and contexts.
4) Summarize whether dynamic emission explains registry-only schemas.

Deliverables
- List of dynamic emission call sites and sources.
- Findings on how registry-only schemas are used.
