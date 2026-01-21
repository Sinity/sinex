# Loop 017 - Meta-Reflection

What worked
- Parsing `#[event_payload]` attributes via a small script produced a concrete set comparison against `registry.json`.
- The diff surfaced non-telemetry mismatches (e.g., shell/terminal/journald entries).

What is incomplete
- I did not verify whether registry-only entries correspond to legacy schemas intended to remain without code emitters.
- I did not check if missing entries are gated behind features that disable inventory registration.

Next time
- Inspect feature flags on missing payloads and confirm whether they are conditionally compiled.
- Trace registry-only schemas to their source repositories or schema files to confirm intended support.
