# Loop 009 - Self-Prompt

Goal: Compare emitted event types against registered payload schemas to find mismatches or gaps.

Process (do not skip):
1. Enumerate schema-registered event types via `#[event_payload]` usage in `sinex-core`.
2. Enumerate event emission sites (`Event::new`, `CoreEvent::new`, `EventType::from` with config strings).
3. Identify event types that are emitted or queried but lack a matching payload schema.
4. Identify payload schemas that have no emission sites.
5. Record concrete evidence with file paths and event type strings.

Deliverables:
- analysis report with coverage map + findings.
- concrete issue list in a separate file.
- meta-reflection on coverage and blind spots.
- short brainstorm on next analysis.
