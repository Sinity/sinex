# Loop 022 - Legacy Schema Origins: journald.satellite.heartbeat and system.*_historical

Goal
- Determine whether legacy schemas like `journald/satellite.heartbeat` and `system/*_historical` have emitters or documentation.
- Identify if these schemas are legacy artifacts or tied to retired components.

Process
1) Inspect the JSON schema files for these event types.
2) Search the codebase for corresponding source/event_type strings.
3) Check docs or diagrams for references to these events.
4) Summarize whether they are active, legacy, or external-only.

Deliverables
- Evidence for or against active emitters.
- Guidance on whether schemas should be kept or pruned.
