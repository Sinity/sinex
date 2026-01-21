# Loop 017 - EventPayload Inventory vs Schema Registry Drift

Goal
- Estimate the number of EventPayload types in code and compare to the schema registry entry count.
- Determine whether schema artifacts likely lag behind the inventory of payloads.

Process
1) Count `#[event_payload(...)]` occurrences across Rust sources.
2) Count entries in `schemas/v1/registry.json`.
3) Compare counts and call out any significant discrepancies.
4) Note caveats (e.g., multiple versions, non-generated schemas, tests).

Deliverables
- Count comparison and interpretation.
- Concrete issues if drift is evident.
