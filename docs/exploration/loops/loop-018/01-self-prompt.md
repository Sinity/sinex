# Loop 018 - Registry-only Schemas vs JSON Files

Goal
- Identify registry entries that have JSON schemas on disk but no `EventPayload` annotation in code.
- Determine if these are legacy schemas or represent missing code emitters.

Process
1) Extract registry entries that are not in the EventPayload inventory.
2) For each, confirm the JSON file exists in `schemas/v1`.
3) Spot-check a few schemas to see if they appear legacy or still relevant.
4) Summarize whether these should be kept or if code is missing.

Deliverables
- List of registry-only entries with file presence.
- Findings on potential legacy schemas.
