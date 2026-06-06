# Projections: MCP, SinexFS, tasks, documents, semantics

## Agent projection / MCP

Agents should see the same object grammar as humans:

- stable refs
- generated_at/freshness
- caveats and redactions
- source/material/provenance refs
- command equivalents
- action availability

The MCP surface should not invent alternate payloads. It should project the same view DTOs or a documented subset.

## SinexFS target

SinexFS should be read-only by default and should expose object refs as files, not bypass authority:

```text
/sinex/events/<event-id>.json
/sinex/events/<event-id>.md
/sinex/sources/<source>/readiness.json
/sinex/materials/<material-id>/manifest.json
/sinex/context-packs/<pack-id>/manifest.json
/sinex/ops/<operation-id>.json
```

Every generated file should contain freshness/caveat/privacy metadata.

## Tasks and domain reducers

Tasks should be presented as event-native projections, not a separate task app. The task UI should show:

- task object state
- source events that created/updated/completed/cancelled it
- confidence/proposal state if derived
- direct command equivalents for manual actions
- relationship to context/moment evidence

## Semantic shadow lanes

Semantic lanes should be treated as provisional derived interpretations:

- lane id/version/status
- source events/materials
- entity/relation outputs
- diffs against canonical graph
- confidence and model-effect provenance
- discard/promote authority
- caveats and stale/replay state

The UI must not collapse semantic outputs into canonical truth without visible promotion/audit.
