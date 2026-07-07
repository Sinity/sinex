---
created: "2026-06-29"
purpose: "Record curated Sinex demo packet copied to /realm/inbox/demos_sinex and next demo-build implications."
status: active
project: sinex
---

# Demo Packet Curation

Operator requested up-to-date good Sinex demo/capability results copied or produced under:

```text
/realm/inbox/demos_sinex
```

## Copied / Produced

- `sinex-recall/`: current committed recall demo and sample output.
- `recall-silo-collapse/`: before/after writeup showing the context recall lens moved from self-observation/desktop silo behavior toward a family-aware shared recall pack.
- `context-pack-current/`: current `sinexctl events context --artifact-dir` markdown/json artifact.
- `agent-forensics-real/`: strong real Polylogue-side forensics report over the live archive. This is Polylogue-native value, included because future Sinex context packs should join this evidence rather than reimplement it.
- `agent-forensics/`: tiny fixture forensics report, retained only as a fixture-shape artifact.
- `ops-demo-walkthrough/`: current deterministic `sinexctl ops verify --demo` walkthrough docs. No `.sinex/demo` generated artifact was present in checkout.
- `crosssource-recall-research/`: negative-result research explaining why a forced multi-source recall demo is currently weak/dishonest.
- `demo-roadmap/`: planning/spec material, clearly separated from proven demos.
- `cross-stack-polylogue-sinex-context.md`: newly written judgment/spec on when Sinex+Polylogue beats Polylogue alone.

## Important Conclusions

- Cross-stack Polylogue/Sinex is superior to Polylogue alone only when Sinex contributes non-transcript evidence: terminal, git/file changes, system/window/activity, source coverage, provenance, caveats.
- A provisional Polylogue bridge is worth building only as a thin, versioned event/material export/import that resembles the eventual backend contract. Do not build a second Polylogue inside Sinex.
- The useful recall-pack shape already exists in current code as shared primitives:
  - `ContextRecallPackView`
  - `ContextFamilyActivityView`
  - `EventCardView`
- Next work is not first promotion, but reuse/enrichment:
  - better salience ranking;
  - richer timeline entries;
  - stronger provenance/coverage/caveats;
  - API/TUI/demo reuse of the same shared view;
  - avoid separate report silos.

## Next Demo-Build Order

1. Improve recall-pack salience/timeline over the existing shared view.
2. Fix ActivityWatch payload extraction and Hyprland real-time focus capture before claiming multi-source attention reconstruction.
3. Add git/file-change family evidence to recall and agent brief.
4. Build agent pre-task briefing as a lens over the same context pack.
5. Add a thin Polylogue bridge only when it directly feeds joined context packs and advances the proper backend contract.

