# Planning Documentation

Development roadmap, priorities, and feature proposals for Sinex.

## Roadmap & Priorities

- [ROADMAP.md](./ROADMAP.md) — Long-term roadmap with links to vision documents
- [development-priorities.md](./development-priorities.md) — Current focus areas
- [testing-priorities-and-roadmap.md](./testing-priorities-and-roadmap.md) — Test infrastructure evolution
- [type-safety-enhancements-roadmap.md](./type-safety-enhancements-roadmap.md) — Type system roadmap

## SDK & DX

SDK development vision is documented in the `sinex-node-sdk` crate:
- `crate/lib/sinex-node-sdk/docs/vision.md` — SDK improvements (SimpleProcessor, Aggregator, sx tool, Tether, Wasm runtime)

## Architecture & Integration

- [datasette-integration-opportunities.md](./datasette-integration-opportunities.md) — Datasette integration options
- [event-sources-coverage.md](./event-sources-coverage.md) — Sensor/capture coverage plans
- [event-relations.md](./event-relations.md) — Event relationship modeling

For long-term architectural evolution, see [../vision/architectural-evolution.md](../vision/architectural-evolution.md).

## UX & Features

- [explore-ux-roadmap.md](./explore-ux-roadmap.md) — CLI/explore experience milestones
- [tagging-system.md](./tagging-system.md) — Tagging system design

## Rapid Development

- [rapid-assembly-estimates.md](./rapid-assembly-estimates.md) — LOC estimates for browser extension, LLM integration, embeddings

## Feature Proposals

See [features/](./features/) for individual feature proposals:
- [semantic-search.md](./features/semantic-search.md) — Embeddings, hybrid search, entity resolution, GPU scale
- Browser extension, email ingestion, LLM orchestration
- Audio transcription, OCR, web archiving, encryption

---

For long-term strategic direction, see [../vision/](../vision/).
For current working architecture, see [../current/](../current/).
