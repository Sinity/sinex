# High-Leverage Implementation Plan — 2026-04-16

> Sequenced by leverage × feasibility. Each wave unlocks the next.
> Items marked ⚡ are parallelizable via subagents.

## Current State (post-verification sprint)

- 762K real events in sinex_prod, 8/8 services active
- Canonicalizer producing 74K derived events (first automata output ever)
- Gateway alive, clean smoke event persisted
- Codebase: 0 errors, 0 warnings, zero fixable bugs found in sweep
- 5 ingestors built, 3 automata deployed, session detector code-complete but undeployed
- Embedding pipeline: 5 schema tables, zero Rust code
- SDK input adapters: only SQLite built; file-tailer/batch-importer/API-poller missing

---

## Wave 1: Verifiability Foundation (enables confident iteration)

**Why first:** Every subsequent wave changes core behavior. Without test coverage for the pipeline's critical paths, we're flying blind. This wave creates the safety net.

| # | Item | Crate | Complexity | Parallel |
|---|------|-------|------------|----------|
| 1.1 | Replay e2e integration test: create events → replay → verify archive + re-create | sinex-gateway tests | 4h | ⚡ |
| 1.2 | Concurrent ingestor + automaton test: terminal + canonicalizer together, verify no loss | e2e tests | 4h | ⚡ |
| 1.3 | Gateway auth boundary test: 50+ endpoints × 3 roles matrix | sinex-gateway tests | 3h | ⚡ |
| 1.4 | Privacy engine property test: proptest over all ProcessingContexts + strategies | sinex-primitives tests | 3h | ⚡ |
| 1.5 | Pipeline throughput benchmark: measure events/sec for NATS→ingestd→DB at various batch sizes | xtask bench | 4h | ⚡ |
| 1.6 | Query latency benchmark: composable query engine P50/P95/P99 at various filter depths | xtask bench | 3h | ⚡ |
| 1.7 | Checkpoint durability test: kill -9 mid-processing, verify checkpoint ≥ N - interval | sinex-node-sdk tests | 3h | ⚡ |
| 1.8 | COPY-schema contract fuzz test: add column, verify panic fires | sinex-db tests | 2h | ⚡ |

**Total: ~26h, fully parallelizable (8 independent tasks)**

**Verification:** `xtask test --heavy` passes, benchmark results recorded in `xtask/config/perf-contracts.toml`

---

## Wave 2: SDK Input Adapters (unblocks 28 data sources)

**Why second:** The SDK has one adapter (SQLite). Adding file-tailer and batch-importer unblocks ~20 of the 28 unwired sources. Each adapter saves ~200 lines per ingestor. This is the single highest-leverage infrastructure investment.

| # | Item | Crate | Complexity | Unlocks |
|---|------|-------|------------|---------|
| 2.1 | File-tailer adapter: inotify + seek-to-offset for append-only files | sinex-node-sdk | 6h | IRC logs, scribe-tap JSONL, power CSV, syslog |
| 2.2 | Batch-importer adapter: directory-scan + process-once-per-file | sinex-node-sdk | 4h | GDPR exports (Reddit, Spotify, Facebook), Takeout |
| 2.3 | API-poller adapter: interval-based HTTP + cursor-state | sinex-node-sdk | 4h | Spotify API, Reddit API, health APIs |
| 2.4 | CSV/TSV parser trait: header detection, column mapping, streaming | sinex-node-sdk | 3h | Power sensor, finance, health data, Goodreads |

**Total: ~17h, partially parallelizable (2.1-2.3 independent, 2.4 shared)**

**Verification:** Each adapter has a `#[sinex_test]` exercising: initialization, checkpoint persistence, gap-fill resume, error handling

---

## Wave 3: Intelligence Layer Activation (leverage existing infrastructure)

**Why third:** The SDK, DerivedNodeAdapter, and event bridge are now working. Three automata already process events. This wave deploys the intelligence features that have been "code-complete but undeployed."

| # | Item | Crate | Complexity | Unlocks |
|---|------|-------|------------|---------|
| 3.1 | Deploy session detector on live data: NixOS service, verify `activity.session.boundary` events | sinex-session-detector + nixos | 2h | Context restoration, day summaries |
| 3.2 | Privacy middleware in DerivedNodeAdapter: run privacy engine on derived outputs | sinex-node-sdk | 3h | Safe derived events (currently inherits ingestor leaks) |
| 3.3 | EmbeddingRepository: implement the repository trait for `core.event_embeddings` | sinex-db | 4h | Hybrid search, semantic queries |
| 3.4 | Embedding automaton: process events → generate embeddings via local model | new crate: sinex-embedding-automaton | 8h | Semantic search across all events |
| 3.5 | Hybrid search stored function: FTS + vector cosine similarity fusion | sinex-db + schema | 4h | `sinexctl query --semantic` |
| 3.6 | Entity extractor automaton: NER on command/window/document events → `core.entities` | new crate: sinex-entity-extractor | 8h | Knowledge graph, cross-source correlation |

**Total: ~29h, partially parallelizable (3.1-3.3 independent, 3.4 depends on 3.3, 3.5 depends on 3.3, 3.6 independent)**

**Verification:** `sinexctl query --source terminal-command-canonicalizer` returns derived events; session boundaries appear in `sinexctl report today`; embedding search returns ranked results

---

## Wave 4: Lynchpin Phase 1 Subsumption (daily-use value)

**Why fourth:** With SDK adapters and intelligence layer operational, subsume the 5 highest-value lynchpin data sources. This eliminates the two-system overhead for daily workflows.

| # | Item | Crate | Complexity | Lynchpin Kill |
|---|------|-------|------------|---------------|
| 4.1 | Git activity capture: post-commit hook → `git.commit` events | sinex-system-ingestor extend | 3h | `lynchpin.sources.git` |
| 4.2 | AI session ingestor: Codex JSONL + Polylogue markdown → `ai.session.*` events | new crate: sinex-ai-session-ingestor | 6h | `lynchpin.sources.codex`, `polylogue` |
| 4.3 | Browser history ingestor: read browser SQLite (Firefox/Chrome) → `browser.visit` events | new crate: sinex-browser-ingestor | 6h | `lynchpin.sources.webhistory` |
| 4.4 | Spotify streaming import: JSON array batch import → `media.play` events | sinex-media-ingestor via batch-importer adapter | 3h | `lynchpin.sources.spotify` |
| 4.5 | `sinexctl report calendar`: cross-source daily/weekly activity view | sinexctl | 4h | `lynchpin.views.calendar_views` |

**Total: ~22h, fully parallelizable (5 independent tasks)**

**Verification:** `sinexctl query --source git` returns commits; `sinexctl report calendar --week` renders cross-source view comparable to lynchpin's calendar_views

---

## Wave 5: Operational Confidence (production hardening)

**Why last:** With the intelligence layer and data sources operational, harden the pipeline for reliable long-term operation.

| # | Item | Crate | Complexity | Impact |
|---|------|-------|------------|--------|
| 5.1 | Self-observation event persistence: ensure heartbeat/batch-stats events reach operator CAs | sinex-ingestd + sinex-node-sdk | 4h | Operator dashboard data |
| 5.2 | DLQ consolidation: define exactly two surfaces (NATS raw-ingest DLQ + DB processing DLQ) | sinex-ingestd + sinex-node-sdk | 4h | Clear failure routing |
| 5.3 | Circuit breaker for NATS publish: back off on persistent failures | sinex-node-sdk | 3h | Prevents cascade failure |
| 5.4 | Batch poison pill isolation: bisect-retry in ingestd (exists in HistoricalImporter, not ingestd) | sinex-ingestd | 4h | One bad event doesn't kill 1000 |
| 5.5 | `sinexctl verify`: trustworthiness invariants check (provenance XOR, anchor bounds, schema consistency) | sinexctl | 4h | Operator confidence |
| 5.6 | Grafana dashboard provisioning: NixOS module for pre-built dashboard | nixos modules | 3h | Visual monitoring |

**Total: ~22h, partially parallelizable**

**Verification:** `sinexctl verify` passes; operator CAs have data; DLQ has clear routing documentation

---

## Summary

| Wave | Items | Hours | Leverage |
|------|-------|-------|----------|
| 1. Verifiability | 8 | 26h | Enables confident iteration on everything below |
| 2. SDK Adapters | 4 | 17h | Unblocks 28 data sources (~200 lines saved each) |
| 3. Intelligence | 6 | 29h | Activates session detection, embeddings, entities |
| 4. Lynchpin P1 | 5 | 22h | Eliminates two-system overhead for daily workflows |
| 5. Operational | 6 | 22h | Production hardening and operator confidence |
| **Total** | **29** | **116h** | — |

All 29 items are concrete, scoped, and executable against the codebase as it exists today. Waves 1-2 are fully parallelizable internally. Waves 3-4 have partial dependencies. Wave 5 is independent.

**Aggressive timeline:** 2-3 weeks with subagent parallelism.
**Conservative timeline:** 4-6 weeks with focused sessions.
