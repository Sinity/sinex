# Semantic Desktop Stream

## Overview
Derive a higher‑level, semantically annotated stream of desktop activity (focus, windows, commands, documents) by correlating low‑level events across satellites into meaningful episodes and contexts.

## Goals
- Turn raw events into task/episode semantics
- Preserve provenance chains; remain explainable
- Enable richer queries and insights

## Architecture
- Automata consume events from files, terminal, desktop, system satellites and synthesize `desktop.*` semantic events.
- Maintain links to source events (`source_event_ids`) and material where applicable.
- Use stable schemas with versioned IDs; emit minimal payloads with references.

## Event Types (examples)
- `desktop.episode.started`: episode_id, kind (coding|writing|browsing), seed_event_id
- `desktop.episode.ended`: episode_id, duration_ms, sources_involved[]
- `desktop.task.switch`: from_episode_id, to_episode_id, reason (focus|idle|command)
- `desktop.context.joined`: episode_id, artifact_ref?, tags[]

## Input Signals & Features
- Window focus changes (title, app), terminal command executions, file open/save events, network activity bursts, idle timers.
- Feature extraction examples: focus dwell time, command frequency, filename/project hashes, app category, URL domains.

## Schema & Provenance
- Schema IDs: `desktop/episode@v1`, `desktop/task_switch@v1`, `desktop/context_joined@v1`.
- Provenance: every semantic event carries `source_event_ids` and optional `source_material_id` for explainability.

## Heuristics & Models
- Rule‑based starters: sustained focus > N seconds + activity threshold → new episode.
- Switch detection: rapid focus change + command run → probable task switch.
- Enrichment: tag inference via filename/project patterns; optional embeddings similarity to prior episodes.

## Queries (examples)
- Recent episodes: `search.search_events(kind=\"desktop.episode.*\", last=\"2h\")`.
- Context join graph: follow `source_event_ids` to raw events and artifacts.
- Productivity windows: aggregate `episode.duration_ms` by hour/day.

## Failure Modes
- Noisy focus flapping → debounce with hysteresis; emit `desktop.episode.adjusted` when merging.
- Missing source links → drop to best‑effort; mark `provenance_complete=false`.

## Validation
- Property tests for episode boundary consistency and monotonic time windows.
- Snapshot tests for episode series given fixed input streams.

## Detection Strategies
- Window focus + terminal command co‑occurrence → task identification
- Temporal proximity clustering with thresholds
- Optional embeddings for semantic similarity

## Roadmap
- P1: Episode boundaries and task switches
- P2: Context enrichment (artifacts, tags)
- P3: Insight generation (patterns, bottlenecks)
