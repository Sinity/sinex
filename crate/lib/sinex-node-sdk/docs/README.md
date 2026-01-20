# Node SDK Documentation

## Core Documentation

- `overview.md` – SDK purpose, constellation architecture, startup patterns
- `stream_processor.md` – Unified stream processor interface and time horizons
- `patterns.md` – Processor vs Automaton patterns and deployment
- `provenance.md` – Ingestion patterns, sensor/ingestor separation, checklists
- `vision.md` – SDK development vision (SimpleProcessor, Aggregator, sx tool, Tether)

## Implementation Guides

- `stage_as_you_go.md` – Stage-as-you-go pattern for real-time provenance
- `coordination.md` – Heartbeat and upgrade recovery flows
- `preflight.md` – Node preflight verification categories
- `annex.md` – Annex subsystem architecture and workflows

## Key Runtime Entry Points

- `NodeInitContext::into_runtime()` yields a `NodeRuntimeState` with ergonomic accessors for acquisition, job, lifecycle, coordination, heartbeat, and replay helpers
- `replay::ReplayService::from_runtime` is the canonical way to construct replay pipelines for CLI and nodes
- Tests can use `sinex_test_utils::TestRuntimeBuilder` to provision ephemeral NATS, PostgreSQL, and emitters

## See Also

- Global architecture: `docs/current/architecture/`
- Event taxonomy: `crate/lib/sinex-schema/docs/event-taxonomy.md`
