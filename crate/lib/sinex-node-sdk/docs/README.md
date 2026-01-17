# node SDK Documentation

Markdown references embedded in rustdoc:

- `overview.md` – SDK purpose, constellation architecture, startup pattern.
- `stream_processor.md` – unified stream processor interface and time horizons.
- `stage_as_you_go.md` – stage-as-you-go pattern and benefits.
- `coordination.md` – heartbeat/upgrade recovery flows.
- `preflight.md` – node preflight verification categories and usage.
- `annex.md` – annex subsystem architecture and workflows.

Key runtime entry points:

- `NodeInitContext::into_runtime()` yields a `NodeRuntimeState` with ergonomic accessors for acquisition, job, lifecycle, coordination, heartbeat, and replay helpers. nodes should stop threading raw pools or emitters and instead consume the runtime snapshot directly.
- `replay::ReplayService::from_runtime` is the canonical way to construct replay pipelines for both the CLI and nodes. It exposes convenience methods such as `replay_into_emitter` to drive events back through the processor's emitter.
- Tests can stand up fully wired processors through `sinex_test_utils::TestRuntimeBuilder`, which provisions ephemeral NATS, a PostgreSQL pool, telemetry emitters, and channel-backed emitters in a single fluent builder.
