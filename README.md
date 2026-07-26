# Sinex

Sinex records local machine activity as typed events in PostgreSQL. It preserves
where each event came from, keeps original and interpretation timestamps
separate, supports replay and revision, and provides explicit tools for
inspection, recovery, and deployment.

The current system captures filesystem, terminal, browser, desktop, and system
activity. Live producers use NATS JetStream. Finite imports such as exports,
recordings, and snapshots can enter the same event-admission path directly.

Sinex is an active personal deployment and a pre-1.0 engineering project. Event
capture, admission, persistence, APIs, replay infrastructure, NixOS deployment,
and recovery tooling are implemented. Generic current-state reducers and the
full proposal, judgment, and finalization workflow remain incomplete.

[Quick start](#quick-start) | [Architecture](#how-events-enter-the-system) | [Documentation](docs/README.md) | [Roadmap](https://sinity.github.io/sinex/beads/)

## What Sinex does

Sinex gives local data sources one consistent event path:

- source material is identified and recorded before parsing;
- parsers emit typed events with provenance;
- one admission layer validates revision, time, and storage rules;
- confirmed events are appended to PostgreSQL;
- replayable consumers build derived state;
- operators inspect the system through `sinexctl`, JSON-RPC, SSE, logs, and
  telemetry;
- snapshots, archives, and restore commands make recovery testable.

This is meant for long-running personal capture and analysis on machines the
operator controls. It is not a hosted analytics service.

## Current inputs and outputs

| Area | Current implementation |
|---|---|
| Live capture | filesystem, terminal, desktop, browser, system, and runtime producers over NATS JetStream |
| Finite inputs | staged exports, recordings, snapshots, and other bounded source material parsed inside `sinexd` |
| Persistence | PostgreSQL 18 through `sinexd::event_engine` |
| Derived work | replay-aware automata and reflection products |
| Query and control | `sinexctl`, JSON-RPC, SSE, and a read-only MCP server |
| Deployment | NixOS modules, systemd units, preflight checks, telemetry, and recovery commands |

## How events enter the system

```text
Live producers                         Finite source material
filesystem, terminal, browser,        exports, recordings, snapshots
system, desktop, runtime hooks                    |
        |                                         v
        v                                  sinexd::sources
+-------------------------+                       |
| NATS JetStream          |                       | direct admission
| live and derived events |                       |
+------------+------------+                       |
             +-------------------+----------------+
                                 v
                         +---------------+
                         | event_engine  |
                         | validate      |
                         | admit         |
                         | persist       |
                         +-------+-------+
                                 v
                         +---------------+
                         | PostgreSQL 18 |
                         | core events   |
                         | reflection    |
                         | source data   |
                         | operations    |
                         +-------+-------+
                                 v
                         +---------------+
                         | sinexd::api   |
                         | JSON-RPC/SSE  |
                         +-------+-------+
                                 v
                         sinexctl, MCP, clients
```

Confirmed events also feed durable consumers. Derived events return through the
same admission and persistence rules rather than writing around the event
engine.

The staged-source parser model is documented in
[`crate/sinexd/docs/sources/staged_source_parser_substrate.md`](crate/sinexd/docs/sources/staged_source_parser_substrate.md).

## Data rules

Several rules define what the event log means:

- **One write boundary.** Canonical event persistence goes through
  `sinexd::event_engine`.
- **Corrections are new records.** Existing events are not edited in place.
  Later interpretations keep links to what they replace.
- **Source time and interpretation time are separate.** `ts_orig` records when
  the source says something happened. `ts_coided` records when Sinex accepted
  that interpretation.
- **Activity and reflection are separate.** `core.events` stores captured
  activity and source interpretation. `reflection.events` stores derived or
  self-observational products with support, derivation, and adjudication
  metadata.
- **Repeated observations follow a revision policy.** Identical values can be
  suppressed. Event types configured for supersession can archive the previous
  interpretation and admit the changed one.
- **Derived state is replayable.** Consumers retain source and replay metadata,
  and long-running work is recorded in `operations_log`.
- **Large payloads are referenced by content hash.** Blobs remain stable across
  replay and repair.

See [the event taxonomy](crate/sinex-db/docs/schema/event-taxonomy.md) and
[the source evidence model](crate/sinexd/docs/sources/evidence_lanes.md).

## Quick start

The supported development environment is Nix-first:

```bash
git clone https://github.com/sinity/sinex.git
cd sinex
direnv allow

xtask infra start
xtask run core --logs
xtask run list
sinexctl events recent -n 10
```

Useful operator commands:

```bash
xtask doctor
xtask status --summary
xtask infra status
journalctl -u sinexd -f
```

## What is implemented and what is not

The event runtime is substantial, but several higher-level programs are still
partial.

Implemented now:

- typed source materials, parser contracts, and event admission;
- PostgreSQL event persistence and source provenance;
- live and derived NATS transport;
- revision classification and supersession on the stream-consumer path;
- replay-aware automata, checkpoints, and operations records;
- API, CLI, SSE, telemetry, private mode, snapshots, restore, and deployment;
- schema and write support for reflection and derivation metadata;
- a shared reducer vocabulary and the `tasks.current` reducer specification.

Still incomplete or limited:

- generic reducer registration and current-object storage across domains;
- complete replay invalidation and cross-domain trace tooling;
- migration of every analytical product onto the reflection control plane;
- universal proposal, judgment, and finalization for model-generated changes;
- a general query language over every planned personal-data domain.

The committed Beads graph records which parts are implemented, blocked, or
proposed. Run `bd ready` and `bd list`, or browse the
[web board](https://sinity.github.io/sinex/beads/).

## Deployment and operations

The canonical deployment is the NixOS module tree under `services.sinex`.
Service configuration, startup preflight, schema validation, source checks,
resource policy, and recovery are part of that deployment.

Current hardening includes:

- TLS-only API RPC;
- stronger policy for non-loopback binds;
- bearer-token authentication and per-token rate limits;
- structured access logs for RPC, SSE, and native messaging;
- systemd sandboxing for long-running and maintenance units;
- typed NATS TLS and subject authorization in the NixOS module.

Conventional agenix secret names and the full module contract are documented in
[nixos/modules/README.md](nixos/modules/README.md).

Deployment and host readiness are checked by `sinexd` startup preflight,
`sinexctl`, and NixOS evaluation. `xtask` owns repository and development
workflows, not the final host proof. See
[xtask/docs/runtime-target-boundaries.md](xtask/docs/runtime-target-boundaries.md).

## Development

Start with:

- [CONTRIBUTING.md](CONTRIBUTING.md)
- [TESTING.md](TESTING.md)
- [xtask/docs/devshell-runtime.md](xtask/docs/devshell-runtime.md)
- [xtask/docs/README.md](xtask/docs/README.md)
- [xtask/docs/sandbox/README.md](xtask/docs/sandbox/README.md)

## Documentation

The [documentation map](docs/README.md) groups architecture, capture, operator,
deployment, and contributor material.

| Task | Start here |
|---|---|
| Understand source parsing | [source docs](crate/sinexd/docs/sources/README.md) |
| Understand event schemas | [event taxonomy](crate/sinex-db/docs/schema/event-taxonomy.md) |
| Define current-state projections | [domain reducers](crate/sinex-primitives/docs/domain_reducers.md) |
| Review generated-change authority | [curation authority](crate/sinex-primitives/docs/curation_authority.md) |
| Expose evidence to agents | [read-only MCP server](crate/sinexctl/docs/mcp_readonly_server.md) |
| Understand replay and source snapshots | [evidence model](crate/sinexd/docs/sources/evidence_lanes.md) |
| Review backpressure and loss policy | [runtime QoS](crate/sinexd/docs/runtime_qos.md) |
| Suppress capture temporarily | [private mode](crate/sinexctl/docs/private_mode.md) |
| Add a staged export parser | [parser guide](crate/sinexd/docs/sources/adding_staged_export_parser.md) |
| Snapshot or restore state | [state snapshot](crate/sinexctl/docs/state_snapshot.md) |
| Configure PostgreSQL recovery | [backup and restore](crate/sinex-db/docs/backup_restore.md) |
| Find the owner of a concern | [authority surfaces](.github/authority-surfaces.md) |

## Security

Sinex assumes a trusted single-user host with full-disk encryption and
capture-time privacy controls. Runtime modules submit events through NATS, while
`sinexd::api` is the hardened external boundary. Direct database access is for
diagnostics; normal writes and queries go through the event engine and API.

Read the deployment and security documentation before exposing any interface
outside the local host.

## Status

Built and operated for personal use. Not yet production-ready for general
deployment.

## License

MIT
