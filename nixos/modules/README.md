# Sinex NixOS Module

The Sinex NixOS module exposes a single entry point – `services.sinex` – that owns
all service wiring, directories, and lifecycle management for the platform. The
option tree mirrors the running system so that every knob you set maps directly
to a systemd unit, CLI argument, or generated configuration file.

## Key Namespaces

| Namespace | Purpose |
|-----------|---------|
| `services.sinex.stateRoot` | Root that all derived paths cascade from (logs, spool, blobs, DLQ). |
| `services.sinex.users` | `target` (captured workstation user) and `satellites` (service account). |
| `services.sinex.database` | PostgreSQL provisioning, connection pool sizing, and migrations. |
| `services.sinex.nats` | NATS/JetStream provisioning and stream bootstrap. |
| `services.sinex.storage` | Dead-letter queue handling and git-annex backed blob store. |
| `services.sinex.core` | Ingestion (`sinex-ingestd`) and gateway service configuration. |
| `services.sinex.satellites` | Filesystem/terminal/desktop/system collectors plus automata. |
| `services.sinex.observability` | Prometheus/Grafana/exporters and structured log retention. |
| `services.sinex.lifecycle` | Pre-flight verification and coordinated update orchestration. |
| `services.sinex.shell` | Developer ergonomics (asciinema capture, Kitty auto-config). |

Every option has a concrete effect – either on a systemd unit, generated script,
or config file. If you do not override a field, the module chooses sensible
values derived from `stateRoot` and the global `logLevel`.

## Quick Start

```nix
{
  services.sinex = {
    enable = true;
    package = pkgs.sinex;
    users.target = "alice";

    stateRoot = "/var/lib/sinex";
    logLevel = "info";

    database.autoSetup = true;
    nats.autoSetup = true; # defaulted on when services.sinex.enable = true

    storage.dlq.cleanup = {
      schedule = "hourly";
      maxAge = "14d";
    };

    satellites.filesystem.watchPaths = [ "/home/alice" "/workspace" ];
    satellites.automata.canonicalizer.profile = "heavy";

    observability.monitoring = {
      enable = true;
      grafana.enable = true;
    };
  };
}
```

### Database
- `database.autoSetup` defaults to `false` unless `services.sinex.enable = true`.
  Flip it on explicitly when you need the cluster even with the main service
disabled (e.g. staging migrations).
- `database.extraDatabases` lets you provision additional DBs (e.g. `sinex_dev`)
  alongside the primary `database.name`; extensions and migrations run against
  each entry automatically.
- Shared preload libraries always include TimescaleDB and `pgx_ulid` to support
  hypertables and ULID generation.
- Pool sizing (`connectionPool.{maxConnections,minConnections,...}`) feeds both
  Postgres `max_connections` and the CLI flags passed to service binaries.

### Storage
- DLQ cleanup runs via `sinex-dlq-cleanup.timer`; schedule, max age, and max file
  count come from `storage.dlq.cleanup`.
- Blob repository lives at `storage.blob.repositoryPath` (default:
  `${stateRoot}/blob-repository`). `autoInit = true` creates the git-annex repo
  on boot.
- `storage.blob.maintenance` controls GC/fsck timers. Enable health checks to log
  repository size warnings.
- `nats.bootstrapStreams.enable` bootstraps standard JetStream streams via the `nats`
  CLI (requires `pkgs.natscli`).

### Core & Satellites
- `core.ingestd` and `core.gateway` expose per-service resources, log levels,
  batches, and extra CLI args.
- Satellite defaults (`satellites.defaults`) cover instances, batching, and
  resource limits. Individual satellites can override by setting their field to
  `null` (inherit) or a concrete value.
- Automata use named profiles defined under `satellites.automata.profiles`; set
  `profile = "light"|"standard"|"heavy"` to select batch and MemoryMax/CPUQuota.
- The module emits deterministic unit names (`sinex-filesystem-1`,
  `sinex-health-aggregator`, etc.) and publishes them via
  `services.sinex.satellites.generatedUnits` for other subsystems (pre-flight,
  tests).

### Observability
- Structured log retention is configured via `observability.logging.retention`.
- Prometheus/Grafana/exporters turn on automatically when
  `observability.monitoring.enable = true`. Extra scrape configs drop straight
  into `services.prometheus.extraScrapeConfigs`.
- `observability.alerts.enable` adds the provided rule files to Prometheus.

### Lifecycle
- Pre-flight verification lives under `lifecycle.preflight`. Disable individual
  phases with `lifecycle.preflight.skip = [ "migrations" "services" ];`.
- Coordinated updates use `lifecycle.updates` for grace periods and roll-back
  policy. The generated `sinex-update` service restarts guarded units in-place,
  preserving DLQ contents when `preserveData = true`.
- Maintenance toggles (`lifecycle.maintenance.tasks`) control DLQ cleanup and
  blob GC/fsck integration.

### Developer Ergonomics
- `shell.asciinema.autoRecord = true` records interactive shells to
  `${stateRoot}/asciinema` by default.
- `shell.kitty.autoConfigure = true` injects the bundled integration snippet into
  the target user’s `kitty.conf`. Set `configFile` and `snippet` to customize.

## File Layout
- `default.nix` – option definitions and shared wiring (tmpfiles, user accounts,
  DLQ timer).
- `database.nix` – PostgreSQL provisioning when `database.autoSetup = true`.
- `nats.nix` – NATS/JetStream provisioning and stream bootstrap when enabled.
- `blob-storage.nix` – git-annex initialization and maintenance timers.
- `monitoring.nix` – Prometheus/Grafana/exporter configuration.
- `preflight-verification.nix` – `sinex-preflight` and `sinex-update` units.
- `satellite-services.nix` – Core ingest/gateway and satellite/automata units.
- `kitty-shell-integration.nix` – Kitty auto-configuration helper.

## Testing Tips
- `just test` to run the Rust workspace (requires TimescaleDB extension).
- `nix run .#check` to validate the module evaluates with your configuration.
- VM scenarios under `tests/e2e/nixos-vm` consume the same option tree – updating
  defaults in the module automatically propagates to the test fixtures.
