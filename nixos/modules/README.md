# Sinex NixOS Module

The Sinex NixOS module exposes a single entry point – `services.sinex` – that owns
all service wiring, directories, and lifecycle management for the platform. The
option tree mirrors the running system so that every knob you set maps directly
to a systemd unit, CLI argument, or generated configuration file.

## Key Namespaces

| Namespace | Purpose |
|-----------|---------|
| `services.sinex.stateRoot` | Root that all derived paths cascade from (logs, spool, blobs, DLQ). |
| `services.sinex.users` | `target` (captured workstation user) and `nodes` (service account). |
| `services.sinex.database` | PostgreSQL provisioning, connection pool sizing, and migrations. |
| `services.sinex.nats` | NATS/JetStream provisioning and stream bootstrap. |
| `services.sinex.storage` | Dead-letter queue handling and git-annex backed blob store. |
| `services.sinex.core` | Ingestion (`sinex-ingestd`) and gateway service configuration. |
| `services.sinex.nodes` | Filesystem/terminal/desktop/system collectors plus automata. |
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

    nodes.filesystem.watchPaths = [ "/home/alice" "/workspace" ];
    nodes.automata.canonicalizer.profile = "heavy";

    observability.monitoring = {
      enable = true;
      grafana = {
        enable = true;
      };
    };
  };
}
```

`core.gateway` is enabled by default, so the quick-start config needs a real
gateway admin token file before the generated unit will start. The module
auto-resolves either an agenix secret named `sinex-gateway-admin-token` or a
declarative `environment.etc."sinex/gateway-admin-token"` entry; set
`services.sinex.secrets.gatewayAdminTokenFile` only when you need a non-standard
path.

### Database
- `database.autoSetup` defaults to `false` unless `services.sinex.enable = true`.
  Flip it on explicitly when you need the cluster even with the main service
disabled (e.g. staging migrations).
- When `services.sinex.enable = true`, `database.name` defaults to
  `sinex_<environment>` and must stay suffixed with
  `services.sinex.nats.environment` so the runtime database cannot silently
  drift away from the active NATS subject namespace.
- `database.extraDatabases` lets you provision additional DBs (e.g. `sinex_dev`)
  alongside the primary `database.name`; the module applies extensions during
  PostgreSQL setup and declarative schema apply against each entry on boot.
- `database.passwordFile` is only needed when local auth is password-based or
  the DB is remote; otherwise loopback deployments can stay passwordless. When
  you do need a file, the module resolves `sinex-local-db` /
  `sinex-remote-db` and the conventional declarative files
  `/etc/sinex/db-password` / `/etc/sinex/remote-db-password` automatically.
- Shared preload libraries always include TimescaleDB support plus schema/vector extensions required by migrations.
- Pool sizing (`connectionPool.{maxConnections,minConnections,...}`) feeds both
  Postgres `max_connections` and the CLI flags passed to service binaries.
- Identifier policy is UUIDv7 in persistence; application code should keep typed
  `Id<T>` wrappers and convert only at storage boundaries.

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

### Core & nodes
- `core.ingestd` and `core.gateway` expose per-service resources, log levels,
  batch/limits knobs, extra CLI args, TCP listen address, and optional
  client-cert enforcement.
- node defaults (`nodes.defaults`) cover instances, batching, and
  resource limits. Individual nodes can override by setting their field to
  `null` (inherit) or a concrete value.
- When `users.target` is set, the module now derives sane workstation defaults
  for terminal history sources and, when the target UID is known at evaluation
  time, the desktop runtime directory. The sinnix bridge can make that access
  explicit with `BindReadOnlyPaths` for the target home and `/run/user/$UID`,
  while the terminal and desktop units still run a root `ExecStartPre` bridge
  that grants the `sinex` service account the ACLs it needs for shell-history
  files and live Wayland/Hyprland sockets.
- When the module is enabled, workstation-facing collectors (`filesystem`,
  `terminal`, `desktop`, `system`) default to singleton startup
  (`instances = 1`) so the first live host enable does not double-run capture
  nodes before coordination is intentionally introduced.
- `nodes.terminal.historySources` and `nodes.desktop.session.*` remain the
  typed override surfaces when the target user layout is non-standard or a
  deployment needs explicit socket/runtime wiring.
- Automata use named profiles defined under `nodes.automata.profiles`; set
  `profile = "light"|"standard"|"heavy"` to select batch and MemoryMax/CPUQuota.
- The module emits deterministic unit names (`sinex-filesystem-1`,
  `sinex-health-automaton`, etc.) and publishes them via
  `config.sinex._generatedUnits` for other subsystems (pre-flight,
  tests).

### Transport Security
- gateway TLS lives under `services.sinex.core.gateway.{tlsCertFile,tlsKeyFile,tlsClientCAFile,requireClientTLS,autoGenerateTls}`
- non-loopback gateway binds require mTLS and a configured `tlsClientCAFile`
- managed local NATS server TLS lives under `services.sinex.nats.tls.{enable,certFile,keyFile,caCertFile,verifyClients,verifyAndMap}`
- managed local NATS subject-level authz for the current shared runtime identity lives under `services.sinex.nats.authorization.sharedClient.*`
- shared NATS client transport lives under `services.sinex.nodes.nats.{servers,tls,auth}`
- NATS mTLS uses `services.sinex.nodes.nats.tls.{caCertFile,clientCertFile,clientKeyFile}`
- choose exactly one NATS auth mode under `services.sinex.nodes.nats.auth.{tokenFile,credsFile,nkeySeedFile}`
- JetStream bootstrap now reuses that same shared client auth/TLS material, so
  secured local NATS deployments do not need a separate bootstrap-only secret path.

### Secret Conventions
- gateway admin token falls back to `sinex-gateway-admin-token`, which can come
  from agenix or from declarative `environment.etc."sinex/gateway-admin-token"`
- database password surfaces fall back to `sinex-local-db` / `sinex-remote-db`
  and the conventional declarative files `/etc/sinex/db-password` /
  `/etc/sinex/remote-db-password`
- local NATS server TLS falls back to `sinex-nats-server-cert`,
  `sinex-nats-server-key`, and `sinex-nats-client-ca`
- shared NATS client TLS/auth falls back to `sinex-nats-ca`,
  `sinex-nats-client-cert`, `sinex-nats-client-key`,
  `sinex-nats-client-creds`, `sinex-nats-client-nkey`, and `sinex-nats-token`
- those NATS/TLS names can also be provided declaratively through
  `environment.etc` under `/etc/sinex/*.pem`, `/etc/sinex/*.creds`, and
  `/etc/sinex/*.nk` using the matching filenames documented in
  `nixos/modules/secrets-management.md`
- compatibility aliases are also accepted for the shared NATS client path: `nats-ca`, `nats-client-cert`, `nats-client-key`, `nats-client-creds`, `nats-client-nkey`, `nats-token`

### Environment Rendering
- the module is the canonical config surface; emitted env vars are an implementation detail of the generated units
- gateway TLS options render `SINEX_GATEWAY_TLS_CERT`, `SINEX_GATEWAY_TLS_KEY`, `SINEX_GATEWAY_TLS_CLIENT_CA`, and `SINEX_GATEWAY_REQUIRE_CLIENT_TLS`
- shared NATS options render `SINEX_NATS_URL`, `SINEX_NATS_MONITORING_PORT`, `SINEX_NATS_REQUIRE_TLS`, `SINEX_NATS_CA_CERT`, `SINEX_NATS_CLIENT_CERT`, `SINEX_NATS_CLIENT_KEY`, and one of `SINEX_NATS_{TOKEN,CREDS,NKEY_SEED}_FILE`
- `services.sinex.nodes.defaults.env` is reserved for genuinely env-only behavior flags, not primary transport or secret wiring

### Observability
- Structured log retention is configured via `observability.logging.retention`.
- Prometheus/exporters turn on automatically when
  `observability.monitoring.enable = true`.
- Grafana stays opt-in under `observability.monitoring.grafana.enable`; when you
  enable it, the module derives a stable local secret key automatically and will
  prefer `sinex-grafana-secret-key` / `grafana-secret-key` from agenix or
  declarative `environment.etc."sinex/grafana-secret-key"`, or an explicit
  `secretKeyFile` when provided.
- Extra scrape configs drop straight into `services.prometheus.extraScrapeConfigs`.
- Grafana provisions a fixed Prometheus datasource (`sinex-prometheus`), a fixed
  PostgreSQL datasource (`sinex-postgres`), and tracked dashboards from
  `nixos/monitoring/grafana-dashboards/`.
- The built-in dashboards intentionally use `sinex_telemetry.*`: continuous
  aggregates for operator telemetry and live event-time views for recent activity.
- Grafana binds to loopback by default; widen it explicitly if you truly need
  remote access and have matching firewall/TLS controls.
- `observability.alerts.enable` adds the provided rule files to Prometheus.

### Lifecycle
- The module now wires a first-boot `sinex-schema-apply` oneshot before guarded
  services and before `sinex-preflight`, so schema creation is part of the real
  deployment path instead of a VM-only convention.
- When `services.sinex.enable = true`, the module emits
  `/etc/sinex/deployment-readiness.json`, the canonical descriptor consumed by
  `xtask doctor --deployment-readiness` and the config-derived preflight
  configuration checks.
- That descriptor now carries the gateway probe base URL, whether the gateway
  requires client TLS, the effective NATS server list, and all secret-material
  paths needed for readiness checks, including the generated gateway TLS trust
  anchor when `core.gateway.autoGenerateTls = true`.
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
- `node-services.nix` – Core ingest/gateway and node/automata units.
- `kitty-shell-integration.nix` – Kitty auto-configuration helper.

## Testing Tips
- `just test` to run the Rust workspace (requires TimescaleDB extension).
- `nix run .#check` to validate the module evaluates with your configuration.
- VM scenarios under `tests/e2e/nixos-vm` consume the same option tree – updating
  defaults in the module automatically propagates to the test fixtures.

## Operational Notes
- Filesystem nodes keep `/home` read-only instead of hidden entirely so the
  default `watchPaths = [ "/home/<target>" ]` setup actually works under systemd
  hardening.
