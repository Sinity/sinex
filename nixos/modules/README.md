# Sinex NixOS Module

The Sinex NixOS module exposes a single entry point – `services.sinex` – that owns
all service wiring, directories, and lifecycle management for the platform. The
option tree mirrors the running system so that every knob you set maps directly
to a systemd unit, CLI argument, or generated configuration file.

Host wiring, systemd ordering, hardening, and secret inventory are documented in
[`deployment-topology.md`](deployment-topology.md).

## Key Namespaces

| Namespace | Purpose |
|-----------|---------|
| `services.sinex.stateRoot` | Root that all derived paths cascade from (logs, spool, blobs, DLQ). |
| `services.sinex.users` | `target` (captured workstation user) and `runtime modules` (service account). |
| `services.sinex.database` | PostgreSQL provisioning, connection pool sizing, and migrations. |
| `services.sinex.nats` | NATS/JetStream provisioning and stream bootstrap. |
| `services.sinex.storage` | Dead-letter queue handling and content-store backed blob storage. |
| `services.sinex.core` | `sinexd` daemon configuration (event engine, API, sources, automata, supervisor). |
| `services.sinex.runtime` | Filesystem/terminal/browser/desktop/system collectors plus automata. |
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
    adminPackage = pkgs.xtask;
    cliPackage = pkgs.sinexctl;
    users.target = "alice";

    stateRoot = "/var/lib/sinex";
    logLevel = "info";

    database.autoSetup = true;
    nats.autoSetup = true; # defaulted on when services.sinex.enable = true

    sources.filesystem.watchPaths = [ "/home/alice" "/workspace" ];
    automata.canonicalizer.profile = "heavy";

    observability.monitoring = {
      enable = true;
      grafana = {
        enable = true;
      };
    };
  };
}
```

`core.sinexd` is enabled by default, so the quick-start config needs a real
API admin token file before the generated unit will start. The module
auto-resolves either an agenix secret named `sinex-api-admin-token` or a
declarative `environment.etc."sinex/api-admin-token"` entry; set
`services.sinex.secrets.apiAdminTokenFile` only when you need a non-standard
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
  Postgres `max_connections` and the CLI flags passed to service binaries. The
  defaults are conservative for the single-host runtime: 4 max / 1 min
  connection per process, plus a small admin/preflight reserve.
- Identifier policy is UUIDv7 in persistence; application code should keep typed
  `Id<T>` wrappers and convert only at storage boundaries.

### Storage
- Raw-ingest DLQ retention is a JetStream concern; configure it through
  `nats.bootstrapStreams.retention`.
- Per-runtime recovery spool files live under the runtime work directories beneath
  `${stateRoot}/spool/runtime`; there is no separate centralized local DLQ path.
- Blob repository lives at `storage.blob.repositoryPath` (default:
  `${stateRoot}/blob-repository`). `autoInit = true` creates the content-store
  root on boot.
- `storage.blob.maintenance` controls legacy git-annex GC/fsck timers (active only
  when `legacyAnnexData = true`). Enable health checks to log repository size
  warnings.
- `storage.blob.cas.maintenance` controls the local BLAKE3 CAS timers (active only
  when `legacyAnnexData = false`). `sweep` runs `sinexctl blob sweep-orphans`
  (default weekly, with `--apply`); `fsck` runs `sinexctl blob fsck` (default
  monthly, dry-run — set `cas.maintenance.fsck.apply = true` to reclaim).
- `nats.bootstrapStreams.enable` bootstraps standard JetStream streams via the `nats`
  CLI (requires `pkgs.natscli`). Existing streams are reconciled with the
  declared `retention` / `maxAge` / `maxMsgs` / `maxBytes` policy on boot, so
  stream-shape changes land without imperative follow-up.
- Source-material streams default to work-queue retention. They are an ingest
  handoff into `event_engine`, not a long-lived archive; once the material assembler
  acknowledges them they should leave JetStream.

### Core & runtime modules
- `core.sinexd` exposes daemon-wide resources, log levels, batch/limits knobs,
  extra CLI args, TCP listen address for the API module, and optional
  client-cert enforcement. Module-scoped knobs live under
  `core.sinexd.event_engine`, `core.sinexd.api`, `core.sinexd.sources`, and
  `core.sinexd.automata`.
- runtime defaults (`runtime.defaults`) cover instances, batching, and
  resource limits. Individual runtime modules can override by setting their field to
  `null` (inherit) or a concrete value. See
  [`resource-scoping.md`](resource-scoping.md).
- `sources.document` is intentionally not a long-running source task. It renders a
  managed oneshot service (`sinex-document-scan.service`) plus an optional
  timer (`sinex-document-scan.timer`), and the module requires that at least
  one of `runOnBoot` or `schedule` is enabled so the surface actually runs.
- When `users.target` is set, the module now derives sane workstation defaults
  for terminal history sources, browser dump/sqlite sources, and, when the
  target UID is known at evaluation time, the desktop runtime directory. The
  sinnix bridge can make that access explicit with `BindReadOnlyPaths` for the
  target home and `/run/user/$UID`, while the terminal, browser, and desktop
  units still run a root `ExecStartPre` bridge that grants the `sinex` service
  account the ACLs it needs for shell-history files, browser SQLite histories,
  and live Wayland/Hyprland sockets.
- When the module is enabled, workstation-facing collectors (`filesystem`,
  `terminal`, `browser`, `desktop`, `system`) default to singleton startup
  (`instances = 1`) so the first live host enable does not double-run capture
  runtime modules before coordination is intentionally introduced.
- `sources.terminal.historySources`, `sources.browser.{dumpSources,sqliteSources}`,
  and `sources.desktop.session.*` remain the typed override surfaces when the
  target user layout is non-standard or a deployment needs explicit socket/runtime
  wiring.
- Desktop target-user access is a two-step bridge. First, the root
  `sinex-desktop-target-access` setup unit grants the `sinex` service account
  read/traverse ACLs for the target user's runtime directory, Hyprland socket
  tree, ActivityWatch SQLite parent, and target-home parents needed by enabled
  desktop sources. Second, it writes `${stateRoot}/run/desktop-target.env`, which
  is consumed as an optional `EnvironmentFile` by `desktop.activitywatch`,
  `desktop.window-manager`, and `desktop.clipboard`. The environment file carries
  the resolved `XDG_RUNTIME_DIR`, `WAYLAND_DISPLAY`,
  `SINEX_HYPRLAND_RUNTIME_DIR`, `SINEX_HYPRLAND_INSTANCE_SIGNATURE`, and explicit
  socket overrides when configured.
- Prefer the typed `sources.desktop.session.{runtimeDir,waylandDisplay,
  hyprlandInstanceSignature,hyprlandEventSocket,hyprlandCommandSocket}` options
  over ad hoc per-unit environment variables. `desktop.window-manager` resolves
  the Hyprland event socket from `SINEX_HYPRLAND_EVENT_SOCKET` first, then from
  `SINEX_HYPRLAND_RUNTIME_DIR`/`XDG_RUNTIME_DIR` plus
  `SINEX_HYPRLAND_INSTANCE_SIGNATURE`/`HYPRLAND_INSTANCE_SIGNATURE`.
  `desktop.clipboard` uses the same runtime/display bridge, while
  `desktop.activitywatch` uses `SINEX_ACTIVITYWATCH_DB_PATH` when the SQLite
  path is explicitly configured.
- Service resource defaults are intentionally workstation-civil: all long-running
  Sinex services default to low CPU/IO scheduler weight (`CPUWeight=10`,
  `IOWeight=10`), idle IO scheduling, and `Nice=10` in addition to their
  per-service `MemoryHigh`, `MemoryMax`, and `CPUQuota` settings.
- Automata use named profiles defined under `automata.profiles`; set
  `profile = "light"|"standard"|"heavy"` to select batch and resource limits.
- The module emits `sinexd` as the only long-running Sinex runtime unit.
  Support oneshots/timers such as preflight, blob init, document scan, and
  target-user access bridges are verified separately and are not source or
  automaton hosts.

### Transport Security
- API TLS lives under `services.sinex.core.api.{tlsCertFile,tlsKeyFile,tlsClientCAFile,requireClientTLS,autoGenerateTls}`
- non-loopback API binds require mTLS and a configured `tlsClientCAFile`
- managed local NATS server TLS lives under `services.sinex.nats.tls.{enable,certFile,keyFile,caCertFile,verifyClients,verifyAndMap}`
- managed local NATS subject-level authz for the current shared runtime identity lives under `services.sinex.nats.authorization.sharedClient.*`
- shared NATS client transport lives under `services.sinex.runtime.nats.{servers,tls,auth}`
- NATS mTLS uses `services.sinex.runtime.nats.tls.{caCertFile,clientCertFile,clientKeyFile}`
- choose exactly one NATS auth mode under `services.sinex.runtime.nats.auth.{tokenFile,credsFile,nkeySeedFile}`
- JetStream bootstrap now reuses that same shared client auth/TLS material, so
  secured local NATS deployments do not need a separate bootstrap-only secret path.

### Secret Conventions
- API admin token falls back to `sinex-api-admin-token`, which can come
  from agenix or from declarative `environment.etc."sinex/api-admin-token"`
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
- API module TLS options render `SINEX_API_TLS_CERT`, `SINEX_API_TLS_KEY`, `SINEX_API_TLS_CLIENT_CA`, and `SINEX_API_REQUIRE_CLIENT_TLS`
- shared NATS options render `SINEX_NATS_URL`, `SINEX_NATS_MONITORING_PORT`, `SINEX_NATS_REQUIRE_TLS`, `SINEX_NATS_CA_CERT`, `SINEX_NATS_CLIENT_CERT`, `SINEX_NATS_CLIENT_KEY`, and one of `SINEX_NATS_{TOKEN,CREDS,NKEY_SEED}_FILE`
- `services.sinex.runtime.defaults.env` is reserved for genuinely env-only behavior flags, not primary transport or secret wiring

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
- The built-in dashboards intentionally use `sinex_telemetry.*`: hourly operator
  views for ingest/runtime telemetry and live event-time views for recent activity.
- Grafana binds to loopback by default; widen it explicitly if you truly need
  remote access and have matching firewall/TLS controls.
- `observability.alerts.enable` adds the provided rule files to Prometheus.

### Runtime gating and deferred start

By default, `services.sinex.runtimeSystem.target.attachToMultiUser = true` makes
every Sinex unit start at boot via `multi-user.target`. The aggregate
`sinex-runtime.target` exists as a stop boundary: `systemctl stop
sinex-runtime.target` brings the whole runtime down without per-unit
orchestration.

To defer the runtime past boot (workstation pattern: let the desktop settle,
then start capture), set:

```nix
services.sinex = {
  runtimeSystem.target.attachToMultiUser = false;
  runtimeSystem.target.includeDatabase = true;       # pull postgresql into the gate
  runtimeSystem.target.extraAfter = [ "network-online.target" ];
  runtime.deferredStart = {
    enable = true;
    delay = "5min";
  };
};
```

The module then:

- strips `wantedBy = [ "multi-user.target" ]` from every Sinex-owned
  long-running service, every bootstrap one-shot (`sinex-tls-init`,
  `sinex-blob-init`, `sinex-nats-bootstrap`,
  `sinex-kitty-setup`, `sinex-preflight`, `sinex-document-scan`, the managed
  `nats.service`), plus generated automaton/support units;
- when `includeDatabase = true`, also strips it from `postgresql.service`,
  `postgresql-setup.service`, and `postgresql.target`;
- populates `sinex-runtimeSystem.target.wants` with the union, so pulling the target
  brings the full runtime online;
- emits `sinex-runtime.timer` with `OnActiveSec = delay` when
  `deferredStart.enable = true`.

The `sinex-runtimeSystem.target.extraAfter` list is appended to `After=` for hosts
that need ordering against units the module itself cannot reference (e.g.
`network-online.target`).

When the database password file is materialized late at boot (agenix,
sops-nix), use `services.sinex.database.setupWaitForPaths` to gate
`postgresql-setup.service` on its readability via `ConditionPathIsReadable=`.

### Lifecycle
- The module separates runtime, admin, and operator package surfaces:
  `services.sinex.package` supplies service binaries, `adminPackage` supplies
  managed deployment helpers such as `xtask`, and `cliPackage` is the human
  operator CLI placed on PATH. Do not use the aggregate runtime package as a
  global CLI surface unless you intentionally want every packaged binary on
  interactive PATH.
- The module enables `SINEX_SCHEMA_APPLY_ON_STARTUP=1` for managed database
  deployments, so `sinexd` applies schema before starting runtime modules.
- When `services.sinex.enable = true`, the module emits
  `/etc/sinex/deployment-readiness.json`, the canonical descriptor consumed by
  `xtask doctor --deployment-readiness` and the config-derived preflight
  configuration checks.
- That descriptor also records the managed document-ingestion surface
  (`allowed_roots`, boot/timer execution mode, and scan/timer unit names) so
  readiness checks can verify that non-daemon runtime surfaces are genuinely
  configured and active.
- That descriptor now carries the API probe base URL, whether the API endpoint
  requires client TLS, the effective NATS server list, and all secret-material
  paths needed for readiness checks, including the generated API TLS trust
  anchor when `core.api.autoGenerateTls = true`.
- The module also emits `/etc/sinex/runtime-target.json`. This narrower
  descriptor is the runtime connection/status target for `sinexctl` and other
  status probes: API URL, auth/TLS material, database URL, NATS servers,
  state directories, managed service units, target kind, and descriptor source.
  When `sinexd::api` stages a role-suffixed admin token from a raw secret,
  the descriptor records `gateway.token_role = "admin"` so clients can derive
  the same bearer token from the readable raw secret file.
- Pre-flight verification lives under `lifecycle.preflight`. Disable individual
  phases with `lifecycle.preflight.skip = [ "migrations" "services" ];`.
- Coordinated updates use `lifecycle.updates` for grace periods and roll-back
  policy. The generated `sinex-update` service restarts guarded units in-place.
- Maintenance toggles (`lifecycle.maintenance.tasks`) control blob GC/fsck
  integration.

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
- `blob-storage.nix` – content-store backend initialization and maintenance timers.
- `monitoring.nix` – Prometheus/Grafana/exporter configuration.
- `preflight-verification.nix` – `sinex-preflight` and `sinex-update` units.
- `sources.nix` – `sinexd` service unit, source-binding, and automata wiring.
- `kitty-shell-integration.nix` – Kitty auto-configuration helper.

## Testing Tips
- `just test` to run the Rust workspace (requires TimescaleDB extension).
- `nix run .#check` to validate the module evaluates with your configuration.
- VM scenarios under `tests/e2e/nixos-vm` consume the same option tree – updating
  defaults in the module automatically propagates to the test fixtures.

## Operational Notes
- Filesystem runtime modules keep `/home` read-only instead of hidden entirely so the
  default `watchPaths = [ "/home/<target>" ]` setup actually works under systemd
  hardening.
