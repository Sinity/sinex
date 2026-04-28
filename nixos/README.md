# Sinex NixOS Deployment Guide

Complete deployment and operations guide for the Sinex Exocortex personal data capture system.

## Documentation Structure

- **examples/workstation.nix** - Minimal workstation deployment (filesystem + terminal + browser nodes)
- **examples/monitoring.nix** - Staging configuration with maintenance + observability stack
- **examples/dev-sandbox.nix** - Comprehensive developer sandbox with all services enabled
- **examples/headless.nix** - Headless/server capture (filesystem + system nodes)
- **examples/remote-node.nix** - Edge node forwarding events to remote ingest
- **examples/coordination.nix** - Hot standby deployment with coordination enabled
- **modules/** - Implementation modules:
  - `default.nix` - Main module entry point and base options
  - `database.nix` - PostgreSQL provisioning, pooling, and health monitoring
  - `node-services.nix` - Ingestor and automaton service configurations
  - `monitoring.nix` - Prometheus/Grafana monitoring setup
  - `preflight-verification.nix` - Pre-deployment validation checks
  - `nats.nix` - NATS JetStream configuration
  - `secrets.nix` - Agenix secrets integration

## Architectural Documentation

Key architectural decisions and implementation details are documented at their implementation points:

### Database Layer
- **PostgreSQL Extensions Setup**: [`modules/database.nix`](modules/database.nix)
  - UUIDv7-native schema provisioning
  - TimescaleDB setup for hypertable partitioning  
  - Guidance for WAL/UUIDv7 write-path tuning
- **TimescaleDB Hypertable Creation**: [`crate/lib/sinex-schema/src/schema/events.rs`](../crate/lib/sinex-schema/src/schema/events.rs)
  - Chunk interval optimization guidelines
  - Compression strategy documentation
- **Identifier model (UUIDv7 + typed wrappers)**: [`crate/lib/sinex-primitives/docs/type_safe_units_and_identifiers.md`](../crate/lib/sinex-primitives/docs/type_safe_units_and_identifiers.md)
  - Persisted identifiers are UUIDv7
  - Rust code keeps compile-time safety with typed `Id<T>`

### Event Processing
- **Ingestion & JetStream Overview**: [`README.md#architecture`](../README.md#architecture)
  - Provenance and Stage-as-you-go responsibilities: [`crate/lib/sinex-node-sdk/docs/provenance.md`](../crate/lib/sinex-node-sdk/docs/provenance.md)
  - Stream bootstrap defaults + environment namespacing: [`modules/nats.nix`](modules/nats.nix)
- **Node SDK Patterns**: [`crate/lib/sinex-node-sdk/docs/overview.md`](../crate/lib/sinex-node-sdk/docs/overview.md)
  - Unified node interface and checkpoint semantics
  - Replay patterns and lifecycle hooks
- **Node stream runtime**: [`sinex-node-sdk/src/runtime/stream/mod.rs`](../crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs)
  - Snapshot, historical, and continuous modes

### Node Implementations
- **Filesystem Monitoring**: [`sinex-fs-ingestor/src/unified_processor.rs:1-62`](../crate/nodes/sinex-fs-ingestor/src/unified_processor.rs#L1-L62)
  - inotify (Linux) implementation details
  - FSEvents (macOS) configuration
  - System limits and overflow handling

## Quick Start

### Minimal Deployment

Add to your NixOS configuration:

```nix
{
  imports = [ ./path/to/sinex/nixos/modules ];

  users.users.yourusername = {
    isNormalUser = true;
    createHome = true;
    extraGroups = [ "wheel" ]; # optional
  };

  services.sinex = {
    enable = true;
    users.target = "yourusername";  # REQUIRED: match the user defined above
  };

  # Gateway admin token MUST come from a runtime-only secret, not a plain text= value.
  # Using environment.etc."...".text = "..." bakes the token into the world-readable
  # Nix store — do NOT do that for real tokens.
  #
  # Recommended: use agenix (token is auto-resolved from sinex-gateway-admin-token.age):
  #   age.secrets.sinex-gateway-admin-token.file = ./secrets/sinex-gateway-admin-token.age;
  #
  # Alternative: point directly at a runtime secret file:
  #   services.sinex.secrets.gatewayAdminTokenFile = "/run/secrets/sinex-gateway-admin-token";
  #
  # The module asserts that one of the above is present and refuses to start without it.
}
```

The module auto-resolves the token from agenix (`sinex-gateway-admin-token`) or from
`services.sinex.secrets.gatewayAdminTokenFile`. It will refuse to start if neither is
configured, preventing accidental no-auth deployments.

Apply with:
```bash
sudo nixos-rebuild switch --flake .#your-host
```

> **REQUIRED**: You MUST apply the sinex flake overlay to your pkgs. The overlay provides:
> - `pkgs.sinex` (all binaries bundled)
> - `pkgs.sinexctl` (CLI tool)
> - `pkgs.sinex-ingestd`, `pkgs.sinex-gateway`, etc. (individual packages)
> - `pkgs.postgresql18Packages.pg_jsonschema` (required PostgreSQL extension)
>
> ```nix
> {
>   inputs.sinex.url = "github:.../sinex";
>
>   outputs = { self, nixpkgs, sinex, ... }: {
>     nixosConfigurations.myhost = nixpkgs.lib.nixosSystem {
>       modules = [
>         # Apply the overlay - REQUIRED
>         ({ ... }: { nixpkgs.overlays = [ sinex.overlays.default ]; })
>         # Plain module export
>         sinex.nixosModules.default
>         ./configuration.nix
>       ];
>     };
>   };
> }
> ```
>
> If you want the convenience wrapper that also imports agenix, use:
> ```nix
> sinex.nixosModules."with-agenix"
> ```
>
> Alternatively, provide packages explicitly without the overlay:
> ```nix
> services.sinex.package = inputs.sinex.packages.${pkgs.stdenv.hostPlatform.system}.sinex;
> ```

### Service Bundle Controls

The upstream module exposes real feature toggles directly:

```nix
services.sinex = {
  core.enable = true;                    # ingestd + gateway
  nodes.enable = true;                   # node units
  lifecycle.maintenance.enable = false;  # DLQ/blob maintenance timers
  observability.enable = false;          # journald/logging integration
  observability.monitoring.enable = false; # Prometheus/Grafana/exporters
};

# Typical development overrides
services.sinex.nodes = {
  enable = true;
  coordination.enable = false;
  filesystem = {
    enable = true;
    instances = 1;
  };
};
```

If you want higher-level activation profiles such as `foundation` / `capture` / `full`, use the
`sinnix` wrapper module. The upstream `services.sinex` module keeps those switches explicit.

### Satellite Secrets & TLS

When deploying nodes across hosts, use the typed NATS TLS options for the shared transport path.
Keep `defaults.env` for actual application behavior flags such as `SINEX_EDGE_MODE`, not for
core transport wiring.

```nix
services.sinex.nodes = {
  nats = {
    servers = [ "tls://core.example.net:4222" ];
    tls = {
      requireTls = true;
      caCertFile = config.age.secrets.nats-ca.path;
      clientCertFile = config.age.secrets.nats-client-cert.path;
      clientKeyFile = config.age.secrets.nats-client-key.path;
    };
    auth.credsFile = config.age.secrets.nats-client-creds.path;
  };

  defaults.env = {
    SINEX_EDGE_MODE = "1";
  };
};
```

The NixOS module exports the corresponding `SINEX_NATS_*` variables to all core services and node
units automatically. Use generic environment injection only for options that do not already have a
typed module surface.

### Shell Helpers

Workstation conveniences live under `services.sinex.shell`:

```nix
services.sinex.shell = {
  asciinema = {
    autoRecord = true;
    recordingsPath = "~/.local/share/asciinema";
  };

  kitty = {
    enable = true;
    autoConfigure = true; # manage kitty.conf automatically
  };
};
```

Disabling `kitty.autoConfigure` keeps the helper scripts available without touching your existing Kitty configuration.

For real secrets, prefer agenix over `environment.etc.*.text`; the inline
`environment.etc` examples here are just the smallest declarative shape that
exercises the module conventions.

### User-Session Node Wiring

Terminal, browser, and desktop capture now default to the interactive target user more
honestly. When `services.sinex.users.target` is set, the module:

- derives terminal history sources from that user's home directory
- derives browser dump/sqlite history sources from that user's home directory
- derives the desktop runtime dir when the target UID is known at evaluation time
- runs a root `ExecStartPre` bridge that grants the `sinex` service account ACL
  access to shell-history files, browser SQLite history, and live Wayland/Hyprland sockets

That means a typical workstation usually only needs the target user:

```nix
services.sinex = {
  enable = true;
  users.target = "myuser";
};
```

Override the typed session surfaces only when the layout is non-standard or you
need to pin a specific runtime/socket mapping:

```nix
services.sinex.nodes = {
  terminal = {
    historySources = [
      {
        path = "/srv/history/zsh_history";
        shell = "zsh";
      }
    ];
  };

  browser = {
    sqliteSources = [
      {
        path = "/srv/history/qutebrowser/history.sqlite";
        browser = "qutebrowser";
        format = "QutebrowserNative";
      }
    ];
  };

  desktop = {
    session = {
      runtimeDir = "/run/user/1001";
      waylandDisplay = "wayland-1";
      hyprlandInstanceSignature = "abc123def456";
    };
  };
};
```

`nodes.terminal.access.bindReadOnlyPaths`,
`nodes.browser.access.bindReadOnlyPaths`, and
`nodes.desktop.access.bindReadOnlyPaths` remain available as escape hatches, but
they are no longer the primary workstation path.

### Production Setup with Hot Standby

For production deployments with zero-downtime upgrades and automatic failover:

```bash
cp nixos/examples/coordination.nix /etc/nixos/sinex.nix
# Edit users.target and coordination settings
sudo nixos-rebuild switch
```

This enables:
- **Multiple instances** of each node service (hot standby pattern)
- **Zero-downtime upgrades** via version-based leadership election
- **Automatic failover** when leader instances fail
- **Coordination monitoring** with health checks and metrics

### Development/Testing Setup

For simpler single-instance deployment:

```bash
cp nixos/examples/workstation.nix /etc/nixos/sinex.nix
# Edit users.target and other settings
sudo nixos-rebuild switch
```

### Evaluating Examples

Each example is exported through the flake. To explore them safely:

```bash
# Boot the minimal example in a disposable VM
nix build .#nixosConfigurations.workstation.config.system.build.vm
./result/bin/run-nixos-vm

# Temporarily apply the developer sandbox on a host (rolls back on reboot)
sudo nixos-rebuild test --flake .#devSandbox
```

Switch permanently only after merging the example into your host configuration.
> **Note**: The remote node example expects an existing remote NATS endpoint (feeding a central
> ingestd/gateway deployment) and explicitly disables local PostgreSQL/NATS provisioning.

Grafana, when enabled, now provisions:
- a fixed Prometheus datasource (`sinex-prometheus`)
- a fixed PostgreSQL datasource (`sinex-postgres`) pointed at the Sinex database
- tracked dashboards from `nixos/monitoring/grafana-dashboards/`

Those dashboards are built around the current `sinex_telemetry.*` surfaces: hourly operator
views for ingest/runtime telemetry and live event-time views for recent activity.

## Architecture Overview

Sinex uses a node architecture:

```
External Data → Nodes → NATS JetStream → sinex-ingestd → PostgreSQL (`core.events`)
                                    ↓
                      confirmations/DLQ → Automata → Gateway/CLI
```

Current implementation:
- Collector nodes publish provisional events and source material slices directly to JetStream (`events.raw.*`, `source_material.*`).
- ingestd consumes from JetStream, validates, persists to PostgreSQL (TimescaleDB), then publishes confirmations (`events.confirmations.*`) and DLQ entries back to JetStream.
- Automata consume confirmations via durable JetStream consumers; Gateway/CLI query PostgreSQL via JSON-RPC or direct DB mode.

**Core Components:**
- **ingestd**: JetStream consumer + validator + single-writer persistence + confirmations/DLQ publisher
- **Gateway**: HTTP/JSON-RPC API for CLI and web access
- **Nodes**: Independent services for data capture and processing
- **PostgreSQL**: Event storage with TimescaleDB for time-series data
- **NATS JetStream**: Message bus for real-time event distribution

## Deployment Scenarios

### 1. Personal Laptop/Desktop (Recommended)

Full-featured setup capturing all digital activity:

```nix
services.sinex = {
  enable = true;
  users.target = "myuser";
  
  nodes = {
    enable = true;
    filesystem = {
      enable = true;
      watchPaths = [ "~/Documents" "~/Projects" ];
    };
    terminal.enable = true;
    browser.enable = true;
    desktop.enable = true;
    system.enable = true;
    automata = {
      canonicalizer.enable = true;     # Command processing
      healthAggregator.enable = true;  # Health monitoring
      analyticsAutomaton.enable = true;  # Sliding-window analytics summaries
      sessionDetector.enable = true;     # Cross-source session boundaries
    };
  };

  shell = {
    asciinema.autoRecord = false;
    kitty.enable = true;
  };

  database.autoSetup = true;
};
```

> **Multiple databases:** use `database.extraDatabases` when you want the module
> to create and prep additional DBs (for example `sinex_dev`) alongside the
> primary `database.name`. Extensions such as TimescaleDB are installed in each
> database listed, so schema/bootstrap tooling works consistently no matter which schema you
> target.

### 2. Server/Headless (Data Collection Only)

Minimal setup for server environments:

```nix
services.sinex = {
  enable = true;
  users.target = "serveruser";
  
  nodes = {
    enable = true;
    filesystem = {
      enable = true;
      watchPaths = [ "/srv/data" "/var/log" ];
    };
    terminal.enable = false;
    browser.enable = false;
    desktop.enable = false;      # No GUI
    system.enable = true;
    automata.healthAggregator.enable = true;
    automata.analyticsAutomaton.enable = true;
    automata.sessionDetector.enable = true;
  };
  
  database.autoSetup = true;
};
```

### 3. Development Environment

Development setup with debugging enabled:

```nix
services.sinex = {
  enable = true;
  users.target = "developer";
  logLevel = "debug";              # Verbose logging
  
  nodes = {
    enable = true;
    defaults.logLevel = "debug";
    filesystem = {
      enable = true;
      watchPaths = [ "~/Projects" ];  # Only watch projects
    };
    terminal.enable = true;
    browser.enable = true;
  };
  
  shell = {
    asciinema = {
      autoRecord = true;
      recordingsPath = "~/Projects/.sinex-recordings";
    };
    kitty.enable = true;
  };
  
  database = {
    autoSetup = true;
    name = "sinex";
    extraDatabases = [ "sinex_dev" ];
  };
};
```

### 4. Testing/CI Environment

Minimal setup for automated testing:

```nix
services.sinex = {
  enable = true;
  users.target = "testuser";
  
  nodes = {
    enable = true;
    filesystem.enable = false;
    terminal.enable = false;
    desktop.enable = false;
    system.enable = false;
  };
  
  shell.asciinema.autoRecord = false;
  
  database = {
    autoSetup = true;
    name = "sinex_test";
  };
  
  # Disable persistent storage
  storage.blob.enable = false;
};
```

## Operations Guide

### Service Management

**Check service status:**
```bash
systemctl status sinex-ingestd
systemctl status sinex-gateway
systemctl status sinex-filesystem-1
systemctl status sinex-terminal-1
systemctl status sinex-browser-1
```

**View logs:**
```bash
journalctl -u sinex-ingestd -f
journalctl -u sinex-gateway -f
journalctl -u sinex-filesystem-1 -f
```

**Restart services:**
```bash
sudo systemctl restart sinex-ingestd
sudo systemctl restart sinex-filesystem-1
```

**Stop all Sinex services:**
```bash
sudo systemctl stop 'sinex-*'
```

**Start all Sinex services:**
```bash
sudo systemctl start sinex-ingestd
sudo systemctl start sinex-gateway
sudo systemctl start sinex-filesystem-1
sudo systemctl start sinex-terminal-1
sudo systemctl start sinex-browser-1
sudo systemctl start sinex-desktop-1
sudo systemctl start sinex-system-1
```

### Coordination System Operations

**View coordination status (hot standby deployments):**
```bash
# Check which instances are running
systemctl status 'sinex-*-*'

# View leadership status (JetStream KV)
nats kv get KV_sinex_leadership <service>

# List healthy instances (KV_sinex_instances uses `<service>.<instance>` keys)
nats kv history KV_sinex_instances --subject '<service>.*'
```

**Monitor coordination events:**
```bash
# Watch coordination activity in logs
journalctl -f | grep -E "(leadership|handoff|coordination)"

# View recent coordination signals
sudo -u sinex psql sinex_prod -c "
SELECT target_instance, signal_type, message, created_at
FROM core.node_signals 
WHERE created_at > NOW() - INTERVAL '1 hour'
ORDER BY created_at DESC;
"
```

**Force leadership election (emergency):**
```bash
# Release current leadership to trigger election
sudo -u sinex psql sinex_prod -c "
DELETE FROM core.service_leadership WHERE service_name = 'sinex-fs-ingestor';
"
# Healthy standby instances will immediately compete for leadership
```

**Zero-downtime upgrade process:**
```bash
# 1. Update configuration with new version
# 2. Apply configuration (new instances start in standby)
sudo nixos-rebuild switch

# 3. Verify new instances are healthy standbys
systemctl status 'sinex-*'

# 4. New instances automatically challenge current leaders
# 5. Graceful handoff occurs automatically
# 6. Monitor transition in logs
journalctl -f | grep -E "(handoff|leadership)"

# 7. Old instances are automatically stopped by systemd
```

### Database Operations

**Access database directly:**
```bash
# Development database
sudo -u sinex psql sinex_dev

# Production database  
sudo -u sinex psql sinex
```

**Common database queries:**
```sql
-- Recent events
SELECT ts_orig, source, event_type, payload 
FROM core.events 
ORDER BY ts_orig DESC 
LIMIT 10;

-- Event counts by source
SELECT source, COUNT(*) as event_count
FROM core.events 
WHERE ts_orig > NOW() - INTERVAL '1 hour'
GROUP BY source 
ORDER BY event_count DESC;

-- Database size
SELECT pg_size_pretty(pg_database_size('sinex_dev'));
```

**Run database migrations:**
```bash
cd /path/to/sinex
nix develop
just migrate
```

### JetStream Operations

The NixOS module enables JetStream on the bundled `nats-server`. Use the `nats` CLI for inspection:

**List streams and consumers:**
```bash
nats --server nats://127.0.0.1:4222 stream ls
nats --server nats://127.0.0.1:4222 consumer ls <stream>
```

**Inspect a stream:**
```bash
nats --server nats://127.0.0.1:4222 stream info <stream>
```

**Tail messages (debugging):**
```bash
nats --server nats://127.0.0.1:4222 consumer next <stream> <consumer>
```

**Remove a stream (DESTRUCTIVE):**
```bash
nats --server nats://127.0.0.1:4222 stream rm <stream> --force
```

Stream names depend on the deployment. Consult `modules/nats.nix` or the node configuration when deciding which streams to inspect or delete.

### Data Management

**Wipe all Sinex data (DESTRUCTIVE):**
```bash
# Stop services
sudo systemctl stop 'sinex-*'

# Drop database
sudo -u postgres dropdb sinex_dev
sudo -u postgres createdb sinex_dev
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE sinex_dev TO sinex;"

# Reset JetStream state (optional, destructive)
sudo systemctl stop nats
sudo rm -rf /var/lib/nats/jetstream/*
sudo systemctl start nats

# Clear filesystem data
sudo rm -rf /var/lib/sinex/*
sudo rm -rf /var/log/sinex/*

# Restart services
sudo systemctl start sinex-ingestd
sudo systemctl start sinex-gateway
# ... other services
```

**Export data:**
```bash
# Database dump
sudo -u sinex pg_dump sinex_dev > sinex_backup.sql

# Event data as JSON
sudo -u sinex psql sinex_dev -c "
COPY (SELECT row_to_json(events) FROM core.events events ORDER BY ts_orig) 
TO STDOUT" > events_export.jsonl
```

**Import data:**
```bash
# Restore database
sudo -u sinex psql sinex_dev < sinex_backup.sql
```

### Health Checks

**Manual health verification:**
```bash
# Check database connectivity
sudo -u sinex psql sinex_dev -c "SELECT 1;"

# Check JetStream status
nats --server nats://127.0.0.1:4222 server report jetstream

# Check the gateway readiness surface
curl -k https://127.0.0.1:9999/ready

# Inspect the managed unit contract
systemctl show sinex-ingestd --property=Type,NotifyAccess,WatchdogUSec

# Run full preflight check
sudo -u sinex /run/current-system/sw/bin/sinex-preflight verify

# Prove passive derived/runtime signals plus managed document scan, enabled collector surfaces,
# and implemented historical backfill surfaces
sinexctl --insecure verify --document-smoke --source-proof --historical-proof
```

**Service health endpoints:**
```bash
# Gateway health
curl -k https://127.0.0.1:9999/health
curl -k https://127.0.0.1:9999/ready

# Check service startup
journalctl -u sinex-ingestd --since "5 minutes ago"
```

## Configuration Reference

### Directory Structure

Default directories (customizable):
- `/var/lib/sinex/` - State data and checkpoints
- `/var/log/sinex/` - Service logs
- `/run/sinex/` - Runtime sockets and PIDs
- `/etc/sinex/` - Configuration files

### Environment Variables

Useful runtime variables for debugging:
```bash
export RUST_LOG=debug                    # Enable debug logging
export SINEX_ENVIRONMENT=dev             # Subject / stream namespace
export SINEX_STATE_DIR=/var/lib/sinex    # Module state root
export DATABASE_URL=postgresql:///sinex_dev  # DB connection when debugging locally
```

The NixOS module owns the steady-state runtime environment; use these variables
for ad-hoc debugging, not as the primary configuration surface.

### Resource Limits

Default resource limits per service:
- **ingestd**: 1GB memory, 100% CPU
- **gateway**: 512MB memory, 50% CPU  
- **nodes**: 256MB memory, 50% CPU each

Adjust in configuration:
```nix
services.sinex.core.ingestd.resources = {
  memoryMax = "2G";
  cpuQuota = "200%";
};

services.sinex.nodes.defaults.resources = {
  memoryMax = "384M";
  cpuQuota = "75%";
};
```

## Troubleshooting

### Common Issues

**Services won't start:**
```bash
# Check for port conflicts
sudo netstat -tulpn | grep -E ':(9999|5432|4222)'

# Verify database is running
systemctl status postgresql
sudo -u postgres psql -c "SELECT 1;"

# Check disk space
df -h /var/lib/sinex
```

**Events not being captured:**
```bash
# Check node status
systemctl status sinex-filesystem-1
journalctl -u sinex-filesystem-1 -f

# Verify gateway readiness
curl -k https://127.0.0.1:9999/ready
journalctl -u sinex-ingestd --since "10 minutes ago"

# Check database connectivity
sudo -u sinex psql sinex_dev -c "SELECT COUNT(*) FROM core.events;"
```

**High resource usage:**
```bash
# Check service memory usage
systemctl status sinex-*
ps aux | grep sinex

# Monitor database size
sudo -u sinex psql sinex_dev -c "
SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))
FROM pg_tables WHERE schemaname IN ('core', 'raw')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;"
```

**Database performance issues:**
```bash
# Check slow queries
sudo -u sinex psql sinex_dev -c "
SELECT query, calls, total_time, mean_time
FROM pg_stat_statements 
ORDER BY total_time DESC 
LIMIT 10;"

# Analyze table statistics
sudo -u sinex psql sinex_dev -c "ANALYZE;"
```

### Log Analysis

**Error patterns to look for:**
```bash
# Connection issues
journalctl -u sinex-* | grep -i "connection refused\|timeout\|failed to connect"

# Database errors
journalctl -u sinex-* | grep -i "database\|postgres\|sql"

# gRPC errors
journalctl -u sinex-* | grep -i "grpc\|socket\|transport"

# Memory issues
journalctl -u sinex-* | grep -i "memory\|oom\|killed"
```

### Recovery Procedures

**Service recovery:**
```bash
# Restart single service
sudo systemctl restart sinex-ingestd

# Restart all services in order
sudo systemctl stop 'sinex-*'
sudo systemctl start sinex-ingestd
sleep 2
sudo systemctl start sinex-gateway
sudo systemctl start sinex-filesystem-1
sudo systemctl start sinex-terminal-1
sudo systemctl start sinex-desktop-1
sudo systemctl start sinex-system-1
```

**Database recovery:**
```bash
# Reset checkpoints on corruption
nats kv purge sinex_checkpoints --force

# Rebuild indexes
sudo -u sinex psql sinex_dev -c "REINDEX DATABASE sinex_dev;"
```

## Development & Testing

### Development Setup

For Sinex development:
```bash
cd /realm/project/sinex
direnv allow      # first time only
xtask check       # Quick development cycle
xtask test        # Main local test loop
```

### VM Testing

Run the exported VM compatibility checks:
```bash
xtask test vm --category smoke
xtask test vm --category integration
```

### Integration with Other Systems

**Prometheus monitoring:**
```nix
services.sinex.observability.monitoring = {
  enable = true;
  prometheus = {
    listen = "127.0.0.1";
    port = 9090;
  };
};
```

**Grafana dashboards:**
```nix
services.grafana = {
  enable = true;
  provision.dashboards.settings.providers = [{
    name = "sinex";
    options.path = ./nixos/monitoring/grafana-dashboards;
  }];
};
```

When enabling Grafana through the Sinex module, the module derives a stable
local secret key automatically. Override it with `sinex-grafana-secret-key` /
`grafana-secret-key` from agenix or declarative
`environment.etc."sinex/grafana-secret-key"`, or with
`services.sinex.observability.monitoring.grafana.secretKey{,File}`, only when
you need operator-managed material instead of the declarative default.

## TimescaleDB Operational Guidelines

### Chunk Interval Sizing

TimescaleDB partitions data into chunks for efficient time-series storage. Optimal chunk sizing is critical for performance.

**Default Configuration**: 1 day chunks

**Sizing Guidelines**:
- **Target**: Each chunk should be 10-25% of PostgreSQL RAM allocation
- **High volume** (>20GB/day): Use 6-12 hour chunks
- **Medium volume** (1-20GB/day): Use 1 day chunks (default)
- **Low volume** (<1GB/day): Use 7 day chunks

**Monitor chunk sizes**:
```sql
-- View chunk information
SELECT 
    chunk_name,
    table_bytes,
    index_bytes,
    total_bytes,
    pg_size_pretty(total_bytes) as total_size
FROM timescaledb_information.chunks
WHERE hypertable_name = 'events'
ORDER BY range_start DESC
LIMIT 10;

-- Adjust chunk interval if needed
SELECT set_chunk_time_interval('core.events', INTERVAL '12 hours');
```

### Compression Policy

TimescaleDB compression can achieve 90-95% storage reduction on time-series data.

**Enable compression**:
```sql
-- Configure compression settings
ALTER TABLE core.events SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'ts_coided DESC, id',
    timescaledb.compress_segmentby = 'source, event_type'
);

-- Add automatic compression for chunks older than 7 days
SELECT add_compression_policy('core.events', INTERVAL '7 days');
```

**Compression considerations**:
- JSONB payloads compress less effectively than structured columns
- Extract frequently queried fields to dedicated columns for better compression
- Query performance on compressed data has decompression overhead
- Use `compress_segmentby` columns that match your common WHERE clauses

### Continuous Aggregates

For frequently-run analytical queries, use continuous aggregates:

```sql
-- Example: Hourly event counts by source
CREATE MATERIALIZED VIEW event_counts_hourly
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 hour', ts_coided) AS hour,
    source,
    event_type,
    COUNT(*) as event_count
FROM core.events
GROUP BY hour, source, event_type;

-- Add refresh policy
SELECT add_continuous_aggregate_policy(
    'event_counts_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes'
);
```

### Retention Policies

Automatically drop old data:

```sql
-- Drop chunks older than 1 year
SELECT add_retention_policy('core.events', INTERVAL '1 year');

-- For infinite retention (default), don't add a policy
```

## Development Practices

### Creating New Service Modules

When adding a new Sinex service, follow these patterns:

```nix
# nixos/modules/services/my-new-service.nix
{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex.myService;
  sinexCfg = config.services.sinex;
in
{
  options.services.sinex.myService = {
    enable = mkEnableOption "Sinex My Service";
    
    port = mkOption {
      type = types.port;
      default = 2120;
      description = "Port for the service";
    };

    featureFlag = mkOption {
      type = types.bool;
      default = false;
      description = "Example typed service option.";
    };
  };
  
  config = mkIf (sinexCfg.enable && cfg.enable) {
    systemd.services.sinex-my-service = {
      description = "Sinex My Service";
      wantedBy = [ "multi-user.target" ];
      after = [ "postgresql.service" "sinex-ingestd.service" ];
      
      serviceConfig = {
        Type = "simple";
        User = sinexCfg.database.user;
        Group = sinexCfg.database.user;
        ExecStart = "${sinexCfg.package}/bin/sinex-my-service";
        Restart = "always";
        RestartSec = "10s";
        
        # Resource limits
        MemoryMax = "512M";
        CPUQuota = "50%";
        
        # Security hardening
        PrivateTmp = true;
        ProtectSystem = "strict";
        ProtectHome = "read-only";
        ReadWritePaths = [ sinexCfg.directories.state ];
      };
      
      environment = {
        DATABASE_URL = sinexCfg.database.url;
        RUST_LOG = cfg.logLevel or sinexCfg.logLevel;
        SINEX_MY_SERVICE_PORT = toString cfg.port;
        SINEX_MY_SERVICE_FEATURE_FLAG = lib.boolToString cfg.featureFlag;
      };
    };
    
    # If the service exposes Prometheus metrics, wire it into the monitoring stack.
    services.sinex.observability.monitoring.prometheus.extraScrapeConfigs = [
      {
        job_name = "sinex-my-service";
        static_configs = [{ targets = [ "127.0.0.1:${toString cfg.port}" ]; }];
      }
    ];
  };
}
```

### Best Practices

1. **Service Dependencies**: Always specify proper systemd dependencies
2. **User/Group**: Use the shared `sinex` user for database access
3. **Resource Limits**: Apply appropriate memory and CPU quotas
4. **Security Hardening**: Use systemd security features like PrivateTmp
5. **Configuration**: Prefer typed module options that map to the runtime contract directly
6. **Health Checks**: Integrate with the monitoring framework
7. **Logging**: Use structured logging with configurable levels

## Support & Documentation

- **Architecture**: See `README.md#architecture`
- **Development**: See `CLAUDE.md` for developer reference
- **CLI**: See `crate/cli/README.md` for sinexctl usage
- **Issues**: Report to project repository
- **TimescaleDB**: [Official docs](https://docs.timescale.com/)
- **Performance tuning**: See TimescaleDB best practices guide

## Security Considerations

**Data sensitivity:**
- Sinex captures extensive personal data
- Keep database access restricted
- Use appropriate file permissions
- Consider disk encryption for sensitive data

**Network security:**
- Services run on localhost by default
- gRPC socket uses Unix domain sockets
- No external network exposure by default

**Access control:**
- Services run as dedicated `sinex` user
- Database access limited to `sinex` user
- File permissions restrict access to target user's data
