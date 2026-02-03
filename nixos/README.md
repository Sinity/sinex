# Sinex NixOS Deployment Guide

Complete deployment and operations guide for the Sinex Exocortex personal data capture system.

## Documentation Structure

- **example.nix** - Minimal workstation deployment (filesystem + terminal satellites)
- **example-monitoring.nix** - Staging configuration with maintenance + observability stack
- **example-dev-sandbox.nix** - Comprehensive developer sandbox with all services enabled
- **example-headless.nix** - Headless/server capture (filesystem + system satellites)
- **example-remote-satellite.nix** - Edge satellite forwarding events to remote ingest
- **example-coordination.nix** - Hot standby deployment with coordination enabled
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
  - pgx_ulid provisioning for ULID primary keys
  - TimescaleDB setup for hypertable partitioning  
  - Guidance for WAL/ulid tuning
- **TimescaleDB Hypertable Creation**: [`crate/lib/sinex-schema/src/migrations/m20241028_000001_create_canonical_schema.rs`](../crate/lib/sinex-schema/src/migrations/m20241028_000001_create_canonical_schema.rs)
  - Chunk interval optimization guidelines
  - Compression strategy documentation
- **ULID Implementation**: [`crate/lib/sinex-schema/docs/ulid.md`](../crate/lib/sinex-schema/docs/ulid.md)
  - ULID/UUID casting helpers used by repositories
  - Monotonic generation for high concurrency

### Event Processing
- **Ingestion & JetStream Overview**: [`docs/current/architecture/Core_Architecture.md`](../docs/current/architecture/Core_Architecture.md)
  - Provenance and Stage-as-you-go responsibilities: [`docs/current/architecture/provenance.md`](../docs/current/architecture/provenance.md)
  - Stream bootstrap defaults + environment namespacing: [`modules/nats.nix`](modules/nats.nix)
- **Node SDK Patterns**: [`crate/lib/sinex-node-sdk/docs/overview.md`](../crate/lib/sinex-node-sdk/docs/overview.md)
  - Unified processor interface and checkpoint semantics
  - Replay patterns and lifecycle hooks
- **StatefulStreamProcessor Trait**: [`sinex-node-sdk/src/runtime/stream/mod.rs`](../crate/lib/sinex-node-sdk/src/runtime/stream/mod.rs#L300)
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
}
```

Apply with:
```bash
sudo nixos-rebuild switch --flake .#your-host
```

> **REQUIRED**: You MUST apply the sinex flake overlay to your pkgs. The overlay provides:
> - `pkgs.sinex` (all binaries bundled)
> - `pkgs.sinexctl` (CLI tool)
> - `pkgs.sinex-ingestd`, `pkgs.sinex-gateway`, etc. (individual packages)
> - `pkgs.postgresql16Packages.pg_jsonschema` (required PostgreSQL extension)
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
>         sinex.nixosModules.default
>         ./configuration.nix
>       ];
>     };
>   };
> }
> ```
>
> Alternatively, provide packages explicitly without the overlay:
> ```nix
> services.sinex.package = inputs.sinex.packages.${pkgs.stdenv.hostPlatform.system}.sinex;
> ```

### Service Group Controls

You can toggle major service bundles via `services.sinex.serviceManagement.serviceGroups`:

```nix
services.sinex.serviceManagement.serviceGroups = {
  core = true;        # ingestd, gateway, satellites, NATS
  maintenance = false; # DLQ cleanup, git-annex timers, resource monitors
  monitoring = false;  # Prometheus, Grafana, exporters
};

# Typical development overrides
services.sinex.satellite = {
  enable = true;
  coordination.enable = false;
  eventSources.filesystem = {
    enable = true;
    instances = 1;
  };
};
```

Set the maintenance or monitoring flags to `true` when you need the supporting timers or observability stack.

### Satellite Secrets & TLS

When deploying satellites across hosts (e.g. the remote example), inject shared environment through the module instead of patching systemd units manually:

```nix
services.sinex.satellite = {
  environmentFiles = [ "/etc/sinex/remote-satellite.env" ];
  environment = [
    "SINEX_NATS_CA_CERT=/etc/sinex/nats/ca.pem"
    "SINEX_NATS_CLIENT_CERT=/etc/sinex/nats/client.pem"
    "SINEX_NATS_CLIENT_KEY=/etc/sinex/nats/client.key"
  ];
};

environment.etc."sinex/remote-satellite.env" = {
  text = ''
    # DATABASE_PASSWORD=change-me
    # SINEX_NATS_TOKEN=change-me
  '';
  mode = "0400";
};
```

The values in `environmentFiles` load into every satellite unit (filesystem, terminal, automata, etc.), making it straightforward to distribute secrets via tools like agenix or sops-nix. The `environment` list is appended verbatim for shared TLS paths or feature flags.
Entries in `environment` must be valid `KEY=value` pairs; the module now validates this at evaluation time so a missing value fails fast instead of reaching systemd.

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

### Production Setup with Hot Standby

For production deployments with zero-downtime upgrades and automatic failover:

```bash
cp nixos/example-coordination.nix /etc/nixos/sinex.nix
# Edit targetUser and coordination settings
sudo nixos-rebuild switch
```

This enables:
- **Multiple instances** of each satellite service (hot standby pattern)
- **Zero-downtime upgrades** via version-based leadership election
- **Automatic failover** when leader instances fail
- **Coordination monitoring** with health checks and metrics

### Development/Testing Setup

For simpler single-instance deployment:

```bash
cp nixos/example.nix /etc/nixos/sinex.nix
# Edit targetUser and other settings
sudo nixos-rebuild switch
```

### Evaluating Examples

Each example is exported through the flake. To explore them safely:

```bash
# Boot the minimal example in a disposable VM
nix build .#nixosConfigurations.example.config.system.build.vm
./result/bin/run-nixos-vm

# Temporarily apply the developer sandbox on a host (rolls back on reboot)
sudo nixos-rebuild test --flake .#exampleDevSandbox
```

Switch permanently only after merging the example into your host configuration.
> **Note**: The remote satellite example expects existing PostgreSQL/NATS endpoints and does not provision them locally.

## Architecture Overview

Sinex uses a satellite architecture:

```
External Data → Satellites → NATS JetStream → sinex-ingestd → PostgreSQL (`core.events`)
                                         ↓
                           confirmations/DLQ → Automata → Gateway/CLI
```

Current implementation:
- Satellites publish provisional events and source material slices directly to JetStream (`events.raw.*`, `source_material.*`).
- ingestd consumes from JetStream, validates, persists to PostgreSQL (TimescaleDB), then publishes confirmations (`events.confirmations.*`) and DLQ entries back to JetStream.
- Automata consume confirmations via durable JetStream consumers; Gateway/CLI query PostgreSQL via JSON-RPC or direct DB mode.

**Core Components:**
- **ingestd**: JetStream consumer + validator + single-writer persistence + confirmations/DLQ publisher
- **Gateway**: HTTP/JSON-RPC API for CLI and web access
- **Satellites**: Independent services for data capture and processing
- **PostgreSQL**: Event storage with TimescaleDB for time-series data
- **NATS JetStream**: Message bus for real-time event distribution

## Deployment Scenarios

### 1. Personal Laptop/Desktop (Recommended)

Full-featured setup capturing all digital activity:

```nix
services.sinex = {
  enable = true;
  targetUser = "myuser";
  
  satellite = {
    enable = true;
    eventSources = {
      filesystem = {
        enable = true;
        watchPaths = [ "~/Documents" "~/Projects" ];
      };
      terminal.enable = true;
      desktop.enable = true;
      system.enable = true;
    };
    automata = {
      canonicalCommandSynthesizer.enable = true;  # Command processing
      healthAggregator.enable = true;             # Health monitoring
    };
  };

  shell = {
    asciinema.autoRecord = false;
    kitty.enable = true;
  };
  
  database.autoSetup = true;
  blobStorage.enable = true;
};
```

> **Multiple databases:** use `database.extraDatabases` when you want the module
> to create and prep additional DBs (for example `sinex_dev`) alongside the
> primary `database.name`. Extensions such as TimescaleDB are installed in each
> database listed, so `devenv` migrations “just work” no matter which schema you
> target.

### 2. Server/Headless (Data Collection Only)

Minimal setup for server environments:

```nix
services.sinex = {
  enable = true;
  targetUser = "serveruser";
  
  satellite = {
    enable = true;
    eventSources = {
      filesystem = {
        enable = true;
        watchPaths = [ "/srv/data" "/var/log" ];
      };
      terminal.enable = false;
      desktop.enable = false;      # No GUI
      system.enable = true;
    };
    automata.healthAggregator.enable = true;
  };
  
  database.autoSetup = true;
  security.level = "strict";       # Enhanced security
};
```

### 3. Development Environment

Development setup with debugging enabled:

```nix
services.sinex = {
  enable = true;
  targetUser = "developer";
  logLevel = "debug";              # Verbose logging
  
  satellite = {
    enable = true;
    logLevel = "debug";
    eventSources = {
      filesystem = {
        enable = true;
        watchPaths = [ "~/Projects" ];  # Only watch projects
      };
      terminal.enable = true;
    };
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
  
  monitoring.logging.performance.traceRequests = true;
};
```

### 4. Testing/CI Environment

Minimal setup for automated testing:

```nix
services.sinex = {
  enable = true;
  targetUser = "testuser";
  
  satellite = {
    enable = true;
    eventSources = {
      filesystem.enable = false;
      terminal.enable = false;
      desktop.enable = false;
      system.enable = false;
    };
  };
  
  shell.asciinema.autoRecord = false;
  
  database = {
    autoSetup = true;
    name = "sinex_test";
  };
  
  # Disable persistent storage
  blobStorage.enable = false;
};
```

## Operations Guide

### Service Management

**Check service status:**
```bash
systemctl status sinex-ingestd
systemctl status sinex-gateway
systemctl status sinex-satellite-filesystem
systemctl status sinex-satellite-terminal
```

**View logs:**
```bash
journalctl -u sinex-ingestd -f
journalctl -u sinex-gateway -f
journalctl -u sinex-satellite-filesystem -f
```

**Restart services:**
```bash
sudo systemctl restart sinex-ingestd
sudo systemctl restart sinex-satellite-filesystem
```

**Stop all Sinex services:**
```bash
sudo systemctl stop 'sinex-*'
```

**Start all Sinex services:**
```bash
sudo systemctl start sinex-ingestd
sudo systemctl start sinex-gateway
sudo systemctl start sinex-satellite-filesystem
sudo systemctl start sinex-satellite-terminal
sudo systemctl start sinex-satellite-desktop
sudo systemctl start sinex-satellite-system
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
FROM core.satellite_signals 
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

Stream names depend on the deployment. Consult `modules/nats.nix` or the satellite configuration when deciding which streams to inspect or delete.

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

# Check gRPC socket
ls -la /run/sinex/ingest.sock

# Test event ingestion
curl -X POST http://localhost:8080/health

# Run full preflight check
sudo -u sinex /run/current-system/sw/bin/sinex-preflight verify
```

**Service health endpoints:**
```bash
# Gateway health
curl http://localhost:8080/health
curl http://localhost:8080/ready

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

Key environment variables for debugging:
```bash
export RUST_LOG=debug                    # Enable debug logging
export DATABASE_URL=postgresql:///sinex_dev  # Database connection
export SINEX_WORK_DIR=/tmp/sinex         # Working directory
```

### Security Configuration

Security levels:
- **minimal**: Basic security, maximum functionality
- **balanced**: Default, reasonable security with monitoring
- **strict**: Maximum security, may restrict some features

### Resource Limits

Default resource limits per service:
- **ingestd**: 1GB memory, 100% CPU
- **gateway**: 512MB memory, 50% CPU  
- **satellites**: 256MB memory, 50% CPU each

Adjust in configuration:
```nix
services.sinex.resources.ingestd = {
  memoryMax = "2G";
  cpuQuota = "200%";
};
```

## Troubleshooting

### Common Issues

**Services won't start:**
```bash
# Check for port conflicts
sudo netstat -tulpn | grep -E ':(8080|5432|6379)'

# Verify database is running
systemctl status postgresql
sudo -u postgres psql -c "SELECT 1;"

# Check disk space
df -h /var/lib/sinex
```

**Events not being captured:**
```bash
# Check satellite status
systemctl status sinex-satellite-filesystem
journalctl -u sinex-satellite-filesystem -f

# Verify ingestd socket
ls -la /run/sinex/ingest.sock
sudo -u sinex timeout 5 grpcurl -unix /run/sinex/ingest.sock list

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
sudo systemctl start sinex-satellite-filesystem
sudo systemctl start sinex-satellite-terminal
sudo systemctl start sinex-satellite-desktop
sudo systemctl start sinex-satellite-system
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
cd /path/to/sinex
nix develop
just dev          # Quick development cycle
just test-dev     # Development tests
```

### VM Testing

Run complete VM tests:
```bash
cd test/nixos-vm
./run-vm-tests.sh -c smoke    # Quick smoke tests
./run-vm-tests.sh -c all      # Full test suite
```

### Integration with Other Systems

**Prometheus monitoring:**
```nix
services.sinex.monitoring.prometheus.centralCollector = {
  enable = true;
  port = 2114;
};
```

**Grafana dashboards:**
```nix
services.grafana = {
  enable = true;
  provision.dashboards.settings.providers = [{
    name = "sinex";
    options.path = ./nixos/grafana-dashboards;
  }];
};
```

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
    timescaledb.compress_orderby = 'ts_ingest DESC, event_id',
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
    time_bucket('1 hour', ts_ingest) AS hour,
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
    
    configFile = mkOption {
      type = types.nullOr types.path;
      default = null;
      description = "Path to service configuration file";
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
        SINEX_CONFIG = cfg.configFile or "${pkgs.writeText "my-service.toml" (builtins.toJSON cfg)}";
      };
    };
    
    # Add to health checks
    services.sinex.monitoring.healthChecks = {
      "sinex-my-service" = {
        command = "${pkgs.curl}/bin/curl -f http://localhost:${toString cfg.port}/health";
        interval = "30s";
        timeout = "5s";
      };
    };
  };
}
```

### Best Practices

1. **Service Dependencies**: Always specify proper systemd dependencies
2. **User/Group**: Use the shared `sinex` user for database access
3. **Resource Limits**: Apply appropriate memory and CPU quotas
4. **Security Hardening**: Use systemd security features like PrivateTmp
5. **Configuration**: Support both inline and file-based configuration
6. **Health Checks**: Integrate with the monitoring framework
7. **Logging**: Use structured logging with configurable levels

## Support & Documentation

- **Architecture**: See `docs/current/architecture/Core_Architecture.md`
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
