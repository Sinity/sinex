# Sinex NixOS Deployment Guide

Complete deployment and operations guide for the Sinex Exocortex personal data capture system.

## Documentation Structure

- **example.nix** - Complete configuration example with all options and defaults
- **modules/** - Implementation modules:
  - `sinex-config.nix` - Core configuration interface and PostgreSQL setup
  - `database.nix` - Database connection pooling and health monitoring  
  - `satellite-services.nix` - Individual satellite service configurations
  - `monitoring.nix` - Prometheus/Grafana monitoring setup
  - `preflight-verification.nix` - Pre-deployment validation checks

## Architectural Documentation

Key architectural decisions and implementation details are documented at their implementation points:

### Database Layer
- **PostgreSQL Extensions Setup**: [`modules/sinex-config.nix:285-305`](modules/sinex-config.nix#L285-L305)
  - pgx_ulid configuration for ULID primary keys
  - TimescaleDB setup for hypertable partitioning  
  - Optional monotonic ULID generation instructions
- **TimescaleDB Hypertable Creation**: [`migrations/00000000000002_create_core_tables.sql:1-47`](../migrations/00000000000002_create_core_tables.sql#L1-L47)
  - Chunk interval optimization guidelines
  - Compression strategy documentation
- **ULID Implementation**: [`sinex-ulid/src/lib.rs:188-246`](../crate/sinex-ulid/src/lib.rs#L188-L246)
  - ULID-UUID casting for foreign keys
  - Monotonic generation for high concurrency

### Event Processing
- **Redis Streams Architecture**: [`sinex-satellite-sdk/src/redis_client.rs:1-88`](../crate/sinex-satellite-sdk/src/redis_client.rs#L1-L88)
  - Supersedes ADR-014 routing cache architecture
  - Consumer group pattern documentation
  - Checkpoint hybrid strategy
- **StatefulStreamProcessor Trait**: [`sinex-satellite-sdk/src/stream_processor.rs:381-502`](../crate/sinex-satellite-sdk/src/stream_processor.rs#L381-L502)
  - Unified interface for all satellites
  - Snapshot, historical, and continuous modes

### Satellite Implementations  
- **Filesystem Monitoring**: [`sinex-fs-watcher/src/unified_processor.rs:1-62`](../crate/sinex-fs-watcher/src/unified_processor.rs#L1-L62)
  - inotify (Linux) implementation details
  - FSEvents (macOS) configuration
  - System limits and overflow handling

## Quick Start

### Minimal Deployment

Add to your NixOS configuration:

```nix
{
  imports = [ ./path/to/sinex/nixos/modules ];

  services.sinex = {
    enable = true;
    targetUser = "yourusername";  # REQUIRED: your username
  };
}
```

Apply with:
```bash
sudo nixos-rebuild switch --flake .#your-host
```

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

## Architecture Overview

Sinex uses a satellite architecture:

```
External Data → Ingestors → IngestD (gRPC) → PostgreSQL + Redis → Automata → Synthesis Events
             ↗ (filesystem)     ↗ (hub)     ↗ (storage)  ↗ (bus)    ↗ (processors)
             ↗ (terminal)
             ↗ (desktop) 
             ↗ (system)
```

**Core Components:**
- **IngestD**: Central gRPC hub for event ingestion
- **Gateway**: HTTP/JSON-RPC API for CLI and web access
- **Satellites**: Independent services for data capture and processing
- **PostgreSQL**: Event storage with TimescaleDB for time-series data
- **Redis**: Message bus for real-time event distribution

## Deployment Scenarios

### 1. Personal Laptop/Desktop (Recommended)

Full-featured setup capturing all digital activity:

```nix
services.sinex = {
  enable = true;
  targetUser = "myuser";
  
  # Enable satellite architecture (recommended)
  satellite = {
    enable = true;
    eventSources = {
      filesystem.enable = true;    # File changes
      terminal.enable = true;      # Shell commands
      desktop.enable = true;       # Clipboard, windows
      system.enable = true;        # System events
    };
    automata = {
      canonicalCommandSynthesizer.enable = true;  # Command processing
      healthAggregator.enable = true;             # Health monitoring
    };
  };
  
  # Database auto-setup
  database.autoSetup = true;
  
  # Blob storage for large files
  blobStorage.enable = true;
};
```

### 2. Server/Headless (Data Collection Only)

Minimal setup for server environments:

```nix
services.sinex = {
  enable = true;
  targetUser = "serveruser";
  
  satellite = {
    enable = true;
    eventSources = {
      filesystem.enable = true;
      terminal.enable = true;
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
    eventSources.filesystem = {
      enable = true;
      watchPaths = [ "~/Projects" ];  # Only watch projects
    };
  };
  
  database = {
    autoSetup = true;
    name = "sinex_dev";            # Separate dev database
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

# View leadership status  
sudo -u sinex psql sinex_prod -c "
SELECT service_name, version, acquired_at, last_heartbeat 
FROM core.service_leadership;
"

# View all healthy instances
sudo -u sinex psql sinex_prod -c "
SELECT service_name, instance_id, version, host_name, last_heartbeat
FROM core.satellite_instances 
WHERE last_heartbeat > NOW() - INTERVAL '2 minutes'
ORDER BY service_name, version DESC;
"
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
DELETE FROM core.service_leadership WHERE service_name = 'sinex-fs-watcher';
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

### Redis Operations

**Access Redis:**
```bash
redis-cli
```

**Monitor event stream:**
```bash
redis-cli XREAD STREAMS sinex:events $
```

**Check stream info:**
```bash
redis-cli XINFO STREAM sinex:events
redis-cli XINFO GROUPS sinex:events
```

**Clear Redis data (DESTRUCTIVE):**
```bash
redis-cli FLUSHALL
```

### Data Management

**Wipe all Sinex data (DESTRUCTIVE):**
```bash
# Stop services
sudo systemctl stop 'sinex-*'

# Drop database
sudo -u postgres dropdb sinex_dev
sudo -u postgres createdb sinex_dev
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE sinex_dev TO sinex;"

# Clear Redis
redis-cli FLUSHALL

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

# Check Redis connectivity
redis-cli ping

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
sudo -u sinex psql sinex_dev -c "
UPDATE core.automaton_checkpoints 
SET last_processed_id = NULL 
WHERE automaton_name = 'terminal-command-canonicalizer';"

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

- **Architecture**: See `spec/SADI.md` and `plan.md`
- **Development**: See `CLAUDE.md` for developer reference
- **API**: Check `cli/README.md` for CLI usage
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