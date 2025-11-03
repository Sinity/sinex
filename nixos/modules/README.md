# Sinex NixOS Module Documentation

This directory contains the NixOS module for deploying and managing the Sinex system.

## Module Structure

- `default.nix` - Main module entry point
- `database.nix` - PostgreSQL setup and migrations
- `satellite-services.nix` - Satellite service management
- `monitoring.nix` - Observability and metrics
- `blob-storage.nix` - Git-annex configuration
- `preflight-verification.nix` - Startup checks
- `services/` - Core service definitions

## NixOS Integration

### Declarative Configuration
The `services.sinex` module provides complete system configuration:
- Automatic database setup with migrations
- Database provisioning (`services.sinex.database.autoSetup`) can be enabled
  independently of the main service toggle to pre-create clusters, roles, and
  extensions
- Derived directories cascade from `services.sinex.directories.state`, covering
  logs, spool paths, DLQ storage, and blob repositories without repeating
  boilerplate downstream
- Systemd service generation for all satellites
- User and permission management
- Resource limits and security policies
- Optional CLI package automatically placed on `PATH` when available; the module
  gracefully skips CLI-dependent timers if omitted

### Service Architecture
- **Satellite Services**: Each event source runs as independent systemd service
- **Core Services**: ingestd, gateway, and RPC dispatcher
- **Maintenance Jobs**: Periodic cleanup and optimization

### VM Testing
Comprehensive integration tests validate the full system deployment:
- Database initialization
- Service startup ordering
- Inter-service communication
- Event flow validation

## Monitoring & Observability

### Structured Logging
- All services emit JSON-structured logs
- Centralized in systemd journal
- Queryable via journalctl

### Health Monitoring
- **Heartbeat Pattern**: Services emit regular heartbeats
- **Health Checks**: HTTP endpoints for service status
- **Metrics Export**: Prometheus-compatible metrics

### Performance Tracking
- Resource usage per satellite
- Event processing rates
- Queue depths and latencies
- Database connection pools

## Security & Privacy

### Access Control
- **PostgreSQL Roles**: Least-privilege database access
- **Systemd Users**: Dedicated users per service
- **File Permissions**: Restricted access to state directories

### Process Isolation
- Systemd security directives
- Read-only root filesystems where possible
- Network namespace isolation
- Capability restrictions

### Secrets Management
- **Agenix Integration**: Encrypted secrets in git
- **Runtime Injection**: Secrets loaded at service start
- **No Hardcoded Values**: All sensitive data externalized

### User Consent
- Configurable per event source
- Opt-in data collection
- Clear privacy controls

## Backup & Recovery

### Database Backup (Planned)
- **pgBackRest**: Automated PostgreSQL backups
- Point-in-time recovery support
- Compressed incremental backups
- S3-compatible storage backend

### Blob Storage Backup
- **Git-Annex**: Distributed blob replication
- Multiple backend support
- Automatic verification
- Efficient deduplication

### Configuration Backup
- NixOS configuration in version control
- Reproducible deployments
- Rollback capabilities
- Declarative system state

## Development Guidelines

### Module Structure Pattern
When creating new NixOS modules for Sinex services:

```nix
# modules/services/my-satellite.nix
{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex.mySatellite;
  settingsFormat = pkgs.formats.toml {};
in
{
  options.services.sinex.mySatellite = {
    enable = mkEnableOption "Sinex My Satellite service";

    package = mkOption {
      type = types.package;
      default = pkgs.sinex.mySatellite;
      description = "Package providing the satellite binary";
    };

    settings = mkOption {
      type = types.submodule {
        freeformType = settingsFormat.type;
        options = {
          listen_port = mkOption {
            type = types.port;
            default = 8080;
            description = "Port to listen on";
          };
          log_level = mkOption {
            type = types.enum [ "trace" "debug" "info" "warn" "error" ];
            default = "info";
            description = "Logging level";
          };
        };
      };
      default = {};
      description = "Satellite configuration";
    };
  };

  config = mkIf cfg.enable {
    systemd.services."sinex-my-satellite" = {
      description = "Sinex My Satellite Service";
      wantedBy = [ "multi-user.target" ];
      after = [ "network.target" "postgresql.service" "sinex-ingestd.service" ];
      
      serviceConfig = {
        User = "sinex";
        Group = "sinex";
        ExecStart = "${cfg.package}/bin/my-satellite --config ${
          settingsFormat.generate "my-satellite-config.toml" cfg.settings
        }";
        
        # Security hardening
        NoNewPrivileges = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
        
        # Resource limits
        MemoryMax = "512M";
        CPUQuota = "50%";
        
        # Restart policy
        Restart = "on-failure";
        RestartSec = "10s";
      };
      
      environment = {
        RUST_LOG = "info,my_satellite=debug";
        DATABASE_URL = "postgresql:///sinex_db?host=/run/postgresql";
      };
    };
  };
}
```

### Configuration Best Practices
1. Use `pkgs.formats` for config file generation
2. Provide sensible defaults for all options
3. Use structured `settings` options with freeformType
4. Document all options clearly
5. Follow systemd security best practices

### Service Dependencies
- Always depend on `postgresql.service` if using database
- Depend on `sinex-ingestd.service` for event submission
- Use `after` for ordering, `requires` for hard dependencies

## Usage Example

```nix
{
  services.sinex = {
    enable = true;
    targetUser = "myuser";
    
    database = {
      host = "localhost";
      name = "sinex_prod";
    };
    
    satellite.eventSources = {
      filesystem.enable = true;
      terminal.enable = true;
      desktop.enable = true;
      system.enable = true;
    };
    
    shell.kitty.enable = true;
    
    monitoring = {
      enable = true;
      grafana.enable = true;
    };
  };
}
```

## Troubleshooting

### Service Failures
```bash
# Check service status
systemctl status sinex-ingestd

# View service logs
journalctl -u sinex-fs-watcher -f

# Run preflight checks
systemctl start sinex-preflight
```

### Database Issues
```bash
# Check database connectivity
sudo -u sinex psql -d sinex_db

# Run migrations manually
sinex-migrate

# Verify schema
\dt core.*
```

### Performance Problems
```bash
# Check queue depths
redis-cli xinfo stream unified_hotlog

# Monitor resource usage
systemctl status sinex-*.service

# Analyze slow queries
psql -c "SELECT * FROM pg_stat_statements ORDER BY total_time DESC LIMIT 10"
```
