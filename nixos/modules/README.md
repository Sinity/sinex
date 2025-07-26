# Sinex NixOS Module Documentation

This directory contains the NixOS module for deploying and managing the Sinex system.

## Module Structure

- `default.nix` - Main module entry point
- `database.nix` - PostgreSQL setup and migrations
- `event-sources.nix` - Event source satellite configuration
- `satellite-services.nix` - Satellite service management
- `monitoring.nix` - Observability and metrics
- `blob-storage.nix` - Git-annex configuration
- `preflight-verification.nix` - Startup checks
- `services/` - Core service definitions

## NixOS Integration

### Declarative Configuration
The `services.sinex` module provides complete system configuration:
- Automatic database setup with migrations
- Systemd service generation for all satellites
- User and permission management
- Resource limits and security policies

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
    
    eventSources = {
      filesystem.enable = true;
      terminal.enable = true;
      desktop.enable = true;
      system.enable = true;
    };
    
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