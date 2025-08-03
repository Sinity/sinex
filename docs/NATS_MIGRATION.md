# Sinex NATS Migration Guide

## Overview

This document describes the migration of Sinex satellites from gRPC → ingestd → NATS to direct NATS publishing. This change reduces latency, improves throughput, and simplifies the architecture by removing an unnecessary hop.

## Architecture Changes

### Before (Legacy Mode)
```
Satellites → gRPC → ingestd → NATS JetStream → Automata
```

### After (Direct NATS)
```
Satellites → NATS JetStream → Automata
         ↘                   ↗
          ingestd (DB writes)
```

## Migration Status

### ✅ Completed
- NATS publisher implementation in satellite SDK
- Event processor with NATS transport support
- CLI support for NATS configuration (`--nats-servers` flag)
- Automata migrated to consume from NATS
- NixOS module updated to configure NATS by default

### 🔄 In Progress
- Testing satellite deployments with direct NATS
- Performance benchmarking

### 📋 TODO
- Remove gRPC dependencies from satellites
- Update deployment documentation
- Monitor and tune NATS performance

## Configuration

### Satellite CLI

By default, satellites now use NATS:
```bash
sinex-fs-watcher service  # Uses NATS (default)
sinex-fs-watcher --use-grpc service  # Legacy gRPC mode
```

### Environment Variables

```bash
SINEX_NATS_SERVERS=nats://localhost:4222  # Default
SINEX_USE_GRPC=true  # Force legacy mode
```

### NixOS Configuration

```nix
services.sinex.satellite = {
  enable = true;
  
  nats = {
    servers = "nats://localhost:4222";  # Comma-separated for multiple
  };
  
  # Satellites automatically use NATS
  eventSources.filesystem.enable = true;
  eventSources.terminal.enable = true;
  eventSources.desktop.enable = true;
  eventSources.system.enable = true;
};
```

## NATS JetStream Configuration

### Stream Configuration
- Stream name: `events`
- Subjects: `events.>`
- Retention: 7 days
- Max messages: 10,000,000
- Storage: File-based

### Subject Hierarchy
```
events.<source>.<event_type>
```

Examples:
- `events.filesystem.file_created`
- `events.terminal.command_executed`
- `events.desktop.clipboard_changed`

## Benefits

1. **Reduced Latency**: Direct publishing eliminates gRPC hop
2. **Better Throughput**: NATS designed for high-throughput messaging
3. **Simplified Architecture**: Fewer moving parts
4. **Built-in Features**: Deduplication, replay, persistence
5. **Flexible Routing**: Subject-based filtering for automata

## Rollback

If issues arise, satellites can fall back to gRPC mode:

1. **CLI**: Add `--use-grpc` flag
2. **Environment**: Set `SINEX_USE_GRPC=true`
3. **NixOS**: Temporarily modify service definitions

## Monitoring

### NATS Metrics
- Message rate per subject
- Consumer lag
- Stream size and limits

### Satellite Metrics
- Publishing success/failure rates
- Batch sizes and timings
- Buffer usage

## Troubleshooting

### Common Issues

1. **Connection Failed**
   - Check NATS is running: `systemctl status nats`
   - Verify server URL: `nats://localhost:4222`
   - Check firewall rules

2. **Publishing Errors**
   - Check stream exists: `nats stream ls`
   - Verify subject permissions
   - Check message size limits

3. **Performance Issues**
   - Tune batch size in satellite config
   - Monitor NATS memory usage
   - Check network latency

### Debug Commands

```bash
# Check NATS streams
nats stream ls
nats stream info events

# Monitor subjects
nats sub "events.>"

# Check consumer status
nats consumer ls events
nats consumer info events <consumer-name>
```

## Future Improvements

1. **Subject-based routing**: More granular event routing
2. **Compression**: Enable NATS compression for large payloads
3. **Clustering**: NATS cluster for high availability
4. **Optimizations**: Batch acknowledgments, async publishing