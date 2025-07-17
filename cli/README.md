# Sinex CLI - RPC Integration Guide

The Sinex CLI (`exo.py`) has been migrated to use the sinex-gateway RPC server instead of direct database connections. This provides better security, performance, and maintainability.

## Quick Start

### Default RPC Mode
```bash
# Query events using RPC (default mode)
python3 cli/exo.py query --limit 10

# List event sources using RPC
python3 cli/exo.py sources

# Show statistics using RPC
python3 cli/exo.py stats
```

### Database Fallback Mode
```bash
# Use direct database connection when RPC server is unavailable
python3 cli/exo.py --use-db query --limit 10
python3 cli/exo.py --use-db sources
python3 cli/exo.py --use-db stats
```

## Configuration

### RPC Server URL

1. **Command line option** (highest priority):
   ```bash
   python3 cli/exo.py --rpc-url http://custom-host:8888 query
   ```

2. **Environment variable**:
   ```bash
   export SINEX_RPC_URL=http://custom-host:8888
   python3 cli/exo.py query
   ```

3. **Default**: `http://127.0.0.1:9999`

### Database Connection (Fallback Mode)

When using `--use-db`, the CLI uses the `DATABASE_URL` environment variable:
```bash
export DATABASE_URL=postgresql:///sinex_dev?host=/run/postgresql
python3 cli/exo.py --use-db query
```

## Available Commands

### Core Query Commands

#### query
Query events with various filters:
```bash
# Basic query
python3 cli/exo.py query --limit 20

# Filter by source and event type
python3 cli/exo.py query --source fs --event-type file.created

# Time-based filtering
python3 cli/exo.py query --last 1h
python3 cli/exo.py query --since "2025-07-10 09:00"

# Output formats
python3 cli/exo.py query --output-format json --limit 5
python3 cli/exo.py query --output-format csv --limit 5
```

#### sources
List event sources with statistics:
```bash
# Show all event sources
python3 cli/exo.py sources

# With database mode (more detailed stats)
python3 cli/exo.py --use-db sources
```

#### stats
Show database statistics:
```bash
# Basic statistics via RPC
python3 cli/exo.py stats

# Full statistics via database
python3 cli/exo.py --use-db stats
```

### Legacy Commands (Database Only)

Some commands are only available in database mode and require `--use-db`:

- `schema` - Schema introspection
- `agent` - Agent management
- `blob` - Blob storage operations
- `dlq` - Dead Letter Queue management

```bash
python3 cli/exo.py --use-db schema list
python3 cli/exo.py --use-db agent list
python3 cli/exo.py --use-db dlq list
```

## RPC vs Database Mode Comparison

| Feature | RPC Mode | Database Mode |
|---------|----------|---------------|
| **Performance** | Fast, cached | Direct SQL access |
| **Security** | Secured through RPC layer | Direct DB access required |
| **Functionality** | Core query operations | Full feature set |
| **Dependencies** | RPC server must be running | Database connection required |
| **Error Handling** | Graceful with fallback guidance | Direct database errors |

### RPC Mode Capabilities
- ✅ Event searching and filtering
- ✅ Basic analytics (event counts, activity heatmap)
- ✅ Source statistics
- ✅ All output formats (table, JSON, CSV, YAML)
- ❌ Schema introspection
- ❌ Agent management
- ❌ Blob storage operations
- ❌ DLQ management

### Database Mode Capabilities
- ✅ All RPC mode features
- ✅ Schema introspection
- ✅ Agent management and monitoring
- ✅ Blob storage operations
- ✅ Dead Letter Queue management
- ✅ Full statistics and detailed analytics

## Error Handling

### RPC Connection Errors
When the RPC server is unavailable:
```
RPC Error: RPC Error -32700: Connection error: [Errno 111] Connection refused
Try using --use-db flag for direct database access
```

**Resolution**:
1. Start the sinex-gateway RPC server
2. Use `--use-db` flag for direct database access
3. Check RPC URL configuration

### Database Connection Errors
When using `--use-db` and database is unavailable:
```
Error: could not connect to server: No such file or directory
```

**Resolution**:
1. Ensure PostgreSQL is running
2. Check `DATABASE_URL` environment variable
3. Verify database permissions

## Migration Guide

### From Legacy CLI (Direct DB)
If you were using the CLI with direct database access:

1. **Start using RPC mode** (recommended):
   ```bash
   # Old way
   python3 cli/exo.py query --limit 10
   
   # New way (same command, but uses RPC)
   python3 cli/exo.py query --limit 10
   ```

2. **Continue using database mode** (for full features):
   ```bash
   # Explicitly use database
   python3 cli/exo.py --use-db query --limit 10
   ```

### Environment Variables
Update your environment variables:
```bash
# Add RPC URL (optional, defaults to localhost:9999)
export SINEX_RPC_URL=http://127.0.0.1:9999

# Keep DATABASE_URL for fallback mode
export DATABASE_URL=postgresql:///sinex_dev?host=/run/postgresql
```

### Scripts and Automation
Update scripts to handle RPC failures gracefully:
```bash
#!/bin/bash
# Try RPC first, fallback to database
if ! python3 cli/exo.py query --limit 1 >/dev/null 2>&1; then
    echo "RPC unavailable, using database mode"
    python3 cli/exo.py --use-db query --limit 1
else
    python3 cli/exo.py query --limit 1
fi
```

## Development and Testing

### Testing RPC Integration
```bash
# Test RPC client directly
python3 -c "
import cli.rpc_client as rpc
client = rpc.create_client()
print(f'Server health: {client.health_check()}')
"

# Test CLI commands
python3 cli/exo.py query --limit 1
python3 cli/exo.py sources
python3 cli/exo.py stats
```

### Running Tests
```bash
# Run RPC-specific tests
python3 -m pytest test/integration/cli/test_exo_cli.py::TestRPCIntegration -v

# Run all CLI tests
python3 -m pytest test/integration/cli/test_exo_cli.py -v
```

## Performance Notes

### RPC Mode Benefits
- **Caching**: RPC server can cache frequent queries
- **Connection pooling**: Shared database connections
- **Rate limiting**: Built-in request throttling
- **Security**: No direct database credentials needed

### When to Use Database Mode
- **Development**: Direct access for debugging
- **Administration**: Full feature access required
- **Bulk operations**: Large data exports or imports
- **RPC unavailable**: Fallback when server is down

## Troubleshooting

### Common Issues

1. **"RPC server not responding"**
   - Check if sinex-gateway is running: `systemctl status sinex-gateway`
   - Verify RPC URL: `echo $SINEX_RPC_URL`
   - Test connectivity: `curl http://127.0.0.1:9999` (should return method not allowed)

2. **"Permission denied" in database mode**
   - Check database permissions
   - Verify DATABASE_URL format
   - Ensure PostgreSQL is accessible

3. **"No events found" in RPC mode**
   - Check if RPC server has access to same database
   - Verify time filters (RPC may have different timezone handling)
   - Compare with `--use-db` mode results

### Debug Mode
Enable verbose output:
```bash
# RPC debug (from Python)
python3 -c "
import cli.rpc_client as rpc
client = rpc.create_client()
client.timeout = 60  # Increase timeout
print(client.health_check())
"

# CLI debug
python3 cli/exo.py query --limit 1 -vvv  # If verbose flag is available
```

## Future Enhancements

The RPC integration provides a foundation for:
- **Web interface**: Browser-based event exploration
- **API access**: RESTful endpoints for integration
- **Real-time features**: WebSocket streaming of events
- **Multi-user access**: Shared sinex-gateway instance
- **Advanced caching**: Redis-backed query caching

For the latest updates, see the sinex-gateway RPC server documentation.