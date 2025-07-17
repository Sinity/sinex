# Sinex Development & Debugging Guide

Complete guide for setting up development environments, debugging issues, and testing Sinex systems.

## Quick Development Setup

### 1. Local Development Environment

```bash
# Clone and setup
git clone <sinex-repo>
cd sinex

# Enter Nix development shell (ALWAYS required)
nix develop

# Initial setup
just migrate         # Apply database migrations
just test-dev       # Quick development test cycle

# Development workflow
just dev            # Format, check, fast tests (~30s)
just test-fast      # Unit + property tests
```

### 2. Production-Like Development VM

For testing full system integration without affecting your main system:

```bash
# Create development VM configuration
cat > vm-dev.nix << 'EOF'
{
  imports = [ ./nixos/modules ];
  
  services.sinex = {
    enable = true;
    targetUser = "testuser";
    logLevel = "debug";
    
    satellite = {
      enable = true;
      logLevel = "debug";
      eventSources = {
        filesystem.enable = true;
        terminal.enable = true;
        desktop.enable = false;    # No GUI needed in VM
        system.enable = true;
      };
    };
    
    database.autoSetup = true;
  };
  
  # Create test user
  users.users.testuser = {
    isNormalUser = true;
    initialPassword = "test";
    extraGroups = [ "wheel" ];
  };
}
EOF

# Build and run VM
nixos-rebuild build-vm -I nixos-config=vm-dev.nix
./result/bin/run-nixos-vm
```

### 3. Isolated Testing Environment

Use the comprehensive VM test suite for isolated testing:

```bash
# Quick smoke test
just test-vm

# Full test suite
just test-vm-all

# Debug specific functionality
just test-vm-debug basic-flow
```

## Development Workflows

### Standard Development Cycle

```bash
# 1. Make changes to code
vim crate/sinex-fs-watcher/src/lib.rs

# 2. Quick validation (under 2 minutes)
just dev

# 3. Focused testing
just test-unit
just test-integration

# 4. Database changes? Update SQLX cache
just sqlx-prepare && git add .sqlx/

# 5. Commit changes
git add .
git commit -m "fix: improve filesystem watcher performance"
```

### Database Development

```bash
# Create migration
just migrate-create "add_new_index"

# Edit migration files in migrations/
# Apply migration
just migrate

# Update SQLX cache (REQUIRED for Nix builds)
just sqlx-prepare
git add .sqlx/

# Test with fresh database
just db-reset
just test-integration
```

### Satellite Development

```bash
# Test specific satellite
cargo run --bin sinex-fs-watcher -- scan /home/user/test

# Debug satellite with trace logging
RUST_LOG=trace cargo run --bin sinex-fs-watcher -- service

# Test satellite integration
cargo test -p sinex-fs-watcher --test integration_tests
```

## Debugging Strategies

### 1. Service-Level Debugging

#### Check Service Status
```bash
# All Sinex services
systemctl status 'sinex-*'

# Specific service with detailed info
systemctl status sinex-ingestd -l

# Service logs with follow
journalctl -u sinex-ingestd -f

# Recent errors across all services
journalctl --since "1 hour ago" | grep -i sinex | grep -i error
```

#### Debug Service Startup Issues
```bash
# Enable debug logging
sudo systemctl edit sinex-ingestd
# Add:
# [Service]
# Environment="RUST_LOG=debug"

# Restart and monitor
sudo systemctl restart sinex-ingestd
journalctl -u sinex-ingestd -f

# Check for dependency issues
systemctl list-dependencies sinex-ingestd
```

### 2. Database Debugging

#### Direct Database Access
```bash
# Connect to database
sudo -u sinex psql sinex_dev

# Useful debugging queries
\dt core.*          -- List tables
\d core.events      -- Describe events table

-- Recent events
SELECT ts_orig, source, event_type, payload::text
FROM core.events 
ORDER BY ts_orig DESC 
LIMIT 10;

-- Event count by source (last hour)
SELECT source, COUNT(*) as count
FROM core.events 
WHERE ts_orig > NOW() - INTERVAL '1 hour'
GROUP BY source 
ORDER BY count DESC;

-- Database size and activity
SELECT 
  schemaname, 
  tablename, 
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
  n_tup_ins + n_tup_upd + n_tup_del as activity
FROM pg_tables 
JOIN pg_stat_user_tables USING (schemaname, tablename)
WHERE schemaname IN ('core', 'raw')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

#### Performance Debugging
```bash
# Enable slow query logging
sudo -u sinex psql sinex_dev -c "
ALTER SYSTEM SET log_min_duration_statement = 1000;
SELECT pg_reload_conf();
"

# Check for lock contention
sudo -u sinex psql sinex_dev -c "
SELECT blocked_locks.pid AS blocked_pid,
       blocked_activity.usename AS blocked_user,
       blocking_locks.pid AS blocking_pid,
       blocking_activity.usename AS blocking_user,
       blocked_activity.query AS blocked_statement,
       blocking_activity.query AS current_statement_in_blocking_process
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted;
"
```

### 3. Redis/Message Bus Debugging

```bash
# Monitor Redis streams
redis-cli MONITOR

# Check Sinex event stream
redis-cli XINFO STREAM sinex:events
redis-cli XRANGE sinex:events - + COUNT 10

# Monitor stream consumer groups
redis-cli XINFO GROUPS sinex:events

# Debug consumer group state
redis-cli XPENDING sinex:events command-canonicalizers
```

### 4. Application-Level Debugging

#### Rust Application Debugging
```bash
# Enable comprehensive logging
export RUST_LOG="sinex=trace,sqlx=debug"

# Run with debugger
rust-gdb target/debug/sinex-ingestd
# Or
rust-lldb target/debug/sinex-ingestd

# Capture backtraces on panic
export RUST_BACKTRACE=full

# Profile memory usage
valgrind --tool=massif target/debug/sinex-ingestd
```

#### gRPC Debugging
```bash
# Test gRPC connectivity
grpcurl -unix /run/sinex/ingest.sock list

# Debug gRPC calls
export GRPC_TRACE=all
export GRPC_VERBOSITY=debug
cargo run --bin sinex-fs-watcher -- scan /tmp/test
```

## VM Development & Testing

### Development VM Setup

The Sinex VM test infrastructure provides isolated environments for testing:

#### Quick Development VM
```bash
# Create a development VM with your current code
cd test/nixos-vm
./run-vm-tests.sh --debug basic-flow

# When test fails, you'll get an interactive VM
# Use it for development and debugging
```

#### Custom Development VM
```bash
# Create custom VM configuration
cat > my-dev-vm.nix << 'EOF'
{ pkgs, ... }: {
  imports = [ ./common/test-base.nix ];
  
  virtualisation = {
    memorySize = 4096;      # 4GB RAM
    cores = 4;              # 4 CPU cores
    diskSize = 20480;       # 20GB disk
  };
  
  services.sinex = {
    enable = true;
    targetUser = "testuser";
    logLevel = "debug";
    
    satellite = {
      enable = true;
      eventSources = {
        filesystem = {
          enable = true;
          watchPaths = [ "/home/testuser/watched" ];
        };
      };
    };
  };
  
  # Development tools
  environment.systemPackages = with pkgs; [
    htop
    iotop
    strace
    gdb
    valgrind
  ];
}
EOF

# Build and run
nix-build -E "
  (import <nixpkgs/nixos/tests/make-test-python.nix>) {
    name = \"my-dev-vm\";
    nodes.machine = import ./my-dev-vm.nix;
    testScript = \"start_all()\";
  }
"
./result/bin/nixos-test-driver
```

### VM Testing & Debugging

#### Run Specific Test Categories
```bash
# Quick validation
./run-vm-tests.sh -c smoke

# Full integration testing
./run-vm-tests.sh -c integration

# Performance testing
./run-vm-tests.sh -c performance

# Stress/chaos testing
./run-vm-tests.sh -c chaos
```

#### Debug Failed Tests
```bash
# Run test in debug mode (keeps VM after failure)
./run-vm-tests.sh --debug --verbose basic-flow

# When test fails, you'll see VM build directory
cd /tmp/nix-build-*.drv-0/

# Start interactive session
./bin/nixos-test-driver

# In Python REPL:
>>> machine.shell_interact()
# Now you're in the VM shell

# Check services
systemctl status sinex-ingestd
journalctl -u sinex-ingestd -f

# Debug data
sudo -u sinex psql sinex_dev
```

#### VM Performance Optimization
```bash
# Use appropriate VM profile
# In your test .nix file:
virtualisation.vmProfile = "performance";  # For heavy tests
virtualisation.vmProfile = "minimal";      # For light tests

# Monitor VM resource usage
# Inside VM:
htop
iotop
journalctl --disk-usage
```

## Observability & Monitoring

### Built-in Health Checks

```bash
# Service health endpoints
curl http://localhost:8080/health
curl http://localhost:8080/ready

# Database connectivity
just psql -c "SELECT 1;"

# Redis connectivity
redis-cli ping

# Full system preflight check
sudo -u sinex sinex-preflight verify
```

### Development Monitoring

#### Real-time Event Monitoring
```bash
# Watch events being created
watch -n 1 'sudo -u sinex psql sinex_dev -c "SELECT COUNT(*) FROM core.events;"'

# Monitor event rate
sudo -u sinex psql sinex_dev -c "
SELECT 
  date_trunc('minute', ts_orig) as minute,
  COUNT(*) as events_per_minute
FROM core.events 
WHERE ts_orig > NOW() - INTERVAL '1 hour'
GROUP BY minute 
ORDER BY minute DESC 
LIMIT 10;
"
```

#### Resource Monitoring
```bash
# Service memory usage
ps aux | grep sinex | awk '{print $11, $6}' | sort -k2 -nr

# Database connections
sudo -u sinex psql sinex_dev -c "
SELECT 
  application_name,
  state,
  COUNT(*) as connections
FROM pg_stat_activity 
WHERE application_name LIKE '%sinex%'
GROUP BY application_name, state;
"

# Disk usage for Sinex data
du -sh /var/lib/sinex/*
du -sh /var/log/sinex/*
```

## Common Debugging Scenarios

### Scenario 1: Events Not Being Captured

```bash
# 1. Check satellite service
systemctl status sinex-satellite-filesystem
journalctl -u sinex-satellite-filesystem -f

# 2. Verify ingestd is running and reachable
systemctl status sinex-ingestd
ls -la /run/sinex/ingest.sock

# 3. Test gRPC connectivity
grpcurl -unix /run/sinex/ingest.sock list

# 4. Check database connectivity
sudo -u sinex psql sinex_dev -c "SELECT COUNT(*) FROM core.events;"

# 5. Verify watched paths
sudo -u sinex psql sinex_dev -c "
SELECT DISTINCT source FROM core.events 
WHERE ts_orig > NOW() - INTERVAL '1 hour';
"
```

### Scenario 2: High Memory Usage

```bash
# 1. Identify the culprit
ps aux | grep sinex | sort -k6 -nr

# 2. Check for memory leaks
valgrind --tool=massif --massif-out-file=sinex.massif \
  target/debug/sinex-ingestd

# 3. Monitor database connection pools
sudo -u sinex psql sinex_dev -c "
SELECT COUNT(*) as total_connections,
       COUNT(*) FILTER (WHERE state = 'active') as active_connections
FROM pg_stat_activity;
"

# 4. Check Redis memory usage
redis-cli INFO memory
```

### Scenario 3: Performance Issues

```bash
# 1. Enable query logging
sudo -u sinex psql sinex_dev -c "
ALTER SYSTEM SET log_min_duration_statement = 100;
SELECT pg_reload_conf();
"

# 2. Check slow queries
sudo -u sinex psql sinex_dev -c "
SELECT query, calls, total_time, mean_time
FROM pg_stat_statements 
ORDER BY total_time DESC 
LIMIT 10;
"

# 3. Analyze event processing rate
sudo -u sinex psql sinex_dev -c "
SELECT 
  source,
  COUNT(*) as total_events,
  COUNT(*) / EXTRACT(EPOCH FROM (MAX(ts_orig) - MIN(ts_orig))) as events_per_second
FROM core.events 
WHERE ts_orig > NOW() - INTERVAL '1 hour'
GROUP BY source;
"

# 4. Monitor system resources
iostat -x 1    # I/O statistics
htop          # CPU and memory
```

## Development Best Practices

### 1. Testing Strategy

```bash
# Always test locally first
just dev                    # Quick validation
just test-fast             # Fast test suite

# Test database changes
just db-reset              # Fresh database
just test-integration      # DB integration tests

# Test full system
just test-vm               # VM integration tests
```

### 2. Debugging Workflow

1. **Start with logs**: `journalctl -u sinex-* -f`
2. **Check service health**: `systemctl status sinex-*`
3. **Verify data flow**: Check database, Redis, filesystem
4. **Use VM for isolation**: Test in clean environment
5. **Reproduce in development**: Minimal reproduction case

### 3. Performance Development

```bash
# Use appropriate test datasets
# Small for unit tests
just test-unit

# Medium for integration tests  
just test-integration

# Large for performance tests
just test-performance

# Stress test in VM
./run-vm-tests.sh -c performance
```

### 4. Git Workflow for Development

```bash
# Create feature branch
git checkout -b feature/improve-satellite-performance

# Development cycle
while true; do
  # Make changes
  vim src/lib.rs
  
  # Quick validation
  just dev
  
  # Commit incremental progress
  git add -p
  git commit -m "wip: satellite performance improvements"
done

# Final testing before PR
just test-all
just test-vm

# Clean up commits
git rebase -i main
```

## IDE Setup & Tools

### VS Code Configuration

Create `.vscode/settings.json`:
```json
{
  "rust-analyzer.cargo.buildScripts.enable": true,
  "rust-analyzer.checkOnSave.command": "check",
  "rust-analyzer.checkOnSave.extraArgs": ["--workspace"],
  "files.watcherExclude": {
    "**/target/**": true,
    "**/.direnv/**": true
  }
}
```

### Useful Development Tools

```bash
# Install development tools in nix shell
nix develop -c $SHELL

# Tools available:
bacon          # Continuous testing
cargo-watch    # File watching
cargo-expand   # Macro expansion
cargo-bloat    # Binary size analysis
```

This comprehensive guide covers the full development lifecycle from setup through debugging complex issues in production-like environments.