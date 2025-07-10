# TIM: Troubleshooting Guide

- **TIM Identifier**: TIM-TroubleshootingGuide
- **Category**: Operations
- **Status**: Implemented (Core)
- **Target Component**: Operations Team, Support Engineers
- **Prerequisites**: Access to system logs and database
- **Linked TIMs**: 
  - TIM-OperationsManual
  - TIM-ServiceManagement
  - TIM-ObservabilityStackSetup

## Overview

This guide provides systematic troubleshooting procedures for common issues encountered in Sinex deployments. Each section includes symptoms, diagnostic steps, and resolution procedures.

## 1. Service Issues

### 1.1 Collector Won't Start

**Symptoms:**
- `systemctl status sinex-unified-collector` shows failed/inactive
- No new events appearing in database
- SystemD logs show startup failures

**Diagnostics:**
```bash
# 1. Check detailed status
systemctl status sinex-unified-collector -l

# 2. View recent logs
journalctl -u sinex-unified-collector -n 100 --no-pager

# 3. Test configuration
sinex-collector --validate-config

# 4. Check dependencies
systemctl is-active postgresql
ls -la /var/lib/sinex/
ls -la /etc/sinex/

# 5. Test database connection
psql $DATABASE_URL -c "SELECT 1;"
```

**Common Causes & Solutions:**

#### Configuration File Issues
```bash
# Error: "failed to load configuration"
# Solution: Verify config file exists and is valid
cat /etc/sinex/collector.toml
sinex-collector --config /etc/sinex/collector.toml --validate-config

# Fix permissions
sudo chown sinex:sinex /etc/sinex/collector.toml
sudo chmod 640 /etc/sinex/collector.toml
```

#### Database Connection Failed
```bash
# Error: "connection refused" or "FATAL: password authentication failed"
# Solution: Verify DATABASE_URL
echo $DATABASE_URL

# Test with explicit connection
psql postgresql:///sinex_dev?host=/run/postgresql

# Check PostgreSQL is running
systemctl status postgresql
sudo -u postgres psql -c "\l"

# Verify user exists
sudo -u postgres psql -c "\du sinex"
```

#### Permission Denied
```bash
# Error: "Permission denied" on state directory
# Solution: Fix ownership
sudo chown -R sinex:sinex /var/lib/sinex
sudo chmod 750 /var/lib/sinex

# For systemd private directories
sudo systemctl edit sinex-unified-collector
# Add:
[Service]
StateDirectory=sinex
StateDirectoryMode=0750
```

#### Port Already in Use
```bash
# Error: "address already in use"
# Solution: Find conflicting process
ss -tlnp | grep 2113
lsof -i :2113

# Kill conflicting process or change port
# In collector.toml:
[metrics]
port = 2114  # Different port
```

### 1.2 Worker Processing Stopped

**Symptoms:**
- Queue depth increasing
- No events being promoted
- Worker shows as running but inactive

**Diagnostics:**
```bash
# 1. Check worker status
systemctl status sinex-promo-worker

# 2. View queue status
psql $DATABASE_URL -c "
  SELECT status, COUNT(*), MAX(created_at) as latest
  FROM sinex_schemas.work_queue
  GROUP BY status;"

# 3. Check for stuck items
psql $DATABASE_URL -c "
  SELECT event_id, status, created_at, started_at, worker_id
  FROM sinex_schemas.work_queue
  WHERE status = 'processing'
    AND started_at < NOW() - INTERVAL '10 minutes';"

# 4. Worker logs
journalctl -u sinex-promo-worker -n 100
```

**Common Causes & Solutions:**

#### Database Lock Issues
```sql
-- Check for blocking queries
SELECT 
  blocked_locks.pid AS blocked_pid,
  blocked_activity.usename AS blocked_user,
  blocking_locks.pid AS blocking_pid,
  blocking_activity.usename AS blocking_user,
  blocked_activity.query AS blocked_statement,
  blocking_activity.query AS blocking_statement
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted;

-- Kill blocking query if safe
SELECT pg_cancel_backend(pid);
-- or more forcefully
SELECT pg_terminate_backend(pid);
```

#### Worker Crash Loop
```bash
# Check for repeated restarts
journalctl -u sinex-promo-worker | grep -c "Started Sinex"

# Increase restart delay
sudo systemctl edit sinex-promo-worker
# Add:
[Service]
RestartSec=30s

# Check resource limits
systemctl show sinex-promo-worker | grep -E "(Memory|CPU)"
```

#### Queue Corruption
```sql
-- Reset stuck items
UPDATE sinex_schemas.work_queue
SET status = 'pending', 
    started_at = NULL,
    worker_id = NULL,
    retry_count = retry_count + 1
WHERE status = 'processing'
  AND started_at < NOW() - INTERVAL '30 minutes';

-- Clear failed items if too many
DELETE FROM sinex_schemas.work_queue
WHERE status = 'failed'
  AND retry_count > 5
  AND created_at < NOW() - INTERVAL '24 hours';
```

### 1.3 Service Crashes Repeatedly

**Symptoms:**
- Service restarts every few minutes
- SystemD shows "failed" after multiple restarts
- Memory or CPU limits being hit

**Diagnostics:**
```bash
# 1. Check crash pattern
journalctl -u sinex-unified-collector | grep -E "(signal|panic|error)"

# 2. Resource usage at crash
journalctl -u sinex-unified-collector | grep -B5 "Stopped"

# 3. Core dumps (if enabled)
coredumpctl list sinex-collector
coredumpctl info sinex-collector

# 4. System resources
dmesg | grep -i "killed process"
grep sinex /var/log/messages
```

**Common Causes & Solutions:**

#### Out of Memory
```bash
# Check memory limits
systemctl show sinex-unified-collector | grep MemoryMax

# Increase limit
sudo systemctl set-property sinex-unified-collector MemoryMax=4G

# Or in NixOS config
services.sinex.collector.resources.memoryMax = "4G";

# Check for memory leaks
ps aux | grep sinex | awk '{print $2, $6}' # PID and RSS
# Monitor over time
```

#### Stack Overflow / Infinite Recursion
```bash
# Increase stack size
sudo systemctl edit sinex-unified-collector
# Add:
[Service]
Environment="RUST_MIN_STACK=8388608"  # 8MB stack

# Check for recursive event sources
# Review collector.toml for circular dependencies
```

## 2. Performance Issues

### 2.1 High Database Query Times

**Symptoms:**
- Slow event queries
- Web UI timeouts
- High database CPU usage

**Diagnostics:**
```sql
-- Enable query timing
\timing on

-- Check slow queries
SELECT 
  query,
  calls,
  mean_exec_time,
  total_exec_time,
  stddev_exec_time
FROM pg_stat_statements
WHERE mean_exec_time > 100  -- queries over 100ms
ORDER BY mean_exec_time DESC
LIMIT 20;

-- Check missing indexes
SELECT 
  schemaname,
  tablename,
  attname,
  n_distinct,
  avg_width
FROM pg_stats
WHERE schemaname = 'raw'
  AND n_distinct > 100
  AND tablename = 'events'
ORDER BY n_distinct DESC;

-- Explain plan for slow query
EXPLAIN (ANALYZE, BUFFERS) 
SELECT * FROM raw.events 
WHERE source = 'filesystem' 
  AND ts_ingest > NOW() - INTERVAL '1 hour';
```

**Solutions:**

#### Add Missing Indexes
```sql
-- Common helpful indexes
CREATE INDEX CONCURRENTLY idx_events_source_ts 
  ON raw.events(source, ts_ingest DESC);

CREATE INDEX CONCURRENTLY idx_events_type_ts 
  ON raw.events(event_type, ts_ingest DESC);

CREATE INDEX CONCURRENTLY idx_events_payload_gin 
  ON raw.events USING gin(payload);

-- Monitor index usage
SELECT 
  schemaname,
  tablename,
  indexname,
  idx_scan,
  idx_tup_read,
  idx_tup_fetch
FROM pg_stat_user_indexes
WHERE schemaname = 'raw'
ORDER BY idx_scan;
```

#### Optimize Continuous Aggregates
```sql
-- Check aggregate refresh lag
SELECT 
  view_name,
  refresh_lag,
  last_run_duration
FROM timescaledb_information.continuous_aggregate_stats;

-- Force refresh if behind
CALL refresh_continuous_aggregate('metrics_1min', NULL, NULL);

-- Adjust refresh policy
SELECT alter_continuous_aggregate_policy('metrics_1min',
  start_offset => INTERVAL '2 hours',
  end_offset => INTERVAL '10 minutes',
  schedule_interval => INTERVAL '10 minutes');
```

### 2.2 High Event Ingestion Latency

**Symptoms:**
- Events appear in database minutes after occurrence
- Large gap between ts_orig and ts_ingest
- Event sources report buffer overflow

**Diagnostics:**
```bash
# 1. Check ingestion lag
psql $DATABASE_URL -c "
  SELECT 
    source,
    AVG(EXTRACT(EPOCH FROM (ts_ingest - ts_orig))) as avg_lag_seconds,
    MAX(EXTRACT(EPOCH FROM (ts_ingest - ts_orig))) as max_lag_seconds,
    COUNT(*) as events
  FROM raw.events
  WHERE ts_ingest > NOW() - INTERVAL '10 minutes'
    AND ts_orig IS NOT NULL
  GROUP BY source
  ORDER BY avg_lag_seconds DESC;"

# 2. Check collector metrics
curl -s http://localhost:2113/metrics | grep -E "(buffer|queue|dropped)"

# 3. Database write performance
psql $DATABASE_URL -c "
  SELECT 
    datname,
    xact_commit,
    xact_rollback,
    blks_read,
    blks_hit,
    tup_inserted
  FROM pg_stat_database
  WHERE datname = 'sinex_dev';"
```

**Solutions:**

#### Increase Buffer Sizes
```toml
# In collector.toml
[collector]
channel_buffer_size = 10000  # Increase from default 1000

[batch_writer]
max_batch_size = 500  # Increase from default 100
flush_interval_ms = 5000  # Increase from default 1000
```

#### Database Write Optimization
```sql
-- Tune PostgreSQL for write performance
ALTER SYSTEM SET synchronous_commit = 'off';  -- Acceptable for events
ALTER SYSTEM SET commit_delay = 100;  -- Microseconds
ALTER SYSTEM SET checkpoint_completion_target = 0.9;

-- Requires restart
SELECT pg_reload_conf();
```

#### Parallel Ingestion
```toml
# Enable multiple writers
[collector]
num_db_writers = 4  # Increase from default 1

# Or run multiple collectors
# systemd template instance
systemctl start sinex-unified-collector@2
systemctl start sinex-unified-collector@3
```

## 3. Data Issues

### 3.1 Missing Events

**Symptoms:**
- Expected events not in database
- Gaps in event timeline
- Source reports sending but not stored

**Diagnostics:**
```bash
# 1. Check for dropped events
journalctl -u sinex-unified-collector | grep -i "drop"

# 2. Verify source is enabled
cat /etc/sinex/collector.toml | grep -A5 "event_sources"

# 3. Check event validation failures
psql $DATABASE_URL -c "
  SELECT 
    DATE(created_at) as date,
    COUNT(*) as failed_validations
  FROM sinex_schemas.validation_failures
  WHERE created_at > NOW() - INTERVAL '24 hours'
  GROUP BY DATE(created_at);"

# 4. Dead letter queue
ls -la /var/lib/sinex/dlq/
```

**Solutions:**

#### Source Not Configured
```toml
# Ensure source is enabled in collector.toml
[[event_sources]]
name = "filesystem"
enabled = true

[[event_sources]]
name = "terminal"
enabled = true
```

#### Validation Failures
```bash
# Check validation rules
psql $DATABASE_URL -c "
  SELECT 
    schema_name,
    version,
    created_at
  FROM sinex_schemas.event_payload_schemas
  ORDER BY created_at DESC;"

# Review failed events
cat /var/lib/sinex/dlq/*.json | jq '.error'

# Temporarily disable validation (emergency only)
# In collector.toml:
[validation]
enabled = false
```

### 3.2 Duplicate Events

**Symptoms:**
- Same event appears multiple times
- Duplicate ULID primary keys (should be impossible)
- Event counts higher than expected

**Diagnostics:**
```sql
-- Check for duplicates by content
WITH duplicates AS (
  SELECT 
    source,
    event_type,
    payload,
    COUNT(*) as count
  FROM raw.events
  WHERE ts_ingest > NOW() - INTERVAL '1 hour'
  GROUP BY source, event_type, payload
  HAVING COUNT(*) > 1
)
SELECT * FROM duplicates LIMIT 10;

-- Check for time anomalies
SELECT 
  id,
  ts_orig,
  ts_ingest,
  source
FROM raw.events
WHERE ts_ingest > NOW() - INTERVAL '1 hour'
  AND ts_orig > ts_ingest  -- Original after ingestion?
LIMIT 10;
```

**Solutions:**

#### Source Sending Duplicates
```toml
# Enable deduplication in collector
[collector]
deduplication_window_seconds = 60  # Dedupe within 1 minute

# Or fix at source level
# Check source-specific configuration
```

#### Clock Synchronization Issues
```bash
# Check system time
timedatectl status

# Force NTP sync
sudo systemctl restart systemd-timesyncd
timedatectl set-ntp true

# Verify time sync
journalctl -u systemd-timesyncd -n 20
```

## 4. Resource Issues

### 4.1 Disk Space Exhaustion

**Symptoms:**
- Write failures in logs
- Database errors about disk space
- Services failing to start

**Diagnostics:**
```bash
# 1. Overall disk usage
df -h

# 2. Find large directories
du -sh /var/lib/* | sort -hr | head -20
du -sh /var/lib/postgresql/16/data/base/* | sort -hr | head -10

# 3. Database size breakdown
psql $DATABASE_URL -c "
  SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename)) as indexes_size
  FROM pg_tables 
  WHERE schemaname IN ('raw', 'sinex_schemas', 'metrics')
  ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;"

# 4. Check for bloat
psql $DATABASE_URL -c "
  SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    n_dead_tup,
    n_live_tup,
    round(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) as dead_percent
  FROM pg_stat_user_tables
  WHERE n_dead_tup > 1000
  ORDER BY n_dead_tup DESC;"
```

**Solutions:**

#### Emergency Cleanup
```bash
# 1. Clear old logs
sudo journalctl --vacuum-time=1d
sudo journalctl --vacuum-size=500M

# 2. Remove old DLQ files
find /var/lib/sinex/dlq -type f -mtime +7 -delete

# 3. Clean package cache
sudo nix-collect-garbage -d

# 4. Database cleanup
psql $DATABASE_URL -c "
  -- Remove old metrics (safe)
  DELETE FROM raw.events
  WHERE source LIKE 'sinex.metrics.%'
    AND ts_ingest < NOW() - INTERVAL '7 days';

  -- Clean failed queue items
  DELETE FROM sinex_schemas.work_queue
  WHERE status = 'failed'
    AND created_at < NOW() - INTERVAL '30 days';

  -- Vacuum to reclaim space
  VACUUM FULL ANALYZE raw.events;"
```

#### Long-term Solutions
```sql
-- Enable compression (TimescaleDB)
ALTER TABLE raw.events SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'source',
  timescaledb.compress_orderby = 'ts_ingest DESC'
);

SELECT add_compression_policy('raw.events', INTERVAL '7 days');

-- Implement retention policy
SELECT add_retention_policy('raw.events', INTERVAL '1 year');

-- For metrics specifically
SELECT add_retention_policy('metrics.collector_events', INTERVAL '30 days');
```

### 4.2 Memory Pressure

**Symptoms:**
- OOM killer activating
- Services being killed
- System swap usage high

**Diagnostics:**
```bash
# 1. Current memory state
free -h
vmstat 1 5

# 2. Top memory consumers
ps aux --sort=-%mem | head -20

# 3. Service memory usage
systemctl status sinex-* | grep Memory

# 4. Database memory
psql $DATABASE_URL -c "
  SELECT name, setting, unit 
  FROM pg_settings 
  WHERE name IN ('shared_buffers', 'work_mem', 'maintenance_work_mem', 'effective_cache_size');"

# 5. Check for leaks
pmap -x $(pgrep sinex-collector) | tail -1
```

**Solutions:**

#### Immediate Relief
```bash
# 1. Drop caches
sudo sync && sudo sysctl -w vm.drop_caches=3

# 2. Restart memory-heavy services
sudo systemctl restart sinex-unified-collector
sudo systemctl restart postgresql

# 3. Adjust service limits
sudo systemctl set-property sinex-unified-collector MemoryMax=2G
sudo systemctl set-property sinex-promo-worker MemoryMax=1G
```

#### Tune PostgreSQL Memory
```sql
-- Conservative settings for 8GB system
ALTER SYSTEM SET shared_buffers = '2GB';  -- 25% of RAM
ALTER SYSTEM SET effective_cache_size = '6GB';  -- 75% of RAM
ALTER SYSTEM SET work_mem = '16MB';  -- Per operation
ALTER SYSTEM SET maintenance_work_mem = '256MB';

-- Restart required
sudo systemctl restart postgresql
```

## 5. Network Issues

### 5.1 Connection Timeouts

**Symptoms:**
- "Connection timeout" errors
- Slow API responses
- Intermittent failures

**Diagnostics:**
```bash
# 1. Test connectivity
ping -c 5 localhost
curl -v http://localhost:2113/health

# 2. Check listening ports
ss -tlnp | grep -E "(2113|5432|3000|9090)"

# 3. Firewall rules
sudo iptables -L -n -v
sudo nft list ruleset

# 4. Connection limits
psql $DATABASE_URL -c "SHOW max_connections;"
psql $DATABASE_URL -c "SELECT count(*) FROM pg_stat_activity;"
```

**Solutions:**

#### Database Connection Pool Exhaustion
```toml
# In collector.toml
[database]
max_connections = 50  # Increase pool size
connection_timeout = 30  # Seconds

# Or database side
ALTER SYSTEM SET max_connections = 200;
# Restart required
```

#### Firewall Blocking
```bash
# Allow local connections (NixOS)
networking.firewall.interfaces.lo.allowedTCPPorts = [ 
  2113  # Collector metrics
  2114  # Worker metrics
  5432  # PostgreSQL
];

# Or temporarily
sudo iptables -I INPUT -i lo -j ACCEPT
```

## 6. Monitoring Issues

### 6.1 Grafana Not Showing Data

**Symptoms:**
- Empty graphs in dashboards
- "No data" errors
- Datasource connection failed

**Diagnostics:**
```bash
# 1. Check Grafana service
systemctl status grafana

# 2. Test datasource
curl -u admin:admin http://localhost:3000/api/datasources

# 3. Check Prometheus
curl http://localhost:9090/api/v1/query?query=up

# 4. PostgreSQL datasource
psql $DATABASE_URL -c "SELECT NOW();"
```

**Solutions:**

#### Fix Datasource Configuration
```bash
# In Grafana UI or via API
curl -X POST -H "Content-Type: application/json" -d '{
  "name": "PostgreSQL-Sinex",
  "type": "postgres",
  "url": "localhost:5432",
  "database": "sinex_dev",
  "user": "sinex",
  "secureJsonData": {
    "password": ""
  },
  "jsonData": {
    "sslmode": "disable",
    "postgresVersion": 1600,
    "timescaledb": true
  }
}' http://admin:admin@localhost:3000/api/datasources
```

#### Refresh Dashboard
```bash
# Force dashboard reload
curl -X POST http://admin:admin@localhost:3000/api/admin/provisioning/dashboards/reload

# Check for errors
journalctl -u grafana -n 50 | grep -i error
```

## Quick Reference Card

### Emergency Commands

```bash
# Service control
sudo systemctl stop sinex-promo-worker
sudo systemctl stop sinex-unified-collector
sudo systemctl start sinex-unified-collector
sudo systemctl start sinex-promo-worker

# Quick diagnostics
systemctl status sinex-* --no-pager
journalctl -u sinex-* --since "10 minutes ago" | grep -i error

# Database checks
psql $DATABASE_URL -c "SELECT version();"
psql $DATABASE_URL -c "SELECT COUNT(*) FROM raw.events WHERE ts_ingest > NOW() - INTERVAL '1 minute';"

# Reset stuck queue
psql $DATABASE_URL -c "UPDATE sinex_schemas.work_queue SET status = 'pending' WHERE status = 'processing' AND started_at < NOW() - INTERVAL '1 hour';"

# Emergency space cleanup
sudo journalctl --vacuum-size=100M
find /var/lib/sinex/dlq -type f -mtime +1 -delete
```

### Common Error Patterns

| Error Message | Likely Cause | Quick Fix |
|--------------|-------------|-----------|
| "connection refused" | PostgreSQL down | `systemctl start postgresql` |
| "permission denied" | File permissions | `chown -R sinex:sinex /var/lib/sinex` |
| "no space left" | Disk full | See section 4.1 |
| "too many connections" | Pool exhausted | Increase max_connections |
| "FATAL: role does not exist" | Missing DB user | `createuser sinex` |
| "address already in use" | Port conflict | Change port in config |

Remember: Always check logs first, they usually contain the specific error and solution.