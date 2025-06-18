# TIM: Operations Manual

- **TIM Identifier**: TIM-OperationsManual
- **Category**: Operations
- **Status**: Implemented (Core)
- **Target Component**: Operations Team
- **Prerequisites**: Basic system administration knowledge
- **Linked TIMs**: 
  - TIM-SystemdServiceManagement
  - TIM-ObservabilityStackSetup
  - TIM-PostgreSQLBackupDR_pgBackRest

## Overview

This operations manual provides day-to-day procedures for managing the Sinex event capture system in production environments. It covers routine operations, monitoring, maintenance, and emergency procedures.

## 1. System Overview

### Core Components

1. **sinex-unified-collector**: Main event collection service
2. **sinex-promo-worker**: Event promotion and processing worker
3. **PostgreSQL + TimescaleDB**: Event storage with time-series optimization
4. **Grafana**: Visualization and monitoring dashboards
5. **Prometheus**: Metrics collection and alerting

### Service Dependencies

```
postgresql.service
    ↓
sinex-unified-collector.service
    ↓
sinex-promo-worker.service
    ↓
grafana.service (optional)
prometheus.service (optional)
```

## 2. Daily Operations

### 2.1 Morning Health Check Routine

```bash
# 1. Check all services are running
systemctl status sinex-unified-collector
systemctl status sinex-promo-worker
systemctl status postgresql

# 2. Verify event flow (last 5 minutes)
psql $DATABASE_URL -c "
  SELECT source, COUNT(*) as events
  FROM raw.events 
  WHERE ts_ingest > NOW() - INTERVAL '5 minutes'
  GROUP BY source
  ORDER BY events DESC;"

# 3. Check for errors in last hour
psql $DATABASE_URL -c "
  SELECT source, COUNT(*) as error_count
  FROM raw.events
  WHERE ts_ingest > NOW() - INTERVAL '1 hour'
    AND (event_type LIKE '%error%' OR payload->>'level' = 'error')
  GROUP BY source
  ORDER BY error_count DESC;"

# 4. Review queue status
psql $DATABASE_URL -c "
  SELECT status, COUNT(*) 
  FROM sinex_schemas.promotion_queue
  WHERE created_at > NOW() - INTERVAL '1 hour'
  GROUP BY status;"

# 5. Check disk usage
df -h /var/lib/postgresql
df -h /var/lib/sinex

# 6. Review system resources
free -h
top -bn1 | head -20
```

### 2.2 Service Management

#### Starting Services

```bash
# Start in correct order
sudo systemctl start postgresql
sudo systemctl start sinex-unified-collector
sudo systemctl start sinex-promo-worker

# Verify startup
journalctl -u sinex-unified-collector -n 50
```

#### Stopping Services

```bash
# Stop in reverse order
sudo systemctl stop sinex-promo-worker
sudo systemctl stop sinex-unified-collector
# Note: PostgreSQL typically remains running

# Verify clean shutdown
journalctl -u sinex-unified-collector -n 50 | grep -i shutdown
```

#### Restarting Services

```bash
# Graceful restart (preferred)
sudo systemctl reload-or-restart sinex-unified-collector

# Hard restart (if needed)
sudo systemctl restart sinex-unified-collector
```

### 2.3 Log Management

#### Viewing Logs

```bash
# Real-time logs
journalctl -fu sinex-unified-collector

# Last 100 lines
journalctl -u sinex-unified-collector -n 100

# Time-based queries
journalctl -u sinex-unified-collector --since "1 hour ago"
journalctl -u sinex-unified-collector --since today

# Error filtering
journalctl -u sinex-unified-collector -p err

# All Sinex services
journalctl -u 'sinex-*' -f
```

#### Log Rotation

Logs are automatically rotated by systemd-journald. Configuration:

```bash
# Check current journal size
journalctl --disk-usage

# Manual vacuum if needed
sudo journalctl --vacuum-time=7d
sudo journalctl --vacuum-size=1G
```

## 3. Monitoring and Alerting

### 3.1 Grafana Dashboards

Access Grafana at `http://localhost:3000` (or configured address).

Key dashboards:
- **Sinex Overview**: System-wide metrics and health
- **Event Pipeline**: Ingestion rates and latency
- **System Health**: Component status and resources
- **Worker Performance**: Queue metrics and processing times

### 3.2 Key Metrics to Monitor

| Metric | Normal Range | Warning | Critical |
|--------|-------------|---------|----------|
| Event ingestion rate | 10-1000/sec | <1/sec | 0/sec |
| Queue depth | <1000 | >5000 | >10000 |
| Processing latency | <100ms | >500ms | >1000ms |
| CPU usage | <50% | >70% | >90% |
| Memory usage | <70% | >85% | >95% |
| Disk usage | <70% | >80% | >90% |
| Failed queue items | <1% | >5% | >10% |

### 3.3 Alert Response

When alerts trigger:

1. **Acknowledge alert** in monitoring system
2. **Check service health**: `systemctl status sinex-*`
3. **Review recent logs**: `journalctl -u sinex-* --since "30 minutes ago"`
4. **Check database connectivity**: `psql $DATABASE_URL -c "SELECT 1;"`
5. **Follow specific runbooks** for alert type

## 4. Routine Maintenance

### 4.1 Weekly Tasks

```bash
# 1. Review disk usage trends
df -h /var/lib/postgresql
du -sh /var/lib/sinex/*

# 2. Check database size
psql $DATABASE_URL -c "
  SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
  FROM pg_tables 
  WHERE schemaname IN ('raw', 'sinex_schemas', 'metrics')
  ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;"

# 3. Analyze slow queries
psql $DATABASE_URL -c "
  SELECT 
    query,
    calls,
    mean_exec_time,
    total_exec_time
  FROM pg_stat_statements
  WHERE query NOT LIKE '%pg_stat_statements%'
  ORDER BY mean_exec_time DESC
  LIMIT 10;"

# 4. Vacuum analyze (if not auto)
psql $DATABASE_URL -c "VACUUM ANALYZE;"

# 5. Review failed events
psql $DATABASE_URL -c "
  SELECT 
    DATE(created_at) as date,
    COUNT(*) as failed_count
  FROM sinex_schemas.promotion_queue
  WHERE status = 'failed'
    AND created_at > NOW() - INTERVAL '7 days'
  GROUP BY DATE(created_at)
  ORDER BY date DESC;"
```

### 4.2 Monthly Tasks

```bash
# 1. Full system backup verification
# See backup procedures in section 6

# 2. Certificate renewal check (if using TLS)
openssl x509 -enddate -noout -in /path/to/cert.pem

# 3. Review and update documentation
# Check for outdated procedures

# 4. Performance baseline update
# Record current metrics for trend analysis

# 5. Clean up old failed queue items
psql $DATABASE_URL -c "
  DELETE FROM sinex_schemas.promotion_queue
  WHERE status = 'failed'
    AND created_at < NOW() - INTERVAL '30 days';"
```

## 5. Performance Tuning

### 5.1 Collector Performance

```toml
# /etc/sinex/collector.toml adjustments

# Increase buffer size for high volume
[buffer]
size = 10000  # Default: 1000
flush_interval = "5s"  # Default: "10s"

# Adjust worker pool
[processing]
worker_count = 8  # Default: 4
max_batch_size = 100  # Default: 50
```

### 5.2 Database Performance

```sql
-- Check for missing indexes
SELECT 
  schemaname,
  tablename,
  attname,
  n_distinct,
  correlation
FROM pg_stats
WHERE schemaname = 'raw'
  AND n_distinct > 100
  AND correlation < 0.1
ORDER BY n_distinct DESC;

-- Monitor connection pool
SELECT 
  datname,
  count(*) as connections,
  count(*) FILTER (WHERE state = 'active') as active,
  count(*) FILTER (WHERE state = 'idle') as idle,
  count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction
FROM pg_stat_activity
GROUP BY datname;

-- Tune shared_buffers (25% of RAM)
ALTER SYSTEM SET shared_buffers = '4GB';
-- Requires restart
```

### 5.3 Resource Limits

```bash
# Check current limits
systemctl show sinex-unified-collector | grep -E '(Memory|CPU|Tasks)'

# Adjust limits in NixOS configuration
services.sinex.collector.resources = {
  memoryMax = "4G";  # Increase from default
  cpuQuota = "200%";  # Allow 2 full cores
};
```

## 6. Backup and Recovery

### 6.1 Automated Backups

```bash
# Verify backup job
systemctl status postgresql-backup.timer

# Check last backup
ls -lh /var/backup/postgresql/

# Test backup integrity
pg_restore --list /var/backup/postgresql/latest.dump | head
```

### 6.2 Manual Backup

```bash
# Full database backup
pg_dump -Fc $DATABASE_URL > sinex_$(date +%Y%m%d_%H%M%S).dump

# Events only (last 24h)
pg_dump -Fc -t raw.events --where="ts_ingest > NOW() - INTERVAL '24 hours'" \
  $DATABASE_URL > events_24h_$(date +%Y%m%d_%H%M%S).dump

# Configuration backup
tar -czf sinex_config_$(date +%Y%m%d_%H%M%S).tar.gz \
  /etc/sinex \
  /var/lib/sinex/config
```

### 6.3 Recovery Procedures

```bash
# Service recovery
sudo systemctl stop sinex-promo-worker
sudo systemctl stop sinex-unified-collector

# Database recovery
pg_restore -d $DATABASE_URL -c backup.dump

# Verify recovery
psql $DATABASE_URL -c "SELECT COUNT(*) FROM raw.events;"

# Restart services
sudo systemctl start sinex-unified-collector
sudo systemctl start sinex-promo-worker
```

## 7. Emergency Procedures

### 7.1 Service Won't Start

```bash
# 1. Check system dependencies
systemctl status postgresql

# 2. Verify configuration
sinex-collector --validate-config

# 3. Check file permissions
ls -la /var/lib/sinex/
ls -la /etc/sinex/

# 4. Review startup errors
journalctl -u sinex-unified-collector -n 100 --no-pager

# 5. Start in debug mode
RUST_LOG=debug sinex-collector --dry-run
```

### 7.2 Database Connection Issues

```bash
# 1. Test connection
psql $DATABASE_URL -c "SELECT 1;"

# 2. Check PostgreSQL
systemctl status postgresql
journalctl -u postgresql -n 50

# 3. Verify connection string
echo $DATABASE_URL

# 4. Check connection limits
psql -U postgres -c "SHOW max_connections;"
psql -U postgres -c "SELECT count(*) FROM pg_stat_activity;"

# 5. Emergency connection cleanup
psql -U postgres -c "
  SELECT pg_terminate_backend(pid)
  FROM pg_stat_activity
  WHERE datname = 'sinex_dev'
    AND pid <> pg_backend_pid()
    AND state = 'idle'
    AND state_change < NOW() - INTERVAL '10 minutes';"
```

### 7.3 High Memory Usage

```bash
# 1. Identify memory consumers
ps aux | sort -nrk 4 | head -10

# 2. Check service memory
systemctl status sinex-unified-collector | grep Memory

# 3. Emergency restart
sudo systemctl restart sinex-unified-collector

# 4. Adjust memory limits
systemctl set-property sinex-unified-collector.service MemoryMax=2G
```

### 7.4 Disk Space Emergency

```bash
# 1. Identify large files
du -sh /var/lib/postgresql/* | sort -hr | head -10
du -sh /var/lib/sinex/* | sort -hr | head -10

# 2. Clean journal logs
journalctl --vacuum-time=1d

# 3. Emergency event cleanup (CAUTION)
psql $DATABASE_URL -c "
  DELETE FROM raw.events
  WHERE ts_ingest < NOW() - INTERVAL '7 days'
    AND source LIKE 'sinex.metrics.%';"

# 4. Vacuum to reclaim space
psql $DATABASE_URL -c "VACUUM FULL raw.events;"
```

## 8. Security Procedures

### 8.1 Access Control

```bash
# Review database users
psql $DATABASE_URL -c "\du"

# Check active connections
psql $DATABASE_URL -c "
  SELECT 
    usename,
    application_name,
    client_addr,
    state
  FROM pg_stat_activity
  WHERE datname = 'sinex_dev';"

# Audit file permissions
find /var/lib/sinex -type f -perm /o+w -ls
find /etc/sinex -type f -perm /o+w -ls
```

### 8.2 Security Updates

```bash
# NixOS security updates
sudo nix flake update
sudo nixos-rebuild switch

# Service-specific updates
systemctl restart sinex-unified-collector
systemctl restart sinex-promo-worker
```

## 9. Capacity Planning

### 9.1 Growth Monitoring

```sql
-- Daily growth rate
WITH daily_counts AS (
  SELECT 
    DATE(ts_ingest) as date,
    COUNT(*) as event_count,
    pg_size_pretty(SUM(pg_column_size(payload))) as data_size
  FROM raw.events
  WHERE ts_ingest > NOW() - INTERVAL '30 days'
  GROUP BY DATE(ts_ingest)
)
SELECT 
  date,
  event_count,
  data_size,
  event_count - LAG(event_count) OVER (ORDER BY date) as daily_growth
FROM daily_counts
ORDER BY date DESC
LIMIT 7;

-- Storage projection
SELECT 
  'Current' as period,
  pg_size_pretty(pg_database_size('sinex_dev')) as size
UNION ALL
SELECT 
  '30 day projection',
  pg_size_pretty(
    pg_database_size('sinex_dev') + 
    (pg_database_size('sinex_dev') / 30 * 30)
  )
UNION ALL
SELECT 
  '1 year projection',
  pg_size_pretty(
    pg_database_size('sinex_dev') * 12
  );
```

### 9.2 Scaling Decisions

| Metric | Action Threshold | Scaling Action |
|--------|-----------------|----------------|
| Events/sec > 5000 | Sustained 1 hour | Add collector instances |
| Queue depth > 10k | Sustained 30 min | Add worker instances |
| DB size > 1TB | Projected 30 days | Enable partitioning |
| Query time > 1s | P95 queries | Add read replicas |

## 10. Integration with External Systems

### 10.1 Monitoring Integration

```bash
# Prometheus endpoints
curl http://localhost:2113/metrics  # Collector metrics
curl http://localhost:2114/metrics  # Worker metrics

# Grafana API
curl -H "Authorization: Bearer $GRAFANA_TOKEN" \
  http://localhost:3000/api/dashboards/db/sinex-overview
```

### 10.2 Backup Integration

```bash
# S3 backup upload
aws s3 cp /var/backup/postgresql/latest.dump \
  s3://backup-bucket/sinex/$(date +%Y/%m/%d)/

# Verify upload
aws s3 ls s3://backup-bucket/sinex/ --recursive
```

## Appendix A: Quick Command Reference

```bash
# Service control
systemctl {start|stop|restart|status} sinex-unified-collector
systemctl {start|stop|restart|status} sinex-promo-worker

# Logs
journalctl -fu sinex-unified-collector
journalctl -u sinex-promo-worker --since "1 hour ago"

# Database queries
psql $DATABASE_URL  # Interactive shell

# Health checks
systemctl is-active sinex-unified-collector
curl -s http://localhost:2113/health

# Configuration
cat /etc/sinex/collector.toml
sinex-collector --validate-config

# Metrics
curl -s http://localhost:2113/metrics | grep sinex_
```

## Appendix B: Troubleshooting Decision Tree

```
Service won't start?
├── Check PostgreSQL running?
│   ├── No → Start PostgreSQL
│   └── Yes → Check configuration valid?
│       ├── No → Fix configuration
│       └── Yes → Check permissions?
│           ├── No → Fix permissions
│           └── Yes → Check logs for errors

High resource usage?
├── CPU high?
│   ├── Yes → Check event volume
│   └── No → Memory high?
│       ├── Yes → Check for memory leaks
│       └── No → Disk I/O high?
│           ├── Yes → Check database queries
│           └── No → Network issue

Events not flowing?
├── Collector running?
│   ├── No → Start collector
│   └── Yes → Database accessible?
│       ├── No → Fix database connection
│       └── Yes → Check event sources
│           ├── Disabled → Enable sources
│           └── Enabled → Check source logs
```

## Support and Escalation

For issues beyond this manual:
1. Check comprehensive troubleshooting guide (TIM-TroubleshootingGuide)
2. Review architecture documentation (STAD.md)
3. Consult development team documentation (CLAUDE.md)
4. File GitHub issue with diagnostic information

Remember: Always document unusual events and solutions for future reference.