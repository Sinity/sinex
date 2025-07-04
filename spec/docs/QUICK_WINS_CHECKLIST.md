# Sinex Quick Wins Checklist

*High-impact improvements achievable in 1-2 days each*

## Infrastructure Quick Wins

### ☐ Enable TimescaleDB Compression (0.5 days)
**Impact**: 90% storage reduction  
**Files to modify**:
- `crate/sinex-db/migrations/0003_enable_compression.sql` (create)

```sql
-- Enable compression on hypertable
ALTER TABLE raw.events SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'source',
  timescaledb.compress_orderby = 'id DESC'
);

-- Add compression policy (compress chunks older than 7 days)
SELECT add_compression_policy('raw.events', INTERVAL '7 days');
```

**Verification**:
```bash
just psql -c "SELECT * FROM timescaledb_information.compression_settings;"
```

### ☐ Implement Dead Letter Queue (1-2 days)
**Impact**: Prevent data loss from failed events  
**Files to modify**:
- `crate/sinex-db/migrations/0004_dead_letter_queue.sql` (create)
- `crate/sinex-db/src/queries/dead_letter.rs` (create)
- `crate/sinex-collector/src/pipeline/error_handler.rs` (modify)

```sql
CREATE TABLE IF NOT EXISTS raw.dead_letter_queue (
    id UUID PRIMARY KEY DEFAULT sinex_fn.gen_ulid(),
    original_event JSONB NOT NULL,
    error_message TEXT NOT NULL,
    error_type VARCHAR(100) NOT NULL,
    retry_count INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_retry_at TIMESTAMPTZ
);

CREATE INDEX idx_dlq_created ON raw.dead_letter_queue(created_at);
CREATE INDEX idx_dlq_retry ON raw.dead_letter_queue(retry_count, last_retry_at);
```

### ☐ Add Connection Pool Monitoring (0.5 days)
**Impact**: Visibility into database performance  
**Files to modify**:
- `crate/sinex-db/src/pool.rs` (add metrics)

```rust
// Add to pool creation
let pool = pool_config
    .after_connect(|conn, _| {
        Box::pin(async move {
            POOL_CONNECTIONS.inc();
            Ok(())
        })
    })
    .build()?;
```

## Event Source Quick Wins

### ☐ Enable Ready Event Sources (1 day)
**Impact**: 3x more event types immediately available  
**Files to modify**:
- `config/sinex.toml` or NixOS configuration

```toml
[event_sources.shell_atuin]
enabled = true

[event_sources.shell_history]
enabled = true
paths = ["~/.bash_history", "~/.zsh_history"]

[event_sources.shell_recording]
enabled = true
watch_dir = "~/recordings"
```

**Testing**:
```bash
just unified --dry-run  # Verify sources load
RUST_LOG=debug just unified  # Check event flow
```

### ☐ Add D-Bus System Monitoring (2 days)
**Impact**: System-wide event visibility  
**Files to create**:
- `crate/sinex-events-system/src/dbus.rs`

```rust
use zbus::{Connection, dbus_proxy};
use sinex_core::{EventSource, EventSender, RawEventBuilder};

pub struct DBusMonitor {
    connection: Connection,
}

impl EventSource for DBusMonitor {
    // Monitor system bus for signals
    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        let mut stream = self.connection
            .add_match("type='signal'")
            .await?
            .msg_stream();
            
        while let Some(msg) = stream.next().await {
            let event = RawEventBuilder::new(
                "dbus",
                "signal.received",
                json!({
                    "sender": msg.sender(),
                    "path": msg.path(),
                    "interface": msg.interface(),
                    "member": msg.member(),
                })
            ).build();
            
            tx.send(event).await?;
        }
        Ok(())
    }
}
```

### ☐ Add Journald Streaming (2 days)
**Impact**: Complete system logging capture  
**Files to create**:
- `crate/sinex-events-system/src/journald.rs`

```rust
use systemd::journal::{Journal, JournalSeek};

pub struct JournaldMonitor {
    journal: Journal,
}

impl EventSource for JournaldMonitor {
    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        self.journal.seek(JournalSeek::Tail)?;
        
        loop {
            match self.journal.next_entry() {
                Ok(Some(entry)) => {
                    let event = RawEventBuilder::new(
                        "journald",
                        "entry.written",
                        serde_json::to_value(&entry)?
                    ).build();
                    
                    tx.send(event).await?;
                }
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}
```

## Processing Quick Wins

### ☐ Add Basic Event Enrichment (1 day)
**Impact**: Contextual data for all events  
**Files to modify**:
- `crate/sinex-worker/src/enrichment.rs` (create)

```rust
pub async fn enrich_event(event: &RawEvent) -> Result<JsonValue> {
    let mut enriched = event.payload.clone();
    
    // Add common enrichments
    enriched["enriched_at"] = json!(Utc::now());
    enriched["hostname"] = json!(hostname::get()?.to_string_lossy());
    
    // Source-specific enrichments
    match event.source.as_str() {
        "fs" => enrich_filesystem_event(&mut enriched)?,
        "shell.kitty" => enrich_command_event(&mut enriched)?,
        _ => {}
    }
    
    Ok(enriched)
}
```

### ☐ Implement Promotion Worker (1 day)
**Impact**: Move validated events to domain tables  
**Files to modify**:
- `crate/sinex-worker/src/promotion.rs`

```rust
pub async fn promote_events(pool: &PgPool) -> Result<()> {
    let batch = claim_work_batch(pool, 100).await?;
    
    for work_item in batch {
        let event = get_event_by_id(pool, &work_item.event_id).await?;
        
        match promote_single_event(pool, &event).await {
            Ok(_) => complete_work(pool, &work_item.id).await?,
            Err(e) => fail_work(pool, &work_item.id, &e.to_string()).await?,
        }
    }
    
    Ok(())
}
```

## Monitoring Quick Wins

### ☐ Add Prometheus Metrics (1 day)
**Impact**: Production observability  
**Files to modify**:
- `crate/sinex-core/src/metrics.rs` (create)
- `crate/sinex-collector/src/main.rs` (add endpoint)

```rust
use prometheus::{IntCounter, register_int_counter};

lazy_static! {
    pub static ref EVENTS_INGESTED: IntCounter = 
        register_int_counter!("sinex_events_ingested_total", "Total events ingested")
        .unwrap();
    
    pub static ref EVENTS_FAILED: IntCounter =
        register_int_counter!("sinex_events_failed_total", "Total events failed")
        .unwrap();
}

// Add to collector
async fn metrics_endpoint() -> String {
    prometheus::TextEncoder::new()
        .encode_to_string(&prometheus::gather())
        .unwrap()
}
```

### ☐ Add Health Check Endpoint (0.5 days)
**Impact**: Service monitoring  
**Files to modify**:
- `crate/sinex-collector/src/health.rs` (create)

```rust
#[derive(Serialize)]
struct HealthStatus {
    status: &'static str,
    database: bool,
    sources: Vec<SourceStatus>,
    uptime_seconds: u64,
}

async fn health_check(State(app): State<AppState>) -> Json<HealthStatus> {
    let db_healthy = sqlx::query("SELECT 1")
        .fetch_one(&app.pool)
        .await
        .is_ok();
    
    Json(HealthStatus {
        status: if db_healthy { "healthy" } else { "unhealthy" },
        database: db_healthy,
        sources: app.source_statuses(),
        uptime_seconds: app.start_time.elapsed().as_secs(),
    })
}
```

## Tooling Quick Wins

### ☐ Improve CLI Query Tool (1 day)
**Impact**: Better user experience  
**Files to modify**:
- `cli/exo.py`

```python
@cli.command()
@click.option('--source', help='Filter by source')
@click.option('--type', help='Filter by event type')
@click.option('--since', help='Events since (e.g., "1 hour ago")')
@click.option('--format', type=click.Choice(['json', 'table', 'csv']), default='table')
def query(source, type, since, format):
    """Query events with rich filtering"""
    query = build_query(source, type, since)
    results = execute_query(query)
    
    if format == 'table':
        print_table(results)
    elif format == 'csv':
        print_csv(results)
    else:
        print_json(results)
```

### ☐ Add Event Export (1 day)
**Impact**: Data portability  
**Files to create**:
- `crate/sinex-cli/src/export.rs`

```rust
pub async fn export_events(
    pool: &PgPool,
    filter: EventFilter,
    format: ExportFormat,
    output: &Path,
) -> Result<()> {
    let events = query_events(pool, filter).await?;
    
    match format {
        ExportFormat::Json => export_json(&events, output)?,
        ExportFormat::Csv => export_csv(&events, output)?,
        ExportFormat::Parquet => export_parquet(&events, output)?,
    }
    
    Ok(())
}
```

## Deployment Quick Wins

### ☐ Add Backup Automation (1 day)
**Impact**: Data safety  
**Files to create**:
- `nixos/module/backup.nix`

```nix
systemd.services.sinex-backup = {
  description = "Backup Sinex database";
  startAt = "daily";
  script = ''
    ${pkgs.postgresql}/bin/pg_dump \
      -d sinex \
      -f /backup/sinex-$(date +%Y%m%d).sql \
      --compress=9
    
    # Keep only last 7 days
    find /backup -name "sinex-*.sql.gz" -mtime +7 -delete
  '';
};
```

### ☐ Add Log Rotation (0.5 days)
**Impact**: Prevent disk fill  
**Files to modify**:
- `nixos/module/default.nix`

```nix
systemd.services.sinex-unified-collector = {
  serviceConfig = {
    StandardOutput = "journal";
    StandardError = "journal";
    # Systemd handles rotation automatically
  };
};

# Or add custom rotation
services.logrotate.settings.sinex = {
  files = "/var/log/sinex/*.log";
  rotate = 7;
  compress = true;
  daily = true;
};
```

## Validation Checklist

After implementing each quick win:

1. **Test Locally**:
   ```bash
   just test-all
   cargo check --workspace
   ```

2. **Verify in Dev**:
   ```bash
   just unified --dry-run
   just migrate
   ```

3. **Check Metrics** (if applicable):
   ```bash
   curl localhost:9090/metrics | grep sinex
   ```

4. **Update Documentation**:
   - Add to relevant `spec/` files
   - Update CLAUDE.md if behavior changes

5. **Commit with Context**:
   ```bash
   git add -A
   git commit -m "feat: implement [feature] for [impact]

   - Added [component]
   - Improves [metric] by [amount]
   
   Co-authored-by: Claude <noreply@anthropic.com>"
   ```

## Next Steps After Quick Wins

1. Review metrics from implemented monitoring
2. Analyze captured events for patterns
3. Prioritize medium-effort tasks based on actual usage
4. Consider user feedback before major features
5. Plan for horizontal scaling if needed