# Sinex Quick Wins Checklist

This document lists actionable improvements that can be completed in 1-2 days, providing significant value with minimal effort.

## Database & Storage (0.5-1 day each)

### ✅ Enable TimescaleDB Compression
**Impact**: 90% storage reduction  
**Implementation**:
```sql
-- Add to migration
ALTER TABLE raw.events SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'source',
  timescaledb.compress_orderby = 'ts_ingest DESC'
);

SELECT add_compression_policy('raw.events', INTERVAL '7 days');
```

### ✅ Add Event Count Metrics
**Impact**: Operational visibility  
**Implementation**:
```sql
-- Continuous aggregate for event counts
CREATE MATERIALIZED VIEW event_counts_hourly
WITH (timescaledb.continuous) AS
SELECT 
  time_bucket('1 hour', ts_ingest) AS hour,
  source,
  event_type,
  COUNT(*) as event_count
FROM raw.events
GROUP BY hour, source, event_type;
```

## Event Sources (1-2 days each)

### ✅ Enable Ready Event Sources
**Impact**: 3x more event types captured  
**Implementation**:
1. Enable in `config.toml`:
   ```toml
   [event_sources.shell_atuin]
   enabled = true
   
   [event_sources.shell_history]
   enabled = true
   paths = ["~/.bash_history", "~/.zsh_history"]
   
   [event_sources.shell_recording]
   enabled = true
   ```
2. Test thoroughly: `just test-integration`
3. Deploy: `sudo systemctl restart sinex-update`

### ✅ Add Basic D-Bus Monitoring
**Impact**: System-wide event visibility  
**Implementation**:
```rust
// Use zbus crate (already in dependencies)
use zbus::{Connection, MessageStream};

pub struct DbusEventSource {
    connection: Connection,
}

impl EventSource for DbusEventSource {
    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        let mut stream = MessageStream::from(&self.connection);
        while let Some(msg) = stream.next().await {
            // Convert D-Bus message to RawEvent
            let event = self.dbus_to_event(msg?)?;
            tx.send(event).await?;
        }
        Ok(())
    }
}
```

## Reliability & Operations (1 day each)

### ✅ Implement Dead Letter Queue
**Impact**: Never lose events due to processing errors  
**Implementation**:
```sql
-- Add table
CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    event_id ULID NOT NULL,
    error_message TEXT NOT NULL,
    retry_count INT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT now(),
    FOREIGN KEY (event_id) REFERENCES raw.events(id)
);

-- Add to worker error handling
if let Err(e) = process_event(&event).await {
    insert_dead_letter(&event.id, &e.to_string()).await?;
}
```

### ✅ Add Prometheus Metrics
**Impact**: Production monitoring capability  
**Implementation**:
```rust
use metrics::{counter, histogram, gauge};
use axum::extract::State;

// In collector
counter!("sinex_events_total", 1, "source" => source.name());
histogram!("sinex_event_size_bytes", payload.len() as f64);
gauge!("sinex_active_sources", sources.len() as f64);

// Add /metrics endpoint
async fn metrics_handler() -> String {
    let encoder = TextEncoder::new();
    encoder.encode_to_string(&metrics::registry()).unwrap()
}
```

## User Experience (1-2 days each)

### ✅ Add Event Export Command
**Impact**: Data portability  
**Implementation**:
```python
# In cli/exo.py
@cli.command()
@click.option('--format', type=click.Choice(['csv', 'json', 'jsonl']), default='jsonl')
@click.option('--output', '-o', type=click.File('w'), default='-')
def export(format, output, **filters):
    """Export events in various formats"""
    events = query_events(**filters)
    
    if format == 'csv':
        writer = csv.DictWriter(output, fieldnames=['id', 'source', 'event_type', 'ts_orig', 'payload'])
        writer.writeheader()
        for event in events:
            writer.writerow(event)
    elif format == 'jsonl':
        for event in events:
            output.write(json.dumps(event) + '\n')
```

### ✅ Add Event Stats Command
**Impact**: Quick system overview  
**Implementation**:
```rust
// Add to CLI
pub async fn stats_command(pool: &PgPool) -> Result<()> {
    let stats = sqlx::query!(
        r#"
        SELECT 
            source,
            event_type,
            COUNT(*) as count,
            MIN(ts_orig) as earliest,
            MAX(ts_orig) as latest
        FROM raw.events
        GROUP BY source, event_type
        ORDER BY count DESC
        "#
    )
    .fetch_all(pool)
    .await?;
    
    // Pretty print table
    println!("{:<20} {:<30} {:>10} {:<20} {:<20}", 
             "Source", "Event Type", "Count", "Earliest", "Latest");
    for stat in stats {
        println!("{:<20} {:<30} {:>10} {:<20} {:<20}", 
                 stat.source, stat.event_type, stat.count, 
                 stat.earliest, stat.latest);
    }
    Ok(())
}
```

## Testing & Development (0.5-1 day each)

### ✅ Add Integration Test for Each Event Source
**Impact**: Prevent regressions  
**Implementation**:
```rust
#[sinex_test]
async fn test_dbus_source_integration(ctx: TestContext) -> TestResult {
    // Start D-Bus source
    let source = DbusEventSource::new(test_config()).await?;
    
    // Trigger test event
    trigger_dbus_test_event().await?;
    
    // Verify capture
    ctx.wait_for_events(|events| {
        events.iter().any(|e| e.source == "dbus" && e.event_type == "test.signal")
    }).await?;
    
    Ok(())
}
```

### ✅ Add Performance Benchmarks
**Impact**: Track performance regressions  
**Implementation**:
```rust
use criterion::{criterion_group, criterion_main, Criterion};

fn bench_event_insertion(c: &mut Criterion) {
    c.bench_function("insert_1000_events", |b| {
        b.iter(|| {
            runtime.block_on(async {
                insert_batch_events(&pool, 1000).await.unwrap();
            })
        })
    });
}

criterion_group!(benches, bench_event_insertion);
criterion_main!(benches);
```

## Documentation (0.5 day each)

### ✅ Add Event Source Development Guide
**Impact**: Lower barrier for contributors  
**Implementation**: Create `docs/guides/adding-event-source.md` with:
- Step-by-step tutorial
- EventSource trait explanation
- Testing requirements
- Common pitfalls

### ✅ Add Troubleshooting Guide
**Impact**: Faster problem resolution  
**Implementation**: Create `docs/troubleshooting.md` with:
- Common error messages and solutions
- Debug logging configuration
- Performance tuning tips
- FAQ section

## Selection Criteria

Choose quick wins based on:
1. **Current Pain Points**: What's causing the most friction?
2. **Dependencies**: What unblocks other work?
3. **Skills Match**: What aligns with your expertise?
4. **Time Available**: Can you complete it in one session?

## Implementation Tips

1. **Test First**: Write tests before implementation
2. **Small Commits**: Make atomic, reviewable changes
3. **Document Changes**: Update relevant documentation
4. **Verify Locally**: Test thoroughly before deployment
5. **Monitor Impact**: Check metrics after deployment

Start with the quick win that excites you most - momentum builds success!