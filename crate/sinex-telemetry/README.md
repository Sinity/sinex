# Sinex Telemetry Library

A comprehensive metrics and telemetry library for the Sinex system, implementing a hybrid approach that combines real-time Prometheus metrics with long-term telemetry event storage.

## Overview

This library provides:

- **Real-time Metrics**: Prometheus-compatible metrics for operational monitoring
- **Historical Telemetry**: Event-based telemetry for long-term analysis
- **Automatic Instrumentation**: Procedural macros for zero-effort metrics
- **Per-Component Granularity**: Independent metrics for each service
- **Low Overhead**: ~2000 telemetry events/day vs 400k+ metric updates

## Architecture

### Real-time Metrics (Prometheus)

Metrics are collected in-memory and exposed via a `/metrics` endpoint:

- Function call counts, duration histograms, error rates
- Active request gauges
- System resource usage
- Custom business metrics

### Historical Telemetry (Events)

Periodic summary events are emitted as Sinex events:

- `events.processed` - Event throughput by type
- `operation.performance` - Latency percentiles
- `resource.usage` - Memory and CPU usage
- `system.resources` - System-wide metrics
- `errors.summary` - Error counts by type

## Quick Start

### Basic Setup

```rust
use sinex_telemetry::{init_metrics, TelemetryAccumulator};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    // Initialize metrics system
    init_metrics().await;
    
    // Create event channel
    let (tx, rx) = mpsc::channel(100);
    
    // Set up telemetry
    let telemetry = TelemetryAccumulator::new("my-service")
        .with_event_sender(tx)
        .with_interval(Duration::from_secs(300)); // 5 minutes
    
    // Start background emission
    telemetry.spawn_emitter();
}
```

### Automatic Instrumentation

```rust
use sinex_telemetry::auto_metrics;

#[auto_metrics]
async fn process_request(data: &str) -> Result<Response, Error> {
    // Automatically tracks:
    // - function_calls_total
    // - function_duration_seconds
    // - function_errors_total
    // - function_active_calls
    
    // Your code here
}
```

### Manual Telemetry

```rust
// Record event processing
telemetry.record_event_processed("file.created", 12.5);

// Record operation performance
telemetry.record_operation_latency("scan_directory", 145.2);

// Record resource usage
telemetry.record_resource_usage(memory_mb, cpu_percent);

// Record errors
telemetry.record_error("io_error");
```

## Integration Examples

### Satellite Integration

```rust
// In StreamProcessorContext initialization
let telemetry = TelemetryAccumulator::new(&service_name)
    .with_event_sender(event_sender)
    .with_interval(Duration::from_secs(300));

set_global_telemetry(telemetry.clone()).await;
telemetry.spawn_emitter();
```

### Service Integration (Gateway, Ingestd)

```rust
// Create telemetry with event forwarding
let (tx, mut rx) = mpsc::channel(100);

// Forward events to ingestd
tokio::spawn(async move {
    let mut batch = Vec::new();
    while let Some(event) = rx.recv().await {
        batch.push(event);
        if batch.len() >= 10 {
            ingest_client.ingest_batch(&batch).await?;
            batch.clear();
        }
    }
});

let telemetry = TelemetryAccumulator::new("gateway")
    .with_event_sender(tx);
```

## Querying Telemetry

### Event Throughput

```sql
SELECT 
    payload->>'component' as component,
    SUM((payload->>'count')::int) as total_events,
    payload->'by_type' as event_breakdown
FROM core.events
WHERE source = 'sinex.telemetry' 
  AND event_type = 'events.processed'
  AND ts_ingest > NOW() - INTERVAL '1 hour'
GROUP BY component, payload->'by_type';
```

### Performance Analysis

```sql
SELECT 
    payload->>'component' as component,
    payload->>'operation' as operation,
    (payload->'duration_ms'->>'p50')::float as median_ms,
    (payload->'duration_ms'->>'p95')::float as p95_ms,
    (payload->'duration_ms'->>'p99')::float as p99_ms
FROM core.events
WHERE source = 'sinex.telemetry'
  AND event_type = 'operation.performance'
ORDER BY p99_ms DESC;
```

### Resource Usage Trends

```sql
SELECT 
    date_trunc('hour', ts_ingest) as hour,
    payload->>'component' as component,
    AVG((payload->'memory_mb'->>'avg')::float) as avg_memory,
    MAX((payload->'cpu_percent'->>'peak')::float) as peak_cpu
FROM core.events
WHERE source = 'sinex.telemetry'
  AND event_type = 'resource.usage'
GROUP BY hour, component
ORDER BY hour, component;
```

## Configuration

### Environment Variables

- `PROMETHEUS_PUSH_GATEWAY` - Optional Prometheus push gateway URL
- `METRICS_PORT` - Port for metrics endpoint (default: 9090)

### Telemetry Intervals

Recommended intervals:
- System resources: 1 minute
- Component metrics: 5 minutes
- Error summaries: 5 minutes (or immediate for critical errors)

## Performance Impact

The hybrid approach minimizes overhead:

- Prometheus metrics: In-memory, negligible impact
- Telemetry events: ~2000 events/day per component
- Network: Batched transmission reduces load
- Storage: 100x reduction vs storing every metric

## Examples

See the `examples/` directory for complete examples:

- `telemetry_usage.rs` - Comprehensive telemetry demonstration
- `basic_usage.rs` - Simple metrics setup

## License

Part of the Sinex project.