# Health Monitoring Integration Guide

## Overview

The health monitoring system provides **automatic health tracking for all nodes** via `HealthReporter`. Health monitoring is now enabled by default for all `SimpleNode` implementations.

## Automatic Integration (Preferred)

### SimpleNode - Auto-Enabled

**All `SimpleNode` implementations automatically get health monitoring** with zero configuration required!

When a `SimpleNodeWrapper` initializes in service mode with NATS available:
1. ✅ HealthReporter is automatically created
2. ✅ Success/error tracking happens on every event
3. ✅ Status checks occur every 100 events
4. ✅ health.status events emit automatically on status changes

**No code changes needed** - health monitoring "just works" for:
- All automata (e.g., `sinex-health-automaton`, `sinex-analytics-automaton`)
- All processors using SimpleNode pattern

### Configuration (Optional)

Control via environment variables:

```bash
# Disable health monitoring (default: enabled)
SINEX_HEALTH_MONITORING_ENABLED=false

# Error rate thresholds
SINEX_HEALTH_ERROR_RATE_DEGRADED=0.05  # 5% errors → degraded (default)
SINEX_HEALTH_ERROR_RATE_FAILED=0.20    # 20% errors → failed (default)

# Sliding window for error rate calculation
SINEX_HEALTH_WINDOW_SECONDS=300        # 5 minutes (default)
```

## Manual Integration (Legacy)

### For Non-SimpleNode or Custom Use Cases

```rust
use sinex_node_sdk::{HealthReporter, HealthThresholds, self_observation::SelfObserver};
use std::sync::Arc;

// Create observer (requires NATS client)
let observer = Arc::new(SelfObserver::new(nats_client.clone(), config));

// Create health reporter
let health_reporter = Arc::new(HealthReporter::new(
    "my-service".to_string(),
    observer,
    HealthThresholds::default(),
));
```

### 3. Track Events

```rust
// In your process loop:
match processor.process(event).await {
    Ok(outputs) => {
        health_reporter.record_success();
        // ... handle outputs
    }
    Err(e) => {
        health_reporter.record_error(&e);
        // ... error handling
    }
}

// Periodic health check (every 100 events):
if event_count % 100 == 0 {
    health_reporter.check_and_emit().await?;
}
```

## Configuration

### Environment Variables

```bash
# Error rate thresholds
SINEX_HEALTH_ERROR_RATE_DEGRADED=0.05  # 5% errors → degraded
SINEX_HEALTH_ERROR_RATE_FAILED=0.20    # 20% errors → failed

# Sliding window for error rate calculation
SINEX_HEALTH_WINDOW_SECONDS=300        # 5 minutes
```

### Thresholds

```rust
use sinex_node_sdk::HealthThresholds;

let thresholds = HealthThresholds {
    error_rate_degraded: 0.05,  // 5%
    error_rate_failed: 0.20,     // 20%
    window_seconds: 300,         // 5 minutes
};
```

## Health Status Transitions

The system automatically emits `health.status` events when status changes:

- **Healthy** → **Degraded**: Error rate ≥ 5%
- **Degraded** → **Failed**: Error rate ≥ 20%
- **Failed** → **Degraded**: Error rate < 20%
- **Degraded** → **Healthy**: Error rate < 5%

## Future Enhancement: Auto-Integration

Phase 3 will add automatic integration in `sinex-processor-runtime` where HealthReporter is created automatically for all service-mode nodes.

```rust
// Future: Automatic in processor runtime
// let health_reporter = lifecycle.health_reporter().unwrap();
// Automatically wired into process loop
```

## See Also

- `crate/lib/sinex-node-sdk/src/health_reporter.rs` - Implementation
- `crate/nodes/sinex-health-automaton/` - Health aggregation service
