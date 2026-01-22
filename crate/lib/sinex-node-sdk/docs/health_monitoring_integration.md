# Health Monitoring Integration Guide

## Overview

The health monitoring system provides automatic health tracking for all nodes via `HealthReporter`. This guide shows how to integrate it.

## Quick Start

### 1. Enable in LifecycleManager

```rust
use sinex_node_sdk::{HealthThresholds, LifecycleManager};

let lifecycle = LifecycleManager::new(service_name.clone())
    .with_heartbeat(Duration::from_secs(30))
    .with_health_monitoring(HealthThresholds::from_env()?);
```

### 2. Create HealthReporter Manually (Current Pattern)

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
