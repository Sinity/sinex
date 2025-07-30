# Metrics and Telemetry Architecture

## Overview

Sinex implements a hybrid approach for metrics and telemetry that combines:
1. **Real-time metrics** via Prometheus for operational monitoring
2. **Historical telemetry** stored as events for long-term analysis

This design avoids the overhead of storing every metric update while preserving both real-time visibility and historical data.

## Implementation

The `sinex-telemetry` crate provides comprehensive metrics and telemetry functionality. See the crate documentation for detailed usage:

- **Automatic instrumentation**: Via the `#[auto_metrics]` procedural macro
- **Telemetry accumulation**: Through the `TelemetryAccumulator` system
- **Event emission**: Using standard Sinex event infrastructure

## Architecture Summary

### Real-time Metrics
- Prometheus-compatible metrics exposed via `/metrics` endpoint
- Automatic function instrumentation
- Sub-minute visibility for operational monitoring
- Typically retained for 15-90 days in Prometheus

### Historical Telemetry
- Periodic summary events stored as regular Sinex events
- Source: `sinex.telemetry`
- Event types: `events.processed`, `operation.performance`, `resource.usage`, `system.resources`, `errors.summary`
- ~2000 events/day vs 400k+ individual metric updates

## Design Principles

### Per-Component Telemetry
Each component emits its own telemetry events for:
- Independent scaling and performance characteristics
- Easier debugging and issue identification
- Flexible aggregation in queries when needed
- Component lifecycle independence

### Event Categorization
Events are categorized by metric type rather than bundled together, enabling:
- Type-specific queries without parsing large payloads
- Different retention policies per metric type
- Efficient aggregation queries
- Clear event stream semantics

## Integration Points

### Satellites
Telemetry is integrated into `StreamProcessorContext` and emits events via the standard event sender.

### Services (ingestd, gateway)
Services integrate telemetry by:
- Creating a `TelemetryAccumulator` instance
- Forwarding events to appropriate channels
- Maintaining per-service granularity

## Usage Examples

For comprehensive examples and usage patterns, see:
- `crate/sinex-telemetry/src/telemetry.rs` - Core telemetry documentation
- `crate/sinex-telemetry/README.md` - Library usage guide
- `crate/sinex-telemetry/examples/telemetry_usage.rs` - Complete working example

## See Also

- [Event Schema](./event-schema.md) - General event structure
- [sinex-telemetry README](../../crate/sinex-telemetry/README.md) - Library usage
- [Monitoring Setup](../operations/monitoring.md) - Prometheus/Grafana configuration