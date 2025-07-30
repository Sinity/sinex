# Phase 3: NATS JetStream Migration Status

## Overview

Phase 3 focuses on replacing Redis Streams with NATS JetStream as the primary message bus for event distribution in the Sinex system. This migration provides better performance, built-in persistence, and more sophisticated stream processing capabilities.

## Completed Components

### 1. NATS Integration Module (`sinex-nats`)
- ✅ Created new crate with proper workspace integration
- ✅ Defined comprehensive error types and conversions
- ✅ Integrated with Figment configuration system

### 2. NATS Client (`client.rs`)
- ✅ Connection pooling with automatic reconnection
- ✅ Authentication support (UserPassword, Token, NKey)
- ⚠️  JWT authentication placeholder (complex implementation deferred)
- ✅ Basic TLS support
- ✅ Event callback handlers for connection state monitoring

### 3. Configuration (`config.rs`)
- ✅ Comprehensive NatsConfig with all connection options
- ✅ JetStreamConfig with stream and consumer defaults
- ✅ Retention and discard policies
- ✅ Humantime serialization for duration fields
- ✅ Test configuration helpers

### 4. JetStream Integration (`jetstream.rs`)
- ✅ Context wrapper for JetStream operations
- ✅ Stream CRUD operations (create, get, delete, list)
- ✅ Consumer CRUD operations
- ✅ Publishing with automatic ACK handling
- ⚠️  Account info disabled (API not available in async-nats 0.37)

### 5. Stream Management (`streams.rs`)
- ✅ Predefined stream configurations:
  - SINEX_RAW_EVENTS - Raw event stream (30 day retention)
  - SINEX_PROCESSED_EVENTS - Canonicalized events (90 day retention)
  - SINEX_METRICS - System telemetry (7 day retention)
  - SINEX_ALERTS - System notifications (30 day retention)
  - SINEX_SATELLITE_CONTROL - Satellite coordination (1 hour retention)
- ✅ Subject naming conventions for event routing
- ✅ Stream verification and statistics

### 6. Publisher (`publisher.rs`)
- ✅ Event publishing with structured headers
- ✅ Message buffering for failed publishes
- ✅ Metric and alert publishing helpers
- ✅ Retry logic with buffer management
- ✅ Proper ULID handling for event IDs

### 7. Consumer (`consumer.rs`)
- ✅ Durable consumer configuration
- ✅ Message processing with ACK/NAK
- ✅ Batch processing support
- ✅ Graceful shutdown handling
- ⚠️  Stream API differences in async-nats 0.37

## Known Issues

### API Compatibility
The async-nats 0.37 API has significant differences from newer versions:
- `connect_timeout` → `connection_timeout`
- No `Reconnected` event variant
- Different consumer streaming API
- `stream()` method instead of `messages()` or `fetch()`
- Type mismatches in various places

### Compilation Errors (18 remaining)
1. Timeout option type mismatches
2. Missing methods on consumer types
3. Header value type conversions
4. Borrowed data escaping in async closures
5. Info struct cloning issues

## Migration Tasks Remaining

### Immediate Tasks
1. **Fix compilation errors** - Resolve API compatibility issues
2. **Update to newer async-nats** - Consider upgrading to 0.42+ for better API
3. **Complete consumer implementation** - Fix streaming API usage

### Integration Tasks
1. **Update satellite SDK** - Add NATS support alongside Redis
2. **Migrate ingestd** - Replace Redis publishing with NATS
3. **Update Figment configs** - Add NATS configuration to all services
4. **Create health checks** - NATS connection monitoring
5. **Test infrastructure** - NATS test containers and helpers

### Documentation Tasks
1. **Migration guide** - Step-by-step Redis to NATS migration
2. **Configuration reference** - All NATS options explained
3. **Performance tuning** - Stream and consumer optimization

## Architecture Benefits

### Why NATS JetStream?
1. **Built-in persistence** - No separate persistence layer needed
2. **Exactly-once semantics** - Better than Redis Streams
3. **Stream templates** - Easier multi-tenant support
4. **Work queues** - Built-in load balancing
5. **Mirrors and sources** - Cross-region replication

### Event Flow
```
Satellites → NATS Publisher → JetStream → NATS Consumers → Processing
                   ↓                             ↑
              Buffer/Retry ←─────────────────────┘
```

## Next Steps

1. **Resolve compilation errors** - Focus on API compatibility
2. **Create minimal working example** - Test basic pub/sub flow
3. **Performance benchmarks** - Compare with Redis Streams
4. **Gradual migration** - Run NATS alongside Redis initially
5. **Monitor and optimize** - Tune based on production metrics

## Configuration Example

```toml
[nats]
servers = ["nats://localhost:4222"]
client_name = "sinex-ingestd"
connection_timeout = "10s"
request_timeout = "30s"

[nats.jetstream]
enabled = true

[nats.jetstream.default_stream]
max_age = "7d"
max_msgs = -1  # unlimited
replicas = 3
retention = "limits"

[nats.jetstream.default_consumer]
ack_wait = "30s"
max_deliver = 3
max_ack_pending = 1000
replay = "instant"
```

## Summary

Phase 3 has successfully created the foundation for NATS JetStream integration. The core components are in place but require additional work to resolve API compatibility issues with async-nats 0.37. Once compilation errors are resolved, the system will be ready for integration testing and gradual migration from Redis Streams.