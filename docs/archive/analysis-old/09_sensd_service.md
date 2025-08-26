# Area 9: Sensd Service Analysis

## Overview

The Sensd service is designed as the universal acquisition daemon for the Sinex system, responsible for managing sensor jobs and capturing source material. This analysis examines the implementation completeness, architectural violations, and missing functionality.

## Architecture Design

### Core Components

1. **SensdService** (`src/service.rs`) - Main service orchestrator
2. **JobManager** (`src/job_manager.rs`) - Manages sensor job lifecycle
3. **TemporalLedger** (`src/temporal_ledger.rs`) - Records precise capture timestamps
4. **Sensors** (`src/sensors/`) - Sensor implementations for different data sources
5. **gRPC Server** (`src/grpc_server.rs`) - API for job management and material streaming

### Database Schema

The service operates on two primary tables:
- `raw.sensor_jobs` - Declarative job specifications (the "Spec")
- `raw.sensor_states` - Operational state tracking (the "Status")

This follows a standard Kubernetes-style controller/operator pattern.

## Implementation Status

### ✅ Completed Components

#### 1. Schema Design
- **Status**: Fully implemented
- **Quality**: Excellent
- Complete controller/operator pattern with jobs and states tables
- Proper foreign key relationships and constraints
- Comprehensive indexing for performance

#### 2. gRPC Server
- **Status**: Fully implemented 
- **Quality**: Good
- Complete MaterialSliceStream implementation
- Job creation and status endpoints
- Direct capture with acknowledgment
- Proper error handling and validation

#### 3. Job Manager
- **Status**: Mostly complete
- **Quality**: Good
- Concurrent job processing with limits
- Status tracking and error handling
- Proper cleanup of completed jobs
- Uses database locking for job assignment

#### 4. Basic Sensor Patterns
- **Status**: Implemented
- **Quality**: Good
- Batched pull sensor for accumulating events
- Replace snapshot sensor for full state updates
- Multi-file sensor for directory processing
- All with proper rotation support

### ⚠️ Partially Implemented Components

#### 1. Sensor Implementations
- **AppendStreamSensor**: Implemented but limited
  - ✅ Unix socket reading
  - ✅ Material rotation
  - ✅ Proper ledger recording
  - ❌ Missing: File tailing, database monitoring, other append sources

- **TreeWatchSensor**: Basic implementation
  - ✅ File system watching with notify
  - ✅ Security validation
  - ✅ Path validation
  - ❌ Missing: Recursive monitoring, pattern filtering, performance optimization

#### 2. Temporal Ledger
- **Status**: Basic implementation
- **Issues**: 
  - Limited to in-memory buffering
  - No persistence across restarts
  - Missing background worker implementation
  - No proper error recovery

### ❌ Missing Core Functionality

#### 1. Sensor Guard Enforcement
The `sensor_guard.rs` module defines compile-time guards to prevent satellites from acting as sensors directly, but:
- **Status**: Not enforced in practice
- **Issue**: Satellites still implement direct capture patterns
- **Impact**: Architectural violations throughout the codebase

#### 2. Material Rotation
- **MaterialRotationManager**: Declared but incomplete implementation
- Missing rotation policies and zero-gap guarantees
- No proper overlap handling for continuous streams

#### 3. Sensor Discovery and Registration
- No automatic sensor discovery
- No sensor health monitoring
- No sensor capability reporting

#### 4. Error Recovery and Resilience
- Limited error handling in job processing
- No retry logic for failed jobs
- No circuit breaker patterns

## Architectural Violations

### 1. Satellites Acting as Sensors

**Current State**: Multiple satellites directly capture source material instead of using sensd:

```rust
// In sinex-fs-watcher/src/unified_processor.rs
// This should NOT exist - should use sensd TreeWatch jobs
impl StatefulStreamProcessor for FilesystemProcessor {
    async fn scan(&self, args: ScanArgs) -> SatelliteResult<ScanReport> {
        // Direct filesystem scanning
    }
}
```

**Expected Pattern**: Satellites should:
1. Submit sensor jobs to sensd
2. Consume MaterialSliceStream from sensd
3. Convert material slices to events

### 2. Direct Event Generation

Satellites are generating events directly from raw sources instead of from sensd's captured material:

```rust
// VIOLATION: Direct event creation from filesystem
let event = RawEvent::from_material(
    EventSource::from("filesystem"),
    EventType::from("file_created"),
    payload,
    material_id, // But material_id wasn't captured via sensd!
    offset,
);
```

### 3. Bypassing Material Stream

Satellites implement their own source monitoring instead of using sensd's sensor jobs:

```rust
// VIOLATION: Direct file watching in satellite
let mut watcher = notify::recommended_watcher(move |res| {
    // This should be in sensd TreeWatchSensor
})?;
```

## Integration Patterns

### ✅ Correct Pattern (Terminal Satellite)

The terminal satellite shows the intended architecture:

```rust
// 1. Submit job to sensd
let job_id = processor.submit_atuin_job(db_path).await?;

// 2. Monitor for completed materials
let completed_jobs = query_completed_terminal_jobs().await?;

// 3. Process material slices
for material_id in completed_materials {
    processor.process_material(material_id).await?;
}

// 4. Convert slices to events
async fn slice_to_events(&self, slice: MaterialSlice) -> Vec<RawEvent> {
    // Events from sensd's captured material
}
```

### ❌ Incorrect Pattern (Most Satellites)

```rust
// Direct source monitoring - ARCHITECTURAL VIOLATION
impl StatefulStreamProcessor for SatelliteProcessor {
    async fn sensor_mode(&self) -> SatelliteResult<()> {
        // Should NOT directly monitor sources
        let mut source_monitor = create_source_monitor();
        while let Some(data) = source_monitor.next().await {
            // Direct event generation
            self.emit_event(create_event_from_source(data)).await?;
        }
    }
}
```

## Missing Sensor Implementations

### 1. Database Sensors
- SQLite monitoring (Atuin, browser databases)
- PostgreSQL logical replication
- Change data capture for various databases

### 2. Network Sensors  
- Socket monitoring
- HTTP endpoint polling
- Message queue consumption

### 3. System Sensors
- D-Bus monitoring
- systemd journal streaming
- Process monitoring

### 4. Application Sensors
- Browser data extraction
- Email client integration
- Chat application monitoring

## Data Acquisition Patterns

### Current Implementation Gaps

1. **Continuous Streams**: AppendStreamSensor handles basic socket reading but misses:
   - File tailing with rotation
   - Database transaction logs
   - Message queue consumption
   - Log file monitoring

2. **Batch Processing**: Limited support for:
   - Large file processing
   - Database bulk exports
   - Archive processing

3. **Real-time Monitoring**: TreeWatchSensor needs:
   - Better performance for large directories
   - Pattern-based filtering
   - Recursive monitoring with limits

## Event Routing and Distribution

### MaterialSliceStream

The gRPC streaming interface is well-implemented:
- Proper pagination with offset tracking
- End-of-material signaling
- Error handling in streams
- Material metadata access

### Integration Issues

1. **Discovery**: No automatic discovery of available materials
2. **Filtering**: Limited filtering capabilities in material queries
3. **Replay**: No replay capabilities for historical materials
4. **Subscription**: No pub/sub pattern for new materials

## Critical Missing Functionality

### 1. Sensor Lifecycle Management
- Registration and deregistration
- Health monitoring and failure detection
- Capability advertising
- Resource usage tracking

### 2. Material Management
- Garbage collection of old materials
- Compression and archival
- Storage backend abstraction
- Blob storage integration

### 3. Security and Validation
- Sensor authorization
- Input validation and sanitization
- Rate limiting and resource protection
- Audit logging

### 4. Operations and Monitoring
- Metrics collection and export
- Performance monitoring
- Configuration management
- Deployment automation

## Recommendations

### Immediate Actions

1. **Enforce Sensor Guards**: Make sensor_guard compile-time enforcement work
2. **Refactor Satellites**: Convert satellite processors to use sensd jobs
3. **Complete TemporalLedger**: Implement background worker and persistence
4. **Add Missing Sensors**: Implement database and network sensors

### Architecture Improvements

1. **Strengthen Separation**: Clear API boundaries between sensd and satellites
2. **Add Discovery**: Sensor capability discovery and registration
3. **Improve Error Handling**: Comprehensive retry and recovery patterns
4. **Add Monitoring**: Health checks and performance metrics

### Long-term Goals

1. **Plugin Architecture**: Dynamic sensor loading
2. **Distributed Processing**: Multi-node sensd deployment
3. **Advanced Routing**: Content-based routing and filtering
4. **Storage Optimization**: Intelligent compression and archival

## Conclusion

The Sensd service has a solid architectural foundation with good schema design and basic functionality. However, it suffers from incomplete implementation and widespread architectural violations where satellites bypass sensd entirely. The most critical issue is the lack of enforcement of the sensor/satellite separation, leading to a system where sensd exists but isn't actually used for most data acquisition.

**Priority**: HIGH - Core architectural integrity violations
**Effort**: MEDIUM - Foundation exists, needs refactoring and completion
**Impact**: HIGH - Central to the entire Sinex architecture