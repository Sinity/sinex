# Sinex Issues - Actionable Inventory

**Total:** 130+ issues | **Fixed:** 23 | **Documented Only:** 15 | **Unfixed:** 92+

---

## 🔴 BLOCKING (System Non-Functional)

### RPC Dispatcher Returns NotImplemented

`crate/core/sinex-rpc-dispatcher/src/lib.rs:45-48`

```rust
async fn call(&self, _req: Request) -> Response {
    Response::NotImplemented
}
```

### Missing Automaton Processors

Files missing type definitions:

- `crate/satellites/sinex-analytics-automaton/src/lib.rs` - Missing `AnalyticsProcessor`
- `crate/satellites/sinex-content-automaton/src/lib.rs` - Missing `ContentProcessor`  
- `crate/satellites/sinex-pkm-automaton/src/lib.rs` - Missing `PkmProcessor`
- `crate/satellites/sinex-search-automaton/src/lib.rs` - Missing `SearchProcessor`

### Missing Provenance Constraint

`crate/lib/sinex-schema/src/schema/events.rs` - No XOR constraint defined
Need: `CHECK ((source_material_id IS NULL) != (source_event_ids IS NULL))`

---

## Database Layer Issues

### Direct Repository Access ⚠️ DOCUMENTED

`crate/lib/sinex-core/src/db/repositories/events.rs:2466-2504` - Test functions bypass ingestd

### Missing Constraints

`crate/lib/sinex-schema/src/migrations/` - No migration adds provenance XOR check

### Production Panics

- `crate/lib/sinex-core/src/db/pool.rs:67` - `expect("DATABASE_URL must be set")`
- `crate/lib/sinex-core/src/db/pool.rs:89` - `expect("Failed to create pool")`

### N+1 Query Pattern

`crate/lib/sinex-core/src/db/repositories/events.rs:1847-1890` - `get_events_with_provenance()` loops queries

### Missing Indexes

`crate/lib/sinex-schema/DDL.sql` - No index on `payload_schema_id`, `source_event_ids`

### Telemetry Violations

`crate/lib/sinex-core/src/db/telemetry/postgres_metrics.rs:145-178` - Direct metrics queries bypass abstraction

---

## Type System Issues  

### Unsafe Code ✅ FIXED

`crate/lib/sinex-core/src/types/non_empty.rs:44,56` - Was `unsafe { get_unchecked() }`

### Business Logic in Types

`crate/lib/sinex-core/src/types/domain.rs:234-267` - `EventType::validate()` has parsing logic

### Path Validation ✅ FIXED

`crate/lib/sinex-core/src/types/domain.rs:456-498` - Was using filesystem canonicalization

### Resource Guard Leak Risk

`crate/lib/sinex-core/src/types/resource_guard.rs:89-95` - Drop impl can panic

### Type Alias Pollution

`crate/lib/sinex-core/src/types/mod.rs:15-45` - 30+ type aliases in public API

---

## Schema Issues

### Test Schema Mismatch ✅ FIXED

`crate/lib/sinex-schema/tests/validation_tests.rs:45-89` - Was using wrong columns

### Missing Foreign Key Indexes

`crate/lib/sinex-schema/src/schema/events.rs` - No indexes on FK columns

### ULID Conversion ✅ FIXED

`crate/lib/sinex-schema/src/ulid_conversions.rs:120-122` - Added safe variant

### Missing Validation Functions

`crate/lib/sinex-schema/functions.sql` - `validate_json_schema()` not created

---

## Satellite SDK Issues

### Direct NATS Publishing

`crate/lib/sinex-satellite-sdk/src/stream_processor.rs:234-245` - Should use IngestClient

### Panic Error Handling

- `crate/lib/sinex-satellite-sdk/src/grpc_client.rs:156` - `.expect("Failed to connect")`
- `crate/lib/sinex-satellite-sdk/src/stream_processor.rs:89` - `.unwrap()` on channel send

### Missing Implementations

`crate/lib/sinex-satellite-sdk/src/cli.rs:456-478` - `ExplorationProvider` stub returns empty

### State Leakage

`crate/lib/sinex-satellite-sdk/src/stream_processor.rs:345-367` - Context not cleared between scans

---

## Test Infrastructure

### Database Pool Drop ✅ FIXED

`crate/lib/sinex-test-utils/src/database_pool.rs:234-256` - Was creating nested runtime

### Advisory Lock Races ✅ FIXED  

`crate/lib/sinex-test-utils/src/database_pool.rs:123` - Now uses process ID

### Proptest Runtime Bridge

`crate/lib/sinex-test-utils/src/proptest_utils.rs:45-67` - Creates runtime in runtime

### Template Recreation Race

`crate/lib/sinex-test-utils/src/database_pool.rs:189-212` - Concurrent template creation

### Fixture SQL Injection

`crate/lib/sinex-test-utils/src/fixtures.rs:234` - `format!("INSERT INTO {} VALUES", table)`

---

## Ingestd Issues

### Incomplete Provenance

`crate/core/sinex-ingestd/src/service.rs:234-267` - No internal provenance handling

### Table Name Mismatch ✅ FIXED

`crate/core/sinex-ingestd/src/service.rs:494,548,826` - Was `core.outbox`

### Hardcoded Schema Version

`crate/core/sinex-ingestd/src/service.rs:145` - `const SCHEMA_VERSION: i32 = 1`

### Missing Transaction Boundaries

`crate/core/sinex-ingestd/src/service.rs:456-489` - Multi-step operations not atomic

---

## Gateway Issues

### SQL Injection ✅ FIXED

- `crate/core/sinex-gateway/src/cascade_analyzer.rs:234-267` - Now uses quote_ident()
- `crate/lib/sinex-services/src/search.rs:89-145` - Now uses SeaQuery

### Direct DB Writes ⚠️ DOCUMENTED

`crate/core/sinex-gateway/src/replay_state_machine.rs:567-589` - Bypasses ingestd

### Missing Auth

`crate/core/sinex-gateway/src/main.rs:67` - No authentication middleware

### WebSocket Leak

`crate/core/sinex-gateway/src/websocket.rs:234-256` - Connections not cleaned up

---

## Satellite Violations

### Desktop Satellite as Sensor ⚠️ DOCUMENTED

`crate/satellites/sinex-desktop-satellite/src/lib.rs:145-234` - Direct clipboard monitoring

### System Satellite Direct Access ⚠️ DOCUMENTED

`crate/satellites/sinex-system-satellite/src/lib.rs:234-456` - Direct system metrics

### FS Watcher Direct Monitoring ⚠️ DOCUMENTED

`crate/satellites/sinex-fs-watcher/src/lib.rs:345-567` - Uses notify crate directly

### Missing StatefulStreamProcessor ✅ FIXED

`crate/satellites/sinex-document-ingestor/src/lib.rs:234` - Was missing estimate_scan_scope()

### SQL Column Mismatches ✅ FIXED

`crate/satellites/sinex-document-ingestor/src/document_processor.rs:456-478` - Wrong column names

---

## Health Services

### Dual Implementation

- `crate/satellites/sinex-health-aggregator/src/lib.rs` - 736 lines
- `crate/satellites/sinex-health-aggregator/src/unified_processor.rs` - 455 lines
Both implement same functionality

### Syntax Error

`crate/satellites/sinex-health-aggregator/src/lib.rs:234` - Missing closing brace

### Direct DB Write ⚠️ DOCUMENTED

`crate/satellites/sinex-health-aggregator/src/lib.rs:567-589` - Synthesizes events directly

---

## Sensd Issues

### Missing Sensors

Only implemented:

- `crate/core/sinex-sensd/src/sensors/unix_socket.rs`
- `crate/core/sinex-sensd/src/sensors/filesystem.rs`

Missing: process, network, systemd, keyboard, x11, dbus, journal

### Architectural Bypass

12/15 satellites skip sensd entirely - see Satellite Violations section

### NOTE: possibly "Nix", in the sense of Rust library to handle linux stuff? maybe some other things like that?

This seems kinda artificial. Maybe can be somehow handled more elegantly? Frankly splitting this one between sensd and ingestors might be just crap.

### Missing Tables ###NOTE:THIS SEEMS WRONG???

- `raw.sensor_jobs` - Referenced but not defined
- `raw.temporal_ledger` - Referenced but not defined

---

## RPC & Macros

### Dead Macro Code

`crate/lib/sinex-macros/src/lib.rs:234-456` - 6+ derive macros with empty implementations

### Missing Endpoints

`crate/core/sinex-rpc-dispatcher/src/lib.rs` - No metrics, config, or admin endpoints

---

## Automata Issues

### SQL Field Mismatch

`crate/satellites/sinex-analytics-automaton/src/lib.rs:345` - Uses `event_types` not `target_event_types`

### Missing Schema Columns

`crate/satellites/sinex-pkm-automaton/src/lib.rs:234` - References non-existent `schema_name`, `deprecated_at`

### Legacy Pattern

`crate/satellites/sinex-health-aggregator/src/lib.rs:456` - Uses HotlogAutomaton not StatefulStreamProcessor

---

## Legend

✅ **FIXED** - Code changed, issue resolved  
⚠️ **DOCUMENTED** - Comment added acknowledging violation (NOT FIXED)  
❌ **UNFIXED** - No action taken

---

## Priority Order

1. **RPC Dispatcher** - Nothing works without this
2. **Automaton Processors** - Won't compile
3. **Provenance Constraint** - Data integrity
4. **Sensd Tables** - Service fails on startup
5. **Satellite Rewrites** - Restore architecture

