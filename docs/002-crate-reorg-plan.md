---
created: "2026-01-24T13:05:00+01:00"
purpose: "Crate reorganization plan to dissolve sinex-test-utils into production code"
status: "planned"
source_session: "91e3c662-a725-4a29-9ceb-49968d591c3a"
---

# Crate Reorganization Plan: Dissolve Test Infrastructure

## Executive Summary

**Key discovery**: `Event<T>` does NOT derive `FromRow` - only `EventRecord` (DTO) does. This means the primitives/db split is viable.

## I. Current Coupling Analysis

```
Domain Types (Event<T>, Id<T>, EventSource, etc.)
    ↓
    ├─ Serialization (serde::Serialize/Deserialize) ✅ Independent
    ├─ Validation (path, JSON validation)            ✅ Independent
    └─ SQL Mapping (FromRow, Type mapping)           ⚠️ TIGHT COUPLING

SQL Mapping Details:
┌────────────────────────────────────────────────────────┐
│ Event<T> (domain)                                       │
│   ↓ Does NOT derive FromRow                             │
│   ↓ Conversion happens via intermediate types           │
│                                                          │
│ EventRecord  ────────[derive(FromRow)]────► sqlx        │
│   ↓                                                      │
│   ↓ .try_to_event()                                     │
│                                                          │
│ Event<JsonValue> (domain)                               │
└────────────────────────────────────────────────────────┘

KEY INSIGHT: Event<T> does NOT derive FromRow!
  - EventRecord is the SQL type (derives FromRow)
  - Event<T> is pure domain (no SQL coupling)
  - Conversion: EventRecord → Event<JsonValue> via trait

Other types that DO derive FromRow:
  ✓ EventPayloadSchema
  ✓ EventAnnotation
  ✓ BatchViolation
  ✓ SuspiciousEvent
  ✓ EventSearchRow

These are query-specific DTOs, NOT core domain types.
```

## II. Proposed Crate Structure

UPDATE: WE STILL WANT SPLIT OF SINEX-CORE INTO PRIMITIVES AND DB.

```
┌────────────────────────────────────────────────────────┐
│ LAYER 1: PURE DOMAIN (zero external deps)              │
├────────────────────────────────────────────────────────┤
│ sinex-primitives/                                      │
│   ├─ event.rs           Event<T>, Provenance          │
│   ├─ id.rs              Id<T> (phantom typed ULIDs)   │
│   ├─ domain.rs          EventSource, EventType, etc   │
│   ├─ validation/        Path, JSON, Unicode           │
│   └─ error.rs           SinexError (domain errors)    │
│                                                        │
│   deps: serde, chrono, ulid, validator                │
│   NO sqlx, NO async-nats                              │
└────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────┐
│ LAYER 2: PERSISTENCE & INFRASTRUCTURE                 │
├────────────────────────────────────────────────────────┤
│ sinex-db/                                              │
│   ├─ models/                                           │
│   │   └─ event_record.rs   EventRecord: FromRow       │
│   ├─ repositories/         All SQL repositories       │
│   │   ├─ events/                                       │
│   │   │   ├─ persistence.rs                            │
│   │   │   └─ conversions.rs  EventRecord ↔ Event      │
│   │   ├─ blobs/                                        │
│   │   └─ state/                                        │
│   ├─ pool.rs              DbPool, connection mgmt     │
│   ├─ isolation.rs         🆕 DB isolation/namespacing │
│   └─ schema.rs            Table definitions           │
│                                                        │
│   deps: sinex-primitives, sqlx, sea-orm-migration     │
│   Includes: DB isolation infrastructure               │
└────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────┐
│ LAYER 3: SCHEMA & MIGRATIONS                          │
├────────────────────────────────────────────────────────┤
│ sinex-schema/                                          │
│   ├─ migrations/          SQL migration files         │
│   ├─ ulid.rs              ULID ↔ UUID conversions     │
│   └─ bin/schema-info.rs   🔻 Move to xtask?           │
│                                                        │
│   deps: sinex-db, sea-orm-migration                   │
└────────────────────────────────────────────────────────┘

UPDATE: tesst-utils distributed, but differently, mostly to xtask. test-macros somewhere. btw sinex-schema, maybe should be tucked in somewhere, nicely? ofc the binary does move to xtask if necessatrt at all.

┌────────────────────────────────────────────────────────┐
│ LAYER 4: TESTING INFRASTRUCTURE                       │
├────────────────────────────────────────────────────────┤
│ sinex-test-macros/  (already exists)                  │
│   └─ sinex_test.rs        #[sinex_test] macro         │
│                                                        │
│ NO sinex-test-utils crate!                            │
│ Test utilities distributed:                           │
│   ├─ DB isolation     → sinex-db::isolation           │
│   ├─ TestContext      → sinex-db::testing::context    │
│   ├─ Event factories  → sinex-primitives::testing     │
│   ├─ NATS helpers     → sinex-infra::nats::testing    │
│   ├─ TLS fixtures     → xtask/tls/fixtures.rs         │
│   ├─ Config builders  → sinex-cli/testing/fixtures.rs │
│   └─ TestDir          → sinex-infra::fs (generic)     │
└────────────────────────────────────────────────────────┘

UPDATE: xtask is actually binary/library and we simply use that, dont need sinex-infra.

┌────────────────────────────────────────────────────────┐
│ LAYER 5: INFRASTRUCTURE SERVICES                      │
├────────────────────────────────────────────────────────┤
│ sinex-infra/  🆕 (extracted from test-utils)          │
│   ├─ nats/                                             │
│   │   ├─ ephemeral.rs     Spawn temporary NATS        │
│   │   ├─ client.rs        NATS client utilities       │
│   │   └─ testing/         Test-specific NATS helpers  │
│   ├─ fs/                                               │
│   │   ├─ temp_dir.rs      RAII temp directories       │
│   │   └─ watch.rs         File watching utilities     │
│   └─ coordination/                                     │
│       ├─ timing.rs        Wait/retry patterns         │
│       └─ sync.rs          Async coordination          │
│                                                        │
│   PURPOSE: Reusable infrastructure (not just tests)   │
│   USE CASES:                                           │
│     - Tests (primary)                                  │
│     - sx dev mode (ephemeral NATS)                    │
│     - Demos/staging (isolated environments)           │
│     - CLI integration tests                           │
└────────────────────────────────────────────────────────┘

UPDATE: SUCH things might be convenient to do as a first stage. then in second stage, migrate everythng cleanly instead of hallucinating about some indefininite management into the future.
┌────────────────────────────────────────────────────────┐
│ LAYER 6: FACADE (convenience re-exports)              │
├────────────────────────────────────────────────────────┤
│ sinex-core/                                            │
│   └─ lib.rs:                                           │
│       pub use sinex_primitives::*;                     │
│       pub use sinex_db::*;                             │
│       pub use sinex_schema::*;                         │
│                                                        │
│   Features:                                            │
│     default = ["full"]                                 │
│     full = ["primitives", "db", "schema"]              │
│     primitives = ["dep:sinex-primitives"]              │
│     db = ["primitives", "dep:sinex-db"]                │
│     testing = ["db", "sinex-db/isolation"]             │
│                                                        │
│   PURPOSE: Backwards compat, ergonomic imports         │
│   DEPRECATION PATH: Encourage direct crate use         │
└────────────────────────────────────────────────────────┘
```

## V. Key Architectural Decisions

```
DECISION 1: Split primitives from DB
───────────────────────────────────
Rationale: Event<T> has no FromRow coupling
Impact: CLIs can use Event without sqlx
Tradeoff: More crates, but cleaner boundaries

DECISION 2: Dissolve test-utils
───────────────────────────────
Rationale: Test infrastructure IS production infrastructure
Impact: Better cohesion (TLS fixtures near TLS code)
Tradeoff: No single "test utilities" crate to discover

DECISION 5: schema-info → xtask
───────────────────────────────
Rationale: Schema introspection is dev-time tooling
Impact: One less binary, fits xtask's purpose
Tradeoff: None (pure win)

DECISION 6: DB isolation in sinex-db
────────────────────────────────────
Rationale: Isolation IS a DB concern, not separate
Impact: Tests use sinex-db::testing::context
Tradeoff: sinex-db grows slightly
```

## IX. Open Questions (RESOLVED)

1. **Provenance in primitives?**
   - **Resolution:** `SourceMaterial` is defined as `struct SourceMaterial;` (a marker type) in `db::models::event`. It has no dependencies.
   - **Plan:** Move `SourceMaterial`, `Provenance`, and `Id<T>` to `sinex-primitives`.
   - **Outcome:** **Full Domain Model Retention**. We keep `Id<SourceMaterial>` and strong typing. No loss of expressiveness.

2. **sinex-node-sdk dependency chain?**
   - **Resolution:** `sinex-node-sdk` will depend on `sinex-primitives` (for types) and `async-nats` directly.
   - **Testing:** `xtask` handles test infrastructure. `sinex-infra` is optional or can be focused purely on production runtime utilities if needed.

3. **Where do repositories live?**
   - **Decision:** `sinex-db`. This is the persistence layer.

## X. Technical Validation

1. **Id<T> Coupling:**
   - Verified `src/types/ids.rs` uses `#[cfg(feature = "sqlx")]`.
   - **Result:** `Id<T>` can move to `sinex-primitives` with an *optional* `sqlx` feature. This achieves the goal of a lightweight, DB-agnostic primitives crate while supporting DB usage.

2. **Infrastructure Strategy:**
   - `xtask` now successfully handles development/testing environments (`xtask/sandbox`).
   - `sinex-infra` is not strictly required for tests anymore.
   - `node-sdk` can use raw `async-nats` or a lightweight wrapper in `sinex-core` if preferred.

---
