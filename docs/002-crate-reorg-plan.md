---
created: "2026-01-24T13:05:00+01:00"
purpose: "Crate reorganization plan to dissolve sinex-test-utils into production code"
status: "planned"
source_session: "91e3c662-a725-4a29-9ceb-49968d591c3a"
---

# Crate Reorganization Plan: Dissolve Test Infrastructure

## Executive Summary

**Core insight**: Test infrastructure (DB isolation, ephemeral NATS, temp dirs, timing utils) is actually *production infrastructure* that tests happen to use heavily. Reframing this dissolves the "test-utils is too heavy for CLI" problem.

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

## III. Test Infrastructure Distribution

```
WHERE THINGS GO (dissolving sinex-test-utils):
═══════════════════════════════════════════════

19k LOC → distributed to:

sinex-primitives/testing/
  ├─ event_factory.rs      Event builders, test_event()
  ├─ id_generators.rs      Test ULID generation
  └─ fixtures.rs           Domain-level test data

sinex-db/testing/
  ├─ context.rs            TestContext (DB + optional NATS)
  ├─ assertions.rs         DB-aware assertions
  ├─ seeding.rs            Dataset seeding
  └─ isolation.rs          DB namespace management

sinex-infra/nats/testing/
  ├─ ephemeral.rs          EphemeralNats
  ├─ pipeline_scope.rs     PipelineScope
  └─ jetstream_helper.rs   JetStream test utilities

sinex-infra/fs/
  ├─ temp_dir.rs           TestDir (PRODUCTION utility!)
  └─ permissions.rs        File permission helpers

sinex-infra/coordination/
  ├─ timing.rs             Timeouts, WaitHelpers (PRODUCTION!)
  └─ sync.rs               Async coordination patterns

xtask/tls/
  └─ fixtures.rs           TLS cert fixtures (only xtask uses)

sinex-cli/testing/
  └─ fixtures.rs           Config builders (YAML/TOML)

sinex-test-macros/        (already separate)
  └─ sinex_test.rs         #[sinex_test] macro

Property testing:
  → sinex-primitives/testing/property.rs (strategies)
```

## IV. Dependency Flows

```
LIGHTWEIGHT CLI TESTS:
sinexctl tests ─┬─► sinex-primitives (Event<T>, Id<T>)
xtask tests    ─┤
                └─► sinex-infra/fs (TestDir)

FULL INTEGRATION TESTS:
sinex-core tests ──┬─► sinex-primitives/testing
sinex-db tests ────┼─► sinex-db/testing (TestContext)
node tests ────────┼─► sinex-infra/nats/testing
                   └─► sinex-test-macros (#[sinex_test])

PRODUCTION USE:
sx dev mode ───────► sinex-infra/nats::ephemeral
demos/staging ──────► sinex-db::isolation
CLI tools ───────────► sinex-infra/coordination::timing
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

DECISION 3: Create sinex-infra
──────────────────────────────
Rationale: Ephemeral NATS, temp dirs useful beyond tests
Impact: sx can spawn NATS, demos can use isolation
Tradeoff: New top-level crate

DECISION 4: Keep sinex-core as facade
─────────────────────────────────────
Rationale: Existing code imports sinex_core::*
Impact: No immediate breakage, gradual migration
Tradeoff: Indirection layer

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

## VI. Migration Sequence (Clean Break)

```
PHASE 1: Create new crates (no code movement)
──────────────────────────────────────────────
1. cargo new crate/lib/sinex-primitives
2. cargo new crate/lib/sinex-db
3. cargo new crate/lib/sinex-infra

PHASE 2: Move code atomically
──────────────────────────────
1. sinex-core/types → sinex-primitives
2. sinex-core/db → sinex-db
3. Extract test-utils pieces:
   - DB isolation → sinex-db/testing
   - NATS helpers → sinex-infra/nats
   - TestDir → sinex-infra/fs
   - Timing utils → sinex-infra/coordination
   - Event factories → sinex-primitives/testing

PHASE 3: Update all imports
────────────────────────────
Global search-replace:
  use sinex_core::Event → use sinex_primitives::Event
  use sinex_test_utils::TestContext → use sinex_db::testing::TestContext

PHASE 4: Delete old crates
──────────────────────────
rm -rf crate/lib/sinex-test-utils
rm -rf crate/lib/sinex-core (becomes facade only)

NO BACKWARDS COMPATIBILITY - clean break
```

## VII. Final Crate Count

```
BEFORE:
 - sinex-macros
 - sinex-core (monolith)
 - sinex-schema
 - sinex-test-utils (19k LOC)
 TOTAL: 4 core crates

AFTER:
 - sinex-macros
 - sinex-primitives (pure domain)
 - sinex-db (persistence + isolation)
 - sinex-schema (migrations + ULID helpers)
 - sinex-infra (NATS, fs, coordination)
 - sinex-test-macros (already separate)
 - sinex-core (facade, eventually deprecated)
 TOTAL: 7 crates (+3 net, but cleaner boundaries)
```

## VIII. Production Use Cases Enabled

```bash
# Safe experimentation (new!)
sx env create --isolated
sx env exec --env myexp -- "replay operation X"
sx env destroy myexp

# CI/staging: ephemeral environments (new!)
sinex-infra spawn --ttl 1h  # Auto-cleanup staging DB

# Demos: instant clean environment (new!)
sinex-infra demo  # Start isolated DB + NATS for demo

# Local dev: multiple projects (new!)
sinex-infra dev --namespace project-a
sinex-infra dev --namespace project-b
```

## IX. Open Questions

1. **Provenance in primitives?** Currently references SourceMaterial (DB model). Need:
   ```rust
   // In sinex-primitives (no DB):
   pub struct Provenance {
       source_material_id: Option<Ulid>,  // Just an ID, not a DB relation
       parent_event_ids: Vec<Ulid>,
       anchor_byte: Option<u64>,
   }
   ```
   ULID↔UUID becomes purely a schema-level concern.

2. **Where do repositories live?** Currently in sinex-core/db/repositories. Options:
   - sinex-db/repositories (proposed)
   - Keep in sinex-core, just re-export from sinex-db

3. **sinex-node-sdk dependency chain?** Currently depends on sinex-core. Would need:
   - sinex-node-sdk → sinex-db (for persistence)
   - sinex-node-sdk → sinex-infra (for NATS)

---

## Status

- **Planned**: 2026-01-24
- **Not started**: No implementation begun
- **Session ended with**: `>>> What to work on next? "tests" | "crate-restructure" | "something else"`
